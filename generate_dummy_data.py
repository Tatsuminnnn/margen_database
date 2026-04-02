"""
上部消化管グループ 症例登録DB — ダミーデータ生成スクリプト
100件のリアルな分布の症例データを投入

使用方法:
    python generate_dummy_data.py          # 100件投入
    python generate_dummy_data.py --reset  # 既存ダミーデータ削除→再投入
"""

import sys
import os
import random
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import init_db, get_db, hash_password, DB_PATH

random.seed(42)

N = 300  # 生成件数

# ==============================================================
# 定数: 実際の分布に基づく
# ==============================================================

# 執刀医 5名（上部消化管外科の典型的なチーム構成）
SURGEONS = ["田中", "山本", "佐藤", "鈴木", "中村"]
SURGEON_WEIGHTS = [30, 25, 20, 15, 10]  # 経験年数順に症例数多い

ASSISTANTS = ["木村", "林", "井上", "松本", "渡辺", "加藤", "吉田", "小林"]

# 疾患分類の分布（胃癌が最多）
DISEASE_CLASS = [1, 1, 1, 1, 1, 1, 2, 3, 4, 9]  # 60%胃癌, 10%食道, 10%EGJ, 10%GIST, 10%その他

# 胃癌術式の分布
GASTRIC_PROCEDURES = {
    1: 40,  # 幽門側胃切除 DG
    2: 25,  # 胃全摘 TG
    3: 10,  # 噴門側胃切除 PG
    5: 8,   # PPG
    4: 7,   # 局所切除
    7: 5,   # 審査腹腔鏡
    6: 3,   # 残胃全摘
    8: 2,   # バイパス
}

# 到達法の分布
APPROACH_DIST = {
    1: 25,  # 開腹
    2: 35,  # 腹腔鏡
    3: 40,  # ロボット
}

# 郭清度
DISSECTION_DIST = {1: 10, 2: 55, 3: 35}  # D1, D2, D3(D2+)

# 再建法（DGの場合）
RECON_DG = {1: 40, 2: 50, 3: 10}    # BI, RY, その他
RECON_TG = {2: 90, 4: 10}            # RY, その他
RECON_PG = {5: 30, 6: 30, 2: 20, 9: 20}  # DFT, SOFY, RY, その他

# 吻合法
ANASTOMOSIS = {
    1: 5,    # 三角吻合
    2: 15,   # デルタ
    3: 5,    # π型
    4: 10,   # DST
    5: 10,   # FEEA
    6: 15,   # Overlap
    7: 5,    # 上川
    8: 10,   # mSOFY
    9: 5,    # OrVil
    10: 10,  # 手縫い
    11: 5,   # 端端
    12: 5,   # 端側
}

# cStage 分布（胃癌取扱い規約15版ベース）
C_STAGE_DIST = {1: 30, 2: 20, 3: 15, 4: 15, 5: 10, 6: 5, 7: 5}
# 1=IA, 2=IB, 3=IIA, 4=IIB, 5=IIIA, 6=IIIB, 7=IIIC

# pStage 分布
P_STAGE_DIST = {1: 25, 2: 15, 3: 15, 4: 15, 5: 12, 6: 10, 7: 8}

# cT 分布
C_DEPTH_DIST = {1: 25, 2: 15, 3: 20, 4: 25, 5: 10, 6: 5}
# 1=T1a, 2=T1b, 3=T2, 4=T3, 5=T4a, 6=T4b

# cN 分布
C_LN_DIST = {0: 50, 1: 25, 2: 15, 3: 10}

# 組織型
HISTOLOGY_DIST = {1: 30, 2: 20, 3: 15, 4: 15, 5: 10, 6: 5, 7: 5}
# 1=tub1, 2=tub2, 3=por1, 4=por2, 5=sig, 6=muc, 7=pap

# 合併症の発生率（%）
COMP_RATES = {
    "comp_ssi": 8, "comp_wound_dehiscence": 2, "comp_intra_abd_abscess": 5,
    "comp_bleeding": 3, "comp_ileus": 4, "comp_dvt_pe": 1,
    "comp_pneumonia": 5, "comp_atelectasis": 3, "comp_uti": 2,
    "comp_delirium": 6, "comp_cardiac": 2, "comp_dge": 8,
    "comp_perforation": 1, "comp_cholelithiasis": 1,
    "comp_anastomotic_leak": 4, "comp_anastomotic_stricture": 3,
    "comp_anastomotic_bleeding": 2, "comp_pancreatic_fistula": 5,
    "comp_bile_leak": 1, "comp_duodenal_stump_leak": 1,
    "comp_rln_palsy": 1, "comp_chylothorax": 1,
    "comp_empyema": 1, "comp_pneumothorax": 1,
    "comp_ards": 0.5, "comp_dic": 0.5, "comp_sepsis": 1,
    "comp_renal_failure": 0.5, "comp_hepatic_failure": 0.5,
}

# CD グレード分布（合併症あり時）
CD_GRADE_DIST = {1: 20, 2: 40, 3: 20, 4: 10, 5: 5, 6: 3, 7: 2}

# ==============================================================
# ヘルパー
# ==============================================================
def weighted_choice(dist):
    """辞書 {value: weight} から重み付きランダム選択。"""
    keys = list(dist.keys())
    weights = list(dist.values())
    return random.choices(keys, weights=weights, k=1)[0]

def rand_date(start_year=2020, end_year=2025):
    """ランダムな日付を生成。"""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).strftime("%Y-%m-%d")

def rand_normal_clipped(mean, sd, lo, hi):
    """正規分布からクリップした値。"""
    v = random.gauss(mean, sd)
    return max(lo, min(hi, round(v)))

def rand_float_clipped(mean, sd, lo, hi, digits=1):
    v = random.gauss(mean, sd)
    return max(lo, min(hi, round(v, digits)))


# ==============================================================
# メイン
# ==============================================================
def generate():
    init_db()

    with get_db() as conn:
        # 既存ダミーデータのチェック
        existing = conn.execute("SELECT COUNT(*) FROM patients").fetchone()[0]
        if existing > 0 and "--reset" not in sys.argv:
            print(f"⚠️ 既に {existing} 件のデータがあります。")
            print("  --reset オプションで全削除→再投入できます。")
            ans = input("  追加投入しますか？ (y/N): ").strip().lower()
            if ans != "y":
                print("中止しました。")
                return

        if "--reset" in sys.argv:
            for tbl in ["outcome", "adjuvant_chemo", "pathology", "surgery",
                        "neoadjuvant", "tumor_preop", "lymph_nodes",
                        "gist_detail", "palliative_chemo", "patients"]:
                conn.execute(f"DELETE FROM {tbl}")
            # study_id カウンター用にシーケンスリセット
            try:
                conn.execute("DELETE FROM sqlite_sequence WHERE name='patients'")
            except:
                pass
            print("🗑️ 既存データを削除しました。")

        print(f"📊 ダミーデータ {N} 件を生成中...")

        for i in range(1, N + 1):
            # --- 基本情報 ---
            year = random.choices([2020, 2021, 2022, 2023, 2024, 2025],
                                   weights=[10, 15, 18, 22, 25, 10], k=1)[0]
            month = random.randint(1, 12)
            day = random.randint(1, 28)
            surgery_date = f"{year}-{month:02d}-{day:02d}"
            admission_date = (datetime.strptime(surgery_date, "%Y-%m-%d") - timedelta(days=random.randint(1, 3))).strftime("%Y-%m-%d")

            sex = random.choices([1, 2], weights=[65, 35], k=1)[0]  # 男性多め（胃癌）
            age = rand_normal_clipped(70, 10, 35, 92)
            birthyear = year - age
            birthdate = f"{birthyear}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"

            disease_class = random.choice(DISEASE_CLASS)
            disease_category = 1 if disease_class in [1, 2, 3] else 2

            height = rand_float_clipped(163 if sex == 1 else 153, 7, 140, 185)
            weight = rand_float_clipped(62 if sex == 1 else 52, 10, 35, 100)
            bmi = round(weight / (height / 100) ** 2, 1)

            ps = random.choices([0, 1, 2, 3], weights=[60, 25, 10, 5], k=1)[0]
            asa = random.choices([1, 2, 3, 4], weights=[15, 50, 30, 5], k=1)[0]

            # 手術時間・出血量（到達法で変わる）
            approach = weighted_choice(APPROACH_DIST)

            if disease_class == 1:  # 胃癌
                procedure = weighted_choice(GASTRIC_PROCEDURES)
            else:
                procedure = random.randint(1, 4)

            if approach == 3:  # ロボット
                op_time = rand_normal_clipped(340, 70, 180, 600)
                blood_loss = rand_normal_clipped(30, 40, 5, 300)
                console_time = rand_normal_clipped(240, 60, 120, 480)
            elif approach == 2:  # 腹腔鏡
                op_time = rand_normal_clipped(300, 60, 150, 550)
                blood_loss = rand_normal_clipped(50, 60, 5, 500)
                console_time = None
            else:  # 開腹
                op_time = rand_normal_clipped(250, 60, 120, 480)
                blood_loss = rand_normal_clipped(150, 120, 10, 800)
                console_time = None

            # 胃全摘は時間長め
            if procedure == 2:
                op_time += rand_normal_clipped(40, 15, 10, 80)
                blood_loss += rand_normal_clipped(30, 20, 0, 100)

            # 執刀医・助手
            surgeon = random.choices(SURGEONS, weights=SURGEON_WEIGHTS, k=1)[0]
            asst_pool = [a for a in ASSISTANTS]
            random.shuffle(asst_pool)
            assistant1 = asst_pool[0]
            assistant2 = asst_pool[1] if random.random() < 0.7 else None
            scopist = asst_pool[2] if approach in [2, 3] else None

            # 郭清
            dissection = weighted_choice(DISSECTION_DIST)

            # 再建法
            if procedure == 1:  # DG
                reconstruction = weighted_choice(RECON_DG)
            elif procedure == 2:  # TG
                reconstruction = weighted_choice(RECON_TG)
            elif procedure == 3:  # PG
                reconstruction = weighted_choice(RECON_PG)
            else:
                reconstruction = random.choice([1, 2, 9])

            anastomosis = weighted_choice(ANASTOMOSIS)

            # 在院日数
            pod = rand_normal_clipped(12, 5, 7, 60)
            discharge_date = (datetime.strptime(surgery_date, "%Y-%m-%d") + timedelta(days=pod)).strftime("%Y-%m-%d")

            # --- 合併症 ---
            comp_data = {}
            any_comp = 0
            max_cd = 0
            for comp_name, rate in COMP_RATES.items():
                if random.random() * 100 < rate:
                    cd = weighted_choice(CD_GRADE_DIST)
                    comp_data[comp_name] = cd
                    max_cd = max(max_cd, cd)
                    any_comp = 1
                    # 発症日
                    comp_day = random.randint(1, min(pod, 30))
                    comp_date_val = (datetime.strptime(surgery_date, "%Y-%m-%d") + timedelta(days=comp_day)).strftime("%Y-%m-%d")
                    comp_data[f"{comp_name}_date"] = comp_date_val
                else:
                    comp_data[comp_name] = 0

            # 合併症ありなら在院日数延長
            if any_comp and max_cd >= 3:
                extra = rand_normal_clipped(10, 5, 3, 40)
                pod += extra
                discharge_date = (datetime.strptime(surgery_date, "%Y-%m-%d") + timedelta(days=pod)).strftime("%Y-%m-%d")

            # study_id
            seq = conn.execute("SELECT COALESCE(MAX(id),0)+1 FROM patients").fetchone()[0]
            study_id = f"UGI-{year}-{seq:03d}"

            # --- INSERT patients ---
            comor_ht = 1 if random.random() < 0.35 else 0
            comor_dm = 1 if random.random() < 0.20 else 0
            comor_cv = 1 if random.random() < 0.10 else 0
            comor_resp = 1 if random.random() < 0.08 else 0
            comor_renal = 1 if random.random() < 0.05 else 0
            comor_hepat = 1 if random.random() < 0.05 else 0

            conn.execute("""
                INSERT INTO patients (
                    study_id, sex, birthdate, surgery_date, admission_date, discharge_date,
                    disease_class, disease_category, height_cm, weight_admission,
                    ps, asa, data_status, created_by,
                    comor_hypertension, comor_diabetes, comor_cardiovascular,
                    comor_respiratory, comor_renal, comor_hepatic,
                    smoking, alcohol
                ) VALUES (?,?,?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?, ?,?)
            """, (
                study_id, sex, birthdate, surgery_date, admission_date, discharge_date,
                disease_class, disease_category, height, weight,
                ps, asa, "approved", 1,
                comor_ht, comor_dm, comor_cv,
                comor_resp, comor_renal, comor_hepat,
                random.choices([0, 1, 2], weights=[50, 30, 20], k=1)[0],
                random.choices([0, 1, 2], weights=[40, 40, 20], k=1)[0],
            ))
            pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            # --- INSERT tumor_preop ---
            c_depth = weighted_choice(C_DEPTH_DIST)
            c_ln = weighted_choice(C_LN_DIST)
            c_distant = 1 if random.random() < 0.08 else 0
            c_stage = weighted_choice(C_STAGE_DIST)
            tumor_size = rand_normal_clipped(40, 20, 5, 150)

            preop_alb = rand_float_clipped(3.8, 0.5, 2.0, 5.0)
            preop_hb = rand_float_clipped(12.5 if sex == 1 else 11.5, 1.5, 6.0, 17.0)
            preop_crp = rand_float_clipped(0.3, 0.8, 0.01, 15.0)
            preop_cea = rand_float_clipped(3.0, 5.0, 0.5, 100.0)
            preop_ca199 = rand_float_clipped(15.0, 30.0, 1.0, 500.0)

            conn.execute("""
                INSERT INTO tumor_preop (
                    patient_id, c_depth, c_ln_metastasis, c_distant_metastasis,
                    c_stage, c_tumor_size_major_mm, c_histology1,
                    preop_alb, preop_hb, preop_crp, preop_cea, preop_ca199
                ) VALUES (?,?,?,?, ?,?,?, ?,?,?,?,?)
            """, (
                pid, c_depth, c_ln, c_distant,
                c_stage, tumor_size, weighted_choice(HISTOLOGY_DIST),
                preop_alb, preop_hb, preop_crp, preop_cea, preop_ca199,
            ))

            # --- INSERT neoadjuvant ---
            nac_yn = 1 if c_stage >= 4 and random.random() < 0.6 else 0
            nac_regimen = random.choice([1, 2, 3]) if nac_yn else None
            conn.execute("""
                INSERT INTO neoadjuvant (patient_id, nac_yn, nac_regimen)
                VALUES (?,?,?)
            """, (pid, nac_yn, nac_regimen))

            # --- INSERT surgery ---
            surg_cols = [
                "patient_id", "op_surgeon", "op_assistant1", "op_assistant2", "op_scopist",
                "op_emergency", "op_approach", "op_procedure", "op_dissection",
                "op_reconstruction", "op_anastomosis_method",
                "op_time_min", "op_console_time_min", "op_blood_loss_ml",
                "op_transfusion_intra", "op_transfusion_post",
                "op_icu_days", "op_reop_yn", "op_complication_yn", "op_cd_grade_max",
                "readmission_30d", "mortality_30d", "mortality_inhospital",
            ]
            surg_vals = [
                pid, surgeon, assistant1, assistant2, scopist,
                0, approach, procedure, dissection,
                reconstruction, anastomosis,
                op_time, console_time, blood_loss,
                1 if blood_loss > 300 else 0,  # 輸血
                1 if blood_loss > 500 else 0,
                rand_normal_clipped(0, 1, 0, 10),
                1 if random.random() < 0.03 else 0,
                any_comp, max_cd,
                1 if random.random() < 0.05 else 0,
                1 if random.random() < 0.01 else 0,
                1 if random.random() < 0.02 else 0,
            ]

            # 合併症カラム追加
            for comp_name in COMP_RATES:
                surg_cols.append(comp_name)
                surg_vals.append(comp_data.get(comp_name, 0))
                date_key = f"{comp_name}_date"
                if date_key in comp_data:
                    surg_cols.append(date_key)
                    surg_vals.append(comp_data[date_key])

            placeholders = ",".join(["?"] * len(surg_vals))
            col_str = ",".join(surg_cols)
            conn.execute(f"INSERT INTO surgery ({col_str}) VALUES ({placeholders})", surg_vals)

            # --- INSERT pathology ---
            p_depth = weighted_choice(P_STAGE_DIST)  # 簡略化
            p_ln = weighted_choice(C_LN_DIST)
            p_stage = weighted_choice(P_STAGE_DIST)
            p_residual = random.choices([0, 1, 2], weights=[85, 10, 5], k=1)[0]

            msi = random.choices([0, 1, 2], weights=[80, 15, 5], k=1)[0]  # 0=stable, 1=low, 2=high
            her2 = random.choices([0, 1, 2, 3], weights=[60, 15, 10, 15], k=1)[0]
            pdl1_cps = rand_float_clipped(5, 15, 0, 100)
            ebv = random.choices([0, 1], weights=[95, 5], k=1)[0]

            conn.execute("""
                INSERT INTO pathology (
                    patient_id, p_depth, p_ln_metastasis, p_stage,
                    p_residual_tumor, p_histology1,
                    p_ly, p_v, p_inf,
                    msi_status, her2_status, pdl1_cps, ebv_status
                ) VALUES (?,?,?,?, ?,?, ?,?,?, ?,?,?,?)
            """, (
                pid, p_depth, p_ln, p_stage,
                p_residual, weighted_choice(HISTOLOGY_DIST),
                random.randint(0, 3), random.randint(0, 3), random.randint(0, 2),
                msi, her2, pdl1_cps, ebv,
            ))

            # --- INSERT adjuvant_chemo ---
            adj_yn = 1 if p_stage >= 3 and random.random() < 0.7 else 0
            conn.execute("""
                INSERT INTO adjuvant_chemo (patient_id, adj_yn, adj_regimen)
                VALUES (?,?,?)
            """, (pid, adj_yn, random.choice([1, 2, 3, 4]) if adj_yn else None))

            # --- INSERT outcome ---
            # 再発: StageIIIなら20%, IVなら50%
            if p_stage >= 6:
                recurrence = 1 if random.random() < 0.50 else 0
            elif p_stage >= 4:
                recurrence = 1 if random.random() < 0.20 else 0
            else:
                recurrence = 1 if random.random() < 0.05 else 0

            # 生死
            if recurrence:
                vital = random.choices([1, 2], weights=[60, 40], k=1)[0]
            else:
                vital = random.choices([1, 2], weights=[95, 5], k=1)[0]

            last_fu_days = random.randint(90, 1800)
            last_alive = (datetime.strptime(surgery_date, "%Y-%m-%d") + timedelta(days=last_fu_days)).strftime("%Y-%m-%d")
            death_date_val = last_alive if vital == 2 else None

            recurrence_date_val = None
            if recurrence:
                rec_days = random.randint(90, min(last_fu_days, 1095))
                recurrence_date_val = (datetime.strptime(surgery_date, "%Y-%m-%d") + timedelta(days=rec_days)).strftime("%Y-%m-%d")

            rec_peritoneal = 1 if recurrence and random.random() < 0.35 else 0
            rec_liver = 1 if recurrence and random.random() < 0.30 else 0
            rec_lung = 1 if recurrence and random.random() < 0.15 else 0
            rec_ln = 1 if recurrence and random.random() < 0.20 else 0
            rec_local = 1 if recurrence and random.random() < 0.10 else 0

            conn.execute("""
                INSERT INTO outcome (
                    patient_id, vital_status, recurrence_yn, recurrence_date,
                    last_alive_date, death_date,
                    rec_peritoneal, rec_liver, rec_lung, rec_lymph_node, rec_local
                ) VALUES (?,?,?,?, ?,?, ?,?,?,?,?)
            """, (
                pid, vital, recurrence, recurrence_date_val,
                last_alive, death_date_val,
                rec_peritoneal, rec_liver, rec_lung, rec_ln, rec_local,
            ))

            if i % 10 == 0:
                print(f"  {i}/{N} 件完了...")

        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM patients").fetchone()[0]
        print(f"\n✅ 完了！ 合計 {total} 件の症例データがDBに格納されました。")

        # サマリー
        print("\n--- サマリー ---")
        for label, q in [
            ("疾患分類", "SELECT disease_class, COUNT(*) as n FROM patients GROUP BY disease_class ORDER BY n DESC"),
            ("執刀医", "SELECT op_surgeon, COUNT(*) as n FROM surgery WHERE op_surgeon IS NOT NULL GROUP BY op_surgeon ORDER BY n DESC"),
            ("到達法", "SELECT op_approach, COUNT(*) as n FROM surgery GROUP BY op_approach ORDER BY n DESC"),
            ("手術年", "SELECT substr(surgery_date,1,4) as yr, COUNT(*) as n FROM patients GROUP BY yr ORDER BY yr"),
        ]:
            print(f"\n{label}:")
            for row in conn.execute(q).fetchall():
                print(f"  {dict(row)}")


if __name__ == "__main__":
    generate()
