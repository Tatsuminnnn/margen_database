"""
上部消化管グループ 統合症例登録DB — required_fields.py
術式-項目必須マトリクス + Phase提出時バリデーション

術式ごとに Phase1 提出時に必須となるフィールドを定義し、
提出前チェックを行う。
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import get_db
from codebook import COLUMN_LABELS

# ============================================================
# 術式コード定数
# ============================================================
# 胃癌
DG = 1    # 幽門側胃切除
TG = 2    # 胃全摘
PG = 3    # 噴門側胃切除
LR = 4    # 胃局所切除
PPG = 5   # 幽門保存胃切除
CG = 6    # 残胃全摘
SL = 7    # 審査腹腔鏡
BP = 8    # バイパス手術
OTHER_G = 9  # その他（胃）

# 食道癌
RT_ESO = 11   # 右開胸食道切除
LT_ESO = 12   # 左開胸食道切除
THE    = 13   # 食道抜去術
NTES   = 14   # 非開胸食道切除
VATS   = 15   # 鏡視下食道切除
ROBOT  = 16   # ロボット支援食道切除
PHLE   = 17   # 咽頭喉頭食道切除
OTHER_E = 19  # その他（食道）

# 術式グループ
GASTRIC_RESECTION = {DG, TG, PG, LR, PPG, CG}  # 胃切除群
GASTRIC_MINOR = {SL, BP}                        # 非切除群
GASTRIC_ALL = GASTRIC_RESECTION | GASTRIC_MINOR | {OTHER_G}

ESO_RESECTION = {RT_ESO, LT_ESO, THE, NTES, VATS, ROBOT, PHLE}
ESO_ALL = ESO_RESECTION | {OTHER_E}


# ============================================================
# 必須フィールド定義
# ============================================================
# 共通必須（全術式共通、Phase1提出時に必ず必要）
_COMMON_REQUIRED = {
    # patients テーブル
    "sex", "birthdate", "surgery_date", "admission_date",
    "discharge_date", "height_cm", "weight_admission", "asa", "ps",
    "disease_category",
}

# surgery テーブル共通必須
_SURGERY_COMMON = {
    "surgery.op_procedure", "surgery.op_approach", "surgery.op_time_min",
    "surgery.op_blood_loss_ml",
}

# --- 術式別 追加必須フィールド ---
# table.column 形式で指定

# 胃癌切除群: 腫瘍情報 + 病理が必須
_GASTRIC_RESECTION_REQUIRED = {
    # 術前腫瘍
    "tumor_preop.c_macroscopic_type",
    "tumor_preop.c_depth",
    "tumor_preop.c_ln_metastasis",
    "tumor_preop.c_stage",
    # 手術
    "surgery.op_dissection",
    "surgery.op_reconstruction",
    # 病理
    "pathology.p_histology1",
    "pathology.p_depth",
    "pathology.p_ln_metastasis",
    "pathology.p_stage",
    "pathology.p_residual_tumor",
}

# 審査腹腔鏡: 腫瘍最低限
_SL_REQUIRED = {
    "tumor_preop.c_macroscopic_type",
    "tumor_preop.c_depth",
    "tumor_preop.c_stage",
}

# バイパス: 腫瘍ステージのみ
_BP_REQUIRED = {
    "tumor_preop.c_stage",
}

# 食道切除群: 食道腫瘍 + 食道手術 + 食道病理
_ESO_RESECTION_REQUIRED = {
    # 食道腫瘍
    "eso_tumor.eso_macroscopic_type",
    "eso_tumor.eso_tumor_location",
    "eso_tumor.c_depth_eso",
    "eso_tumor.c_ln_eso",
    "eso_tumor.c_stage_eso",
    # 食道手術
    "eso_surgery.eso_approach",
    "eso_surgery.eso_reconstruction",
    "eso_surgery.eso_dissection",
    # 食道病理
    "eso_pathology.p_depth_eso",
    "eso_pathology.p_ln_eso",
    "eso_pathology.p_stage_eso",
    "eso_pathology.p_histology_eso",
    "eso_pathology.p_residual_tumor_eso",
}


def get_required_fields(op_procedure, disease_category=1):
    """術式コードに基づいて Phase1 提出必須フィールドセットを返す。

    Args:
        op_procedure: 術式コード（int）
        disease_category: 1=胃癌, 2=食道癌

    Returns:
        set[str]: 必須フィールド集合。patients カラムはそのまま、
                  子テーブルカラムは "table.column" 形式。
    """
    required = set(_COMMON_REQUIRED) | set(_SURGERY_COMMON)

    if disease_category == 1:
        # 胃癌
        if op_procedure in GASTRIC_RESECTION:
            required |= _GASTRIC_RESECTION_REQUIRED
        elif op_procedure == SL:
            required |= _SL_REQUIRED
        elif op_procedure == BP:
            required |= _BP_REQUIRED
        # OTHER_G は共通のみ
    elif disease_category == 2:
        # 食道癌
        if op_procedure in ESO_RESECTION:
            required |= _ESO_RESECTION_REQUIRED
        # OTHER_E は共通のみ

    return required


# ============================================================
# Phase提出バリデーション
# ============================================================
def validate_phase1_submission(patient_id):
    """Phase1 提出前バリデーション。

    患者データを読み込み、術式に応じた必須フィールドの充足状況をチェックする。

    Args:
        patient_id: 患者DB ID

    Returns:
        (can_submit: bool, missing_fields: list[dict])
        missing_fields: [{"field": "table.col or col", "label": "日本語ラベル"}, ...]
    """
    with get_db() as conn:
        # 患者基本情報
        patient = conn.execute(
            "SELECT * FROM patients WHERE id = ? AND is_deleted = 0", (patient_id,)
        ).fetchone()
        if not patient:
            return False, [{"field": "_", "label": "患者データが見つかりません"}]

        # 術式・疾患分類取得
        disease_cat = patient["disease_category"] or 1
        surgery = conn.execute(
            "SELECT * FROM surgery WHERE patient_id = ?", (patient_id,)
        ).fetchone()
        op_proc = surgery["op_procedure"] if surgery else None

        # 術式が未入力の場合
        if op_proc is None:
            return False, [{"field": "surgery.op_procedure", "label": "術式が未入力です"}]

        required = get_required_fields(op_proc, disease_cat)

        # テーブルデータ一括取得
        table_data = {"patients": dict(patient)}
        _child_tables = [
            "tumor_preop", "neoadjuvant", "surgery", "pathology",
            "gist_detail", "adjuvant_chemo", "outcome",
            "eso_tumor", "eso_surgery", "eso_pathology", "eso_course",
            "radiation_therapy",
        ]
        for tbl in _child_tables:
            row = conn.execute(
                f"SELECT * FROM {tbl} WHERE patient_id = ?", (patient_id,)
            ).fetchone()
            if row:
                table_data[tbl] = dict(row)
            else:
                table_data[tbl] = {}

    # 必須フィールドチェック
    missing = []
    for field_spec in sorted(required):
        if "." in field_spec:
            tbl, col = field_spec.split(".", 1)
        else:
            tbl = "patients"
            col = field_spec

        data = table_data.get(tbl, {})
        val = data.get(col)

        if val is None or (isinstance(val, str) and val.strip() == ""):
            label = COLUMN_LABELS.get(col, col)
            if tbl != "patients":
                tbl_jp = _table_jp_name(tbl)
                label = f"[{tbl_jp}] {label}"
            missing.append({"field": field_spec, "label": label})

    can_submit = len(missing) == 0
    return can_submit, missing


def _table_jp_name(tbl):
    """テーブル名の日本語ラベル。"""
    return {
        "tumor_preop":    "術前腫瘍",
        "neoadjuvant":    "術前治療",
        "surgery":        "手術",
        "pathology":      "病理",
        "gist_detail":    "GIST詳細",
        "adjuvant_chemo": "術後化学療法",
        "outcome":        "転帰",
        "eso_tumor":      "食道腫瘍",
        "eso_surgery":    "食道手術",
        "eso_pathology":  "食道病理",
        "eso_course":     "食道経過",
        "radiation_therapy": "放射線治療",
    }.get(tbl, tbl)


# ============================================================
# マトリクス表示用ヘルパー
# ============================================================
def get_requirement_matrix():
    """全術式の必須フィールドマトリクスを返す（UI表示用）。

    Returns:
        dict: {
            "procedures": [(code, label), ...],
            "fields": [field_spec, ...],
            "matrix": {procedure_code: set(required_fields)},
        }
    """
    from codebook import COMMON_ENTRIES

    procedures = []
    # 胃癌
    if "op_procedure_gastric" in COMMON_ENTRIES:
        for entry in COMMON_ENTRIES["op_procedure_gastric"]:
            procedures.append((entry[0], entry[1], 1))  # code, label, disease_cat
    # 食道
    if "op_procedure_eso" in COMMON_ENTRIES:
        for entry in COMMON_ENTRIES["op_procedure_eso"]:
            procedures.append((entry[0], entry[1], 2))

    # 全フィールド合計
    all_fields = set()
    matrix = {}
    for code, label, dcat in procedures:
        req = get_required_fields(code, dcat)
        matrix[code] = req
        all_fields |= req

    return {
        "procedures": [(c, l) for c, l, _ in procedures],
        "fields": sorted(all_fields),
        "matrix": matrix,
    }


# ============================================================
# セルフテスト
# ============================================================
if __name__ == "__main__":
    os.environ["UGI_DB_PATH"] = "/tmp/test_reqfields.db"
    if os.path.exists("/tmp/test_reqfields.db"):
        os.remove("/tmp/test_reqfields.db")

    import database as _db_mod
    _db_mod.DB_PATH = "/tmp/test_reqfields.db"
    from database import init_db, hash_password, get_db, upsert_record

    init_db()
    from codebook import populate_codebook
    populate_codebook()

    # ユーザー作成
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (username, password_hash, display_name, role) "
            "VALUES (?, ?, ?, ?)",
            ("test", hash_password("test1234"), "Test", "admin")
        )
        uid = conn.execute("SELECT id FROM users WHERE username='test'").fetchone()["id"]

    # --- Test 1: 必須フィールドセット ---
    print("=== Test 1: 必須フィールドセット ===")
    # 幽門側胃切除 (DG=1)
    req_dg = get_required_fields(1, disease_category=1)
    assert "surgery.op_dissection" in req_dg
    assert "pathology.p_stage" in req_dg
    assert "sex" in req_dg
    print(f"  DG(1): {len(req_dg)} 必須フィールド")

    # 審査腹腔鏡 (SL=7) — 病理不要
    req_sl = get_required_fields(7, disease_category=1)
    assert "pathology.p_stage" not in req_sl
    assert "tumor_preop.c_macroscopic_type" in req_sl
    print(f"  SL(7): {len(req_sl)} 必須フィールド")

    # 食道切除 (VATS=15)
    req_vats = get_required_fields(15, disease_category=2)
    assert "eso_tumor.c_depth_eso" in req_vats
    assert "eso_pathology.p_histology_eso" in req_vats
    print(f"  VATS(15): {len(req_vats)} 必須フィールド")

    assert len(req_dg) > len(req_sl), "DG should have more required fields than SL"
    print("  ✅ PASS")

    # --- Test 2: Phase1提出バリデーション（欠損あり）---
    print("\n=== Test 2: Phase1提出バリデーション（欠損あり）===")
    with get_db() as conn:
        conn.execute(
            """INSERT INTO patients (study_id, sex, surgery_date, birthdate,
               admission_date, discharge_date, height_cm, weight_admission,
               asa, ps, disease_category, created_by)
               VALUES ('RF-001', 1, '2025-06-01', '1960-01-01',
               '2025-05-28', '2025-06-15', 170, 65, 2, 0, 1, ?)""",
            (uid,)
        )
        pid = conn.execute("SELECT id FROM patients WHERE study_id='RF-001'").fetchone()["id"]
        # surgery は op_procedure のみ（他の必須欠損）
        upsert_record(conn, "surgery", pid, {"op_procedure": 1}, user_id=uid)

    can_submit, missing = validate_phase1_submission(pid)
    assert not can_submit, "Should not be able to submit with missing fields"
    assert len(missing) > 0
    print(f"  提出可: {can_submit}, 欠損フィールド: {len(missing)}")
    for m in missing[:5]:
        print(f"    - {m['label']} ({m['field']})")
    print("  ✅ PASS")

    # --- Test 3: Phase1提出バリデーション（全充足）---
    print("\n=== Test 3: Phase1提出バリデーション（全充足）===")
    with get_db() as conn:
        upsert_record(conn, "surgery", pid, {
            "op_procedure": 7,  # 審査腹腔鏡（必須少ない）
            "op_approach": 1,
            "op_time_min": 120,
            "op_blood_loss_ml": 50,
        }, user_id=uid)
        upsert_record(conn, "tumor_preop", pid, {
            "c_macroscopic_type": 1,
            "c_depth": 3,
            "c_stage": 2,
        }, user_id=uid)

    can_submit, missing = validate_phase1_submission(pid)
    if not can_submit:
        print(f"  Still missing: {[m['field'] for m in missing]}")
    assert can_submit, f"Should submit, missing: {[m['field'] for m in missing]}"
    print(f"  提出可: {can_submit}")
    print("  ✅ PASS")

    # --- Test 4: マトリクス表示 ---
    print("\n=== Test 4: マトリクス表示 ===")
    mat = get_requirement_matrix()
    print(f"  術式数: {len(mat['procedures'])}")
    print(f"  フィールド合計: {len(mat['fields'])}")
    print(f"  例: DG(1) → {len(mat['matrix'].get(1, set()))} 項目")
    print(f"  例: SL(7) → {len(mat['matrix'].get(7, set()))} 項目")
    print("  ✅ PASS")

    print("\n✅ required_fields.py 全テスト PASS")
