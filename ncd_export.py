"""
上部消化管グループ 統合症例登録DB — ncd_export.py
NCD (National Clinical Database) CSV エクスポート

使用方法:
    from ncd_export import export_ncd_csv, get_ncd_warnings

    # NCD CSV 出力
    csv_bytes, warnings = export_ncd_csv(patient_ids=[1, 2, 3])
    # warnings: 不完全データの警告リスト

NCD登録階層:
    L0 共通 → L1 外科共通 → L2 消化器外科 → L3 胃癌/食道癌登録 → L4 内視鏡/ロボット
"""

import csv
import io
from datetime import datetime, date

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import get_db, decrypt_value, ENCRYPTED_COLUMNS


# ============================================================
# NCD フィールドマッピング定義
# ============================================================
# UGI_DB カラム → NCD 項目名
# 値変換関数を3番目に指定（Noneなら直接出力）

def _yn(val):
    """0/1 → Y/N"""
    if val is None or val == "":
        return ""
    return "Y" if int(val) >= 1 else "N"


def _sex(val):
    """1=男, 2=女 → M/F"""
    if val == 1:
        return "M"
    elif val == 2:
        return "F"
    return ""


def _asa(val):
    """ASA code → NCD ASA"""
    mapping = {1: "I", 2: "II", 3: "III", 4: "IV", 5: "V", 6: "VI"}
    return mapping.get(val, "")


def _emergency(val):
    """0=予定, 1=緊急 → Elective/Emergency"""
    if val == 0:
        return "Elective"
    elif val == 1:
        return "Emergency"
    return ""


def _comor_detail(val, field_name):
    """多値フィールド → NCD Y/N + detail"""
    if val is None or val == "" or val == 0:
        return "N", ""
    if val == 8:  # 詳細不明
        return "Y", ""  # Y だが detail 空 → warning 対象
    return "Y", str(val)


def _diabetes_ncd(val):
    """comor_diabetes → NCD format"""
    mapping = {0: "N", 1: "食事療法のみ", 2: "経口薬", 3: "インスリン",
               4: "インスリン＋経口薬", 8: "Y"}
    return mapping.get(val, "")


def _hypertension_ncd(val):
    """comor_hypertension → NCD format"""
    mapping = {0: "N", 1: "未治療", 2: "治療中", 8: "Y"}
    return mapping.get(val, "")


def _codebook_ncd(conn, field_name, code):
    """codebook の ncd_mapping を参照して NCD 値を取得"""
    if code is None or code == "":
        return ""
    row = conn.execute(
        "SELECT ncd_mapping FROM codebook WHERE field_name=? AND code=? AND is_active=1",
        (field_name, code)
    ).fetchone()
    if row and row["ncd_mapping"]:
        return row["ncd_mapping"]
    return str(code)


# ============================================================
# NCD 出力カラム定義
# ============================================================
# (NCD項目名, テーブル, カラム名, 変換関数)
# 変換関数: None=直接出力, callable=関数適用, "codebook"=codebook参照

NCD_COLUMNS = [
    # --- L0: 共通 ---
    ("施設コード",        "patients", "facility_id",   None),
    ("NCD症例ID",        "patients", "ncd_case_id",    None),
    ("性別",             "patients", "sex",            _sex),
    ("生年月日",          "patients", "birthdate",      None),
    ("身長",             "patients", "height_cm",      None),
    ("体重",             "patients", "weight_admission", None),
    ("入院日",           "patients", "admission_date",  None),
    ("手術日",           "patients", "surgery_date",    None),
    ("退院日",           "patients", "discharge_date",  None),

    # --- L1: 外科共通 ---
    ("ASA",              "patients", "asa",            _asa),
    ("PS",               "patients", "ps",             None),
    ("緊急手術",          "surgery",  "op_emergency",   _emergency),
    ("手術時間",          "surgery",  "op_time_min",    None),
    ("出血量",           "surgery",  "op_blood_loss_ml", None),
    ("術中輸血",          "surgery",  "op_transfusion_intra", _yn),
    ("術後輸血",          "surgery",  "op_transfusion_post",  _yn),
    ("ICU入室日数",       "surgery",  "op_icu_days",    None),

    # --- L1: 併存疾患 ---
    ("喫煙",             "patients", "smoking",        _yn),
    ("糖尿病",           "patients", "comor_diabetes", _diabetes_ncd),
    ("高血圧",           "patients", "comor_hypertension", _hypertension_ncd),
    ("心血管疾患",        "patients", "comor_cardiovascular", _yn),
    ("虚血性心疾患",      "patients", "comor_ihd",      _yn),
    ("心不全",           "patients", "comor_chf",       _yn),
    ("不整脈",           "patients", "comor_arrhythmia", _yn),
    ("弁膜症",           "patients", "comor_valvular",  _yn),
    ("大動脈疾患",        "patients", "comor_aortic",    _yn),
    ("末梢血管疾患",      "patients", "comor_pvd",       _yn),
    ("脳血管障害",        "patients", "comor_cerebrovascular", _yn),
    ("脳梗塞",           "patients", "comor_cerebral_infarction", _yn),
    ("脳出血",           "patients", "comor_cerebral_hemorrhage", _yn),
    ("TIA",              "patients", "comor_tia",       _yn),
    ("くも膜下出血",      "patients", "comor_sah",       _yn),
    ("呼吸器疾患",        "patients", "comor_respiratory", _yn),
    ("透析",             "patients", "comor_renal_dialysis", _yn),
    ("肝疾患",           "patients", "comor_hepatic",   _yn),

    # --- L2: 消化器外科 ---
    ("術式",             "surgery",  "op_procedure",   "codebook:op_procedure"),
    ("到達法",           "surgery",  "op_approach",    "codebook:op_approach"),
    ("麻酔法",           "surgery",  "op_anesthesia_type", "codebook:op_anesthesia_type"),
    ("郭清度",           "surgery",  "op_dissection",  "codebook:op_dissection"),
    ("再建法",           "surgery",  "op_reconstruction", "codebook:op_reconstruction"),

    # --- L2: 合併症 ---
    ("合併症あり",        "surgery",  "op_complication_yn", _yn),
    ("SSI_表層",         "surgery",  "comp_ssi_superficial", _yn),
    ("SSI_深部",         "surgery",  "comp_ssi_deep",  _yn),
    ("SSI_臓器体腔",     "surgery",  "comp_ssi_organ", _yn),
    ("DVT",              "surgery",  "comp_dvt",       _yn),
    ("PE",               "surgery",  "comp_pe",        _yn),
    ("肺炎",             "surgery",  "comp_pneumonia", _yn),
    ("縫合不全",          "surgery",  "comp_anastomotic_leak", _yn),
    ("膵液瘻",           "surgery",  "comp_pancreatic_fistula", _yn),
    ("敗血症",           "surgery",  "comp_sepsis",    _yn),
    ("敗血症性ショック",   "surgery",  "comp_septic_shock", _yn),
    ("イレウス",          "surgery",  "comp_ileus",     _yn),
    ("再手術",           "surgery",  "op_reop_yn",     _yn),
    ("30日再入院",        "surgery",  "readmission_30d", _yn),

    # --- L3: アウトカム ---
    ("30日死亡",          "outcome",  "mortality_30d",       _yn),
    ("在院死亡",          "outcome",  "mortality_inhospital", _yn),
    ("退院先",           "patients", "discharge_destination", "codebook:discharge_destination"),

    # --- L3: 病理 ---
    ("残存腫瘍",          "pathology", "p_residual_tumor", "codebook:residual_tumor"),
]

# NCD 必須項目（空欄の場合 warning を生成）
NCD_REQUIRED_FIELDS = {
    "性別", "生年月日", "手術日", "入院日", "退院日", "ASA", "PS",
    "緊急手術", "手術時間", "出血量", "術式", "到達法",
    "合併症あり", "30日死亡", "在院死亡",
}


# ============================================================
# エクスポート関数
# ============================================================
def export_ncd_csv(patient_ids=None, include_all=False):
    """NCD CSV ファイルを生成する。

    Args:
        patient_ids: list[int] — 対象患者ID。Noneの場合は全件。
        include_all: bool — is_deleted=1 も含めるか

    Returns:
        (csv_bytes: bytes, warnings: list[dict])
        warnings: [{"study_id": str, "field": str, "message": str}, ...]
    """
    warnings = []

    with get_db() as conn:
        # 対象患者取得
        if patient_ids:
            placeholders = ",".join("?" for _ in patient_ids)
            patients = conn.execute(
                f"SELECT * FROM patients WHERE id IN ({placeholders})"
                + ("" if include_all else " AND is_deleted = 0"),
                patient_ids
            ).fetchall()
        else:
            patients = conn.execute(
                "SELECT * FROM patients WHERE is_deleted = 0"
                if not include_all else "SELECT * FROM patients"
            ).fetchall()

        if not patients:
            return b"", [{"study_id": "-", "field": "-",
                          "message": "対象症例がありません"}]

        # ヘッダ行
        ncd_headers = [col[0] for col in NCD_COLUMNS]
        ncd_headers.insert(0, "Study_ID")

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(ncd_headers)

        for pat in patients:
            pid = pat["id"]
            study_id = pat["study_id"]

            # 各テーブルのデータをロード
            table_data = {"patients": dict(pat)}
            for tbl in ("surgery", "pathology", "outcome", "tumor_preop"):
                row = conn.execute(
                    f"SELECT * FROM {tbl} WHERE patient_id=?", (pid,)
                ).fetchone()
                table_data[tbl] = dict(row) if row else {}

            # 暗号化カラムの復号
            for col in ENCRYPTED_COLUMNS:
                if col in table_data["patients"] and table_data["patients"][col]:
                    table_data["patients"][col] = decrypt_value(
                        table_data["patients"][col]
                    )

            # 各NCD項目を変換
            row_values = [study_id]
            for ncd_name, tbl, col, converter in NCD_COLUMNS:
                raw_val = table_data.get(tbl, {}).get(col)

                if converter is None:
                    val = str(raw_val) if raw_val is not None else ""
                elif callable(converter):
                    try:
                        val = converter(raw_val)
                    except Exception:
                        val = ""
                elif isinstance(converter, str) and converter.startswith("codebook:"):
                    cb_field = converter.split(":")[1]
                    val = _codebook_ncd(conn, cb_field, raw_val) if raw_val else ""
                else:
                    val = str(raw_val) if raw_val is not None else ""

                row_values.append(val)

                # 必須項目チェック
                if ncd_name in NCD_REQUIRED_FIELDS and (val == "" or val is None):
                    warnings.append({
                        "study_id": study_id,
                        "field": ncd_name,
                        "message": f"{ncd_name} が未入力です",
                    })

                # 詳細不明チェック（糖尿病・高血圧）
                if col in ("comor_diabetes", "comor_hypertension") and raw_val == 8:
                    warnings.append({
                        "study_id": study_id,
                        "field": ncd_name,
                        "message": f"{ncd_name}: 「詳細不明」— NCD登録前にサブカテゴリを確認してください",
                    })

            writer.writerow(row_values)

    csv_bytes = output.getvalue().encode("utf-8-sig")  # BOM付きUTF-8
    return csv_bytes, warnings


def get_ncd_warnings(patient_ids=None):
    """NCD 出力の警告のみを取得する（CSV生成せず）。"""
    _, warnings = export_ncd_csv(patient_ids=patient_ids)
    return warnings


# ============================================================
# セルフテスト
# ============================================================
if __name__ == "__main__":
    from database import init_db
    init_db()

    # テスト用データ挿入
    with get_db() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO patients
                (study_id, sex, birthdate, surgery_date, admission_date,
                 discharge_date, height_cm, weight_admission, asa, ps,
                 comor_diabetes, comor_hypertension, smoking, data_status)
            VALUES ('NCD-TEST-0001', 1, '1960-05-15', '2025-03-01',
                    '2025-02-28', '2025-03-15', 170.0, 65.0, 2, 0,
                    3, 2, 1, 'draft')
        """)
        pid = conn.execute(
            "SELECT id FROM patients WHERE study_id='NCD-TEST-0001'"
        ).fetchone()["id"]
        conn.execute("""
            INSERT OR IGNORE INTO surgery
                (patient_id, op_emergency, op_time_min, op_blood_loss_ml,
                 op_procedure, op_approach, op_anesthesia_type,
                 op_complication_yn, op_transfusion_intra, op_transfusion_post,
                 comp_ssi_superficial, comp_pneumonia)
            VALUES (?, 0, 240, 50, 3, 2, 2, 1, 0, 0, 0, 1)
        """, (pid,))
        conn.execute("""
            INSERT OR IGNORE INTO outcome
                (patient_id, mortality_30d, mortality_inhospital)
            VALUES (?, 0, 0)
        """, (pid,))
        conn.execute("""
            INSERT OR IGNORE INTO pathology
                (patient_id, p_residual_tumor)
            VALUES (?, 0)
        """, (pid,))

    csv_bytes, warnings = export_ncd_csv()
    csv_text = csv_bytes.decode("utf-8-sig")
    lines = csv_text.strip().split("\n")
    assert len(lines) >= 2, f"CSVが2行以上必要: {len(lines)}"

    header = lines[0]
    assert "Study_ID" in header
    assert "性別" in header
    assert "糖尿病" in header
    print(f"✅ CSV ヘッダ {len(header.split(','))} カラム生成")

    data_line = lines[1]
    fields = data_line.split(",")
    assert fields[0] == "NCD-TEST-0001"
    print(f"✅ データ行: {fields[0]}")

    # 性別=M (sex=1)
    sex_idx = lines[0].split(",").index("性別")
    assert fields[sex_idx] == "M"
    print("✅ 性別変換 PASS")

    # 糖尿病=インスリン (comor_diabetes=3)
    dm_idx = lines[0].split(",").index("糖尿病")
    assert fields[dm_idx] == "インスリン"
    print("✅ 糖尿病変換 PASS")

    # 警告チェック: 郭清度、再建法など未入力のはず
    warn_fields = {w["field"] for w in warnings}
    print(f"✅ 警告 {len(warnings)} 件: {warn_fields}")

    print("\n✅ NCD export 全テスト PASS")
