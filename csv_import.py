"""
上部消化管グループ 統合症例登録DB — csv_import.py
CSVインポート機能: テンプレート生成 + バリデーション + 一括登録
"""

import csv
import io
import re
from datetime import datetime, date

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import get_db, init_db, generate_study_id, upsert_record, log_audit
from codebook import COLUMN_LABELS, get_codebook, COMMON_ENTRIES


# ============================================================
# テーブル ↔ カラム マッピング
# ============================================================
# patients テーブルのうちインポート対象カラム（システムカラムを除外）
_SYSTEM_COLS = {
    "id", "study_id", "created_by", "created_at", "updated_by", "updated_at",
    "data_status",  # システム管理（import時は常に'draft'）
    "phase1_status", "phase1_submitted_at", "phase1_submitted_by",
    "phase1_approved_at", "phase1_approved_by",
    "phase3_status", "phase3_submitted_at", "phase3_submitted_by",
    "phase3_approved_at", "phase3_approved_by",
    "phase4_status", "phase4_submitted_at", "phase4_submitted_by",
    "phase4_approved_at", "phase4_approved_by",
    "is_deleted", "facility_id",
    # classification_version_id はインポート時に設定可能（_SYSTEM_COLSから除外）
}

# インポート対象の 1:1 テーブル（alias, table_name）
IMPORT_TABLES = [
    ("patients",        "patients"),
    ("tumor_preop",     "tumor_preop"),
    ("neoadjuvant",     "neoadjuvant"),
    ("surgery",         "surgery"),
    ("pathology",       "pathology"),
    ("lymph_nodes",     "lymph_nodes"),
    ("gist_detail",     "gist_detail"),
    ("adjuvant_chemo",  "adjuvant_chemo"),
    ("outcome",         "outcome"),
    ("eso_tumor",       "eso_tumor"),
    ("eso_surgery",     "eso_surgery"),
    ("eso_pathology",   "eso_pathology"),
    ("eso_course",      "eso_course"),
    ("eso_lymph_nodes", "eso_lymph_nodes"),
    ("radiation_therapy", "radiation_therapy"),
]

# palliative_chemo は 1:N (line_number=1..5) なので特別扱い
# CSVヘッダーは palliative_chemo.line{N}_{col} 形式
_PAL_MAX_LINES = 5
_PAL_COLS = ["regimen", "regimen_other", "start_date", "courses", "adverse_event"]


_CSV_ALLOWED_TABLES = frozenset([
    "patients", "tumor_preop", "neoadjuvant", "surgery",
    "pathology", "lymph_nodes", "gist_detail",
    "adjuvant_chemo", "palliative_chemo", "outcome",
    "eso_tumor", "eso_surgery", "eso_course", "eso_lymph_nodes", "eso_pathology",
    "radiation_therapy", "tumor_markers",
])


def _get_table_columns(conn, table_name):
    """テーブルのカラム情報を取得（name, type, notnull, default）。"""
    if table_name not in _CSV_ALLOWED_TABLES:
        raise ValueError(f"Invalid table name: {table_name}")
    info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    cols = []
    for row in info:
        name = row["name"]
        if table_name == "patients" and name in _SYSTEM_COLS:
            continue
        if table_name != "patients" and name in ("id", "patient_id", "updated_at"):
            continue
        cols.append({
            "name": name,
            "type": row["type"],
            "notnull": bool(row["notnull"]),
            "default": row["dflt_value"],
        })
    return cols


# ============================================================
# テンプレートCSV生成
# ============================================================
def generate_import_template(disease_category=None):
    """インポート用CSVテンプレートを生成する。

    Args:
        disease_category: 1=胃癌のみ, 2=食道癌のみ, None=全カラム

    Returns:
        str: BOM付きUTF-8 CSV文字列（ヘッダー2行: 日本語ラベル + DB列名）
    """
    with get_db() as conn:
        header_jp = []
        header_db = []

        for _, tbl in IMPORT_TABLES:
            # 食道/胃癌フィルター
            if disease_category == 1 and tbl.startswith("eso_"):
                continue
            if disease_category == 2 and tbl in ("gist_detail",):
                continue

            cols = _get_table_columns(conn, tbl)
            for col in cols:
                db_col = col["name"]
                # テーブル名プレフィックスを付与（patients以外）
                if tbl != "patients":
                    csv_col = f"{tbl}.{db_col}"
                else:
                    csv_col = db_col

                jp_label = COLUMN_LABELS.get(db_col, db_col)
                if tbl != "patients":
                    jp_label = f"[{_table_jp(tbl)}] {jp_label}"

                header_jp.append(jp_label)
                header_db.append(csv_col)

        # palliative_chemo: line1〜line5 を展開
        if disease_category != 2:  # 食道癌のみの場合は除外しない（胃癌も使う）
            for line_n in range(1, _PAL_MAX_LINES + 1):
                for col in _PAL_COLS:
                    csv_col = f"palliative_chemo.line{line_n}_{col}"
                    jp_lbl = COLUMN_LABELS.get(col, col)
                    header_jp.append(f"[術後化学療法{line_n}次] {jp_lbl}")
                    header_db.append(csv_col)

    output = io.StringIO()
    output.write("\ufeff")  # BOM
    writer = csv.writer(output)
    writer.writerow(header_jp)
    writer.writerow(header_db)
    # サンプル行（空行1行）
    writer.writerow([""] * len(header_db))
    return output.getvalue()


def _table_jp(tbl):
    """テーブル名の日本語ラベル。"""
    return {
        "tumor_preop":    "術前腫瘍",
        "neoadjuvant":    "術前治療",
        "surgery":        "手術",
        "pathology":      "病理",
        "lymph_nodes":    "リンパ節",
        "gist_detail":    "GIST詳細",
        "adjuvant_chemo": "術後化学療法",
        "outcome":        "転帰",
        "eso_tumor":      "食道腫瘍",
        "eso_surgery":    "食道手術",
        "eso_pathology":  "食道病理",
        "eso_course":     "食道経過",
        "eso_lymph_nodes": "食道リンパ節",
        "radiation_therapy": "放射線治療",
    }.get(tbl, tbl)


# ============================================================
# CSVバリデーション
# ============================================================
_DATE_RE = re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$")
_REQUIRED_PATIENTS_COLS = {"sex", "surgery_date"}  # 最低限必須

# 日付カラム一覧（全テーブルの TEXT 型日付カラム）
_DATE_COLUMNS = {
    # --- patients ---
    "birthdate", "first_visit_date", "admission_date", "surgery_date",
    "discharge_date", "first_treatment_completion_date",
    # --- neoadjuvant ---
    "nac_start_date",
    # --- adjuvant_chemo ---
    "adj_start_date",
    # --- palliative_chemo ---
    "start_date",
    # --- outcome ---
    "recurrence_date", "last_alive_date", "death_date",
    # --- radiation_therapy ---
    "rt_start_date",
    # --- surgery: 合併症日付 (31 complications + NCD sub) ---
    "comp_ssi_date", "comp_wound_dehiscence_date", "comp_intra_abd_abscess_date",
    "comp_bleeding_date", "comp_ileus_date", "comp_dvt_pe_date",
    "comp_pneumonia_date", "comp_atelectasis_date", "comp_uti_date",
    "comp_delirium_date", "comp_cardiac_date", "comp_dge_date",
    "comp_perforation_date", "comp_cholelithiasis_date",
    "comp_anastomotic_leak_date", "comp_anastomotic_stricture_date",
    "comp_anastomotic_bleeding_date", "comp_pancreatic_fistula_date",
    "comp_bile_leak_date", "comp_duodenal_stump_leak_date",
    "comp_rln_palsy_date", "comp_chylothorax_date", "comp_empyema_date",
    "comp_pneumothorax_date", "comp_ards_date", "comp_dic_date",
    "comp_sepsis_date", "comp_renal_failure_date", "comp_hepatic_failure_date",
    "comp_other_date",
    # NCD sub-complication dates
    "comp_ssi_superficial_date", "comp_ssi_deep_date", "comp_ssi_organ_date",
    "comp_dvt_date", "comp_pe_date", "comp_septic_shock_date",
    # --- eso_course: 食事・ドレーン・経過 ---
    "icu_discharge_date",
    "meal_water_date", "meal_liquid_date", "meal_3bu_date",
    "meal_5bu_date", "meal_zenkayu_date",
    "npo_date",
    "meal_water_date2", "meal_liquid_date2", "meal_3bu_date2",
    "meal_5bu_date2", "meal_zenkayu_date2",
    "drain_left_chest_date", "drain_right_chest_date", "drain_neck_date",
    "drain_other1_date", "drain_other2_date",
    "tube_feeding_start", "tube_feeding_end",
    "reop_date", "reop2_date",
    "stricture_first_date",
}

# 整数カラムでコードブック検証が必要なフィールド
_CODEBOOK_FIELDS = {
    "sex", "smoking", "alcohol", "ps", "asa", "adl_status",
    "hp_eradication", "discharge_destination", "disease_class",
    "disease_category",
    "comor_diabetes", "comor_hypertension", "comor_cirrhosis",
    "comor_hepatitis_virus", "smoking_type",
}

# 日付順序ルール: (earlier, later, description)
_DATE_ORDER_RULES = [
    ("birthdate",      "surgery_date",    "生年月日 ≤ 手術日"),
    ("admission_date", "surgery_date",    "入院日 ≤ 手術日"),
    ("surgery_date",   "discharge_date",  "手術日 ≤ 退院日"),
]


def validate_csv(csv_text):
    """CSVテキストをバリデーションする。

    Args:
        csv_text: UTF-8 CSVテキスト（BOM付きも可）

    Returns:
        (rows: list[dict], errors: list[str], warnings: list[str])
        rows: DB列名→値 のdict一覧（ヘッダー行除く）
        errors: 致命的エラー（インポート不可）
        warnings: 警告（インポート可だが確認推奨）
    """
    errors = []
    warnings = []

    # BOM除去
    if csv_text.startswith("\ufeff"):
        csv_text = csv_text[1:]

    lines = csv_text.strip().split("\n")
    if len(lines) < 2:
        return [], ["CSVにデータ行がありません（ヘッダー行のみ）"], []

    reader = csv.reader(io.StringIO(csv_text.strip()))
    all_rows = list(reader)

    # ヘッダー検出: 2行目がDB列名（テンプレート形式）か1行目がDB列名か
    header_row = None
    data_start = 1

    # テンプレート形式: 1行目=日本語, 2行目=DB列名
    def _looks_like_db_header(row):
        """行がDB列名ヘッダーかどうか判定（table.col 形式が含まれるか）"""
        pattern = re.compile(r"^[a-z_]+\.[a-z_]+$")
        return any(pattern.match(cell.strip()) for cell in row if cell.strip())

    def _looks_like_column_names(row):
        """行がカラム名を含むか（COLUMN_LABELS にマッチ or table.col 形式）"""
        matches = sum(1 for cell in row if cell.strip() in COLUMN_LABELS)
        return matches >= 2  # 少なくとも2カラム以上がマッチ

    if len(all_rows) >= 2:
        if _looks_like_db_header(all_rows[1]):
            # テンプレート形式: 1行目=日本語, 2行目=DB列名
            header_row = all_rows[1]
            data_start = 2
        elif _looks_like_column_names(all_rows[0]):
            header_row = all_rows[0]
            data_start = 1
        else:
            header_row = all_rows[0]
            data_start = 1

    if header_row is None:
        return [], ["ヘッダー行を検出できません"], []

    # 空セル除去してヘッダー列名リスト化
    headers = [h.strip() for h in header_row]

    # ヘッダー検証
    with get_db() as conn:
        valid_columns = set()
        for _, tbl in IMPORT_TABLES:
            cols = _get_table_columns(conn, tbl)
            for col in cols:
                valid_columns.add(col["name"])
                if tbl != "patients":
                    valid_columns.add(f"{tbl}.{col['name']}")

    # palliative_chemo の展開カラムを追加
    for line_n in range(1, _PAL_MAX_LINES + 1):
        for col in _PAL_COLS:
            valid_columns.add(f"palliative_chemo.line{line_n}_{col}")

    unknown_cols = []
    for h in headers:
        if h and h not in valid_columns:
            unknown_cols.append(h)
    if unknown_cols:
        warnings.append(f"不明なカラム（無視されます）: {', '.join(unknown_cols[:10])}")

    # データ行パース
    rows = []
    for row_idx, row in enumerate(all_rows[data_start:], start=data_start + 1):
        if not any(cell.strip() for cell in row):
            continue  # 空行スキップ

        record = {}
        for col_idx, val in enumerate(row):
            if col_idx >= len(headers):
                break
            col_name = headers[col_idx]
            if not col_name or col_name not in valid_columns:
                continue
            record[col_name] = val.strip() if val.strip() else None

        if not record:
            continue

        # --- 行レベルバリデーション ---
        row_errors, row_warnings = _validate_row(record, row_idx)
        errors.extend(row_errors)
        warnings.extend(row_warnings)

        rows.append(record)

    if not rows:
        errors.append("インポート可能なデータ行がありません")

    return rows, errors, warnings


def _validate_row(record, row_num):
    """1行分のバリデーション。"""
    errors = []
    warnings = []
    prefix = f"行{row_num}"

    # 必須カラムチェック
    for req_col in _REQUIRED_PATIENTS_COLS:
        val = record.get(req_col)
        if val is None or val == "":
            errors.append(f"{prefix}: 必須項目「{COLUMN_LABELS.get(req_col, req_col)}」が空です")

    # 日付フォーマット
    parsed_dates = {}
    for col_name, val in record.items():
        bare_col = col_name.split(".")[-1]  # table.col → col
        if bare_col in _DATE_COLUMNS and val:
            if not _DATE_RE.match(val):
                errors.append(
                    f"{prefix}: 「{COLUMN_LABELS.get(bare_col, bare_col)}」の日付形式が不正: {val}"
                    f"（YYYY-MM-DD 形式で入力してください）"
                )
            else:
                try:
                    d = datetime.strptime(val.replace("/", "-"), "%Y-%m-%d").date()
                    parsed_dates[bare_col] = d
                    # 未来日チェック
                    if d > date.today():
                        warnings.append(
                            f"{prefix}: 「{COLUMN_LABELS.get(bare_col, bare_col)}」が未来の日付: {val}"
                        )
                except ValueError:
                    errors.append(
                        f"{prefix}: 「{COLUMN_LABELS.get(bare_col, bare_col)}」が無効な日付: {val}"
                    )

    # 日付順序チェック（警告: NAC再入院や入力ミスで逆転するケースが頻発するため）
    for early, late, desc in _DATE_ORDER_RULES:
        d_early = parsed_dates.get(early)
        d_late = parsed_dates.get(late)
        if d_early and d_late and d_early > d_late:
            warnings.append(f"{prefix}: 日付順序警告 — {desc}（{d_early} > {d_late}）")

    # コードブック値チェック
    for col_name, val in record.items():
        bare_col = col_name.split(".")[-1]
        if bare_col in _CODEBOOK_FIELDS and val is not None:
            try:
                int_val = int(val)
                cb = get_codebook(bare_col)
                if cb and int_val not in cb:
                    warnings.append(
                        f"{prefix}: 「{COLUMN_LABELS.get(bare_col, bare_col)}」のコード値 {int_val} "
                        f"がコードブックに存在しません"
                    )
            except (ValueError, TypeError):
                errors.append(
                    f"{prefix}: 「{COLUMN_LABELS.get(bare_col, bare_col)}」は整数で入力してください: {val}"
                )

    # 数値カラムの型チェック（height, weight, BMI等）
    _numeric_cols = {
        "height_cm", "weight_admission", "weight_discharge",
        "bmi", "bmi_change_pct", "smoking_bi",
        "op_time_min", "blood_loss_ml", "console_time_min",
    }
    for col_name, val in record.items():
        bare_col = col_name.split(".")[-1]
        if bare_col in _numeric_cols and val is not None:
            try:
                float(val)
            except (ValueError, TypeError):
                errors.append(
                    f"{prefix}: 「{COLUMN_LABELS.get(bare_col, bare_col)}」は数値で入力してください: {val}"
                )

    # 範囲チェック（ハードリミット）
    _hard_limits = {
        "height_cm":       (50, 250),
        "weight_admission": (10, 300),
        "weight_discharge": (10, 300),
        "op_time_min":     (0, 1800),
        "blood_loss_ml":   (0, 30000),
    }
    for col_name, val in record.items():
        bare_col = col_name.split(".")[-1]
        if bare_col in _hard_limits and val is not None:
            try:
                fval = float(val)
                lo, hi = _hard_limits[bare_col]
                if fval < lo or fval > hi:
                    errors.append(
                        f"{prefix}: 「{COLUMN_LABELS.get(bare_col, bare_col)}」が範囲外: "
                        f"{fval}（許容: {lo}–{hi}）"
                    )
            except (ValueError, TypeError):
                pass  # 型チェックは上で実施済み

    return errors, warnings


# ============================================================
# インポート実行
# ============================================================
def _generate_study_id_in_conn(conn):
    """既存のconnection内でstudy_idを生成する（トランザクション内で連番確保）。"""
    year = datetime.now().year
    prefix = f"UGI-{year}-"
    row = conn.execute(
        "SELECT study_id FROM patients WHERE study_id LIKE ? ORDER BY study_id DESC LIMIT 1",
        (f"{prefix}%",),
    ).fetchone()
    if row:
        last_num = int(row["study_id"].split("-")[-1])
        return f"{prefix}{last_num + 1:04d}"
    return f"{prefix}0001"


def import_csv_records(rows, user_id, disease_category=1):
    """バリデーション済みのレコード群をDBに一括登録する。

    Args:
        rows: validate_csv() が返した行リスト（dict群）
        user_id: 実行ユーザーID
        disease_category: 1=胃癌, 2=食道癌

    Returns:
        (success_count: int, error_list: list[str])
    """
    success_count = 0
    error_list = []

    with get_db() as conn:
        for row_idx, record in enumerate(rows, start=1):
            try:
                # study_id 生成（同一トランザクション内で連番を確保）
                sid = _generate_study_id_in_conn(conn)

                # テーブル別にデータを振り分け
                patients_data = {"disease_category": disease_category}
                table_data = {}  # {table_name: {col: val}}

                for col_name, val in record.items():
                    if val is None:
                        continue

                    if "." in col_name:
                        tbl, col = col_name.split(".", 1)
                        if tbl not in table_data:
                            table_data[tbl] = {}
                        table_data[tbl][col] = _convert_value(col, val)
                    else:
                        patients_data[col_name] = _convert_value(col_name, val)

                # patients INSERT
                patients_data["data_status"] = "draft"
                # study_id, created_by は generate_study_id で生成済み
                conn.execute(
                    """INSERT INTO patients (study_id, created_by, disease_category, data_status)
                       VALUES (?, ?, ?, 'draft')""",
                    (sid, user_id, disease_category)
                )
                pid = conn.execute(
                    "SELECT id FROM patients WHERE study_id = ?", (sid,)
                ).fetchone()["id"]

                # patients の残りカラムを UPDATE
                patients_update = {
                    k: v for k, v in patients_data.items()
                    if k not in ("disease_category", "data_status")
                }
                if patients_update:
                    set_clause = ", ".join(f"{k}=?" for k in patients_update)
                    vals = list(patients_update.values()) + [pid]
                    conn.execute(
                        f"UPDATE patients SET {set_clause} WHERE id = ?", vals
                    )

                # 子テーブル INSERT (1:1 テーブル)
                for tbl_name, cols in table_data.items():
                    if tbl_name == "palliative_chemo":
                        continue  # 後で別処理
                    if not cols:
                        continue
                    upsert_record(conn, tbl_name, pid, cols, user_id=user_id)

                # palliative_chemo (1:N) — line{N}_col → 行ごとにINSERT
                pal_lines = _extract_palliative_lines(record)
                for line_n, line_data in pal_lines.items():
                    if not any(v is not None for v in line_data.values()):
                        continue
                    line_data["line_number"] = line_n
                    # UPSERT: patient_id + line_number でユニーク
                    existing = conn.execute(
                        "SELECT id FROM palliative_chemo WHERE patient_id=? AND line_number=?",
                        (pid, line_n)
                    ).fetchone()
                    if existing:
                        set_clause = ", ".join(f"{k}=?" for k in line_data)
                        vals = list(line_data.values()) + [existing["id"]]
                        conn.execute(
                            f"UPDATE palliative_chemo SET {set_clause} WHERE id=?", vals
                        )
                    else:
                        line_data["patient_id"] = pid
                        cols_str = ", ".join(line_data.keys())
                        placeholders = ", ".join("?" * len(line_data))
                        conn.execute(
                            f"INSERT INTO palliative_chemo ({cols_str}) VALUES ({placeholders})",
                            list(line_data.values())
                        )

                # 監査ログ
                log_audit(conn, user_id, "CSV_IMPORT", "patients", pid,
                          comment=f"CSV import: {sid}")

                success_count += 1

            except Exception as e:
                error_list.append(f"行{row_idx}: {e}")

    return success_count, error_list


def _extract_palliative_lines(record):
    """CSVレコードから palliative_chemo.line{N}_{col} を抽出して
    {line_number: {col: val}} に変換する。"""
    pal_re = re.compile(r"^palliative_chemo\.line(\d+)_(.+)$")
    lines = {}
    for col_name, val in record.items():
        m = pal_re.match(col_name)
        if m:
            line_n = int(m.group(1))
            col = m.group(2)
            if line_n not in lines:
                lines[line_n] = {}
            lines[line_n][col] = _convert_value(col, val) if val else None
    return lines


def _convert_value(col_name, val):
    """文字列値をDB格納用に型変換する。"""
    if val is None or val == "":
        return None

    bare = col_name.split(".")[-1] if "." in col_name else col_name

    # 日付: /→- 正規化
    if bare in _DATE_COLUMNS:
        return val.replace("/", "-")

    # 整数カラム
    _int_cols = _CODEBOOK_FIELDS | {
        "preop_weight_loss_10pct",
        "sym_asymptomatic", "sym_epigastric_pain", "sym_dysphagia",
        "sym_weight_loss", "sym_anemia", "sym_melena",
        "sym_hematemesis", "sym_nausea_vomiting",
        "med_antihypertensive", "med_antithrombotic",
        "med_oral_hypoglycemic", "med_insulin",
        "med_steroid_immunosup", "med_antineoplastic",
        "med_thyroid", "med_psychotropic",
        "comor_cardiovascular", "comor_cerebrovascular",
        "comor_respiratory", "comor_renal", "comor_renal_dialysis",
        "comor_hepatic", "comor_endocrine", "comor_collagen",
        "comor_hematologic", "comor_neurologic", "comor_psychiatric",
        "neo_yn", "adj_yn", "recurrence_yn",
        "op_emergency", "op_combined_resection",
        "residual_tumor", "op_approach",
    }
    if bare in _int_cols:
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return val

    # REAL カラム
    _real_cols = {
        "height_cm", "weight_admission", "weight_discharge",
        "bmi", "bmi_change_pct", "smoking_bi",
        "op_time_min", "blood_loss_ml", "console_time_min",
        "tumor_major_mm", "tumor_minor_mm",
    }
    if bare in _real_cols:
        try:
            return float(val)
        except (ValueError, TypeError):
            return val

    return val


# ============================================================
# セルフテスト
# ============================================================
if __name__ == "__main__":
    import tempfile

    os.environ["UGI_DB_PATH"] = "/tmp/test_csv_import.db"
    if os.path.exists("/tmp/test_csv_import.db"):
        os.remove("/tmp/test_csv_import.db")

    import database as _db_mod
    _db_mod.DB_PATH = "/tmp/test_csv_import.db"
    init_db()

    # codebook populate
    from codebook import populate_codebook
    populate_codebook()

    # テスト用ユーザー作成
    from database import hash_password
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (username, password_hash, display_name, role) "
            "VALUES (?, ?, ?, ?)",
            ("test_import", hash_password("test1234"), "テストインポーター", "admin")
        )
        uid = conn.execute("SELECT id FROM users WHERE username='test_import'").fetchone()["id"]

    # --- Test 1: テンプレート生成 ---
    print("=== Test 1: テンプレート生成 ===")
    tmpl = generate_import_template(disease_category=1)
    lines = tmpl.replace("\ufeff", "").strip().split("\n")
    assert len(lines) >= 2, f"テンプレート行数不足: {len(lines)}"
    print(f"  ヘッダー(日本語): {len(lines[0].split(','))} カラム")
    print(f"  ヘッダー(DB列名): {len(lines[1].split(','))} カラム")
    print("  ✅ PASS")

    # --- Test 2: バリデーション（正常データ） ---
    print("\n=== Test 2: バリデーション（正常） ===")
    csv_ok = """sex,surgery_date,birthdate,admission_date,discharge_date,height_cm,weight_admission,asa,ps
1,2025-06-15,1965-03-20,2025-06-10,2025-06-25,170.5,68.2,2,0
2,2025-07-01,1970-11-05,2025-06-28,2025-07-10,158.0,52.3,1,0
"""
    rows, errs, warns = validate_csv(csv_ok)
    assert len(rows) == 2, f"行数: {len(rows)}"
    assert len(errs) == 0, f"エラー: {errs}"
    print(f"  {len(rows)} 行, エラー {len(errs)} 件, 警告 {len(warns)} 件")
    print("  ✅ PASS")

    # --- Test 3: バリデーション（エラー検出） ---
    print("\n=== Test 3: バリデーション（エラー検出） ===")
    csv_bad = """sex,surgery_date,birthdate,height_cm,admission_date,discharge_date,weight_admission,asa,ps
abc,not-a-date,1965-03-20,999,2025-06-10,2025-06-01,68,2,0
,2025-06-15,1965-03-20,170,2025-06-10,2025-06-20,68,2,0
"""
    rows, errs, warns = validate_csv(csv_bad)
    print(f"  {len(rows)} 行, エラー {len(errs)} 件, 警告 {len(warns)} 件")
    for e in errs:
        print(f"    ❌ {e}")
    # sex=abc → 整数エラー, surgery_date=not-a-date → 日付エラー,
    # height=999 → 範囲外, discharge < admission → 日付順序
    # 行3: sex 空 → 必須エラー
    assert len(errs) >= 3, f"エラー数不足: {len(errs)}"
    print("  ✅ PASS")

    # --- Test 4: インポート実行 ---
    print("\n=== Test 4: インポート実行 ===")
    csv_import = """sex,surgery_date,birthdate,admission_date,discharge_date,height_cm,weight_admission,asa,ps,surgery.op_procedure,surgery.op_time_min,surgery.blood_loss_ml
1,2025-08-01,1960-01-15,2025-07-28,2025-08-15,165.0,60.5,2,0,1,240,150
2,2025-08-10,1975-06-20,2025-08-07,2025-08-22,155.0,48.0,1,0,2,180,80
"""
    rows, errs, warns = validate_csv(csv_import)
    assert len(errs) == 0, f"バリデーションエラー: {errs}"

    success, import_errs = import_csv_records(rows, uid, disease_category=1)
    assert success == 2, f"成功数: {success}"
    assert len(import_errs) == 0, f"インポートエラー: {import_errs}"

    # DB確認
    with get_db() as conn:
        cnt = conn.execute(
            "SELECT COUNT(*) AS c FROM patients WHERE is_deleted=0"
        ).fetchone()["c"]
        assert cnt >= 2, f"患者数: {cnt}"

        # surgery テーブルにもデータが入っているか
        surg = conn.execute(
            "SELECT COUNT(*) AS c FROM surgery WHERE op_time_min IS NOT NULL"
        ).fetchone()["c"]
        assert surg >= 2, f"手術データ数: {surg}"

    print(f"  {success} 件インポート成功")
    print("  ✅ PASS")

    # --- Test 5: テンプレート形式（2行ヘッダー）のインポート ---
    print("\n=== Test 5: テンプレート形式（2行ヘッダー） ===")
    csv_tmpl = """性別,手術日,生年月日,[手術] 術式,[手術] 手術時間_min
sex,surgery_date,birthdate,surgery.op_procedure,surgery.op_time_min
1,2025-09-01,1955-12-25,3,300
"""
    rows, errs, warns = validate_csv(csv_tmpl)
    assert len(errs) == 0, f"エラー: {errs}"
    assert len(rows) == 1
    success, import_errs = import_csv_records(rows, uid, disease_category=1)
    assert success == 1, f"成功数: {success}"
    print(f"  {success} 件インポート成功（テンプレート形式）")
    print("  ✅ PASS")

    print("\n✅ csv_import.py 全テスト PASS")
