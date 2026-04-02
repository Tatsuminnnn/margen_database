"""
上部消化管グループ 統合症例登録DB — validation.py
入力バリデーション（ハードリミット + 1.5×IQR 統計的外れ値検出）

使用方法:
    from validation import validate_record, get_soft_limits

    # 保存前バリデーション
    errors, warnings = validate_record("patients", data_dict)
    # errors: ハードリミット違反（保存ブロック）
    # warnings: ソフトリミット外れ値（警告のみ、保存可）

    # 術式×到達法グループ別の外れ値閾値取得
    limits = get_soft_limits("op_time_min", op_procedure=3, op_approach=2)
"""

import sys
import os
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import get_db


# ============================================================
# 1. ハードリミット定義
# ============================================================
# 形式: field_name → (min, max, label)
# min/max が None の場合は片側制約のみ
# 「絶対にありえない値」をブロックする目的

HARD_LIMITS = {
    # --- 患者背景 ---
    "height_cm":            (80,   220,  "身長(cm)"),
    "weight_admission":     (20,   250,  "入院時体重(kg)"),
    "weight_discharge":     (20,   250,  "退院時体重(kg)"),
    "smoking_bi":           (0,    4000, "BI指数"),

    # --- 術前検査値 ---
    "preop_alb":            (0.5,  7.0,  "術前Alb(g/dL)"),
    "preop_hb":             (2.0,  25.0, "術前Hb(g/dL)"),
    "preop_wbc":            (100,  99000, "術前WBC(/μL)"),
    "preop_plt":            (0.1,  200,  "術前Plt(×10⁴/μL)"),
    "preop_crp":            (0,    50,   "術前CRP(mg/dL)"),
    "preop_cea":            (0,    10000, "術前CEA(ng/mL)"),
    "preop_ca199":          (0,    50000, "術前CA19-9(U/mL)"),
    "preop_cr":             (0.1,  30,   "術前Cr(mg/dL)"),
    "preop_tbil":           (0.1,  40,   "術前T-Bil(mg/dL)"),
    "preop_hba1c":          (3.0,  20.0, "術前HbA1c(%)"),

    # --- 腫瘍径 ---
    "c_tumor_size_major_mm": (0,   500,  "腫瘍長径(mm)"),
    "c_tumor_size_minor_mm": (0,   500,  "腫瘍短径(mm)"),
    "p_size_major_mm":       (0,   500,  "病理腫瘍長径(mm)"),
    "p_size_minor_mm":       (0,   500,  "病理腫瘍短径(mm)"),
    "c_egj_distance_mm":     (-100, 100, "EGJ距離(mm)"),
    "c_esophageal_invasion_mm": (0, 200, "食道浸潤長(mm)"),

    # --- 手術 ---
    "op_time_min":           (10,  1800, "手術時間(min)"),
    "op_console_time_min":   (0,   1500, "コンソール時間(min)"),
    "op_blood_loss_ml":      (0,   30000, "出血量(mL)"),
    "op_icu_days":           (0,   365,  "ICU日数"),
    "op_transfusion_intra_rbc": (0, 100, "術中RBC(単位)"),
    "op_transfusion_intra_ffp": (0, 100, "術中FFP(単位)"),
    "op_transfusion_intra_pc":  (0, 100, "術中PC(単位)"),
    "op_transfusion_post_rbc":  (0, 100, "術後RBC(単位)"),
    "op_transfusion_post_ffp":  (0, 100, "術後FFP(単位)"),
    "op_transfusion_post_pc":   (0, 100, "術後PC(単位)"),

    # --- 病理マージン ---
    "p_pm_mm":               (0,   200,  "近位断端(mm)"),
    "p_dm_mm":               (0,   200,  "遠位断端(mm)"),
    "p_rm_mm":               (0,   200,  "RM(mm)"),

    # --- バイオマーカー ---
    "pdl1_cps":              (0,   200,  "PD-L1 CPS"),
    "pdl1_tps":              (0,   100,  "PD-L1 TPS(%)"),

    # --- 腫瘍マーカー (tumor_markers) ---
    "cea":                   (0,   50000, "CEA(ng/mL)"),
    "ca199":                 (0,   100000, "CA19-9(U/mL)"),
    "ca125":                 (0,   50000, "CA125(U/mL)"),
    "afp":                   (0,   500000, "AFP(ng/mL)"),
    "scc_ag":                (0,   500,   "SCC(ng/mL)"),
    "cyfra":                 (0,   500,   "CYFRA(ng/mL)"),

    # --- 化学療法 ---
    "rt_total_dose_gy":      (0,   100,   "放射線総線量(Gy)"),

    # --- RECIST ---
    "recist_shrinkage_pct":  (-100, 100,  "RECIST縮小率(%)"),
    "primary_shrinkage_pct": (-100, 100,  "原発巣縮小率(%)"),
}


# ============================================================
# 2. 日付バリデーションルール
# ============================================================
# (field_A, field_B, label) — field_A ≤ field_B であること

DATE_ORDER_RULES = [
    ("birthdate",       "first_visit_date",    "生年月日 ≤ 初診日"),
    ("first_visit_date", "admission_date",      "初診日 ≤ 入院日"),
    ("admission_date",   "surgery_date",        "入院日 ≤ 手術日"),
    ("surgery_date",     "discharge_date",      "手術日 ≤ 退院日"),
    ("nac_start_date",   "surgery_date",        "NAC開始日 ≤ 手術日"),
]


# ============================================================
# 3. ソフトリミット（1.5×IQR 統計的外れ値検出）
# ============================================================
# op_procedure × op_approach グループ別に Q1, Q3 を計算し、
# [Q1 - 1.5×IQR, Q3 + 1.5×IQR] の範囲外を警告
# N < 10 のグループは外れ値判定をスキップ（偽陽性回避）

SOFT_LIMIT_FIELDS = [
    # (table, field, label)
    ("surgery",    "op_time_min",        "手術時間(min)"),
    ("surgery",    "op_console_time_min", "コンソール時間(min)"),
    ("surgery",    "op_blood_loss_ml",   "出血量(mL)"),
    ("surgery",    "op_icu_days",        "ICU日数"),
    ("patients",   "height_cm",          "身長(cm)"),
    ("patients",   "weight_admission",   "入院時体重(kg)"),
    ("patients",   "weight_discharge",   "退院時体重(kg)"),
    ("tumor_preop", "c_tumor_size_major_mm", "腫瘍長径(mm)"),
    ("pathology",  "p_size_major_mm",    "病理腫瘍長径(mm)"),
]

# IQR 倍率
IQR_MULTIPLIER = 1.5
# グループ最小症例数（これ未満は外れ値判定スキップ）
MIN_GROUP_SIZE = 10


def _compute_iqr_bounds(values):
    """値リストから Q1, Q3, IQR を計算し (lower_bound, upper_bound) を返す。"""
    if not values:
        return None, None
    s = sorted(values)
    n = len(s)
    q1_idx = n * 0.25
    q3_idx = n * 0.75

    def _percentile(sorted_vals, frac_idx):
        lower = int(frac_idx)
        upper = min(lower + 1, len(sorted_vals) - 1)
        frac = frac_idx - lower
        return sorted_vals[lower] * (1 - frac) + sorted_vals[upper] * frac

    q1 = _percentile(s, q1_idx)
    q3 = _percentile(s, q3_idx)
    iqr = q3 - q1
    lower = q1 - IQR_MULTIPLIER * iqr
    upper = q3 + IQR_MULTIPLIER * iqr
    return lower, upper


def get_soft_limits(field_name, op_procedure=None, op_approach=None):
    """
    指定フィールドの IQR ベース外れ値閾値を返す。
    op_procedure / op_approach が指定されていればグループ別に計算。
    戻り値: {"lower": float, "upper": float, "n": int} or None (N < MIN_GROUP_SIZE)
    """
    target = None
    for tbl, fld, lbl in SOFT_LIMIT_FIELDS:
        if fld == field_name:
            target = (tbl, fld, lbl)
            break
    if target is None:
        return None

    tbl, fld, lbl = target

    with get_db() as conn:
        # surgery テーブルのフィールドは surgery テーブルから直接取得
        # patients テーブルのフィールドは patients テーブルから取得
        # いずれの場合も op_procedure/op_approach でグループ化するために JOIN
        if tbl == "surgery":
            base_query = f"""
                SELECT s.{fld} FROM surgery s
                JOIN patients p ON s.patient_id = p.id
                WHERE s.{fld} IS NOT NULL
                  AND p.is_deleted = 0
            """
        elif tbl == "patients":
            base_query = f"""
                SELECT p.{fld} FROM patients p
                WHERE p.{fld} IS NOT NULL
                  AND p.is_deleted = 0
            """
        elif tbl in ("tumor_preop", "pathology"):
            base_query = f"""
                SELECT t.{fld} FROM {tbl} t
                JOIN patients p ON t.patient_id = p.id
                WHERE t.{fld} IS NOT NULL
                  AND p.is_deleted = 0
            """
        else:
            return None

        params = []
        # グループ条件追加
        if op_procedure is not None:
            if tbl == "surgery":
                base_query += " AND s.op_procedure = ?"
            else:
                base_query += """
                    AND p.id IN (SELECT patient_id FROM surgery WHERE op_procedure = ?)
                """
            params.append(op_procedure)
        if op_approach is not None:
            if tbl == "surgery":
                base_query += " AND s.op_approach = ?"
            else:
                base_query += """
                    AND p.id IN (SELECT patient_id FROM surgery WHERE op_approach = ?)
                """
            params.append(op_approach)

        rows = conn.execute(base_query, params).fetchall()
        values = [r[0] for r in rows if r[0] is not None]

    if len(values) < MIN_GROUP_SIZE:
        return None

    lower, upper = _compute_iqr_bounds(values)
    if lower is None:
        return None

    return {
        "lower": round(lower, 2),
        "upper": round(upper, 2),
        "n": len(values),
        "label": lbl,
    }


# ============================================================
# 4. バリデーション実行
# ============================================================

def validate_record(table, data, context=None):
    """
    レコードデータを検証する。

    Args:
        table: テーブル名 ("patients", "surgery", etc.)
        data: dict — フォーム入力データ
        context: dict — 追加コンテキスト
            - op_procedure: 術式コード（ソフトリミットのグループ化用）
            - op_approach: 到達法コード
            - patient_data: patients テーブルのデータ（日付整合性チェック用）
            - surgery_data: surgery テーブルのデータ

    Returns:
        (errors, warnings)
        errors: list of dict — ハードリミット違反（保存ブロック）
            [{"field": str, "message": str, "value": any, "type": "hard_limit"|"date_order"}]
        warnings: list of dict — ソフトリミット外れ値（警告のみ）
            [{"field": str, "message": str, "value": any, "type": "soft_limit",
              "lower": float, "upper": float, "n": int}]
    """
    if context is None:
        context = {}

    errors = []
    warnings = []

    # -------------------------------------------------------
    # (A) ハードリミットチェック
    # -------------------------------------------------------
    for field, value in data.items():
        if value is None or value == "":
            continue
        if field not in HARD_LIMITS:
            continue

        try:
            num_val = float(value)
        except (ValueError, TypeError):
            continue

        vmin, vmax, label = HARD_LIMITS[field]
        if vmin is not None and num_val < vmin:
            errors.append({
                "field": field,
                "message": f"{label}: {num_val} は許容範囲外です（{vmin}〜{vmax}）",
                "value": num_val,
                "type": "hard_limit",
            })
        elif vmax is not None and num_val > vmax:
            errors.append({
                "field": field,
                "message": f"{label}: {num_val} は許容範囲外です（{vmin}〜{vmax}）",
                "value": num_val,
                "type": "hard_limit",
            })

    # -------------------------------------------------------
    # (B) 日付整合性チェック
    # -------------------------------------------------------
    # 日付フィールドを統合（patients + 他テーブル）
    all_dates = {}
    if context.get("patient_data"):
        all_dates.update(context["patient_data"])
    if context.get("surgery_data"):
        all_dates.update(context["surgery_data"])
    all_dates.update(data)

    for field_a, field_b, label in DATE_ORDER_RULES:
        val_a = all_dates.get(field_a)
        val_b = all_dates.get(field_b)
        if not val_a or not val_b:
            continue
        try:
            date_a = _parse_date(val_a)
            date_b = _parse_date(val_b)
            if date_a and date_b and date_a > date_b:
                errors.append({
                    "field": field_b,
                    "message": f"日付順序エラー: {label}（{val_a} > {val_b}）",
                    "value": val_b,
                    "type": "date_order",
                })
        except Exception:
            pass

    # -------------------------------------------------------
    # (C) 手術時年齢チェック（生年月日 + 手術日から）
    # -------------------------------------------------------
    bd = all_dates.get("birthdate")
    sd = all_dates.get("surgery_date")
    if bd and sd:
        try:
            birth = _parse_date(bd)
            surg = _parse_date(sd)
            if birth and surg:
                age = (surg - birth).days / 365.25
                if age < 0 or age > 120:
                    errors.append({
                        "field": "birthdate",
                        "message": f"手術時年齢 {age:.1f} 歳は不正です",
                        "value": bd,
                        "type": "hard_limit",
                    })
        except Exception:
            pass

    # -------------------------------------------------------
    # (D) ソフトリミット（1.5×IQR）チェック
    # -------------------------------------------------------
    op_proc = context.get("op_procedure") or data.get("op_procedure")
    op_appr = context.get("op_approach") or data.get("op_approach")

    for tbl, fld, lbl in SOFT_LIMIT_FIELDS:
        if fld not in data:
            continue
        value = data[fld]
        if value is None or value == "":
            continue
        try:
            num_val = float(value)
        except (ValueError, TypeError):
            continue

        # まずグループ別で試行 → N < 10 なら全体で試行
        bounds = get_soft_limits(fld, op_procedure=op_proc, op_approach=op_appr)
        if bounds is None:
            bounds = get_soft_limits(fld)  # 全体フォールバック
        if bounds is None:
            continue

        if num_val < bounds["lower"] or num_val > bounds["upper"]:
            group_desc = ""
            if op_proc is not None or op_appr is not None:
                group_desc = f"（術式={op_proc}, 到達法={op_appr}）"
            warnings.append({
                "field": fld,
                "message": (
                    f"{lbl}: {num_val} は統計的外れ値です"
                    f"{group_desc}"
                    f"（1.5×IQR範囲: {bounds['lower']}〜{bounds['upper']}, "
                    f"N={bounds['n']}）"
                ),
                "value": num_val,
                "type": "soft_limit",
                "lower": bounds["lower"],
                "upper": bounds["upper"],
                "n": bounds["n"],
            })

    # -------------------------------------------------------
    # (E) コンソール時間 ≤ 手術時間チェック
    # -------------------------------------------------------
    ot = data.get("op_time_min") or all_dates.get("op_time_min")
    ct = data.get("op_console_time_min") or all_dates.get("op_console_time_min")
    if ot is not None and ct is not None:
        try:
            if float(ct) > float(ot):
                errors.append({
                    "field": "op_console_time_min",
                    "message": f"コンソール時間({ct}min) > 手術時間({ot}min) は不正です",
                    "value": ct,
                    "type": "hard_limit",
                })
        except (ValueError, TypeError):
            pass

    # -------------------------------------------------------
    # (F) 腫瘍径 major ≥ minor チェック
    # -------------------------------------------------------
    for prefix in ("c_tumor_size", "p_size"):
        major_key = f"{prefix}_major_mm"
        minor_key = f"{prefix}_minor_mm"
        maj = data.get(major_key)
        mino = data.get(minor_key)
        if maj is not None and mino is not None:
            try:
                if float(mino) > float(maj):
                    warnings.append({
                        "field": minor_key,
                        "message": f"短径({mino}mm) > 長径({maj}mm): 入力を確認してください",
                        "value": mino,
                        "type": "soft_limit",
                        "lower": 0,
                        "upper": float(maj),
                        "n": 0,
                    })
            except (ValueError, TypeError):
                pass

    return errors, warnings


def _parse_date(val):
    """日付文字列をdateオブジェクトに変換する。"""
    if isinstance(val, (date, datetime)):
        return val if isinstance(val, date) else val.date()
    if not isinstance(val, str) or not val.strip():
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(val.strip(), fmt).date()
        except ValueError:
            continue
    return None


# ============================================================
# 5. Streamlit UI ヘルパー
# ============================================================

def format_validation_results(errors, warnings):
    """
    Streamlit で表示しやすい形式に整形する。

    Returns:
        dict with keys:
            "has_errors": bool
            "has_warnings": bool
            "error_messages": list[str]
            "warning_messages": list[str]
            "error_fields": set[str]  — エラーのあるフィールド名
            "warning_fields": set[str]
    """
    return {
        "has_errors": len(errors) > 0,
        "has_warnings": len(warnings) > 0,
        "error_messages": [e["message"] for e in errors],
        "warning_messages": [w["message"] for w in warnings],
        "error_fields": {e["field"] for e in errors},
        "warning_fields": {w["field"] for w in warnings},
    }


def get_hard_limit(field_name):
    """指定フィールドのハードリミットを返す。(min, max, label) or None。"""
    return HARD_LIMITS.get(field_name)


def get_all_hard_limits():
    """全ハードリミット定義を返す。"""
    return HARD_LIMITS.copy()


# ============================================================
# 6. セルフテスト
# ============================================================
if __name__ == "__main__":
    print("=== validation.py セルフテスト ===")

    # (A) ハードリミットテスト
    test_data = {
        "height_cm": 180,
        "weight_admission": 70,
        "op_time_min": 300,
        "op_blood_loss_ml": 50,
        "preop_alb": 4.0,
    }
    errs, warns = validate_record("surgery", test_data)
    assert len(errs) == 0, f"正常値でエラー発生: {errs}"
    print("✅ 正常値テスト PASS")

    # (B) ハードリミット違反テスト
    bad_data = {
        "height_cm": 10,   # < 80
        "op_time_min": 5000,  # > 1800
        "preop_alb": 0.1,  # < 0.5
    }
    errs, warns = validate_record("surgery", bad_data)
    assert len(errs) == 3, f"3件のエラー期待、実際: {len(errs)}"
    print("✅ ハードリミット違反テスト PASS")

    # (C) 日付順序テスト
    date_data = {}
    ctx = {"patient_data": {
        "birthdate": "1960-01-01",
        "admission_date": "2025-03-01",
        "surgery_date": "2025-02-15",  # 入院日より前 → エラー
        "discharge_date": "2025-03-10",
    }}
    errs, warns = validate_record("patients", date_data, context=ctx)
    date_errs = [e for e in errs if e["type"] == "date_order"]
    assert len(date_errs) >= 1, f"日付順序エラー期待、実際: {date_errs}"
    print("✅ 日付順序テスト PASS")

    # (D) コンソール時間チェック
    console_data = {"op_time_min": 200, "op_console_time_min": 250}
    errs, warns = validate_record("surgery", console_data)
    assert any(e["field"] == "op_console_time_min" for e in errs)
    print("✅ コンソール > 手術時間テスト PASS")

    # (E) 腫瘍径チェック
    tumor_data = {"c_tumor_size_major_mm": 30, "c_tumor_size_minor_mm": 50}
    errs, warns = validate_record("tumor_preop", tumor_data)
    assert any(w["field"] == "c_tumor_size_minor_mm" for w in warns)
    print("✅ 腫瘍短径 > 長径テスト PASS")

    # (F) format_validation_results テスト
    errs, warns = validate_record("surgery", bad_data)
    result = format_validation_results(errs, warns)
    assert result["has_errors"] is True
    assert len(result["error_fields"]) == 3
    print("✅ format_validation_results テスト PASS")

    print("\n✅ 全テスト PASS — validation.py は正常です。")
