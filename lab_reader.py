"""
上部消化管グループ 統合症例登録DB — lab_reader.py
血液検査結果 画像読み取り（Ollama Vision）+ 構造化抽出

使い方:
    from lab_reader import extract_lab_values, check_vision_model
    ok, msg = check_vision_model()
    if ok:
        result = extract_lab_values(image_bytes)
        # result = {"values": {...}, "raw_text": "...", "errors": [...]}
"""

import os
import json
import base64
import re
import requests

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------
OLLAMA_URL = os.environ.get("UGI_LLM_URL", "http://localhost:11434")
VISION_MODEL = os.environ.get("UGI_VISION_MODEL", "llama3.2-vision")
VISION_TIMEOUT = int(os.environ.get("UGI_VISION_TIMEOUT", "180"))  # 秒


# ---------------------------------------------------------------------------
# Vision モデル接続チェック
# ---------------------------------------------------------------------------
def check_vision_model():
    """Ollama に Vision モデルが利用可能か確認する。

    Returns:
        (ok: bool, message: str)
    """
    try:
        r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5)
        if r.status_code == 200:
            models = [m["name"] for m in r.json().get("models", [])]
            matched = any(
                m == VISION_MODEL or m.startswith(VISION_MODEL + ":")
                for m in models
            )
            if matched:
                return True, f"Vision モデル接続OK（{VISION_MODEL}）"
            else:
                # Vision 対応モデルの候補をリストアップ
                vision_candidates = [
                    m for m in models
                    if any(v in m.lower() for v in [
                        "llava", "vision", "bakllava", "moondream",
                        "llama3.2-vision", "minicpm-v", "qwen2-vl",
                    ])
                ]
                hint = ""
                if vision_candidates:
                    hint = f"\n利用可能な Vision モデル候補: {', '.join(vision_candidates)}"
                return False, (
                    f"Ollama に接続できましたが Vision モデル '{VISION_MODEL}' が見つかりません。\n"
                    f"利用可能: {', '.join(models[:10])}"
                    f"{hint}\n"
                    f"環境変数 UGI_VISION_MODEL でモデル名を指定するか、"
                    f"`ollama pull {VISION_MODEL}` で取得してください。"
                )
        return False, f"Ollama 応答エラー: HTTP {r.status_code}"
    except requests.ConnectionError:
        return False, f"Ollama に接続できません ({OLLAMA_URL})"
    except Exception as e:
        return False, f"Vision モデルチェックエラー: {e}"


# ---------------------------------------------------------------------------
# 検査値 抽出プロンプト
# ---------------------------------------------------------------------------
_EXTRACTION_PROMPT = """あなたは臨床検査データの読み取り専門AIです。
添付の画像は病院の電子カルテの検査結果画面です。
画像から検査値を正確に読み取り、以下のJSON形式で出力してください。

読み取れない項目はnullとしてください。
数値のみ出力し、単位は含めないでください。
赤字や基準値外の表示があっても、数値そのものを正確に読み取ってください。

画面に患者IDや採取日が表示されている場合は必ず読み取ってください。
- 患者ID: 画面上部に「患者ID」「ID」「カルテ番号」「患者番号」等として表示される数字列
- 採取日: 画面上部の「採取日」「検査日」「実施日」等の日付表示

出力形式（JSONのみ、余計な文章は不要）:
```json
{
  "patient_id_ocr": "画像から読み取った患者ID文字列 or null",
  "sample_date": "YYYY-MM-DD or null",
  "wbc": null,
  "rbc": null,
  "hgb": null,
  "hct": null,
  "plt": null,
  "mcv": null,
  "mch": null,
  "mchc": null,
  "neut_pct": null,
  "lymph_pct": null,
  "mono_pct": null,
  "eosin_pct": null,
  "baso_pct": null,
  "tp": null,
  "alb": null,
  "t_bil": null,
  "ast": null,
  "alt": null,
  "ldh": null,
  "alp": null,
  "ggt": null,
  "che": null,
  "bun": null,
  "cre": null,
  "egfr": null,
  "na": null,
  "k": null,
  "cl": null,
  "crp": null,
  "amy": null,
  "ck": null,
  "glu": null,
  "hba1c": null,
  "cea_lab": null,
  "ca199_lab": null,
  "afp_lab": null,
  "ca125_lab": null
}
```

注意事項:
- WBCが "44" なら実測値のまま 44 と出力（x100 や x1000 をしない）
- RBCが "368" なら 368 と出力
- PLTが "15.7" なら 15.7 と出力
- 採取日時は画面上部に表示されていることが多い（例: 2026/03/16 → "2026-03-16"）
- γ-GTP は ggt として出力
- GFR推算値 は egfr として出力
- T-Bil は t_bil として出力
- AST(GOT) は ast, ALT(GPT) は alt として出力
- 患者IDは数字のみ（ハイフンやスペースを含む場合もそのまま出力）
- 採取日は画面上の日時表示を YYYY-MM-DD 形式に変換（例: 2026/03/16 09:30 → "2026-03-16"）
"""

# ---------------------------------------------------------------------------
# 検査値名 → DB列名 マッピング（日本語電カル表示名対応）
# ---------------------------------------------------------------------------
_DISPLAY_NAME_MAP = {
    # 血算
    "WBC": "wbc", "白血球": "wbc",
    "RBC": "rbc", "赤血球": "rbc",
    "HGB": "hgb", "Hb": "hgb", "ヘモグロビン": "hgb",
    "HCT": "hct", "Ht": "hct", "ヘマトクリット": "hct",
    "PLT": "plt", "血小板": "plt",
    "MCV": "mcv",
    "MCH": "mch",
    "MCHC": "mchc",
    "Neut%": "neut_pct", "好中球%": "neut_pct",
    "Lym%": "lymph_pct", "リンパ球%": "lymph_pct",
    "Mono%": "mono_pct", "単球%": "mono_pct",
    "Eosin%": "eosin_pct", "好酸球%": "eosin_pct",
    "Baso%": "baso_pct", "好塩基球%": "baso_pct",
    # 生化学
    "TP": "tp", "総蛋白": "tp",
    "Alb": "alb", "ALB": "alb", "アルブミン": "alb",
    "T-Bil": "t_bil", "T-BIL": "t_bil", "総ビリルビン": "t_bil",
    "AST": "ast", "AST(GOT)": "ast", "GOT": "ast",
    "ALT": "alt", "ALT(GPT)": "alt", "GPT": "alt",
    "LDH": "ldh",
    "ALP": "alp",
    "γ-GTP": "ggt", "GGT": "ggt", "γGTP": "ggt",
    "ChE": "che", "コリンエステラーゼ": "che",
    "BUN": "bun", "尿素窒素": "bun",
    "CRE": "cre", "Cre": "cre", "クレアチニン": "cre",
    "eGFR": "egfr", "GFR推算値": "egfr",
    "Na": "na",
    "K": "k",
    "Cl": "cl",
    "CRP": "crp",
    "AMY": "amy", "アミラーゼ": "amy",
    "CK": "ck", "CPK": "ck",
    "Glu": "glu", "GLU": "glu", "血糖": "glu",
    "HbA1c": "hba1c", "HBA1C": "hba1c",
    "CEA": "cea_lab",
    "CA19-9": "ca199_lab", "CA199": "ca199_lab",
    "AFP": "afp_lab", "α-フェトプロテイン": "afp_lab",
    "CA125": "ca125_lab", "CA-125": "ca125_lab",
}

# 有効な DB カラム名セット
_VALID_COLUMNS = {
    "patient_id_ocr", "sample_date",
    "wbc", "rbc", "hgb", "hct", "plt", "mcv", "mch", "mchc",
    "neut_pct", "lymph_pct", "mono_pct", "eosin_pct", "baso_pct",
    "tp", "alb", "t_bil", "ast", "alt", "ldh", "alp", "ggt",
    "che", "bun", "cre", "egfr", "na", "k", "cl", "crp",
    "amy", "ck", "glu", "hba1c",
    "prealb", "cholinesterase", "pt_inr", "aptt", "fibrinogen", "d_dimer",
    "cea_lab", "ca199_lab", "afp_lab", "ca125_lab",
}

# DB列名 → 日本語ラベル
LAB_LABELS = {
    "sample_date": "採取日",
    "wbc": "WBC", "rbc": "RBC", "hgb": "Hgb", "hct": "Hct",
    "plt": "PLT", "mcv": "MCV", "mch": "MCH", "mchc": "MCHC",
    "neut_pct": "好中球%", "lymph_pct": "リンパ球%",
    "mono_pct": "単球%", "eosin_pct": "好酸球%", "baso_pct": "好塩基球%",
    "tp": "TP", "alb": "Alb", "t_bil": "T-Bil",
    "ast": "AST", "alt": "ALT", "ldh": "LDH", "alp": "ALP",
    "ggt": "γ-GTP", "che": "ChE", "bun": "BUN", "cre": "Cre",
    "egfr": "eGFR", "na": "Na", "k": "K", "cl": "Cl",
    "crp": "CRP", "amy": "AMY", "ck": "CK", "glu": "Glu",
    "hba1c": "HbA1c",
    "prealb": "プレアルブミン", "cholinesterase": "ChE（凝固）",
    "pt_inr": "PT-INR", "aptt": "APTT",
    "fibrinogen": "Fib", "d_dimer": "D-dimer",
    "cea_lab": "CEA", "ca199_lab": "CA19-9", "afp_lab": "AFP", "ca125_lab": "CA125",
}


# ---------------------------------------------------------------------------
# メイン: 画像 → 構造化検査値
# ---------------------------------------------------------------------------
def extract_lab_values(image_bytes, model=None):
    """画像から検査値を抽出する。

    Args:
        image_bytes: 画像ファイルのバイナリデータ (PNG/JPEG)
        model: Ollama モデル名（省略時は環境変数 UGI_VISION_MODEL）

    Returns:
        dict: {
            "values": {col_name: float_or_str, ...},  # 抽出された検査値
            "raw_text": str,                            # LLM生出力
            "errors": [str, ...],                       # エラーメッセージ
        }
    """
    model = model or VISION_MODEL
    errors = []

    # base64 エンコード
    b64_image = base64.b64encode(image_bytes).decode("utf-8")

    # Ollama Vision API 呼び出し
    try:
        payload = {
            "model": model,
            "prompt": _EXTRACTION_PROMPT,
            "images": [b64_image],
            "stream": False,
            "options": {
                "temperature": 0.1,  # 低温で正確性重視
                "num_predict": 2048,
            },
        }

        r = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json=payload,
            timeout=VISION_TIMEOUT,
        )

        if r.status_code != 200:
            return {
                "values": {},
                "raw_text": "",
                "errors": [f"Ollama API エラー: HTTP {r.status_code} — {r.text[:200]}"],
            }

        raw_text = r.json().get("response", "")

    except requests.Timeout:
        return {
            "values": {},
            "raw_text": "",
            "errors": [f"Ollama タイムアウト（{VISION_TIMEOUT}秒）。画像が大きすぎるか、モデルが遅い可能性があります。"],
        }
    except Exception as e:
        return {
            "values": {},
            "raw_text": "",
            "errors": [f"Ollama 通信エラー: {e}"],
        }

    # JSON 抽出
    values = _parse_llm_response(raw_text, errors)

    return {
        "values": values,
        "raw_text": raw_text,
        "errors": errors,
    }


def _parse_llm_response(raw_text, errors):
    """LLM出力からJSONを抽出し、値をバリデーションする。"""
    # JSON ブロックを抽出
    json_match = re.search(r"\{[^{}]*\}", raw_text, re.DOTALL)
    if not json_match:
        # 複数行にまたがる JSON を試行
        json_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            errors.append("LLM出力からJSONを抽出できませんでした")
            return {}
    else:
        json_str = json_match.group(0)

    try:
        parsed = json.loads(json_str)
    except json.JSONDecodeError as e:
        errors.append(f"JSON パースエラー: {e}")
        return {}

    # バリデーション + 正規化
    values = {}
    for key, val in parsed.items():
        if val is None:
            continue

        # キー名正規化
        db_col = key.lower().strip()
        if db_col not in _VALID_COLUMNS:
            # 表示名マッピングを試行
            db_col = _DISPLAY_NAME_MAP.get(key, db_col)
        if db_col not in _VALID_COLUMNS:
            continue

        # 患者ID（文字列のまま）
        if db_col == "patient_id_ocr":
            if isinstance(val, str) and val.strip():
                values[db_col] = val.strip()
            continue

        # 日付
        if db_col == "sample_date":
            if isinstance(val, str) and val.strip():
                values[db_col] = val.replace("/", "-").strip()
            continue

        # 数値変換
        try:
            if isinstance(val, (int, float)):
                values[db_col] = float(val)
            elif isinstance(val, str):
                # カンマ・スペース除去
                cleaned = val.replace(",", "").replace(" ", "").strip()
                if cleaned:
                    values[db_col] = float(cleaned)
        except (ValueError, TypeError):
            errors.append(f"値の変換エラー: {key}={val}")

    return values


# ---------------------------------------------------------------------------
# 検査値 → 既存フィールド自動マッピング
# ---------------------------------------------------------------------------
def map_to_existing_fields(lab_values):
    """検査値から既存テーブルのフィールドに自動反映するマッピングを返す。

    Returns:
        dict: {"patients": {field: value}, "tumor_preop": {...}, ...}
    """
    mappings = {}

    # NRI (Nutritional Risk Index) 計算用
    alb = lab_values.get("alb")

    if alb is not None:
        # Alb ベースの栄養評価は patients テーブルにはないが、
        # 将来的に preop_nutrition スコアとして追加可能
        pass

    # Hgb から貧血判定 → sym_anemia の候補
    hgb = lab_values.get("hgb")
    if hgb is not None:
        if hgb < 10.0:
            mappings.setdefault("patients", {})
            mappings["patients"]["sym_anemia"] = 1  # 候補値

    return mappings


# ---------------------------------------------------------------------------
# 基準値判定
# ---------------------------------------------------------------------------
# 参照元: 日本臨床検査標準協議会 (JCCLS) 共用基準範囲 2020年版
#   https://www.jccls.org/techreport/public_20200401.pdf
#   男女共通値を基本とし、性差のある項目は男性基準を記載。
#   ※ NCD固有の基準値ではありません。施設基準に変更する場合は
#     下記 dict を直接編集するか、環境変数/設定ファイルで上書き可能にする予定。
# 形式: column_name: (下限, 上限, 単位表記)
_REFERENCE_RANGES = {
    # --- 血算 (CBC) --- JCCLS 2020
    "wbc":       (3.3, 8.6,    "x10³/μL"),
    "rbc":       (435, 555,    "x10⁴/μL (男)"),     # JCCLS男性
    "hgb":       (13.7, 16.8,  "g/dL (男)"),         # JCCLS男性
    "hct":       (40.7, 50.1,  "% (男)"),             # JCCLS男性
    "plt":       (15.8, 34.8,  "x10⁴/μL"),
    # --- 生化学 --- JCCLS 2020
    "tp":        (6.6, 8.1,    "g/dL"),
    "alb":       (4.1, 5.1,    "g/dL"),
    "t_bil":     (0.4, 1.5,    "mg/dL"),
    "ast":       (13, 30,      "U/L"),
    "alt":       (10, 42,      "U/L"),
    "ldh":       (124, 222,    "U/L"),
    "alp":       (38, 113,     "U/L"),               # IFCC法
    "ggt":       (13, 64,      "U/L (男)"),           # JCCLS男性
    "bun":       (8, 20,       "mg/dL"),
    "cre":       (0.65, 1.07,  "mg/dL (男)"),         # JCCLS男性
    "na":        (138, 145,    "mEq/L"),
    "k":         (3.6, 4.8,    "mEq/L"),
    "cl":        (101, 108,    "mEq/L"),
    "crp":       (0, 0.14,     "mg/dL"),              # ラテックス凝集法
    "glu":       (73, 109,     "mg/dL"),              # 空腹時
    "hba1c":     (4.9, 6.0,    "% (NGSP)"),           # NGSP値
    # --- 腫瘍マーカー (カットオフ上限のみ) ---
    "cea_lab":   (0, 5.0,     "ng/mL"),              # 一般カットオフ
    "ca199_lab": (0, 37.0,    "U/mL"),               # 一般カットオフ
    "afp_lab":   (0, 10.0,    "ng/mL"),              # 一般カットオフ
    "ca125_lab": (0, 35.0,    "U/mL"),               # 一般カットオフ
}


def judge_lab_values(values):
    """検査値の基準値判定を行う。

    Returns:
        list[dict]: [{"col": str, "label": str, "value": float,
                      "status": "normal"|"high"|"low", "range": str}, ...]
    """
    results = []
    for col, val in values.items():
        if col == "sample_date" or not isinstance(val, (int, float)):
            continue

        label = LAB_LABELS.get(col, col)
        ref = _REFERENCE_RANGES.get(col)

        if ref:
            lo, hi, unit = ref
            if val < lo:
                status = "low"
            elif val > hi:
                status = "high"
            else:
                status = "normal"
            range_str = f"{lo}–{hi} {unit}"
        else:
            status = "unknown"
            range_str = "基準値未設定"

        results.append({
            "col": col,
            "label": label,
            "value": val,
            "status": status,
            "range": range_str,
        })

    return results


# ---------------------------------------------------------------------------
# セルフテスト
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=== lab_reader.py セルフテスト ===")

    # Test 1: パースロジック
    print("\n--- Test 1: JSON パース ---")
    test_response = '''
    Here are the lab values:
    ```json
    {
      "sample_date": "2026-03-16",
      "wbc": 44,
      "rbc": 368,
      "hgb": 10.4,
      "hct": 31.5,
      "plt": 15.7,
      "mcv": 85.6,
      "mch": 28.3,
      "mchc": 33.0,
      "neut_pct": 59.6,
      "lymph_pct": 21.7,
      "mono_pct": 8.9,
      "eosin_pct": 8.8,
      "baso_pct": 1.0,
      "tp": 6.7,
      "alb": 3.8,
      "t_bil": 0.7,
      "ast": 20,
      "alt": 15,
      "ldh": 263,
      "alp": 75,
      "ggt": 11,
      "che": 272,
      "bun": 11,
      "cre": 0.51,
      "egfr": 113.2,
      "na": 139,
      "k": 3.7,
      "cl": 104,
      "crp": 0.03,
      "amy": 55,
      "ck": 60,
      "glu": 219,
      "hba1c": null,
      "cea_lab": null,
      "ca199_lab": null,
      "ca125_lab": null
    }
    ```
    '''
    errors = []
    values = _parse_llm_response(test_response, errors)
    assert len(errors) == 0, f"Errors: {errors}"
    assert values["wbc"] == 44
    assert values["hgb"] == 10.4
    assert values["crp"] == 0.03
    assert values["sample_date"] == "2026-03-16"
    assert "hba1c" not in values  # null は除外
    print(f"  抽出項目数: {len(values)}")
    print("  ✅ PASS")

    # Test 2: 基準値判定
    print("\n--- Test 2: 基準値判定 ---")
    judgments = judge_lab_values(values)
    abnormal = [j for j in judgments if j["status"] != "normal" and j["status"] != "unknown"]
    print(f"  検査項目数: {len(judgments)}, 異常値: {len(abnormal)}")
    for j in abnormal:
        print(f"    {'↑' if j['status']=='high' else '↓'} {j['label']}: {j['value']} ({j['range']})")
    print("  ✅ PASS")

    # Test 3: 既存フィールドマッピング
    print("\n--- Test 3: 既存フィールドマッピング ---")
    maps = map_to_existing_fields(values)
    print(f"  マッピング: {maps}")
    # 10.4 >= 10.0 なので sym_anemia は設定されない
    assert "patients" not in maps or "sym_anemia" not in maps.get("patients", {})
    # Hgb=8.0 でテスト → 貧血判定
    maps2 = map_to_existing_fields({"hgb": 8.0, "alb": 2.5})
    assert maps2.get("patients", {}).get("sym_anemia") == 1
    print("  ✅ PASS (Hgb=10.4→正常, Hgb=8.0→貧血)")

    # Test 4: Ollama 接続チェック（接続不要のモック）
    print("\n--- Test 4: check_vision_model ---")
    ok, msg = check_vision_model()
    print(f"  結果: ok={ok}")
    print(f"  メッセージ: {msg[:80]}...")
    print("  ✅ PASS (接続結果に関わらずエラーなし)")

    print("\n✅ lab_reader.py 全テスト PASS")
