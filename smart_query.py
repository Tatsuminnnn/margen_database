"""
上部消化管グループ 症例登録DB — smart_query.py
自然言語クエリ → SQL変換モジュール（ローカルLLM専用）

対応バックエンド:
  - Ollama  (デフォルト: http://localhost:11434)
  - vLLM    (OpenAI互換エンドポイント)
  - llama.cpp server
  - その他 OpenAI互換 API

使用方法:
    from smart_query import ask, check_llm_connection
"""

import os
import json
import re
import sqlite3
import pandas as pd
import requests
from database import DB_PATH

# ---------------------------------------------------------------------------
# 設定（環境変数 or デフォルト）
# ---------------------------------------------------------------------------
LLM_BACKEND = os.environ.get("UGI_LLM_BACKEND", "ollama")       # ollama / openai_compat
LLM_BASE_URL = os.environ.get("UGI_LLM_URL", "http://localhost:11434")
LLM_MODEL = os.environ.get("UGI_LLM_MODEL", "qwen2.5:7b")      # Ollama モデル名
LLM_TIMEOUT = int(os.environ.get("UGI_LLM_TIMEOUT", "120"))     # 秒

# ---------------------------------------------------------------------------
# DB スキーマ定義（LLM に渡すプロンプト用）
# ---------------------------------------------------------------------------
SCHEMA_DESCRIPTION = """
-- 主要テーブルとカラム（SQLite）

CREATE TABLE patients (
  id INTEGER PRIMARY KEY,
  study_id TEXT,          -- 症例ID (UGI-YYYY-NNN)
  patient_id TEXT,        -- 患者ID ※暗号化
  initials TEXT,          -- イニシャル ※暗号化
  sex INTEGER,            -- 1=男性, 2=女性
  birthdate TEXT,         -- 生年月日 ※暗号化
  surgery_date TEXT,      -- 手術日 (YYYY-MM-DD)
  admission_date TEXT,    -- 入院日
  discharge_date TEXT,    -- 退院日
  disease_class INTEGER,  -- 疾患分類: 1=胃癌, 2=食道癌, 3=食道胃接合部癌, 4=GIST, 5=粘膜下腫瘍(GIST以外), 6=食道裂孔ヘルニア/GERD, 9=その他
  disease_category INTEGER, -- 1=悪性, 2=良性/機能性
  ps INTEGER,             -- PS (0-4)
  asa INTEGER,            -- ASA (1-6)
  height_cm REAL,
  weight_kg REAL,
  bmi REAL,
  data_status TEXT        -- draft/submitted/verified/approved
);

-- ★★★ 重要: surgery テーブルには study_id, surgery_date は存在しない ★★★
-- ★★★ 重要: icu_days, mortality_30d, comp_*, readmission_30d は全て surgery テーブル（patients ではない）★★★
-- 日付は必ず patients.surgery_date を使うこと
CREATE TABLE surgery (
  id INTEGER PRIMARY KEY,
  patient_id INTEGER REFERENCES patients(id),  -- ★ patients.id と JOIN する
  op_surgeon TEXT,          -- 執刀医（名前文字列）
  op_assistant1 TEXT,       -- 第1助手
  op_assistant2 TEXT,       -- 第2助手
  op_scopist TEXT,          -- スコピスト
  op_emergency INTEGER,     -- 0=予定, 1=緊急
  op_approach INTEGER,      -- 1=開腹, 2=腹腔鏡, 3=ロボット, 4=開胸, 5=胸腔鏡
  op_procedure INTEGER,     -- 術式コード（胃: 1=幽門側胃切除, 2=胃全摘, 3=噴門側胃切除, 4=局所切除, 5=PPG, 6=残胃全摘, 7=審査腹腔鏡, 8=バイパス）
  op_dissection INTEGER,    -- 郭清度コード
  op_reconstruction INTEGER,-- 再建法コード
  op_anastomosis_method INTEGER, -- 吻合法コード
  op_time_min INTEGER,      -- 手術時間（分）
  op_console_time_min INTEGER, -- コンソール時間（分、ロボット時）
  op_blood_loss_ml INTEGER, -- 出血量（mL）
  op_transfusion_intra INTEGER, -- 術中輸血
  op_transfusion_post INTEGER,  -- 術後輸血
  op_icu_days INTEGER,
  op_reop_yn INTEGER,       -- 再手術 0=なし, 1=あり
  op_complication_yn INTEGER, -- 合併症 0=なし, 1=あり
  op_cd_grade_max INTEGER,  -- 最大 Clavien-Dindo グレード (1=I, 2=II, 3=IIIa, 4=IIIb, 5=IVa, 6=IVb, 7=V)
  readmission_30d INTEGER,
  mortality_30d INTEGER,    -- 0=なし, 1=あり
  mortality_inhospital INTEGER,
  -- 術後合併症（各 0=なし, 1以上=CDグレード）
  comp_ssi INTEGER, comp_wound_dehiscence INTEGER, comp_intra_abd_abscess INTEGER,
  comp_bleeding INTEGER, comp_ileus INTEGER, comp_dvt_pe INTEGER,
  comp_pneumonia INTEGER, comp_atelectasis INTEGER, comp_uti INTEGER,
  comp_delirium INTEGER, comp_cardiac INTEGER, comp_dge INTEGER,
  comp_perforation INTEGER, comp_cholelithiasis INTEGER,
  comp_anastomotic_leak INTEGER, comp_anastomotic_stricture INTEGER,
  comp_anastomotic_bleeding INTEGER, comp_pancreatic_fistula INTEGER,
  comp_bile_leak INTEGER, comp_duodenal_stump_leak INTEGER,
  comp_rln_palsy INTEGER, comp_chylothorax INTEGER,
  comp_empyema INTEGER, comp_pneumothorax INTEGER,
  comp_ards INTEGER, comp_dic INTEGER, comp_sepsis INTEGER,
  comp_renal_failure INTEGER, comp_hepatic_failure INTEGER
  -- ★★★ このテーブルに study_id, surgery_date は無い ★★★
);

CREATE TABLE tumor_preop (
  patient_id INTEGER REFERENCES patients(id),
  c_depth INTEGER, c_ln_metastasis INTEGER, c_distant_metastasis INTEGER,
  c_stage INTEGER,
  c_tumor_size_major_mm INTEGER,
  preop_alb REAL, preop_hb REAL, preop_crp REAL
);

CREATE TABLE pathology (
  patient_id INTEGER REFERENCES patients(id),
  p_depth INTEGER, p_ln_metastasis INTEGER, p_stage INTEGER,
  p_residual_tumor INTEGER, p_histology1 INTEGER,
  p_ly INTEGER, p_v INTEGER, p_inf INTEGER,
  msi_status INTEGER, her2_status INTEGER, pdl1_cps REAL,
  ebv_status INTEGER
);

CREATE TABLE outcome (
  patient_id INTEGER REFERENCES patients(id),
  vital_status INTEGER,   -- 1=生存, 2=死亡
  recurrence_yn INTEGER,  -- 0=なし, 1=あり
  recurrence_date TEXT,
  last_alive_date TEXT,
  death_date TEXT
);

CREATE TABLE neoadjuvant (
  patient_id INTEGER REFERENCES patients(id),
  nac_yn INTEGER,        -- 0=なし, 1=あり
  nac_regimen INTEGER
);

CREATE TABLE adjuvant_chemo (
  patient_id INTEGER REFERENCES patients(id),
  adj_yn INTEGER,        -- 0=なし, 1=あり
  adj_regimen INTEGER
);
"""

# ---------------------------------------------------------------------------
# システムプロンプト
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = f"""あなたは上部消化管外科の症例データベース専用のSQLアシスタントです。
ユーザーの自然言語の質問を受け取り、以下のスキーマに対する SELECT 文のみを生成してください。

{SCHEMA_DESCRIPTION}

重要ルール:
1. SELECT 文のみ生成すること（INSERT/UPDATE/DELETE/DROP は絶対に生成しない）
2. 個人情報カラム（patient_id, birthdate, initials）は SELECT しない
3. 日付の比較は patients.surgery_date カラム（YYYY-MM-DD 形式）を使用
4. 「昨年」「今年」「最近3ヶ月」等の相対日付は date('now') 関数を使う
5. 合併症率の計算: COUNT(CASE WHEN comp_xxx > 0 THEN 1 END) * 100.0 / COUNT(*)
6. 結果は日本語のカラム別名（AS）で返す
7. study_id は patients テーブルにしかない。他テーブルのクエリでも必ず patients を JOIN して p.study_id を使う
8. JOINは必ず患者IDベース: patients.id = surgery.patient_id, patients.id = tumor_preop.patient_id 等
9. surgery テーブルに study_id, surgery_date は存在しない。日付は常に patients.surgery_date を使う
10. 出力はSQLのみ。説明文は不要。```sql``` マークも不要。

よく使うJOINパターン:
  SELECT p.study_id, s.op_time_min, s.op_blood_loss_ml
  FROM patients p
  JOIN surgery s ON p.id = s.patient_id
  WHERE p.surgery_date BETWEEN '2024-01-01' AND '2024-12-31'

応答は純粋なSQL文1つだけを返してください。"""


# ---------------------------------------------------------------------------
# LLM 接続チェック
# ---------------------------------------------------------------------------
def check_llm_connection():
    """ローカルLLMに接続できるか確認。(ok, message) を返す。"""
    try:
        if LLM_BACKEND == "ollama":
            r = requests.get(f"{LLM_BASE_URL}/api/tags", timeout=5)
            if r.status_code == 200:
                models = [m["name"] for m in r.json().get("models", [])]
                # 完全一致 or タグなし名でマッチ (e.g. "qwen2.5:7b" matches "qwen2.5:7b-instruct-...")
                matched = any(m == LLM_MODEL or m.startswith(LLM_MODEL) for m in models)
                if matched:
                    return True, f"Ollama 接続OK（モデル: {LLM_MODEL}）"
                return False, (
                    f"Ollama に接続できましたが、モデル '{LLM_MODEL}' が見つかりません。\n"
                    f"利用可能: {', '.join(models)}\n"
                    f"`ollama pull {LLM_MODEL}` で取得してください。"
                )
            return False, f"Ollama サーバー応答エラー (HTTP {r.status_code})"
        else:
            # OpenAI互換 (vLLM, llama.cpp 等)
            r = requests.get(f"{LLM_BASE_URL}/v1/models", timeout=5)
            if r.status_code == 200:
                return True, f"LLM サーバー接続OK（{LLM_BASE_URL}）"
            return False, f"LLM サーバー応答エラー (HTTP {r.status_code})"
    except requests.ConnectionError:
        return False, f"LLM サーバー ({LLM_BASE_URL}) に接続できません。サーバーが起動しているか確認してください。"
    except Exception as e:
        return False, f"接続エラー: {e}"


# ---------------------------------------------------------------------------
# LLM 呼び出し
# ---------------------------------------------------------------------------
def _call_llm(user_message: str) -> str:
    """ローカルLLMにリクエストを送り、応答テキストを返す。"""
    if LLM_BACKEND == "ollama":
        url = f"{LLM_BASE_URL}/api/chat"
        payload = {
            "model": LLM_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            "stream": False,
            "options": {
                "temperature": 0.0,
                "num_predict": 1024,
            },
        }
    else:
        # OpenAI互換エンドポイント (vLLM, llama.cpp server 等)
        url = f"{LLM_BASE_URL}/v1/chat/completions"
        payload = {
            "model": LLM_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.0,
            "max_tokens": 1024,
        }

    r = requests.post(url, json=payload, timeout=LLM_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    if LLM_BACKEND == "ollama":
        return data.get("message", {}).get("content", "").strip()
    else:
        return data["choices"][0]["message"]["content"].strip()


# ---------------------------------------------------------------------------
# SQL サニタイズ
# ---------------------------------------------------------------------------
_DANGEROUS_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|REPLACE|ATTACH|DETACH|PRAGMA|VACUUM)\b",
    re.IGNORECASE,
)

def _sanitize_sql(sql: str) -> str:
    """LLM出力からSQLを抽出し、安全性を検証する。"""
    # ```sql ... ``` ブロックがあれば中身だけ取る
    m = re.search(r"```(?:sql)?\s*\n?(.*?)```", sql, re.DOTALL)
    if m:
        sql = m.group(1).strip()

    # 複数文を禁止（セミコロンで分割して最初だけ）
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    if not statements:
        raise ValueError("LLM がSQL文を生成できませんでした。")
    sql = statements[0]

    # 危険なキーワードチェック
    if _DANGEROUS_KEYWORDS.search(sql):
        raise ValueError("安全でないSQL文が検出されました。SELECT 文のみ許可されています。")

    # SELECTで始まるか
    if not sql.strip().upper().startswith("SELECT"):
        raise ValueError("SELECT 文以外は実行できません。")

    # --- LLM がよく間違えるパターンの自動修正 ---
    # s.surgery_date → p.surgery_date (surgery テーブルに surgery_date は無い)
    sql = re.sub(r'\bs\.surgery_date\b', 'p.surgery_date', sql)
    # s.study_id → p.study_id
    sql = re.sub(r'\bs\.study_id\b', 'p.study_id', sql)
    # surgery.surgery_date → patients.surgery_date  (alias なし)
    sql = re.sub(r'\bsurgery\.surgery_date\b', 'patients.surgery_date', sql)
    sql = re.sub(r'\bsurgery\.study_id\b', 'patients.study_id', sql)

    # p.icu_days → s.op_icu_days (icu_days は surgery テーブルの op_icu_days)
    sql = re.sub(r'\bp\.icu_days\b', 's.op_icu_days', sql)
    sql = re.sub(r'\bpatients\.icu_days\b', 'surgery.op_icu_days', sql)
    sql = re.sub(r'\bicu_days\b(?!.*\bAS\b)', 's.op_icu_days', sql)  # 裸の icu_days も修正
    # p.mortality_30d → s.mortality_30d (mortality_30d は surgery テーブル)
    sql = re.sub(r'\bp\.mortality_30d\b', 's.mortality_30d', sql)
    sql = re.sub(r'\bpatients\.mortality_30d\b', 'surgery.mortality_30d', sql)
    # p.op_* → s.op_* (op_ で始まるカラムは全て surgery テーブル)
    sql = re.sub(r'\bp\.(op_\w+)\b', r's.\1', sql)
    sql = re.sub(r'\bpatients\.(op_\w+)\b', r'surgery.\1', sql)
    # p.comp_* → s.comp_* (comp_ で始まる合併症カラムは全て surgery テーブル)
    sql = re.sub(r'\bp\.(comp_\w+)\b', r's.\1', sql)
    sql = re.sub(r'\bpatients\.(comp_\w+)\b', r'surgery.\1', sql)
    # p.readmission_30d → s.readmission_30d
    sql = re.sub(r'\bp\.readmission_30d\b', 's.readmission_30d', sql)
    sql = re.sub(r'\bpatients\.readmission_30d\b', 'surgery.readmission_30d', sql)
    # p.mortality_inhospital → s.mortality_inhospital
    sql = re.sub(r'\bp\.mortality_inhospital\b', 's.mortality_inhospital', sql)
    sql = re.sub(r'\bpatients\.mortality_inhospital\b', 'surgery.mortality_inhospital', sql)

    # FROM surgery だけで patients が無い場合、JOIN を自動挿入
    if re.search(r'\bFROM\s+surgery\b', sql, re.IGNORECASE) and \
       not re.search(r'\bpatients\b', sql, re.IGNORECASE):
        sql = re.sub(
            r'\bFROM\s+surgery\s+(\w+)\b',
            r'FROM patients p JOIN surgery \1 ON p.id = \1.patient_id',
            sql, flags=re.IGNORECASE
        )
        # エイリアスなしの場合
        sql = re.sub(
            r'\bFROM\s+surgery\b(?!\s+\w)',
            'FROM patients p JOIN surgery s ON p.id = s.patient_id',
            sql, flags=re.IGNORECASE
        )

    # 個人情報カラムの参照チェック（SELECT句のみ — WHERE条件での使用は許可しない方が安全）
    pii_pattern = re.compile(r"\bpatient_id\b|\bbirthdate\b|\binitials\b", re.IGNORECASE)
    # SELECT ... FROM の間をチェック
    select_clause = re.match(r"SELECT\s+(.*?)\s+FROM", sql, re.IGNORECASE | re.DOTALL)
    if select_clause:
        sel_text = select_clause.group(1)
        if pii_pattern.search(sel_text):
            raise ValueError("個人情報カラム（patient_id, birthdate, initials）は取得できません。")

    return sql


# ---------------------------------------------------------------------------
# メイン関数
# ---------------------------------------------------------------------------
def ask(question: str) -> dict:
    """
    自然言語の質問をSQLに変換し、実行結果を返す。

    Returns:
        {
            "success": bool,
            "question": str,
            "sql": str or None,
            "dataframe": pd.DataFrame or None,
            "error": str or None,
            "row_count": int,
        }
    """
    result = {
        "success": False,
        "question": question,
        "sql": None,
        "dataframe": None,
        "error": None,
        "row_count": 0,
    }

    try:
        # 1. LLMでSQL生成
        raw_sql = _call_llm(question)
        sql = _sanitize_sql(raw_sql)
        result["sql"] = sql

        # 2. 読み取り専用で実行
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            df = pd.read_sql_query(sql, conn)
            result["dataframe"] = df
            result["row_count"] = len(df)
            result["success"] = True
        finally:
            conn.close()

    except requests.ConnectionError:
        result["error"] = "ローカルLLMサーバーに接続できません。Ollama が起動しているか確認してください。"
    except requests.Timeout:
        result["error"] = f"LLM応答がタイムアウトしました（{LLM_TIMEOUT}秒）。"
    except ValueError as e:
        result["error"] = str(e)
    except sqlite3.OperationalError as e:
        result["error"] = f"SQLの実行に失敗しました: {e}"
    except Exception as e:
        result["error"] = f"予期しないエラー: {e}"

    return result


# ---------------------------------------------------------------------------
# 質問例（UIで表示用）
# ---------------------------------------------------------------------------
EXAMPLE_QUESTIONS = [
    "2024年3月から5月までの手術件数は？",
    "DGE（胃内容排出遅延）の2020年までとそれ以降の合併症率を比較して",
    "〇〇先生の手術時間や出血量などの手術成績をまとめて",
    "執刀医ごとの周術期成績や入院期間、合併症率を比較して",
    "昨年1年間のグループの手術成績をまとめて",
    "最近、縫合不全をおこした症例は誰やったっけ？",
    "ロボット手術の症例数と合併症率は？",
    "胃全摘と幽門側胃切除の出血量を比較して",
]
