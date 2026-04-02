"""
上部消化管グループ 統合症例登録DB — database.py
スキーマ設計書 v2 完全対応版
"""

import sqlite3
import os
import shutil
import hashlib
from datetime import datetime
from contextlib import contextmanager

try:
    import bcrypt
    _HAS_BCRYPT = True
except ImportError:
    _HAS_BCRYPT = False

try:
    from cryptography.fernet import Fernet
    _HAS_FERNET = True
except ImportError:
    _HAS_FERNET = False

DB_PATH = os.environ.get("UGI_DB_PATH", "ugi_database.db")

# ---------------------------------------------------------------------------
# 個人情報カラム暗号化（Fernet / AES-128）
# ---------------------------------------------------------------------------
_ENCRYPTION_KEY = os.environ.get("UGI_DB_ENCRYPTION_KEY", "")
_cipher = None

def _get_cipher():
    """暗号化キーが設定されている場合のみ Fernet cipher を返す。"""
    global _cipher
    if _cipher is not None:
        return _cipher
    if _HAS_FERNET and _ENCRYPTION_KEY:
        try:
            _cipher = Fernet(_ENCRYPTION_KEY.encode() if isinstance(_ENCRYPTION_KEY, str) else _ENCRYPTION_KEY)
        except Exception:
            _cipher = None
    return _cipher

def encrypt_value(plaintext):
    """個人情報を暗号化する。暗号化キー未設定なら平文のまま返す。"""
    if plaintext is None:
        return None
    cipher = _get_cipher()
    if cipher is None:
        return str(plaintext)
    return cipher.encrypt(str(plaintext).encode()).decode()

def decrypt_value(encrypted_text):
    """暗号化された値を復号する。暗号化キー未設定 or 平文なら そのまま返す。"""
    if encrypted_text is None:
        return None
    cipher = _get_cipher()
    if cipher is None:
        return encrypted_text
    try:
        return cipher.decrypt(encrypted_text.encode()).decode()
    except Exception:
        return encrypted_text  # 平文データ（移行前）はそのまま返す

def generate_encryption_key():
    """新しい暗号化キーを生成して表示する（初回セットアップ用）。"""
    if _HAS_FERNET:
        key = Fernet.generate_key().decode()
        print(f"暗号化キー: {key}")
        print("この値を環境変数 UGI_DB_ENCRYPTION_KEY に設定してください。")
        return key
    else:
        print("cryptography パッケージが未インストールです: pip install cryptography")
        return None

# 暗号化対象カラム
ENCRYPTED_COLUMNS = {"patient_id", "birthdate", "initials"}


# ---------------------------------------------------------------------------
# DB接続
# ---------------------------------------------------------------------------
@contextmanager
def get_db():
    """SQLite接続のコンテキストマネージャ。WALモード + FK有効。"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 認証ヘルパー（bcrypt 対応・SHA-256 からの自動移行付き）
# ---------------------------------------------------------------------------
def hash_password(pw: str) -> str:
    """パスワードをハッシュ化する。bcrypt が利用可能ならbcrypt、なければSHA-256。"""
    if _HAS_BCRYPT:
        return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()
    return hashlib.sha256(pw.encode()).hexdigest()


def _verify_password(password: str, stored_hash: str) -> bool:
    """保存されたハッシュと照合する。bcrypt / SHA-256 両方に対応。"""
    if _HAS_BCRYPT and stored_hash.startswith("$2"):
        # bcrypt ハッシュ
        return bcrypt.checkpw(password.encode(), stored_hash.encode())
    # SHA-256 (旧形式) フォールバック
    return hashlib.sha256(password.encode()).hexdigest() == stored_hash


# ---------------------------------------------------------------------------
# パスワード強度チェック
# ---------------------------------------------------------------------------
import re as _re

# 最小要件: 8文字以上、大文字・小文字・数字・記号のうち3種以上
PASSWORD_MIN_LENGTH = 8
PASSWORD_MIN_CATEGORIES = 3

def validate_password_strength(password: str) -> tuple:
    """パスワード強度を検証する。

    Returns:
        (is_valid: bool, messages: list[str])
        is_valid=False の場合、messages に不備の理由が入る。
    """
    messages = []

    if len(password) < PASSWORD_MIN_LENGTH:
        messages.append(f"パスワードは{PASSWORD_MIN_LENGTH}文字以上にしてください")

    categories = 0
    if _re.search(r'[a-z]', password):
        categories += 1
    if _re.search(r'[A-Z]', password):
        categories += 1
    if _re.search(r'[0-9]', password):
        categories += 1
    if _re.search(r'[^a-zA-Z0-9]', password):
        categories += 1

    if categories < PASSWORD_MIN_CATEGORIES:
        messages.append(
            f"大文字・小文字・数字・記号のうち{PASSWORD_MIN_CATEGORIES}種類以上を含めてください"
            f"（現在{categories}種類）"
        )

    # よくある弱いパスワードのブロック
    weak_passwords = {"password", "admin", "12345678", "password1", "admin123",
                      "qwerty", "letmein", "welcome", "abc12345"}
    if password.lower() in weak_passwords:
        messages.append("よくある弱いパスワードは使用できません")

    return len(messages) == 0, messages


# ---------------------------------------------------------------------------
# ログイン試行制限
# ---------------------------------------------------------------------------
# {username: [(timestamp, success), ...]} — メモリ内で管理
_login_attempts: dict = {}
LOGIN_MAX_ATTEMPTS = 5       # 最大試行回数
LOGIN_LOCKOUT_SECONDS = 300  # ロックアウト時間（5分）


def _record_login_attempt(username: str, success: bool):
    """ログイン試行を記録する。"""
    now = datetime.now()
    if username not in _login_attempts:
        _login_attempts[username] = []
    _login_attempts[username].append((now, success))
    # 古い記録を削除（ロックアウト期間を超えたもの）
    cutoff = now - __import__('datetime').timedelta(seconds=LOGIN_LOCKOUT_SECONDS)
    _login_attempts[username] = [
        (ts, s) for ts, s in _login_attempts[username] if ts > cutoff
    ]


def _is_locked_out(username: str) -> tuple:
    """ロックアウト中か判定する。

    Returns:
        (is_locked: bool, remaining_seconds: int)
    """
    if username not in _login_attempts:
        return False, 0
    now = datetime.now()
    cutoff = now - __import__('datetime').timedelta(seconds=LOGIN_LOCKOUT_SECONDS)
    recent = [(ts, s) for ts, s in _login_attempts[username] if ts > cutoff]
    failed = [ts for ts, s in recent if not s]
    if len(failed) >= LOGIN_MAX_ATTEMPTS:
        latest_fail = max(failed)
        remaining = LOGIN_LOCKOUT_SECONDS - (now - latest_fail).total_seconds()
        if remaining > 0:
            return True, int(remaining)
    return False, 0


def authenticate(username: str, password: str):
    """認証を行う。ログイン試行制限付き。

    Returns:
        dict (成功時) / None (失敗時) / "locked" (ロックアウト中)
    """
    # ロックアウト判定
    locked, remaining = _is_locked_out(username)
    if locked:
        return "locked"

    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE username=? AND is_active=1",
            (username,),
        ).fetchone()
        if row and _verify_password(password, row["password_hash"]):
            _record_login_attempt(username, True)
            conn.execute(
                "UPDATE users SET last_login=? WHERE id=?",
                (datetime.now().isoformat(), row["id"]),
            )
            # SHA-256 → bcrypt 自動移行
            if _HAS_BCRYPT and not row["password_hash"].startswith("$2"):
                new_hash = hash_password(password)
                conn.execute(
                    "UPDATE users SET password_hash=? WHERE id=?",
                    (new_hash, row["id"]),
                )
            return dict(row)

    _record_login_attempt(username, False)
    return None


# ---------------------------------------------------------------------------
# study_id 生成
# ---------------------------------------------------------------------------
def generate_study_id():
    year = datetime.now().year
    prefix = f"UGI-{year}-"
    with get_db() as conn:
        row = conn.execute(
            "SELECT study_id FROM patients WHERE study_id LIKE ? ORDER BY study_id DESC LIMIT 1",
            (f"{prefix}%",),
        ).fetchone()
        if row:
            last_num = int(row["study_id"].split("-")[-1])
            return f"{prefix}{last_num + 1:04d}"
        return f"{prefix}0001"


# ---------------------------------------------------------------------------
# 監査ログ
# ---------------------------------------------------------------------------
def log_audit(conn, user_id, action, table_name=None, record_id=None,
              field_name=None, old_value=None, new_value=None, ip_address=None,
              phase=None, comment=None, export_filter=None, export_count=None):
    conn.execute(
        """INSERT INTO audit_log
           (user_id, action, table_name, record_id, field_name,
            old_value, new_value, ip_address, phase, comment,
            export_filter, export_count)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (user_id, action, table_name, record_id,
         field_name, str(old_value) if old_value is not None else None,
         str(new_value) if new_value is not None else None, ip_address,
         phase, comment, export_filter, export_count),
    )


# ---------------------------------------------------------------------------
# スキーマ定義
# ---------------------------------------------------------------------------
def init_db():
    """全テーブルを作成し、デフォルト管理者を登録する。"""
    with get_db() as conn:
        # ==============================================================
        # 1. classification_versions（規約バージョン管理）
        # ==============================================================
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS classification_versions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL,
            version         TEXT NOT NULL,
            effective_date  TEXT,
            is_active       INTEGER DEFAULT 0,
            notes           TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name, version)
        );

        -- ==============================================================
        -- 2. codebook（選択肢マスタ）
        -- ==============================================================
        CREATE TABLE IF NOT EXISTS codebook (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id       INTEGER REFERENCES classification_versions(id),
            field_name       TEXT NOT NULL,
            code             INTEGER NOT NULL,
            label            TEXT NOT NULL,
            label_en         TEXT,
            sort_order       INTEGER DEFAULT 0,
            is_active        INTEGER DEFAULT 1,
            ncd_mapping      TEXT,
            registry_mapping TEXT,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(version_id, field_name, code)
        );
        CREATE INDEX IF NOT EXISTS idx_codebook_field
            ON codebook(version_id, field_name);

        -- ==============================================================
        -- 3. users（ユーザー管理）
        -- ==============================================================
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            display_name  TEXT NOT NULL,
            role          TEXT NOT NULL DEFAULT 'entry',
            is_active     INTEGER DEFAULT 1,
            last_login    TIMESTAMP,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # ==============================================================
        # 4. patients（患者基本情報 — 中心テーブル）
        # ==============================================================
        conn.execute("""
        CREATE TABLE IF NOT EXISTS patients (
            id                              INTEGER PRIMARY KEY AUTOINCREMENT,
            study_id                        TEXT UNIQUE NOT NULL,
            patient_id                      TEXT,
            initials                        TEXT,
            sex                             INTEGER,
            birthdate                       TEXT,
            first_visit_date                TEXT,
            admission_date                  TEXT,
            surgery_date                    TEXT,
            discharge_date                  TEXT,
            height_cm                       REAL,
            weight_admission                REAL,
            weight_discharge                REAL,
            smoking                         INTEGER,
            alcohol                         INTEGER,
            ps                              INTEGER,
            asa                             INTEGER,
            adl_status                      INTEGER,
            preop_weight_loss_10pct         INTEGER,
            hp_eradication                  INTEGER,
            discharge_destination           INTEGER,
            first_treatment_completion_date TEXT,
            ncd_case_id                     TEXT,
            disease_category                INTEGER DEFAULT 1,  -- 1=胃癌, 2=食道癌
            disease_class                   INTEGER,  -- 胃癌分類 (1-9)
            classification_version_id       INTEGER REFERENCES classification_versions(id),
            data_status                     TEXT DEFAULT 'draft',
            facility_id                     INTEGER DEFAULT 1,  -- 多施設対応用
            is_deleted                      INTEGER DEFAULT 0,  -- 論理削除フラグ
            -- Phase承認フロー (Phase1=周術期, Phase3=術後3年, Phase4=術後5年)
            phase1_status       TEXT DEFAULT 'draft',
            phase1_submitted_at TIMESTAMP,
            phase1_submitted_by INTEGER REFERENCES users(id),
            phase1_approved_at  TIMESTAMP,
            phase1_approved_by  INTEGER REFERENCES users(id),
            phase3_status       TEXT DEFAULT 'draft',
            phase3_submitted_at TIMESTAMP,
            phase3_submitted_by INTEGER REFERENCES users(id),
            phase3_approved_at  TIMESTAMP,
            phase3_approved_by  INTEGER REFERENCES users(id),
            phase4_status       TEXT DEFAULT 'draft',
            phase4_submitted_at TIMESTAMP,
            phase4_submitted_by INTEGER REFERENCES users(id),
            phase4_approved_at  TIMESTAMP,
            phase4_approved_by  INTEGER REFERENCES users(id),
            created_by                      INTEGER REFERENCES users(id),
            created_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by                      INTEGER REFERENCES users(id),
            updated_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- 症状フラグ (sym_)
            sym_asymptomatic        INTEGER DEFAULT 0,
            sym_epigastric_pain     INTEGER DEFAULT 0,
            sym_dysphagia           INTEGER DEFAULT 0,
            sym_weight_loss         INTEGER DEFAULT 0,
            sym_anemia              INTEGER DEFAULT 0,
            sym_melena              INTEGER DEFAULT 0,
            sym_hematemesis         INTEGER DEFAULT 0,
            sym_nausea_vomiting     INTEGER DEFAULT 0,
            sym_abdominal_distension INTEGER DEFAULT 0,
            sym_obstruction         INTEGER DEFAULT 0,
            sym_other               INTEGER DEFAULT 0,
            sym_other_detail        TEXT,
            sym_confirmed           INTEGER DEFAULT 0,

            -- 併存疾患フラグ (comor_)
            comor_hypertension      INTEGER DEFAULT 0,  -- 0=なし,1=未治療,2=治療中,8=詳細不明
            comor_cardiovascular    INTEGER DEFAULT 0,  -- 包括フラグ (0/1)
            comor_cerebrovascular   INTEGER DEFAULT 0,  -- 包括フラグ (0/1)
            comor_respiratory       INTEGER DEFAULT 0,  -- COPD (0/1)
            comor_renal             INTEGER DEFAULT 0,
            comor_renal_dialysis    INTEGER DEFAULT 0,
            comor_hepatic           INTEGER DEFAULT 0,  -- 包括フラグ (0/1)
            comor_diabetes          INTEGER DEFAULT 0,  -- 0=なし,1=食事療法,2=経口薬,3=インスリン,4=インスリン+経口薬,8=詳細不明
            comor_endocrine         INTEGER DEFAULT 0,
            comor_collagen          INTEGER DEFAULT 0,
            comor_hematologic       INTEGER DEFAULT 0,
            comor_neurologic        INTEGER DEFAULT 0,
            comor_psychiatric       INTEGER DEFAULT 0,
            comor_other             INTEGER DEFAULT 0,
            comor_other_detail      TEXT,
            comor_confirmed         INTEGER DEFAULT 0,
            -- NCD併存疾患サブカラム: 心血管系 (comor_cardiovascular=1のとき展開)
            comor_ihd               INTEGER DEFAULT 0,  -- 虚血性心疾患
            comor_chf               INTEGER DEFAULT 0,  -- 心不全
            comor_arrhythmia        INTEGER DEFAULT 0,  -- 不整脈
            comor_valvular          INTEGER DEFAULT 0,  -- 弁膜症
            comor_aortic            INTEGER DEFAULT 0,  -- 大動脈疾患
            comor_pvd               INTEGER DEFAULT 0,  -- 末梢血管疾患
            -- NCD併存疾患サブカラム: 脳血管障害 (comor_cerebrovascular=1のとき展開)
            comor_cerebral_infarction  INTEGER DEFAULT 0,  -- 脳梗塞
            comor_cerebral_hemorrhage  INTEGER DEFAULT 0,  -- 脳出血
            comor_tia               INTEGER DEFAULT 0,  -- TIA
            comor_sah               INTEGER DEFAULT 0,  -- くも膜下出血
            -- NCD併存疾患サブカラム: 肝疾患 (comor_hepatic=1のとき展開)
            comor_cirrhosis         INTEGER DEFAULT 0,  -- 0=なし,1=ChildA,2=ChildB,3=ChildC
            comor_portal_htn        INTEGER DEFAULT 0,  -- 門脈圧亢進
            comor_hepatitis_virus   INTEGER DEFAULT 0,  -- 0=なし,1=HBV,2=HCV,3=両方
            -- NCD併存疾患サブカラム: 呼吸器 (comor_respiratory=1のとき展開)
            comor_dyspnea           INTEGER DEFAULT 0,  -- 呼吸困難 (旧・後方互換)
            comor_ventilator        INTEGER DEFAULT 0,  -- 人工呼吸器使用 (旧・後方互換)
            comor_copd              INTEGER DEFAULT 0,  -- COPD
            comor_ild               INTEGER DEFAULT 0,  -- 間質性肺炎
            comor_asthma            INTEGER DEFAULT 0,  -- 喘息
            comor_resp_unknown      INTEGER DEFAULT 0,  -- 呼吸器その他・詳細不明
            -- NCD併存疾患サブカラム: 心血管 追加 (v7.0)
            comor_mi                INTEGER DEFAULT 0,  -- 心筋梗塞
            comor_angina            INTEGER DEFAULT 0,  -- 狭心症
            comor_structural_vascular INTEGER DEFAULT 0, -- 器質的血管疾患
            comor_cv_unknown        INTEGER DEFAULT 0,  -- 心疾患詳細不明
            -- NCD併存疾患サブカラム: 脳血管 追加 (v7.0)
            comor_ci_no_sequela     INTEGER DEFAULT 0,  -- 脳梗塞（TIAまたは後遺症なし）
            comor_ci_with_sequela   INTEGER DEFAULT 0,  -- 脳梗塞（後遺症あり）
            comor_cerebro_unknown   INTEGER DEFAULT 0,  -- 脳血管障害詳細不明
            -- NCD併存疾患サブカラム: 精神疾患 (v7.0)
            comor_dementia          INTEGER DEFAULT 0,  -- 認知症
            comor_mood_disorder     INTEGER DEFAULT 0,  -- 気分障害
            comor_schizophrenia     INTEGER DEFAULT 0,  -- 統合失調症
            comor_developmental     INTEGER DEFAULT 0,  -- 発達知的障害
            comor_psy_unknown       INTEGER DEFAULT 0,  -- 精神疾患詳細不明
            -- 併存疾患統合: 手術既往・術前人工呼吸器 (v7.1)
            comor_prior_cardiac_surgery   INTEGER DEFAULT 0,  -- 心臓外科手術既往
            comor_prior_abdominal_surgery INTEGER DEFAULT 0,  -- 腹部手術既往
            comor_preop_ventilator        INTEGER DEFAULT 0,  -- 術前人工呼吸器管理下（CPAP除く）
            -- NCD併存疾患サブカラム: 高血圧 (comor_hypertension=1のとき展開)
            comor_ht_treatment      INTEGER,  -- 1=未治療,2=治療中,8=詳細不明
            -- NCD併存疾患サブカラム: 糖尿病 (comor_diabetes=1のとき展開)
            comor_dm_treatment      INTEGER,  -- 0=--,1=食事療法のみ,2=内服治療,3=インスリン(＋内服),4=未治療
            -- 喫煙サブカラム (smoking≠0のとき展開)
            smoking_type            INTEGER,  -- 0=紙巻,1=加熱式,2=両方
            smoking_bi              INTEGER,  -- BI指数
            -- ADLサブカラム
            adl_status_preop        INTEGER,  -- 術直前ADL (adl_statusと同選択肢)
            -- CRF追加: 手術既往・NCD共通
            prior_abdominal_surgery_yn      INTEGER,  -- 腹部手術既往: 0=なし,1=あり
            prior_abdominal_surgery_detail  TEXT,     -- 腹部手術既往の詳細（フリーテキスト）
            prior_cardiac_surgery_yn        INTEGER,  -- 心臓外科手術既往: 0=なし,1=あり
            preop_ventilator_yn             INTEGER,  -- 術前人工呼吸器管理下: 0=なし,1=あり（CPAP除く）
            legal_capacity_admission        INTEGER,  -- 入院時法的判断能力: 0=代理人同意,1=患者自身同意
            emergency_transport_yn      INTEGER,  -- 緊急搬送の有無: 0=なし,1=あり（NCD共通）
            consent_refusal_date        TEXT,     -- NCD拒否受付日
            anesthesiologist_yn         INTEGER,  -- 麻酔科医の関与: 0=なし,1=あり (※surgery側にも可)

            -- 内服薬フラグ (med_)
            -- ※ med_antihypertensive, med_oral_hypoglycemic, med_insulin は
            --   v7.1 で併存疾患サブ(comor_ht_treatment, comor_dm_treatment)に統合し廃止
            med_antithrombotic      INTEGER DEFAULT 0,
            med_steroid_immunosup   INTEGER DEFAULT 0,
            med_antineoplastic      INTEGER DEFAULT 0,
            med_thyroid             INTEGER DEFAULT 0,
            med_psychotropic        INTEGER DEFAULT 0,
            med_other               INTEGER DEFAULT 0,
            med_other_detail        TEXT,
            med_confirmed           INTEGER DEFAULT 0,

            -- 癌家族歴フラグ (fhx_)
            fhx_gastric             INTEGER DEFAULT 0,
            fhx_esophageal          INTEGER DEFAULT 0,
            fhx_colorectal          INTEGER DEFAULT 0,
            fhx_lung                INTEGER DEFAULT 0,
            fhx_liver               INTEGER DEFAULT 0,
            fhx_pancreas            INTEGER DEFAULT 0,
            fhx_breast              INTEGER DEFAULT 0,
            fhx_other               INTEGER DEFAULT 0,
            fhx_other_detail        TEXT,
            fhx_confirmed           INTEGER DEFAULT 0,

            -- 重複癌
            synchronous_cancer_yn       INTEGER,
            synchronous_cancer_organ    INTEGER,
            synchronous_cancer_other    TEXT,
            metachronous_cancer_yn      INTEGER,
            metachronous_cancer_organ   INTEGER,
            metachronous_cancer_other   TEXT,

            -- 重複癌 flag_group 形式 (v2.3)
            sync_org_oral_pharynx   INTEGER DEFAULT 0,
            sync_org_esophagus      INTEGER DEFAULT 0,
            sync_org_stomach        INTEGER DEFAULT 0,
            sync_org_colorectum     INTEGER DEFAULT 0,
            sync_org_lung           INTEGER DEFAULT 0,
            sync_org_hepatobiliary  INTEGER DEFAULT 0,
            sync_org_pancreas       INTEGER DEFAULT 0,
            sync_org_breast         INTEGER DEFAULT 0,
            sync_org_urological     INTEGER DEFAULT 0,
            sync_org_gynecological  INTEGER DEFAULT 0,
            sync_org_thyroid        INTEGER DEFAULT 0,
            sync_org_nervous_system INTEGER DEFAULT 0,
            sync_org_hematologic    INTEGER DEFAULT 0,
            sync_org_skin           INTEGER DEFAULT 0,
            sync_org_other          INTEGER DEFAULT 0,
            sync_org_confirmed      INTEGER DEFAULT 0,

            meta_org_oral_pharynx   INTEGER DEFAULT 0,
            meta_org_esophagus      INTEGER DEFAULT 0,
            meta_org_stomach        INTEGER DEFAULT 0,
            meta_org_colorectum     INTEGER DEFAULT 0,
            meta_org_lung           INTEGER DEFAULT 0,
            meta_org_hepatobiliary  INTEGER DEFAULT 0,
            meta_org_pancreas       INTEGER DEFAULT 0,
            meta_org_breast         INTEGER DEFAULT 0,
            meta_org_urological     INTEGER DEFAULT 0,
            meta_org_gynecological  INTEGER DEFAULT 0,
            meta_org_thyroid        INTEGER DEFAULT 0,
            meta_org_nervous_system INTEGER DEFAULT 0,
            meta_org_hematologic    INTEGER DEFAULT 0,
            meta_org_skin           INTEGER DEFAULT 0,
            meta_org_other          INTEGER DEFAULT 0,
            meta_org_confirmed      INTEGER DEFAULT 0
        )""")
        for _idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_patients_study ON patients(study_id)",
            "CREATE INDEX IF NOT EXISTS idx_patients_surgery ON patients(surgery_date)",
            "CREATE INDEX IF NOT EXISTS idx_patients_status ON patients(data_status)",
            "CREATE INDEX IF NOT EXISTS idx_patients_disease ON patients(disease_class)",
        ]:
            try:
                conn.execute(_idx_sql)
            except Exception:
                pass  # マイグレーション後に再試行

        # ==============================================================
        # 5. tumor_preop（腫瘍情報・術前検査）
        # ==============================================================
        conn.execute("""
        CREATE TABLE IF NOT EXISTS tumor_preop (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id              INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            disease_class_other     TEXT,
            remnant_stomach_yn      INTEGER,
            remnant_initial_disease INTEGER,
            remnant_initial_disease_other TEXT,
            remnant_interval_years  REAL,
            remnant_location        INTEGER,
            c_tumor_number          INTEGER,
            c_location_long         TEXT,
            c_location_short        TEXT,
            c_location_egj          INTEGER,
            c_egj_distance_mm       INTEGER,
            c_esophageal_invasion_mm INTEGER,
            c_macroscopic_type      INTEGER,
            c_type0_subclass        TEXT,    -- 複数選択: カンマ区切り (例: "1,4")
            c_tumor_size_major_mm   INTEGER,
            c_tumor_size_minor_mm   INTEGER,
            c_histology1            INTEGER,
            c_histology2            INTEGER,
            c_histology3            INTEGER,
            c_depth                 INTEGER,
            c_ln_metastasis         INTEGER,
            c_distant_metastasis    INTEGER,
            c_peritoneal            INTEGER,
            c_liver_metastasis      INTEGER,
            c_stage                 INTEGER,
            preop_alb               REAL,
            preop_hb                REAL,
            preop_wbc               REAL,
            preop_plt               REAL,
            preop_crp               REAL,
            preop_cea               REAL,
            preop_ca199             REAL,
            preop_cr                REAL,
            preop_tbil              REAL,
            preop_hba1c             REAL,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- 臨床的浸潤臓器フラグ (c_inv_)
            c_inv_pancreas              INTEGER DEFAULT 0,
            c_inv_liver                 INTEGER DEFAULT 0,
            c_inv_transverse_colon      INTEGER DEFAULT 0,
            c_inv_spleen                INTEGER DEFAULT 0,
            c_inv_diaphragm             INTEGER DEFAULT 0,
            c_inv_esophagus             INTEGER DEFAULT 0,
            c_inv_duodenum              INTEGER DEFAULT 0,
            c_inv_aorta                 INTEGER DEFAULT 0,
            c_inv_abdominal_wall        INTEGER DEFAULT 0,
            c_inv_adrenal               INTEGER DEFAULT 0,
            c_inv_kidney                INTEGER DEFAULT 0,
            c_inv_small_intestine       INTEGER DEFAULT 0,
            c_inv_retroperitoneum       INTEGER DEFAULT 0,
            c_inv_transverse_mesocolon  INTEGER DEFAULT 0,
            c_inv_other                 INTEGER DEFAULT 0,
            c_inv_other_detail          TEXT,
            c_inv_confirmed             INTEGER DEFAULT 0,

            -- 臨床的遠隔転移部位フラグ (c_meta_)
            c_meta_peritoneal       INTEGER DEFAULT 0,
            c_meta_liver            INTEGER DEFAULT 0,
            c_meta_lung             INTEGER DEFAULT 0,
            c_meta_lymph_node       INTEGER DEFAULT 0,
            c_meta_bone             INTEGER DEFAULT 0,
            c_meta_brain            INTEGER DEFAULT 0,
            c_meta_ovary            INTEGER DEFAULT 0,
            c_meta_adrenal          INTEGER DEFAULT 0,
            c_meta_pleura           INTEGER DEFAULT 0,
            c_meta_skin             INTEGER DEFAULT 0,
            c_meta_marrow           INTEGER DEFAULT 0,
            c_meta_meninges         INTEGER DEFAULT 0,
            c_meta_other            INTEGER DEFAULT 0,
            c_meta_other_detail     TEXT,
            c_meta_confirmed        INTEGER DEFAULT 0
        )""")

        # ==============================================================
        # 6. neoadjuvant（術前療法）
        # ==============================================================
        conn.execute("""
        CREATE TABLE IF NOT EXISTS neoadjuvant (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id                  INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            nac_yn                      INTEGER,
            nac_regimen                 INTEGER,
            nac_regimen_other           TEXT,
            nac_start_date              TEXT,
            nac_courses                 INTEGER,
            nac_completion              INTEGER,
            nac_adverse_event           TEXT,
            recist_target1              TEXT,
            recist_target2              TEXT,
            recist_target3              TEXT,
            recist_shrinkage_pct        REAL,
            recist_target_response      INTEGER,
            recist_nontarget1           TEXT,
            recist_nontarget2           TEXT,
            recist_nontarget3           TEXT,
            recist_nontarget_response   INTEGER,
            recist_new_lesion           INTEGER,
            recist_new_lesion_detail    TEXT,
            recist_overall              INTEGER,
            primary_shrinkage_pct       REAL,
            primary_elevation           INTEGER,
            primary_depression          INTEGER,
            primary_stenosis            INTEGER,
            primary_overall_response    INTEGER,
            updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # ==============================================================
        # 7. surgery（手術情報）
        # ==============================================================
        conn.execute("""
        CREATE TABLE IF NOT EXISTS surgery (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id                  INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            op_emergency                INTEGER,
            op_anesthesia_type          INTEGER,  -- 旧・後方互換（単一選択）
            anest_general               INTEGER DEFAULT 0,  -- 全身麻酔
            anest_epidural              INTEGER DEFAULT 0,  -- 硬膜外麻酔
            anest_ivpca                 INTEGER DEFAULT 0,  -- IVPCA
            anest_spinal                INTEGER DEFAULT 0,  -- 脊椎麻酔
            anest_local                 INTEGER DEFAULT 0,  -- 局所麻酔
            anest_anest_other           INTEGER DEFAULT 0,  -- その他の麻酔
            op_approach                 INTEGER,
            op_completion               INTEGER,
            op_conversion_yn            INTEGER,
            op_procedure                INTEGER,
            op_procedure_other          TEXT,
            op_dissection               INTEGER,
            op_reconstruction           INTEGER,
            op_reconstruction_other     TEXT,
            op_anastomosis_method       INTEGER,
            op_anastomosis_method_other TEXT,
            op_peristalsis_direction    INTEGER,
            op_reconstruction_route     INTEGER,
            op_time_min                 INTEGER,
            op_console_time_min         INTEGER,
            op_blood_loss_ml            INTEGER,
            op_surgeon                  TEXT,
            op_assistant1               TEXT,
            op_assistant2               TEXT,
            op_scopist                  TEXT,
            op_transfusion_preop        INTEGER,  -- 術前72h以内輸血 (0/1) NCD項目
            op_transfusion_preop_rbc    INTEGER,  -- 術前RBC (単位数)
            op_transfusion_preop_ffp    INTEGER,  -- 術前FFP (単位数)
            op_transfusion_preop_pc     INTEGER,  -- 術前PC (単位数)
            op_transfusion_intra        INTEGER,  -- 包括フラグ (0/1)
            op_transfusion_intra_rbc    INTEGER,  -- 術中RBC (単位数)
            op_transfusion_intra_ffp    INTEGER,  -- 術中FFP (単位数)
            op_transfusion_intra_pc     INTEGER,  -- 術中PC (単位数)
            op_transfusion_post         INTEGER,  -- 包括フラグ (0/1)
            op_transfusion_post_rbc     INTEGER,  -- 術後RBC (単位数)
            op_transfusion_post_ffp     INTEGER,  -- 術後FFP (単位数)
            op_transfusion_post_pc      INTEGER,  -- 術後PC (単位数)
            op_icu_days                 INTEGER,
            op_reop_yn                  INTEGER,
            op_reop_30d                 INTEGER,
            readmission_30d             INTEGER,
            readmission_30d_reason      TEXT,
            op_complication_yn          INTEGER,
            op_cd_grade_max             INTEGER,
            updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- 合併切除臓器フラグ (comb_)
            comb_yn                     INTEGER,           -- 0=なし,1=あり
            comb_splenectomy            INTEGER DEFAULT 0,
            comb_cholecystectomy        INTEGER DEFAULT 0,
            comb_distal_pancreatectomy  INTEGER DEFAULT 0,
            comb_transverse_colectomy   INTEGER DEFAULT 0,
            comb_partial_hepatectomy    INTEGER DEFAULT 0,
            comb_diaphragm              INTEGER DEFAULT 0,
            comb_adrenalectomy          INTEGER DEFAULT 0,
            comb_pancreatoduodenectomy  INTEGER DEFAULT 0,
            comb_appleby                INTEGER DEFAULT 0,
            comb_portal_vein            INTEGER DEFAULT 0,
            comb_ovary                  INTEGER DEFAULT 0,
            comb_small_intestine        INTEGER DEFAULT 0,
            comb_abdominal_wall         INTEGER DEFAULT 0,
            comb_thoracic_esophagus     INTEGER DEFAULT 0,
            comb_transverse_mesocolon   INTEGER DEFAULT 0,
            comb_kidney                 INTEGER DEFAULT 0,
            comb_other                  INTEGER DEFAULT 0,
            comb_other_detail           TEXT,
            comb_confirmed              INTEGER DEFAULT 0,

            -- 術後合併症 (comp_) CDグレード(0=なし,1-7) + 発症日 + 処置内容
            comp_ssi                    INTEGER DEFAULT 0,
            comp_ssi_date               TEXT,
            comp_ssi_tx                 TEXT,
            comp_wound_dehiscence       INTEGER DEFAULT 0,
            comp_wound_dehiscence_date  TEXT,
            comp_wound_dehiscence_tx    TEXT,
            comp_intra_abd_abscess      INTEGER DEFAULT 0,
            comp_intra_abd_abscess_date TEXT,
            comp_intra_abd_abscess_tx   TEXT,
            comp_bleeding               INTEGER DEFAULT 0,
            comp_bleeding_date          TEXT,
            comp_bleeding_tx            TEXT,
            comp_ileus                  INTEGER DEFAULT 0,
            comp_ileus_date             TEXT,
            comp_ileus_tx               TEXT,
            comp_dvt_pe                 INTEGER DEFAULT 0,
            comp_dvt_pe_date            TEXT,
            comp_dvt_pe_tx              TEXT,
            comp_pneumonia              INTEGER DEFAULT 0,
            comp_pneumonia_date         TEXT,
            comp_pneumonia_tx           TEXT,
            comp_atelectasis            INTEGER DEFAULT 0,
            comp_atelectasis_date       TEXT,
            comp_atelectasis_tx         TEXT,
            comp_uti                    INTEGER DEFAULT 0,
            comp_uti_date               TEXT,
            comp_uti_tx                 TEXT,
            comp_delirium               INTEGER DEFAULT 0,
            comp_delirium_date          TEXT,
            comp_delirium_tx            TEXT,
            comp_cardiac                INTEGER DEFAULT 0,
            comp_cardiac_date           TEXT,
            comp_cardiac_tx             TEXT,
            comp_dge                    INTEGER DEFAULT 0,
            comp_dge_date               TEXT,
            comp_dge_tx                 TEXT,
            comp_perforation            INTEGER DEFAULT 0,
            comp_perforation_date       TEXT,
            comp_perforation_tx         TEXT,
            comp_cholelithiasis         INTEGER DEFAULT 0,
            comp_cholelithiasis_date    TEXT,
            comp_cholelithiasis_tx      TEXT,
            comp_anastomotic_leak       INTEGER DEFAULT 0,
            comp_anastomotic_leak_date  TEXT,
            comp_anastomotic_leak_tx    TEXT,
            comp_anastomotic_stricture  INTEGER DEFAULT 0,
            comp_anastomotic_stricture_date TEXT,
            comp_anastomotic_stricture_tx TEXT,
            comp_anastomotic_bleeding   INTEGER DEFAULT 0,
            comp_anastomotic_bleeding_date TEXT,
            comp_anastomotic_bleeding_tx TEXT,
            comp_pancreatic_fistula     INTEGER DEFAULT 0,
            comp_pancreatic_fistula_date TEXT,
            comp_pancreatic_fistula_tx  TEXT,
            comp_bile_leak              INTEGER DEFAULT 0,
            comp_bile_leak_date         TEXT,
            comp_bile_leak_tx           TEXT,
            comp_duodenal_stump_leak    INTEGER DEFAULT 0,
            comp_duodenal_stump_leak_date TEXT,
            comp_duodenal_stump_leak_tx TEXT,
            comp_rln_palsy              INTEGER DEFAULT 0,
            comp_rln_palsy_date         TEXT,
            comp_rln_palsy_tx           TEXT,
            comp_chylothorax            INTEGER DEFAULT 0,
            comp_chylothorax_date       TEXT,
            comp_chylothorax_tx         TEXT,
            comp_empyema                INTEGER DEFAULT 0,
            comp_empyema_date           TEXT,
            comp_empyema_tx             TEXT,
            comp_pneumothorax           INTEGER DEFAULT 0,
            comp_pneumothorax_date      TEXT,
            comp_pneumothorax_tx        TEXT,
            comp_ards                   INTEGER DEFAULT 0,
            comp_ards_date              TEXT,
            comp_ards_tx                TEXT,
            comp_dic                    INTEGER DEFAULT 0,
            comp_dic_date               TEXT,
            comp_dic_tx                 TEXT,
            comp_sepsis                 INTEGER DEFAULT 0,
            comp_sepsis_date            TEXT,
            comp_sepsis_tx              TEXT,
            comp_renal_failure          INTEGER DEFAULT 0,
            comp_renal_failure_date     TEXT,
            comp_renal_failure_tx       TEXT,
            comp_hepatic_failure        INTEGER DEFAULT 0,
            comp_hepatic_failure_date   TEXT,
            comp_hepatic_failure_tx     TEXT,
            comp_other                  INTEGER DEFAULT 0,
            comp_other_date             TEXT,
            comp_other_tx               TEXT,
            comp_other_detail           TEXT,
            comp_confirmed              INTEGER DEFAULT 0,
            -- NCD合併症サブカラム: SSI 3サブタイプ
            comp_ssi_superficial        INTEGER DEFAULT 0,  -- 表層SSI (CDgrade)
            comp_ssi_superficial_date   TEXT,
            comp_ssi_superficial_tx     TEXT,
            comp_ssi_deep               INTEGER DEFAULT 0,  -- 深部SSI (CDgrade)
            comp_ssi_deep_date          TEXT,
            comp_ssi_deep_tx            TEXT,
            comp_ssi_organ              INTEGER DEFAULT 0,  -- 臓器/体腔SSI (CDgrade)
            comp_ssi_organ_date         TEXT,
            comp_ssi_organ_tx           TEXT,
            -- NCD合併症サブカラム: DVT/PE分離
            comp_dvt                    INTEGER DEFAULT 0,  -- DVT (CDgrade)
            comp_dvt_date               TEXT,
            comp_dvt_tx                 TEXT,
            comp_pe                     INTEGER DEFAULT 0,  -- PE (CDgrade)
            comp_pe_date                TEXT,
            comp_pe_tx                  TEXT,
            -- NCD合併症サブカラム: 敗血症性ショック
            comp_septic_shock           INTEGER DEFAULT 0,  -- (CDgrade)
            comp_septic_shock_date      TEXT,
            comp_septic_shock_tx        TEXT,
            -- NCD合併症サブカラム: ISGPS分類
            comp_anastomotic_leak_type  INTEGER,  -- 1=TypeA,2=TypeB,3=TypeC
            comp_pancreatic_fistula_isgpf INTEGER, -- 0=なし,1=BL,2=GradeA,3=B,4=C
            comp_dge_isgps              INTEGER,  -- 0=なし,1=GradeA,2=B,3=C
            -- CRF追加項目: 高優先度
            robot_system_type           INTEGER,  -- 使用機器: 1=da Vinci S,2=Si,3=Xi,4=X,5=SP,6=da Vinci 5,7=hinotori,8=Hugo,9=Senhance,10=Saroa,11=ANSUR,99=その他
            robot_system_other          TEXT,     -- 使用機器その他
            surgeon_jses_certification  INTEGER,  -- JSES技術認定医: 0=なし,1=あり
            lap_detail_type             INTEGER,  -- 腹腔鏡詳細: 1=完全腹腔鏡下,2=用手補助(HALS),3=腹腔鏡補助下,4=開腹移行
            robot_detail_type           INTEGER,  -- ロボット詳細: 1=完全ロボット支援下,2=腹腔鏡併用,3=小開腹併用,9=その他
            robot_detail_other          TEXT,     -- ロボット詳細その他
            -- CRF追加項目: 中優先度
            op_adverse_event_yn         INTEGER DEFAULT 0,  -- 術中有害事象: 0=なし,1=あり
            op_adverse_event_detail     TEXT,     -- 術中有害事象詳細（自由記載）
            op_intra_injury_yn          INTEGER DEFAULT 0,  -- 術中損傷: 0=なし,1=あり
            op_intra_injury_detail      TEXT,     -- 術中損傷詳細（自由記載）
            concurrent_procedure_yn     INTEGER DEFAULT 0,  -- 併施手術: 0=なし,1=あり
            concurrent_procedure_detail TEXT,     -- 併施手術詳細（自由記載）
            op_transfusion_post_autologous INTEGER  -- 術後自己血輸血: 0=なし,1=あり
        )""")

        # ==============================================================
        # 8. pathology（病理診断）
        # ==============================================================
        conn.execute("""
        CREATE TABLE IF NOT EXISTS pathology (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id              INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            p_tumor_number          INTEGER,
            p_location_long         TEXT,
            p_location_short        TEXT,
            p_location_egj          INTEGER,
            p_egj_distance_mm       INTEGER,
            p_esoph_invasion_mm     INTEGER,
            p_macroscopic_type      INTEGER,
            p_type0_subclass        TEXT,    -- 複数選択: カンマ区切り (例: "2,4")
            p_size_major_mm         INTEGER,
            p_size_minor_mm         INTEGER,
            p_histology1            INTEGER,
            p_histology2            INTEGER,
            p_histology3            INTEGER,
            p_depth                 INTEGER,
            p_inf                   INTEGER,
            p_ly                    INTEGER,
            p_v                     INTEGER,
            p_pm                    INTEGER,
            p_pm_mm                 REAL,
            p_dm                    INTEGER,
            p_dm_mm                 REAL,
            p_ln_metastasis         INTEGER,
            p_ln_positive_total     INTEGER,
            p_distant_metastasis    INTEGER,
            p_peritoneal            INTEGER,
            p_cytology              INTEGER,
            p_liver                 INTEGER,
            p_stage                 INTEGER,
            p_residual_tumor        INTEGER,
            p_chemo_effect          INTEGER,
            p_ln_chemo_effect       INTEGER,
            msi_status              INTEGER,
            her2_status             INTEGER,
            pdl1_status             INTEGER,
            pdl1_cps                REAL,
            pdl1_tps                REAL,
            claudin18_status        INTEGER,
            fgfr2b_status           INTEGER,
            ebv_status              INTEGER,
            -- CRF追加項目: 接合部癌・EGJ
            egj_cancer_yn           INTEGER,  -- 接合部癌: 0=いいえ,1=はい
            egj_center_position     INTEGER,  -- 腫瘍中心位置: 1=OE,2=OEG,3=OE=G,4=OGE,5=OG
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- 病理的浸潤臓器フラグ (p_inv_)
            p_inv_pancreas              INTEGER DEFAULT 0,
            p_inv_liver                 INTEGER DEFAULT 0,
            p_inv_transverse_colon      INTEGER DEFAULT 0,
            p_inv_spleen                INTEGER DEFAULT 0,
            p_inv_diaphragm             INTEGER DEFAULT 0,
            p_inv_esophagus             INTEGER DEFAULT 0,
            p_inv_duodenum              INTEGER DEFAULT 0,
            p_inv_aorta                 INTEGER DEFAULT 0,
            p_inv_abdominal_wall        INTEGER DEFAULT 0,
            p_inv_adrenal               INTEGER DEFAULT 0,
            p_inv_kidney                INTEGER DEFAULT 0,
            p_inv_small_intestine       INTEGER DEFAULT 0,
            p_inv_retroperitoneum       INTEGER DEFAULT 0,
            p_inv_transverse_mesocolon  INTEGER DEFAULT 0,
            p_inv_unknown               INTEGER DEFAULT 0,
            p_inv_other                 INTEGER DEFAULT 0,
            p_inv_other_detail          TEXT,
            p_inv_confirmed             INTEGER DEFAULT 0,

            -- 病理的遠隔転移部位フラグ (p_meta_)
            p_meta_peritoneal       INTEGER DEFAULT 0,
            p_meta_liver            INTEGER DEFAULT 0,
            p_meta_lung             INTEGER DEFAULT 0,
            p_meta_lymph_node       INTEGER DEFAULT 0,
            p_meta_bone             INTEGER DEFAULT 0,
            p_meta_brain            INTEGER DEFAULT 0,
            p_meta_ovary            INTEGER DEFAULT 0,
            p_meta_adrenal          INTEGER DEFAULT 0,
            p_meta_pleura           INTEGER DEFAULT 0,
            p_meta_skin             INTEGER DEFAULT 0,
            p_meta_marrow           INTEGER DEFAULT 0,
            p_meta_meninges         INTEGER DEFAULT 0,
            p_meta_cytology         INTEGER DEFAULT 0,
            p_meta_other            INTEGER DEFAULT 0,
            p_meta_other_detail     TEXT,
            p_meta_confirmed        INTEGER DEFAULT 0
        )""")

        # ==============================================================
        # 9. lymph_nodes（LN詳細 25ステーション）
        # ==============================================================
        ln_stations = [
            # 胃領域リンパ節（腹部）
            "1", "2", "3a", "3b", "4sa", "4sb", "4d",
            "5", "6", "7", "8a", "8p", "9", "10",
            "11p", "11d", "12a", "14v", "16", "19", "20",
            # 節外転移
            "extranodal",
        ]
        ln_cols = ",\n            ".join(
            [f"ln_{s}_m INTEGER DEFAULT 0, ln_{s}_l INTEGER DEFAULT 0"
             for s in ln_stations]
        )
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS lymph_nodes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id  INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            {ln_cols},
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # ==============================================================
        # 10. gist_detail（GIST専用）
        # ==============================================================
        conn.execute("""
        CREATE TABLE IF NOT EXISTS gist_detail (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id      INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            gist_kit        INTEGER,
            gist_cd34       INTEGER,
            gist_desmin     INTEGER,
            gist_s100       INTEGER,
            gist_mitosis    INTEGER,
            gist_rupture    INTEGER,
            gist_fletcher   INTEGER,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # ==============================================================
        # 11. adjuvant_chemo（術後補助化学療法）
        # ==============================================================
        conn.execute("""
        CREATE TABLE IF NOT EXISTS adjuvant_chemo (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id          INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            adj_yn              INTEGER,
            adj_start_date      TEXT,
            adj_regimen         INTEGER,
            adj_regimen_other   TEXT,
            adj_courses         INTEGER,
            adj_completion      INTEGER,
            adj_adverse_event   TEXT,
            updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # ==============================================================
        # 12. palliative_chemo（1st-5th line）
        # ==============================================================
        conn.execute("""
        CREATE TABLE IF NOT EXISTS palliative_chemo (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id      INTEGER REFERENCES patients(id) ON DELETE CASCADE,
            line_number     INTEGER NOT NULL,
            regimen         INTEGER,
            regimen_other   TEXT,
            start_date      TEXT,
            courses         INTEGER,
            adverse_event   TEXT,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            locked_by_phase TEXT,  -- 'phase3'/'phase4'/NULL — レコード単位ロック
            UNIQUE(patient_id, line_number)
        )""")

        # ==============================================================
        # 13. outcome（再発・予後）
        # ==============================================================
        conn.execute("""
        CREATE TABLE IF NOT EXISTS outcome (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id          INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            mortality_30d       INTEGER,
            mortality_inhospital INTEGER,
            recurrence_yn       INTEGER,
            recurrence_date     TEXT,
            vital_status        INTEGER,
            last_alive_date     TEXT,
            death_date          TEXT,
            death_cause         INTEGER,
            death_cause_other   TEXT,
            death_cause_detail  INTEGER,  -- 死因詳細: 1=原病死,2=他病死,3=他癌死
            outcome_detail      TEXT,
            -- CRF追加: 30日/90日転帰
            outcome_30d         INTEGER,  -- 術後30日転帰: 1=生存,2=原病死,3=他病死,4=他癌死
            outcome_90d         INTEGER,  -- 術後90日転帰: 1=生存,2=原病死,3=他病死,4=他癌死
            updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- 再発形式フラグ (rec_)
            rec_peritoneal      INTEGER DEFAULT 0,
            rec_liver           INTEGER DEFAULT 0,
            rec_lung            INTEGER DEFAULT 0,
            rec_lymph_node      INTEGER DEFAULT 0,
            rec_local           INTEGER DEFAULT 0,
            rec_bone            INTEGER DEFAULT 0,
            rec_brain           INTEGER DEFAULT 0,
            rec_ovary           INTEGER DEFAULT 0,
            rec_adrenal         INTEGER DEFAULT 0,
            rec_other           INTEGER DEFAULT 0,
            rec_other_detail    TEXT,
            rec_confirmed       INTEGER DEFAULT 0
        )""")

        # ==============================================================
        # 13b. outcome_snapshots（予後スナップショット — Phase3/4承認時に凍結）
        # ==============================================================
        conn.execute("""
        CREATE TABLE IF NOT EXISTS outcome_snapshots (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id          INTEGER REFERENCES patients(id) ON DELETE CASCADE,
            phase               TEXT NOT NULL,  -- 'phase3' / 'phase4'
            snapshot_date       TEXT NOT NULL,
            approved_by         INTEGER REFERENCES users(id),
            mortality_30d       INTEGER,
            mortality_inhospital INTEGER,
            recurrence_yn       INTEGER,
            recurrence_date     TEXT,
            vital_status        INTEGER,
            last_alive_date     TEXT,
            death_date          TEXT,
            death_cause         INTEGER,
            death_cause_other   TEXT,
            outcome_detail      TEXT,
            rec_peritoneal      INTEGER DEFAULT 0,
            rec_liver           INTEGER DEFAULT 0,
            rec_lung            INTEGER DEFAULT 0,
            rec_lymph_node      INTEGER DEFAULT 0,
            rec_local           INTEGER DEFAULT 0,
            rec_bone            INTEGER DEFAULT 0,
            rec_brain           INTEGER DEFAULT 0,
            rec_ovary           INTEGER DEFAULT 0,
            rec_adrenal         INTEGER DEFAULT 0,
            rec_other           INTEGER DEFAULT 0,
            rec_other_detail    TEXT,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(patient_id, phase)
        )""")

        # ==============================================================
        # 14. audit_log（監査証跡）
        # ==============================================================
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id         INTEGER REFERENCES users(id),
            action          TEXT NOT NULL,
            table_name      TEXT,
            record_id       INTEGER,
            phase           TEXT,
            field_name      TEXT,
            old_value       TEXT,
            new_value       TEXT,
            comment         TEXT,
            export_filter   TEXT,
            export_count    INTEGER,
            ip_address      TEXT,
            timestamp       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(timestamp);
        CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id);
        CREATE INDEX IF NOT EXISTS idx_audit_record
            ON audit_log(table_name, record_id);
        """)

        # ==============================================================
        # Phase 2: 食道癌拡張テーブル
        # ==============================================================

        # 15. eso_tumor
        conn.execute("""
        CREATE TABLE IF NOT EXISTS eso_tumor (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id              INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            c_location_eso          TEXT,
            c_macroscopic_type_eso  INTEGER,
            c_multiple_cancer_eso   INTEGER,
            c_depth_jce             INTEGER,
            c_depth_uicc            INTEGER,
            c_ln_jce                INTEGER,
            c_ln_uicc               INTEGER,
            c_distant_jce           INTEGER,
            c_distant_uicc          INTEGER,
            c_stage_jce             INTEGER,
            c_stage_uicc            INTEGER,
            c_pet_yn                INTEGER,
            c_pet_accumulation      INTEGER,
            c_pet_site              TEXT,
            c_ln_detail             TEXT,
            yc_depth_jce            INTEGER,
            yc_depth_uicc           INTEGER,
            yc_ln_jce               INTEGER,
            yc_ln_uicc              INTEGER,
            yc_stage_jce            INTEGER,
            yc_stage_uicc           INTEGER,
            nac_endoscopy_response  INTEGER,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # 16. eso_surgery
        conn.execute("""
        CREATE TABLE IF NOT EXISTS eso_surgery (
            id                              INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id                      INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            op_type                         INTEGER,
            op_surgery_type                 INTEGER,
            op_surgery_type_other           TEXT,
            op_approach_detail              TEXT,
            op_approach_other               TEXT,
            op_endoscopic                   INTEGER,
            op_endoscopic_other             TEXT,
            op_conversion_detail            INTEGER,
            op_conversion_reason            TEXT,
            op_resection_extent             INTEGER,
            op_resection_other              TEXT,
            op_reconstruction_route_eso     INTEGER,
            op_reconstruction_route_other   TEXT,
            op_reconstruction_organ         INTEGER,
            op_reconstruction_organ_other   TEXT,
            op_anastomosis_site             INTEGER,
            op_anastomosis_site_other       TEXT,
            op_dissection_field             INTEGER,
            op_anesthesia_time_min          INTEGER,
            op_thoracic_time_min            INTEGER,
            op_thoracic_blood_loss_ml       INTEGER,
            op_surgeons                     TEXT,
            hiatal_hernia_yn                INTEGER,
            hiatal_hernia_type              INTEGER,
            gerd_la                         INTEGER,
            hiatal_hernia_op                TEXT,
            hiatal_hernia_gate_mm           REAL,
            hiatal_mesh                     TEXT,
            fundoplication                  INTEGER,
            vagus_nerve                     INTEGER,
            -- CRF追加項目: 食道手術詳細
            thoracic_position               INTEGER,  -- 胸腔操作体位: 1=左側臥位,2=右側臥位,3=腹臥位,4=仰臥位
            thoracoscope_detail_type        INTEGER,  -- 胸腔鏡詳細: 1=右完全,2=左完全,3=右小開胸併設,4=左小開胸併設,9=その他
            thoracoscope_detail_other       TEXT,
            reconstruction_stage            INTEGER,  -- 一期/二期再建: 1=一期,2=二期
            op_abdominal_time_min           INTEGER,  -- 腹部操作時間(分)
            updated_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # 17. eso_pathology
        conn.execute("""
        CREATE TABLE IF NOT EXISTS eso_pathology (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id              INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            p_pretreatment          INTEGER,
            p_depth_jce             INTEGER,
            p_depth_uicc            INTEGER,
            p_ln_jce                INTEGER,
            p_ln_uicc               INTEGER,
            p_stage_jce             INTEGER,
            p_stage_uicc            INTEGER,
            p_rm                    INTEGER,
            p_rm_mm                 REAL,
            p_im_eso                INTEGER,
            p_im_stomach            INTEGER,
            p_multiple_cancer_eso   INTEGER,
            p_curability            INTEGER,
            p_residual_factor       TEXT,
            p_treatment_effect      INTEGER,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # 18. eso_course
        conn.execute("""
        CREATE TABLE IF NOT EXISTS eso_course (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id              INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            icu_discharge_date      TEXT,
            meal_water_date         TEXT,
            meal_liquid_date        TEXT,
            meal_3bu_date           TEXT,
            meal_5bu_date           TEXT,
            meal_zenkayu_date       TEXT,
            npo_date                TEXT,
            meal_water_date2        TEXT,
            meal_liquid_date2       TEXT,
            meal_3bu_date2          TEXT,
            meal_5bu_date2          TEXT,
            meal_zenkayu_date2      TEXT,
            drain_left_chest_date   TEXT,
            drain_right_chest_date  TEXT,
            drain_neck_date         TEXT,
            drain_other1            TEXT,
            drain_other1_date       TEXT,
            drain_other2            TEXT,
            drain_other2_date       TEXT,
            tube_feeding_yn         INTEGER,
            tube_feeding_start      TEXT,
            tube_feeding_end        TEXT,
            icu_type                INTEGER,
            reintubation_yn         INTEGER,
            readmission_yn          INTEGER,
            readmission_reason      TEXT,
            reop_date               TEXT,
            reop2_date              TEXT,
            reop_detail             TEXT,
            course_notes            TEXT,
            stricture_yn            INTEGER,
            stricture_first_date    TEXT,
            stricture_count         INTEGER,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # 19. radiation_therapy
        conn.execute("""
        CREATE TABLE IF NOT EXISTS radiation_therapy (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id              INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            rt_yn                   INTEGER,
            rt_start_date           TEXT,
            rt_purpose              INTEGER,
            rt_purpose_other        TEXT,
            rt_combination          INTEGER,
            rt_combination_other    TEXT,
            rt_total_dose_gy        REAL,
            rt_fractions            INTEGER,
            rt_target_volume1       INTEGER,
            rt_target_volume2       INTEGER,
            rt_target_volume_other  TEXT,
            rt_prophylactic_ln      INTEGER,
            rt_device               INTEGER,
            rt_planning             INTEGER,
            rt_planning_other       TEXT,
            rt_completion           INTEGER,
            rt_adverse_event        TEXT,
            updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # 20. notification_settings（通知設定）
        conn.execute("""
        CREATE TABLE IF NOT EXISTS notification_settings (
            id                              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id                         INTEGER UNIQUE REFERENCES users(id),
            line_notify_token               TEXT,
            line_user_id                    TEXT,
            email_address                   TEXT,
            enable_app_notification         INTEGER DEFAULT 1,
            notify_phase1_deadline          INTEGER DEFAULT 1,
            notify_phase1_approval_deadline INTEGER DEFAULT 1,
            notify_phase2_deadline          INTEGER DEFAULT 0,
            notify_case_returned            INTEGER DEFAULT 1,
            notify_case_approved            INTEGER DEFAULT 0,
            notify_case_submitted           INTEGER DEFAULT 0,
            updated_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # 21. notifications（アプリ内通知）
        conn.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER REFERENCES users(id),
            title       TEXT NOT NULL,
            message     TEXT,
            link_page   TEXT,
            link_study_id TEXT,
            is_read     INTEGER DEFAULT 0,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(user_id, is_read)")

        # 22. tumor_markers（腫瘍マーカー）
        conn.execute("""
        CREATE TABLE IF NOT EXISTS tumor_markers (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id      INTEGER REFERENCES patients(id) ON DELETE CASCADE,
            timing          TEXT NOT NULL,   -- 'preop','postop_first','recurrence','other'
            measurement_date TEXT,
            -- 胃癌マーカー
            cea             REAL,
            ca199           REAL,
            ca125           REAL,
            afp             REAL,
            -- 食道癌マーカー
            p53_antibody    REAL,
            cyfra           REAL,
            scc_ag          REAL,
            kl6             REAL,
            notes           TEXT,
            created_by      INTEGER REFERENCES users(id),
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            locked_by_phase TEXT  -- 'phase3'/'phase4'/NULL — レコード単位ロック
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tm_patient ON tumor_markers(patient_id)")

        # 23. eso_lymph_nodes（食道癌リンパ節: 頸部 + 胸部）
        eso_ln_stations = [
            # 頸部
            "100L", "100R",
            "101L", "101R",
            "102midL", "102midR", "102upL", "102upR",
            "103", "104L", "104R",
            # 胸部
            "105",
            "106recL", "106recR", "106pre", "106tbL", "106tbR",
            "107", "108", "109L", "109R",
            "110", "111",
            "112aoA", "112aoP", "112pulL", "112pulR",
            "113", "114",
            # 節外転移
            "extranodal",
        ]
        eso_ln_cols = ",\n            ".join(
            [f"ln_{s}_m INTEGER DEFAULT 0, ln_{s}_l INTEGER DEFAULT 0"
             for s in eso_ln_stations]
        )
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS eso_lymph_nodes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id  INTEGER UNIQUE REFERENCES patients(id) ON DELETE CASCADE,
            {eso_ln_cols},
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # 24. NCD年度バージョン管理
        conn.execute("""
        CREATE TABLE IF NOT EXISTS ncd_versions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            year        INTEGER UNIQUE NOT NULL,     -- NCD登録年度 (例: 2025)
            version     TEXT NOT NULL,               -- バージョン名 (例: "v13.0")
            is_active   INTEGER DEFAULT 1,           -- 有効フラグ
            notes       TEXT,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # 25. NCD フィールド定義（年度ごとの項目リスト）
        conn.execute("""
        CREATE TABLE IF NOT EXISTS ncd_field_defs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ncd_version_id  INTEGER REFERENCES ncd_versions(id),
            ncd_field_name  TEXT NOT NULL,            -- NCD項目名 (例: "性別")
            level           TEXT DEFAULT 'L0',        -- NCD階層 (L0-L4)
            is_required     INTEGER DEFAULT 0,        -- 1=必須, 0=任意
            field_type      TEXT DEFAULT 'text',      -- text/int/date/select
            db_table        TEXT,                     -- マッピング先テーブル
            db_column       TEXT,                     -- マッピング先カラム
            converter       TEXT,                     -- 変換関数名 or NULL
            sort_order      INTEGER DEFAULT 0,        -- 表示順
            UNIQUE(ncd_version_id, ncd_field_name)
        )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ncd_fd_ver ON ncd_field_defs(ncd_version_id)"
        )

        # 26. lab_results（検査値）
        conn.execute("""
        CREATE TABLE IF NOT EXISTS lab_results (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id      INTEGER REFERENCES patients(id) ON DELETE CASCADE,
            timing          TEXT NOT NULL DEFAULT 'preop',  -- preop/postop/recurrence
            sample_date     TEXT,                           -- 採取日 YYYY-MM-DD
            -- 血算 (CBC)
            wbc             REAL,   -- x10^3/μL or /μL
            rbc             REAL,   -- x10^4/μL
            hgb             REAL,   -- g/dL
            hct             REAL,   -- %
            plt             REAL,   -- x10^4/μL
            mcv             REAL,   -- fL
            mch             REAL,   -- pg
            mchc            REAL,   -- g/dL
            neut_pct        REAL,   -- %
            lymph_pct       REAL,   -- %
            mono_pct        REAL,   -- %
            eosin_pct       REAL,   -- %
            baso_pct        REAL,   -- %
            -- 生化学
            tp              REAL,   -- g/dL
            alb             REAL,   -- g/dL
            t_bil           REAL,   -- mg/dL
            ast             REAL,   -- U/L
            alt             REAL,   -- U/L
            ldh             REAL,   -- U/L
            alp             REAL,   -- U/L
            ggt             REAL,   -- γ-GTP U/L
            che             REAL,   -- U/L
            bun             REAL,   -- mg/dL
            cre             REAL,   -- mg/dL
            egfr            REAL,   -- mL/min/1.73m²
            na              REAL,   -- mEq/L
            k               REAL,   -- mEq/L
            cl              REAL,   -- mEq/L
            crp             REAL,   -- mg/dL
            amy             REAL,   -- U/L
            ck              REAL,   -- U/L
            glu             REAL,   -- mg/dL
            hba1c           REAL,   -- %
            -- 栄養・凝固
            prealb          REAL,   -- mg/dL (トランスサイレチン)
            cholinesterase  REAL,   -- U/L
            pt_inr          REAL,
            aptt            REAL,   -- 秒
            fibrinogen      REAL,   -- mg/dL
            d_dimer         REAL,   -- μg/mL
            -- 腫瘍マーカー（オプション：別テーブルにもある）
            cea_lab         REAL,
            ca199_lab       REAL,
            afp_lab         REAL,   -- ng/mL
            ca125_lab       REAL,   -- U/mL
            -- メタ情報
            source_type     TEXT DEFAULT 'manual',  -- manual/ocr
            raw_ocr_text    TEXT,                   -- OCR生テキスト（デバッグ用）
            notes           TEXT,
            created_by      INTEGER REFERENCES users(id),
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lab_patient ON lab_results(patient_id, timing)"
        )

        # ==============================================================
        # 子テーブル patient_id インデックス（一括追加）
        # ==============================================================
        _child_tables = [
            "tumor_preop", "neoadjuvant", "surgery", "pathology",
            "lymph_nodes", "gist_detail", "adjuvant_chemo", "palliative_chemo",
            "outcome", "outcome_snapshots", "eso_tumor", "eso_surgery",
            "eso_pathology", "eso_course", "radiation_therapy", "eso_lymph_nodes",
        ]
        for tbl in _child_tables:
            try:
                conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_pid ON {tbl}(patient_id)")
            except Exception:
                pass

        # NOTE: is_deleted インデックスはマイグレーション後に作成（後方参照）

        # ==============================================================
        # デフォルト管理者ユーザー
        # ==============================================================
        existing = conn.execute(
            "SELECT id FROM users WHERE username='admin'"
        ).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO users (username, password_hash, display_name, role) "
                "VALUES (?, ?, ?, ?)",
                ("admin", hash_password("admin"), "管理者", "admin"),
            )

        # ==============================================================
        # デフォルト規約バージョン
        # ==============================================================
        existing = conn.execute(
            "SELECT id FROM classification_versions WHERE name='胃癌取扱い規約' AND version='15'"
        ).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO classification_versions (name, version, is_active, notes) "
                "VALUES (?, ?, ?, ?)",
                ("胃癌取扱い規約", "15", 1, "第15版（現行）"),
            )
            conn.execute(
                "INSERT INTO classification_versions (name, version, is_active, notes) "
                "VALUES (?, ?, ?, ?)",
                ("UICC-TNM", "8", 1, "UICC第8版（現行）"),
            )
            conn.execute(
                "INSERT INTO classification_versions (name, version, is_active, notes) "
                "VALUES (?, ?, ?, ?)",
                ("食道癌取扱い規約", "12", 1, "第12版（現行）"),
            )
            conn.execute(
                "INSERT INTO classification_versions (name, version, is_active, notes) "
                "VALUES (?, ?, ?, ?)",
                ("胃癌取扱い規約", "13", 0, "第13版（1999年、〜2009年手術症例の旧データ移行用）"),
            )
            conn.execute(
                "INSERT INTO classification_versions (name, version, is_active, notes) "
                "VALUES (?, ?, ?, ?)",
                ("胃癌取扱い規約", "14", 0, "第14版（2010年、2010〜2016年手術症例の旧データ移行用）"),
            )

        # ==============================================================
        # デフォルトNCDバージョン（2025年度）
        # ==============================================================
        existing_ncd = conn.execute(
            "SELECT id FROM ncd_versions WHERE year=2025"
        ).fetchone()
        if not existing_ncd:
            _seed_ncd_version(conn, 2025, "v13.0")

    # ------------------------------------------------------------------
    # マイグレーション: 既存DBに不足カラムを追加（ALTER TABLE）
    # ------------------------------------------------------------------
    _migrations = [
        ("tumor_preop", "c_inv_confirmed",  "INTEGER DEFAULT 0"),
        ("tumor_preop", "c_meta_confirmed", "INTEGER DEFAULT 0"),
        ("pathology",   "p_inv_confirmed",  "INTEGER DEFAULT 0"),
        ("pathology",   "p_meta_cytology",  "INTEGER DEFAULT 0"),
        ("pathology",   "p_meta_confirmed", "INTEGER DEFAULT 0"),
        # v2.1: 術後転帰を surgery テーブルに追加
        ("surgery", "mortality_30d",        "INTEGER"),
        ("surgery", "mortality_inhospital", "INTEGER"),
        # v2.1: 術前療法後診断 (ycTNM) を neoadjuvant テーブルに追加
        ("neoadjuvant", "yc_macroscopic_type",    "INTEGER"),
        ("neoadjuvant", "yc_histology",           "INTEGER"),
        ("neoadjuvant", "yc_tumor_size_major_mm", "INTEGER"),
        ("neoadjuvant", "yc_tumor_size_minor_mm", "INTEGER"),
        ("neoadjuvant", "yc_location_long",       "TEXT"),
        ("neoadjuvant", "yc_depth",               "INTEGER"),
        ("neoadjuvant", "yc_ln_metastasis",       "INTEGER"),
        ("neoadjuvant", "yc_distant_metastasis",  "INTEGER"),
        ("neoadjuvant", "yc_stage",               "INTEGER"),
        # v2.2: 追加フィールド
        ("neoadjuvant", "yc_tumor_number",        "INTEGER"),
        ("neoadjuvant", "yc_location_short",      "TEXT"),
        ("neoadjuvant", "yc_peritoneal",          "INTEGER"),
        ("neoadjuvant", "yc_liver_metastasis",    "INTEGER"),
        ("tumor_preop", "remnant_location",       "INTEGER"),
        # 遠隔転移 新フラグ (cytology, unknown)
        ("tumor_preop", "c_meta_cytology",        "INTEGER DEFAULT 0"),
        ("tumor_preop", "c_meta_unknown",         "INTEGER DEFAULT 0"),
        # 浸潤臓器 新フラグ (unknown)
        ("tumor_preop", "c_inv_unknown",          "INTEGER DEFAULT 0"),
        ("pathology",   "p_inv_unknown",          "INTEGER DEFAULT 0"),
        # v2.3: 重複癌 flag_group 形式
        ("patients", "sync_org_oral_pharynx",   "INTEGER DEFAULT 0"),
        ("patients", "sync_org_esophagus",      "INTEGER DEFAULT 0"),
        ("patients", "sync_org_stomach",        "INTEGER DEFAULT 0"),
        ("patients", "sync_org_colorectum",     "INTEGER DEFAULT 0"),
        ("patients", "sync_org_lung",           "INTEGER DEFAULT 0"),
        ("patients", "sync_org_hepatobiliary",  "INTEGER DEFAULT 0"),
        ("patients", "sync_org_pancreas",       "INTEGER DEFAULT 0"),
        ("patients", "sync_org_breast",         "INTEGER DEFAULT 0"),
        ("patients", "sync_org_urological",     "INTEGER DEFAULT 0"),
        ("patients", "sync_org_gynecological",  "INTEGER DEFAULT 0"),
        ("patients", "sync_org_thyroid",        "INTEGER DEFAULT 0"),
        ("patients", "sync_org_nervous_system", "INTEGER DEFAULT 0"),
        ("patients", "sync_org_hematologic",    "INTEGER DEFAULT 0"),
        ("patients", "sync_org_skin",           "INTEGER DEFAULT 0"),
        ("patients", "sync_org_other",          "INTEGER DEFAULT 0"),
        ("patients", "sync_org_confirmed",      "INTEGER DEFAULT 0"),
        ("patients", "meta_org_oral_pharynx",   "INTEGER DEFAULT 0"),
        ("patients", "meta_org_esophagus",      "INTEGER DEFAULT 0"),
        ("patients", "meta_org_stomach",        "INTEGER DEFAULT 0"),
        ("patients", "meta_org_colorectum",     "INTEGER DEFAULT 0"),
        ("patients", "meta_org_lung",           "INTEGER DEFAULT 0"),
        ("patients", "meta_org_hepatobiliary",  "INTEGER DEFAULT 0"),
        ("patients", "meta_org_pancreas",       "INTEGER DEFAULT 0"),
        ("patients", "meta_org_breast",         "INTEGER DEFAULT 0"),
        ("patients", "meta_org_urological",     "INTEGER DEFAULT 0"),
        ("patients", "meta_org_gynecological",  "INTEGER DEFAULT 0"),
        ("patients", "meta_org_thyroid",        "INTEGER DEFAULT 0"),
        ("patients", "meta_org_nervous_system", "INTEGER DEFAULT 0"),
        ("patients", "meta_org_hematologic",    "INTEGER DEFAULT 0"),
        ("patients", "meta_org_skin",           "INTEGER DEFAULT 0"),
        ("patients", "meta_org_other",          "INTEGER DEFAULT 0"),
        ("patients", "meta_org_confirmed",      "INTEGER DEFAULT 0"),
        # v2.4: 疾患分類の2階層化
        ("patients", "disease_category",        "INTEGER DEFAULT 1"),
        # v2.7: 術前診断 組織型 3カラム化 (c_histology → c_histology1/2/3)
        ("tumor_preop", "c_histology1",            "INTEGER"),
        ("tumor_preop", "c_histology2",            "INTEGER"),
        ("tumor_preop", "c_histology3",            "INTEGER"),
        # v2.5: 術後合併症 発症日カラム追加
        ("surgery", "comp_ssi_date",                    "TEXT"),
        ("surgery", "comp_wound_dehiscence_date",       "TEXT"),
        ("surgery", "comp_intra_abd_abscess_date",      "TEXT"),
        ("surgery", "comp_bleeding_date",               "TEXT"),
        ("surgery", "comp_ileus_date",                  "TEXT"),
        ("surgery", "comp_dvt_pe_date",                 "TEXT"),
        ("surgery", "comp_pneumonia_date",              "TEXT"),
        ("surgery", "comp_atelectasis_date",            "TEXT"),
        ("surgery", "comp_uti_date",                    "TEXT"),
        ("surgery", "comp_delirium_date",               "TEXT"),
        ("surgery", "comp_cardiac_date",                "TEXT"),
        ("surgery", "comp_dge_date",                    "TEXT"),
        ("surgery", "comp_perforation_date",            "TEXT"),
        ("surgery", "comp_cholelithiasis_date",         "TEXT"),
        ("surgery", "comp_anastomotic_leak_date",       "TEXT"),
        ("surgery", "comp_anastomotic_stricture_date",  "TEXT"),
        ("surgery", "comp_anastomotic_bleeding_date",   "TEXT"),
        ("surgery", "comp_pancreatic_fistula_date",     "TEXT"),
        ("surgery", "comp_bile_leak_date",              "TEXT"),
        ("surgery", "comp_duodenal_stump_leak_date",    "TEXT"),
        ("surgery", "comp_rln_palsy_date",              "TEXT"),
        ("surgery", "comp_chylothorax_date",            "TEXT"),
        ("surgery", "comp_empyema_date",                "TEXT"),
        ("surgery", "comp_pneumothorax_date",           "TEXT"),
        ("surgery", "comp_ards_date",                   "TEXT"),
        ("surgery", "comp_dic_date",                    "TEXT"),
        ("surgery", "comp_sepsis_date",                 "TEXT"),
        ("surgery", "comp_renal_failure_date",          "TEXT"),
        ("surgery", "comp_hepatic_failure_date",        "TEXT"),
        ("surgery", "comp_other_date",                  "TEXT"),
        # v2.6: 術後合併症 処置内容カラム追加
        ("surgery", "comp_ssi_tx",                      "TEXT"),
        ("surgery", "comp_wound_dehiscence_tx",         "TEXT"),
        ("surgery", "comp_intra_abd_abscess_tx",        "TEXT"),
        ("surgery", "comp_bleeding_tx",                 "TEXT"),
        ("surgery", "comp_ileus_tx",                    "TEXT"),
        ("surgery", "comp_dvt_pe_tx",                   "TEXT"),
        ("surgery", "comp_pneumonia_tx",                "TEXT"),
        ("surgery", "comp_atelectasis_tx",              "TEXT"),
        ("surgery", "comp_uti_tx",                      "TEXT"),
        ("surgery", "comp_delirium_tx",                 "TEXT"),
        ("surgery", "comp_cardiac_tx",                  "TEXT"),
        ("surgery", "comp_dge_tx",                      "TEXT"),
        ("surgery", "comp_perforation_tx",              "TEXT"),
        ("surgery", "comp_cholelithiasis_tx",           "TEXT"),
        ("surgery", "comp_anastomotic_leak_tx",         "TEXT"),
        ("surgery", "comp_anastomotic_stricture_tx",    "TEXT"),
        ("surgery", "comp_anastomotic_bleeding_tx",     "TEXT"),
        ("surgery", "comp_pancreatic_fistula_tx",       "TEXT"),
        ("surgery", "comp_bile_leak_tx",                "TEXT"),
        ("surgery", "comp_duodenal_stump_leak_tx",      "TEXT"),
        ("surgery", "comp_rln_palsy_tx",                "TEXT"),
        ("surgery", "comp_chylothorax_tx",              "TEXT"),
        ("surgery", "comp_empyema_tx",                  "TEXT"),
        ("surgery", "comp_pneumothorax_tx",             "TEXT"),
        ("surgery", "comp_ards_tx",                     "TEXT"),
        ("surgery", "comp_dic_tx",                      "TEXT"),
        ("surgery", "comp_sepsis_tx",                   "TEXT"),
        ("surgery", "comp_renal_failure_tx",            "TEXT"),
        ("surgery", "comp_hepatic_failure_tx",          "TEXT"),
        ("surgery", "comp_other_tx",                    "TEXT"),
        # v3.0: Phase承認フロー (Phase2廃止 → Phase3/Phase4新設)
        ("patients", "phase1_status",       "TEXT DEFAULT 'draft'"),
        ("patients", "phase1_submitted_at", "TIMESTAMP"),
        ("patients", "phase1_submitted_by", "INTEGER"),
        ("patients", "phase1_approved_at",  "TIMESTAMP"),
        ("patients", "phase1_approved_by",  "INTEGER"),
        ("patients", "phase3_status",       "TEXT DEFAULT 'draft'"),
        ("patients", "phase3_submitted_at", "TIMESTAMP"),
        ("patients", "phase3_submitted_by", "INTEGER"),
        ("patients", "phase3_approved_at",  "TIMESTAMP"),
        ("patients", "phase3_approved_by",  "INTEGER"),
        ("patients", "phase4_status",       "TEXT DEFAULT 'draft'"),
        ("patients", "phase4_submitted_at", "TIMESTAMP"),
        ("patients", "phase4_submitted_by", "INTEGER"),
        ("patients", "phase4_approved_at",  "TIMESTAMP"),
        ("patients", "phase4_approved_by",  "INTEGER"),
        # v3.0: 監査ログ拡張カラム
        ("audit_log", "phase",         "TEXT"),
        ("audit_log", "comment",       "TEXT"),
        ("audit_log", "export_filter", "TEXT"),
        ("audit_log", "export_count",  "INTEGER"),
        # v3.1: LINE Messaging API 移行（line_user_id 追加）
        ("notification_settings", "line_user_id", "TEXT"),
        # v3.2: 執刀医・助手カラム追加
        ("surgery", "op_surgeon",    "TEXT"),
        ("surgery", "op_assistant1", "TEXT"),
        ("surgery", "op_assistant2", "TEXT"),
        ("surgery", "op_scopist",    "TEXT"),
        # v4.0: 多施設・論理削除
        ("patients", "facility_id",   "INTEGER DEFAULT 1"),
        ("patients", "is_deleted",    "INTEGER DEFAULT 0"),
        # v4.0: NCD併存疾患サブカラム — 心血管系
        ("patients", "comor_ihd",                "INTEGER DEFAULT 0"),
        ("patients", "comor_chf",                "INTEGER DEFAULT 0"),
        ("patients", "comor_arrhythmia",         "INTEGER DEFAULT 0"),
        ("patients", "comor_valvular",           "INTEGER DEFAULT 0"),
        ("patients", "comor_aortic",             "INTEGER DEFAULT 0"),
        ("patients", "comor_pvd",                "INTEGER DEFAULT 0"),
        # v4.0: NCD併存疾患サブカラム — 脳血管障害
        ("patients", "comor_cerebral_infarction", "INTEGER DEFAULT 0"),
        ("patients", "comor_cerebral_hemorrhage", "INTEGER DEFAULT 0"),
        ("patients", "comor_tia",                "INTEGER DEFAULT 0"),
        ("patients", "comor_sah",                "INTEGER DEFAULT 0"),
        # v4.0: NCD併存疾患サブカラム — 肝疾患
        ("patients", "comor_cirrhosis",          "INTEGER DEFAULT 0"),
        ("patients", "comor_portal_htn",         "INTEGER DEFAULT 0"),
        ("patients", "comor_hepatitis_virus",    "INTEGER DEFAULT 0"),
        # v4.0: NCD併存疾患サブカラム — 呼吸器
        ("patients", "comor_dyspnea",            "INTEGER DEFAULT 0"),
        ("patients", "comor_ventilator",         "INTEGER DEFAULT 0"),
        # v5.0: 高血圧・糖尿病サブカラム（親=0/1に統一、詳細を分離）
        ("patients", "comor_ht_treatment",       "INTEGER"),
        ("patients", "comor_dm_treatment",       "INTEGER"),
        # v4.0: 喫煙サブカラム
        ("patients", "smoking_type",             "INTEGER"),
        ("patients", "smoking_bi",               "INTEGER"),
        # v4.0: ADLサブカラム
        ("patients", "adl_status_preop",         "INTEGER"),
        # v4.0: 輸血サブカラム（術前72h以内 + 術中 + 術後）
        ("surgery", "op_transfusion_preop",      "INTEGER"),
        ("surgery", "op_transfusion_preop_rbc",  "INTEGER"),
        ("surgery", "op_transfusion_preop_ffp",  "INTEGER"),
        ("surgery", "op_transfusion_preop_pc",   "INTEGER"),
        ("surgery", "op_transfusion_intra_rbc",  "INTEGER"),
        ("surgery", "op_transfusion_intra_ffp",  "INTEGER"),
        ("surgery", "op_transfusion_intra_pc",   "INTEGER"),
        ("surgery", "op_transfusion_post_rbc",   "INTEGER"),
        ("surgery", "op_transfusion_post_ffp",   "INTEGER"),
        ("surgery", "op_transfusion_post_pc",    "INTEGER"),
        # v5.0: AFP 腫瘍マーカー追加
        ("lab_results", "afp_lab",               "REAL"),
        # v5.1: SCC→CA125 置換
        ("lab_results", "ca125_lab",             "REAL"),
        # v4.0: NCD合併症サブカラム — SSI 3サブタイプ
        ("surgery", "comp_ssi_superficial",      "INTEGER DEFAULT 0"),
        ("surgery", "comp_ssi_superficial_date", "TEXT"),
        ("surgery", "comp_ssi_superficial_tx",   "TEXT"),
        ("surgery", "comp_ssi_deep",             "INTEGER DEFAULT 0"),
        ("surgery", "comp_ssi_deep_date",        "TEXT"),
        ("surgery", "comp_ssi_deep_tx",          "TEXT"),
        ("surgery", "comp_ssi_organ",            "INTEGER DEFAULT 0"),
        ("surgery", "comp_ssi_organ_date",       "TEXT"),
        ("surgery", "comp_ssi_organ_tx",         "TEXT"),
        # v4.0: NCD合併症サブカラム — DVT/PE分離
        ("surgery", "comp_dvt",                  "INTEGER DEFAULT 0"),
        ("surgery", "comp_dvt_date",             "TEXT"),
        ("surgery", "comp_dvt_tx",               "TEXT"),
        ("surgery", "comp_pe",                   "INTEGER DEFAULT 0"),
        ("surgery", "comp_pe_date",              "TEXT"),
        ("surgery", "comp_pe_tx",                "TEXT"),
        # v4.0: NCD合併症サブカラム — 敗血症性ショック
        ("surgery", "comp_septic_shock",         "INTEGER DEFAULT 0"),
        ("surgery", "comp_septic_shock_date",    "TEXT"),
        ("surgery", "comp_septic_shock_tx",      "TEXT"),
        # v4.0: NCD合併症サブカラム — ISGPS分類
        ("surgery", "comp_anastomotic_leak_type",    "INTEGER"),
        ("surgery", "comp_pancreatic_fistula_isgpf", "INTEGER"),
        ("surgery", "comp_dge_isgps",                "INTEGER"),
        # v6.0: CRF不足項目 — 高優先度
        ("surgery", "robot_system_type",            "INTEGER"),
        ("surgery", "robot_system_other",           "TEXT"),
        ("surgery", "surgeon_jses_certification",   "INTEGER"),
        ("surgery", "lap_detail_type",              "INTEGER"),
        ("surgery", "robot_detail_type",            "INTEGER"),
        ("surgery", "robot_detail_other",           "TEXT"),
        # v6.0: CRF不足項目 — 中優先度
        ("surgery", "op_adverse_event_yn",          "INTEGER DEFAULT 0"),
        ("surgery", "op_adverse_event_detail",      "TEXT"),
        ("surgery", "op_intra_injury_yn",           "INTEGER DEFAULT 0"),
        ("surgery", "op_intra_injury_detail",       "TEXT"),
        ("surgery", "concurrent_procedure_yn",      "INTEGER DEFAULT 0"),
        ("surgery", "concurrent_procedure_detail",  "TEXT"),
        ("surgery", "op_transfusion_post_autologous", "INTEGER"),
        # v6.0: 食道手術詳細
        ("eso_surgery", "thoracic_position",          "INTEGER"),
        ("eso_surgery", "thoracoscope_detail_type",   "INTEGER"),
        ("eso_surgery", "thoracoscope_detail_other",  "TEXT"),
        ("eso_surgery", "reconstruction_stage",       "INTEGER"),
        ("eso_surgery", "op_abdominal_time_min",      "INTEGER"),
        # v6.0: 病理 — 接合部癌・EGJ位置
        ("pathology", "egj_cancer_yn",                "INTEGER"),
        ("pathology", "egj_center_position",          "INTEGER"),
        # v6.0: 転帰 — 30日/90日転帰・死因詳細
        ("outcome", "outcome_30d",                    "INTEGER"),
        ("outcome", "outcome_90d",                    "INTEGER"),
        ("outcome", "death_cause_detail",             "INTEGER"),
        # v6.0: 患者基本 — 手術既往・NCD共通
        ("patients", "prior_abdominal_surgery_yn",      "INTEGER"),
        ("patients", "prior_abdominal_surgery_detail", "TEXT"),
        ("patients", "prior_cardiac_surgery_yn",       "INTEGER"),
        ("patients", "preop_ventilator_yn",            "INTEGER"),
        ("patients", "legal_capacity_admission",       "INTEGER"),
        # v7.0: 併存疾患サブカラム追加
        ("patients", "comor_copd",                     "INTEGER"),
        ("patients", "comor_ild",                      "INTEGER"),
        ("patients", "comor_asthma",                   "INTEGER"),
        ("patients", "comor_resp_unknown",             "INTEGER"),
        ("patients", "comor_mi",                       "INTEGER"),
        ("patients", "comor_angina",                   "INTEGER"),
        ("patients", "comor_structural_vascular",      "INTEGER"),
        ("patients", "comor_cv_unknown",               "INTEGER"),
        ("patients", "comor_ci_no_sequela",            "INTEGER"),
        ("patients", "comor_ci_with_sequela",          "INTEGER"),
        ("patients", "comor_cerebro_unknown",          "INTEGER"),
        ("patients", "comor_dementia",                 "INTEGER"),
        ("patients", "comor_mood_disorder",            "INTEGER"),
        ("patients", "comor_schizophrenia",            "INTEGER"),
        ("patients", "comor_developmental",            "INTEGER"),
        ("patients", "comor_psy_unknown",              "INTEGER"),
        # v7.1: 手術既往・人工呼吸器を併存疾患フラグに統合
        ("patients", "comor_prior_cardiac_surgery",   "INTEGER DEFAULT 0"),
        ("patients", "comor_prior_abdominal_surgery", "INTEGER DEFAULT 0"),
        ("patients", "comor_preop_ventilator",        "INTEGER DEFAULT 0"),
        ("patients", "emergency_transport_yn",        "INTEGER"),
        ("patients", "consent_refusal_date",          "TEXT"),
        ("patients", "anesthesiologist_yn",            "INTEGER"),
        # v7.2: 合併切除 — 有無フラグ + 新臓器カラム
        ("surgery", "comb_yn",                         "INTEGER"),
        ("surgery", "comb_transverse_mesocolon",       "INTEGER DEFAULT 0"),
        ("surgery", "comb_kidney",                     "INTEGER DEFAULT 0"),
        # v7.0: 麻酔フラグ（複数選択）
        ("surgery", "anest_general",                   "INTEGER DEFAULT 0"),
        ("surgery", "anest_epidural",                  "INTEGER DEFAULT 0"),
        ("surgery", "anest_ivpca",                     "INTEGER DEFAULT 0"),
        ("surgery", "anest_spinal",                    "INTEGER DEFAULT 0"),
        ("surgery", "anest_local",                     "INTEGER DEFAULT 0"),
        ("surgery", "anest_anest_other",               "INTEGER DEFAULT 0"),
        # v4.0: 1:Nテーブル レコード単位ロック
        ("palliative_chemo", "locked_by_phase",  "TEXT"),
        ("tumor_markers",    "locked_by_phase",  "TEXT"),
        # v5.2: 旧DB移行 — 転移陽性リンパ節総数 + 規約バージョン自動設定
        ("pathology", "p_ln_positive_total",     "INTEGER"),
        # v5.3: 胃リンパ節 — 節外転移
        ("lymph_nodes", "ln_extranodal_m",       "INTEGER DEFAULT 0"),
        ("lymph_nodes", "ln_extranodal_l",       "INTEGER DEFAULT 0"),
        # v5.3: 食道リンパ節 — 完全版ステーション追加（既存テーブル拡張）
        ("eso_lymph_nodes", "ln_100L_m",         "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_100L_l",         "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_100R_m",         "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_100R_l",         "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_102midL_m",      "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_102midL_l",      "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_102midR_m",      "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_102midR_l",      "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_102upL_m",       "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_102upL_l",       "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_102upR_m",       "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_102upR_l",       "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_104L_m",         "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_104L_l",         "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_104R_m",         "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_104R_l",         "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_106tbL_m",       "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_106tbL_l",       "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_106tbR_m",       "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_106tbR_l",       "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_107_m",          "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_107_l",          "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_108_m",          "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_108_l",          "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_109L_m",         "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_109L_l",         "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_109R_m",         "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_109R_l",         "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_110_m",          "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_110_l",          "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_111_m",          "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_111_l",          "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_112aoA_m",       "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_112aoA_l",       "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_112aoP_m",       "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_112aoP_l",       "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_112pulL_m",      "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_112pulL_l",      "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_112pulR_m",      "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_112pulR_l",      "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_113_m",          "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_113_l",          "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_114_m",          "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_114_l",          "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_extranodal_m",   "INTEGER DEFAULT 0"),
        ("eso_lymph_nodes", "ln_extranodal_l",   "INTEGER DEFAULT 0"),
        # v7.0: NCD codebook検証 — 新規UIカラム
        ("eso_tumor",     "c_macroscopic_type_eso",  "INTEGER"),
        ("eso_pathology", "p_treatment_effect",      "INTEGER"),
        # v8.0: 手術タブ改修
        ("surgery", "op_transfusion_intra_autologous", "INTEGER"),
        ("surgery", "op_fluid_volume_ml",              "INTEGER"),
        ("surgery", "op_urine_output_ml",              "INTEGER"),
        # comp_dge_grade / comp_pancreatic_fistula_grade: app.py側のf"comp_{suffix}_grade"パターンに合わせた新カラム
        # 旧カラム comp_dge_isgps / comp_pancreatic_fistula_isgpf は使用停止
        ("surgery", "comp_dge_grade",                  "INTEGER"),
        ("surgery", "comp_pancreatic_fistula_grade",   "INTEGER"),
        # v8.0: 病理 — リンパ節治療効果フリーテキスト
        ("pathology", "p_ln_chemo_effect_text",        "TEXT"),
        # v8.0: 血液検査タブ拡張 (tumor_markersテーブルに追加カラム)
        ("tumor_markers", "wbc",          "REAL"),
        ("tumor_markers", "rbc",          "REAL"),
        ("tumor_markers", "hgb",          "REAL"),
        ("tumor_markers", "hct",          "REAL"),
        ("tumor_markers", "plt",          "REAL"),
        ("tumor_markers", "neut",         "REAL"),
        ("tumor_markers", "lymph",        "REAL"),
        ("tumor_markers", "mono",         "REAL"),
        ("tumor_markers", "tp",           "REAL"),
        ("tumor_markers", "alb",          "REAL"),
        ("tumor_markers", "t_bil",        "REAL"),
        ("tumor_markers", "d_bil",        "REAL"),
        ("tumor_markers", "ast",          "REAL"),
        ("tumor_markers", "alt",          "REAL"),
        ("tumor_markers", "ldh",          "REAL"),
        ("tumor_markers", "alp",          "REAL"),
        ("tumor_markers", "ggt",          "REAL"),
        ("tumor_markers", "bun",          "REAL"),
        ("tumor_markers", "cre",          "REAL"),
        ("tumor_markers", "na",           "REAL"),
        ("tumor_markers", "k",            "REAL"),
        ("tumor_markers", "cl",           "REAL"),
        ("tumor_markers", "crp",          "REAL"),
        ("tumor_markers", "glu",          "REAL"),
        ("tumor_markers", "hba1c",        "REAL"),
        ("tumor_markers", "pt_inr",       "REAL"),
        ("tumor_markers", "aptt",         "REAL"),
        ("tumor_markers", "fib",          "REAL"),
        ("tumor_markers", "d_dimer",      "REAL"),
    ]
    with get_db() as conn:
        for tbl, col, col_def in _migrations:
            try:
                conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {col_def}")
            except Exception:
                pass  # カラム既存 or テーブル未存在 → スキップ

        # データ移行: c_histology → c_histology1 (旧カラムにデータがあれば)
        try:
            has_old = conn.execute(
                "SELECT COUNT(*) FROM tumor_preop WHERE c_histology IS NOT NULL"
            ).fetchone()[0]
            if has_old > 0:
                conn.execute(
                    "UPDATE tumor_preop SET c_histology1 = c_histology "
                    "WHERE c_histology IS NOT NULL AND c_histology1 IS NULL"
                )
        except Exception:
            pass

        # v5.0 データ移行: 高血圧・糖尿病の多値→サブカラム分離
        # comor_hypertension > 1 → comor_ht_treatment に退避、親を 1 に
        # comor_diabetes > 1 → comor_dm_treatment に退避、親を 1 に
        _multivalue_to_sub = [
            ("comor_hypertension", "comor_ht_treatment"),
            ("comor_diabetes",     "comor_dm_treatment"),
        ]
        for parent_col, sub_col in _multivalue_to_sub:
            try:
                conn.execute(f"""
                    UPDATE patients SET
                        {sub_col} = {parent_col},
                        {parent_col} = 1
                    WHERE {parent_col} > 1
                      AND ({sub_col} IS NULL OR {sub_col} = 0)
                """)
            except Exception:
                pass

        # v4.0 データ移行: Phase2 → Phase3 への承認状態引き継ぎ
        # 旧 phase2_* カラムが存在する場合、Phase3 へ移行
        try:
            # phase2_statusカラムが存在するか確認
            cols = [r[1] for r in conn.execute("PRAGMA table_info(patients)").fetchall()]
            if "phase2_status" in cols and "phase3_status" in cols:
                conn.execute("""
                    UPDATE patients SET
                        phase3_status       = phase2_status,
                        phase3_submitted_at = phase2_submitted_at,
                        phase3_submitted_by = phase2_submitted_by,
                        phase3_approved_at  = phase2_approved_at,
                        phase3_approved_by  = phase2_approved_by
                    WHERE phase2_status IS NOT NULL
                      AND phase2_status != 'draft'
                      AND (phase3_status IS NULL OR phase3_status = 'draft')
                """)
        except Exception:
            pass

        # v7.1 データ移行: 手術既往・人工呼吸器 旧カラム → comor_ フラグに統合
        _legacy_to_comor = [
            ("prior_cardiac_surgery_yn",    "comor_prior_cardiac_surgery"),
            ("prior_abdominal_surgery_yn",  "comor_prior_abdominal_surgery"),
            ("preop_ventilator_yn",         "comor_preop_ventilator"),
        ]
        for old_col, new_col in _legacy_to_comor:
            try:
                conn.execute(f"""
                    UPDATE patients SET
                        {new_col} = {old_col}
                    WHERE {old_col} = 1
                      AND ({new_col} IS NULL OR {new_col} = 0)
                """)
            except Exception:
                pass

        # マイグレーション完了後: 全インデックスを再試行
        _post_migration_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_patients_deleted ON patients(is_deleted)",
            "CREATE INDEX IF NOT EXISTS idx_patients_study ON patients(study_id)",
            "CREATE INDEX IF NOT EXISTS idx_patients_surgery ON patients(surgery_date)",
            "CREATE INDEX IF NOT EXISTS idx_patients_status ON patients(data_status)",
            "CREATE INDEX IF NOT EXISTS idx_patients_disease ON patients(disease_class)",
            # PERF: 集計クエリ高速化用の複合インデックス
            "CREATE INDEX IF NOT EXISTS idx_patients_disease_surgery ON patients(disease_class, surgery_date)",
        ]
        for _idx_sql in _post_migration_indexes:
            try:
                conn.execute(_idx_sql)
            except Exception:
                pass

    print("✅ Database initialized successfully.")


# ---------------------------------------------------------------------------
# CRUD ヘルパー
# ---------------------------------------------------------------------------
def soft_delete_patient(conn, patient_id, user_id=None):
    """患者レコードを論理削除する（is_deleted=1）。物理削除は行わない。"""
    conn.execute(
        "UPDATE patients SET is_deleted = 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (patient_id,),
    )
    if user_id:
        log_audit(conn, user_id, "SOFT_DELETE", "patients", patient_id)


def restore_patient(conn, patient_id, user_id=None):
    """論理削除された患者レコードを復元する。"""
    conn.execute(
        "UPDATE patients SET is_deleted = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (patient_id,),
    )
    if user_id:
        log_audit(conn, user_id, "RESTORE", "patients", patient_id)


# ---------------------------------------------------------------------------
# Outcome スナップショット（Phase3/4 承認時にアウトカムを凍結保存）
# ---------------------------------------------------------------------------
_OUTCOME_SNAPSHOT_COLS = [
    "mortality_30d", "mortality_inhospital", "recurrence_yn", "recurrence_date",
    "vital_status", "last_alive_date", "death_date", "death_cause",
    "death_cause_other", "outcome_detail",
    "rec_peritoneal", "rec_liver", "rec_lung", "rec_lymph_node", "rec_local",
    "rec_bone", "rec_brain", "rec_ovary", "rec_adrenal", "rec_other",
    "rec_other_detail",
]


def create_outcome_snapshot(conn, patient_id, phase, user_id=None):
    """outcome テーブルの現在データを outcome_snapshots に凍結コピーする。

    Args:
        phase: "phase3" or "phase4"
    Returns:
        snapshot_id (int) or None if outcome data not found
    """
    outcome = conn.execute(
        "SELECT * FROM outcome WHERE patient_id=?", (patient_id,)
    ).fetchone()
    if not outcome:
        return None

    now = datetime.now().isoformat()
    cols = ["patient_id", "phase", "snapshot_date", "approved_by"] + _OUTCOME_SNAPSHOT_COLS
    vals = [patient_id, phase, now, user_id] + [outcome[c] for c in _OUTCOME_SNAPSHOT_COLS]
    placeholders = ", ".join("?" for _ in cols)
    col_str = ", ".join(cols)

    # UPSERT: 同一 patient_id + phase の場合は上書き
    cur = conn.execute(
        f"INSERT INTO outcome_snapshots ({col_str}) VALUES ({placeholders}) "
        f"ON CONFLICT(patient_id, phase) DO UPDATE SET "
        + ", ".join(f"{c}=excluded.{c}" for c in
                    ["snapshot_date", "approved_by"] + _OUTCOME_SNAPSHOT_COLS),
        vals,
    )
    if user_id:
        log_audit(conn, user_id, "SNAPSHOT", "outcome_snapshots", patient_id,
                  field_name="phase", new_value=phase)
    return cur.lastrowid


# ---------------------------------------------------------------------------
# 1:N テーブル レコード単位ロック
# ---------------------------------------------------------------------------
def lock_existing_rows(conn, table, patient_id, phase):
    """1:Nテーブルの既存レコードに locked_by_phase を設定する。
    すでにロック済み(他フェーズ)のレコードは上書きしない。"""
    conn.execute(
        f"UPDATE {table} SET locked_by_phase = ? "
        f"WHERE patient_id = ? AND (locked_by_phase IS NULL OR locked_by_phase = '')",
        (phase, patient_id),
    )


def unlock_rows(conn, table, patient_id, phase):
    """指定フェーズでロックされたレコードのロックを解除する。"""
    conn.execute(
        f"UPDATE {table} SET locked_by_phase = NULL "
        f"WHERE patient_id = ? AND locked_by_phase = ?",
        (patient_id, phase),
    )


# ---------------------------------------------------------------------------
# リマインド通知チェック
# ---------------------------------------------------------------------------
def get_phase_reminders(conn):
    """Phase3/4 の提出・承認リマインドが必要な症例リストを返す。

    判定基準:
        Phase3 提出リマインド: surgery_date + 3年3ヶ月 ≤ 今日 AND phase3_status = 'draft'
        Phase3 承認リマインド: surgery_date + 3年6ヶ月 ≤ 今日 AND phase3_status = 'submitted'
        Phase4 提出リマインド: surgery_date + 5年3ヶ月 ≤ 今日 AND phase4_status = 'draft'
        Phase4 承認リマインド: surgery_date + 5年6ヶ月 ≤ 今日 AND phase4_status = 'submitted'

    Returns:
        list of dict: [{patient_id, study_id, surgery_date, reminder_type, phase}, ...]
    """
    reminders = []
    today = datetime.now().strftime("%Y-%m-%d")

    # Phase3 提出リマインド（術後3年3ヶ月）
    rows = conn.execute("""
        SELECT id, study_id, surgery_date FROM patients
        WHERE is_deleted = 0
          AND phase3_status = 'draft'
          AND surgery_date IS NOT NULL
          AND date(surgery_date, '+3 years', '+3 months') <= date(?)
    """, (today,)).fetchall()
    for r in rows:
        reminders.append({
            "patient_id": r["id"], "study_id": r["study_id"],
            "surgery_date": r["surgery_date"],
            "reminder_type": "submit", "phase": "phase3",
        })

    # Phase3 承認リマインド（術後3年6ヶ月）
    rows = conn.execute("""
        SELECT id, study_id, surgery_date FROM patients
        WHERE is_deleted = 0
          AND phase3_status = 'submitted'
          AND surgery_date IS NOT NULL
          AND date(surgery_date, '+3 years', '+6 months') <= date(?)
    """, (today,)).fetchall()
    for r in rows:
        reminders.append({
            "patient_id": r["id"], "study_id": r["study_id"],
            "surgery_date": r["surgery_date"],
            "reminder_type": "approve", "phase": "phase3",
        })

    # Phase4 提出リマインド（術後5年3ヶ月）
    rows = conn.execute("""
        SELECT id, study_id, surgery_date FROM patients
        WHERE is_deleted = 0
          AND phase4_status = 'draft'
          AND surgery_date IS NOT NULL
          AND date(surgery_date, '+5 years', '+3 months') <= date(?)
    """, (today,)).fetchall()
    for r in rows:
        reminders.append({
            "patient_id": r["id"], "study_id": r["study_id"],
            "surgery_date": r["surgery_date"],
            "reminder_type": "submit", "phase": "phase4",
        })

    # Phase4 承認リマインド（術後5年6ヶ月）
    rows = conn.execute("""
        SELECT id, study_id, surgery_date FROM patients
        WHERE is_deleted = 0
          AND phase4_status = 'submitted'
          AND surgery_date IS NOT NULL
          AND date(surgery_date, '+5 years', '+6 months') <= date(?)
    """, (today,)).fetchall()
    for r in rows:
        reminders.append({
            "patient_id": r["id"], "study_id": r["study_id"],
            "surgery_date": r["surgery_date"],
            "reminder_type": "approve", "phase": "phase4",
        })

    return reminders


class OptimisticLockError(Exception):
    """楽観的ロック競合エラー。他のユーザーが先に更新した場合に発生。"""
    pass


def upsert_record(conn, table, patient_id, data: dict, user_id=None,
                   expected_updated_at=None):
    """患者IDに紐づくレコードをINSERT or UPDATE。変更フィールドのaudit_logも記録。

    Args:
        expected_updated_at: 読み込み時の updated_at 値。
            指定されている場合、UPDATE 時に楽観的ロック（同時編集検出）を行う。
            DB上の updated_at が異なれば OptimisticLockError を送出。
    """
    existing = conn.execute(
        f"SELECT * FROM {table} WHERE patient_id=?", (patient_id,)
    ).fetchone()

    # NULL / 空文字の正規化
    cleaned = {}
    for k, v in data.items():
        if isinstance(v, str) and v.strip() == "":
            cleaned[k] = None
        else:
            cleaned[k] = v

    if existing:
        # 楽観的ロック: updated_at が読み込み時と一致するか検証
        if expected_updated_at is not None:
            db_updated_at = existing["updated_at"]
            if db_updated_at and str(db_updated_at) != str(expected_updated_at):
                raise OptimisticLockError(
                    f"テーブル '{table}' (patient_id={patient_id}) は "
                    f"他のユーザーによって更新されています。"
                    f"（読み込み時: {expected_updated_at}, 現在: {db_updated_at}）"
                    f"\nページを再読み込みして最新データを取得してください。"
                )

        # UPDATE — 変更があるフィールドのみ
        changes = {}
        for k, v in cleaned.items():
            if k in ("id", "patient_id"):
                continue
            old_val = existing[k] if k in existing.keys() else None
            if str(old_val) != str(v) if old_val is not None or v is not None else False:
                if old_val != v:
                    changes[k] = (old_val, v)

        if changes:
            set_clause = ", ".join(f"{k}=?" for k in changes)
            values = [v[1] for v in changes.values()]
            now_ts = datetime.now().isoformat()
            values.extend([now_ts, patient_id])
            conn.execute(
                f"UPDATE {table} SET {set_clause}, updated_at=? WHERE patient_id=?",
                values,
            )
            # 監査ログ
            if user_id:
                for field, (old_v, new_v) in changes.items():
                    log_audit(conn, user_id, "UPDATE", table, patient_id,
                              field, old_v, new_v)
        return existing["id"]
    else:
        # INSERT
        cleaned["patient_id"] = patient_id
        cols = ", ".join(cleaned.keys())
        placeholders = ", ".join("?" for _ in cleaned)
        cur = conn.execute(
            f"INSERT INTO {table} ({cols}) VALUES ({placeholders})",
            list(cleaned.values()),
        )
        if user_id:
            log_audit(conn, user_id, "INSERT", table, cur.lastrowid)
        return cur.lastrowid


# ---------------------------------------------------------------------------
# NCD年度バージョン管理
# ---------------------------------------------------------------------------
def _seed_ncd_version(conn, year, version_name):
    """NCD年度バージョンとフィールド定義のシードデータを投入する。"""
    conn.execute(
        "INSERT OR IGNORE INTO ncd_versions (year, version, is_active, notes) "
        "VALUES (?, ?, 1, ?)",
        (year, version_name, f"NCD {year}年度版")
    )
    ver_id = conn.execute(
        "SELECT id FROM ncd_versions WHERE year=?", (year,)
    ).fetchone()["id"]

    # 現行 NCD_COLUMNS に基づくフィールド定義を投入
    # (ncd_field_name, level, is_required, db_table, db_column, converter, sort_order)
    _fields = [
        # L0: 共通
        ("施設コード",        "L0", 0, "patients", "facility_id",      None, 1),
        ("NCD症例ID",        "L0", 0, "patients", "ncd_case_id",       None, 2),
        ("性別",             "L0", 1, "patients", "sex",              "_sex", 3),
        ("生年月日",          "L0", 1, "patients", "birthdate",        None, 4),
        ("身長",             "L0", 1, "patients", "height_cm",        None, 5),
        ("体重",             "L0", 1, "patients", "weight_admission", None, 6),
        ("入院日",           "L0", 1, "patients", "admission_date",   None, 7),
        ("手術日",           "L0", 1, "patients", "surgery_date",     None, 8),
        ("退院日",           "L0", 1, "patients", "discharge_date",   None, 9),
        # L1: 外科共通
        ("ASA",              "L1", 1, "patients", "asa",              "_asa", 10),
        ("PS",               "L1", 1, "patients", "ps",               None, 11),
        ("術直前ADL",          "L1", 1, "patients", "adl_status",      "codebook:adl_status", 11.5),
        ("緊急手術",          "L1", 0, "surgery",  "op_emergency",    "_emergency", 12),
        ("手術時間",          "L1", 1, "surgery",  "op_time_min",     None, 13),
        ("出血量",           "L1", 1, "surgery",  "op_blood_loss_ml", None, 14),
        ("術前72h以内輸血",   "L1", 0, "surgery",  "op_transfusion_preop", "_yn", 14),
        ("術中輸血",          "L1", 0, "surgery",  "op_transfusion_intra", "_yn", 15),
        ("術後輸血",          "L1", 0, "surgery",  "op_transfusion_post",  "_yn", 16),
        ("ICU入室日数",       "L1", 0, "surgery",  "op_icu_days",     None, 17),
        # L1: 併存疾患
        ("喫煙",             "L1", 1, "patients", "smoking",          "_yn", 20),
        ("糖尿病",           "L1", 1, "patients", "comor_diabetes",   "_diabetes_ncd", 21),
        ("高血圧",           "L1", 1, "patients", "comor_hypertension", "_hypertension_ncd", 22),
        ("心血管疾患",        "L1", 0, "patients", "comor_cardiovascular", "_yn", 23),
        ("虚血性心疾患",      "L1", 0, "patients", "comor_ihd",       "_yn", 24),
        ("心不全",           "L1", 0, "patients", "comor_chf",        "_yn", 25),
        ("不整脈",           "L1", 0, "patients", "comor_arrhythmia", "_yn", 26),
        ("弁膜症",           "L1", 0, "patients", "comor_valvular",   "_yn", 27),
        ("大動脈疾患",        "L1", 0, "patients", "comor_aortic",    "_yn", 28),
        ("末梢血管疾患",      "L1", 0, "patients", "comor_pvd",       "_yn", 29),
        ("脳血管障害",        "L1", 0, "patients", "comor_cerebrovascular", "_yn", 30),
        ("脳梗塞",           "L1", 0, "patients", "comor_cerebral_infarction", "_yn", 31),
        ("脳出血",           "L1", 0, "patients", "comor_cerebral_hemorrhage", "_yn", 32),
        ("TIA",              "L1", 0, "patients", "comor_tia",        "_yn", 33),
        ("くも膜下出血",      "L1", 0, "patients", "comor_sah",       "_yn", 34),
        ("呼吸器疾患",        "L1", 0, "patients", "comor_respiratory", "_yn", 35),
        ("透析",             "L1", 0, "patients", "comor_renal_dialysis", "_yn", 36),
        ("肝疾患",           "L1", 0, "patients", "comor_hepatic",    "_yn", 37),
        # L2: 消化器外科
        ("術式",             "L2", 1, "surgery",  "op_procedure",     "codebook:op_procedure", 40),
        ("到達法",           "L2", 1, "surgery",  "op_approach",      "codebook:op_approach", 41),
        ("麻酔法",           "L2", 0, "surgery",  "op_anesthesia_type", "codebook:op_anesthesia_type", 42),
        ("郭清度",           "L2", 0, "surgery",  "op_dissection",    "codebook:op_dissection", 43),
        ("再建法",           "L2", 0, "surgery",  "op_reconstruction", "codebook:op_reconstruction", 44),
        # L2: 合併症
        ("合併症あり",        "L2", 0, "surgery",  "op_complication_yn", "_yn", 50),
        ("SSI_表層",         "L2", 0, "surgery",  "comp_ssi_superficial", "_yn", 51),
        ("SSI_深部",         "L2", 0, "surgery",  "comp_ssi_deep",    "_yn", 52),
        ("SSI_臓器体腔",     "L2", 0, "surgery",  "comp_ssi_organ",   "_yn", 53),
        ("DVT",              "L2", 0, "surgery",  "comp_dvt",         "_yn", 54),
        ("PE",               "L2", 0, "surgery",  "comp_pe",          "_yn", 55),
        ("肺炎",             "L2", 0, "surgery",  "comp_pneumonia",   "_yn", 56),
        ("縫合不全",          "L2", 0, "surgery",  "comp_anastomotic_leak", "_yn", 57),
        ("膵液瘻",           "L2", 0, "surgery",  "comp_pancreatic_fistula", "_yn", 58),
        ("敗血症",           "L2", 0, "surgery",  "comp_sepsis",      "_yn", 59),
        ("敗血症性ショック",   "L2", 0, "surgery",  "comp_septic_shock", "_yn", 60),
        ("イレウス",          "L2", 0, "surgery",  "comp_ileus",       "_yn", 61),
        ("再手術",           "L2", 0, "surgery",  "op_reop_yn",       "_yn", 62),
        ("30日再入院",        "L2", 0, "surgery",  "readmission_30d",  "_yn", 63),
        # L3: アウトカム
        ("30日死亡",          "L3", 0, "outcome",  "mortality_30d",    "_yn", 70),
        ("在院死亡",          "L3", 0, "outcome",  "mortality_inhospital", "_yn", 71),
        ("退院先",           "L3", 0, "patients", "discharge_destination", "codebook:discharge_destination", 72),
        # L3: 病理
        ("残存腫瘍",          "L3", 0, "pathology", "p_residual_tumor", "codebook:residual_tumor", 80),
    ]

    for (ncd_name, level, required, tbl, col, conv, sort) in _fields:
        conn.execute(
            """INSERT OR IGNORE INTO ncd_field_defs
               (ncd_version_id, ncd_field_name, level, is_required,
                db_table, db_column, converter, sort_order)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (ver_id, ncd_name, level, required, tbl, col, conv, sort)
        )


def get_ncd_versions():
    """有効なNCDバージョン一覧を返す。"""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, year, version, is_active, notes FROM ncd_versions ORDER BY year DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def get_ncd_field_defs(ncd_version_id):
    """指定バージョンのNCDフィールド定義を返す（sort_order順）。"""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT ncd_field_name, level, is_required, db_table, db_column, converter, sort_order
               FROM ncd_field_defs
               WHERE ncd_version_id = ?
               ORDER BY sort_order""",
            (ncd_version_id,)
        ).fetchall()
        return [dict(r) for r in rows]


def add_ncd_version(year, version_name, copy_from_year=None):
    """新しいNCD年度バージョンを追加する。

    Args:
        year: 年度
        version_name: バージョン名
        copy_from_year: 既存年度からフィールド定義をコピーする場合に指定

    Returns:
        int: 新バージョンのID
    """
    with get_db() as conn:
        if copy_from_year:
            # 既存年度からコピー
            src = conn.execute(
                "SELECT id FROM ncd_versions WHERE year=?", (copy_from_year,)
            ).fetchone()
            if not src:
                raise ValueError(f"コピー元の年度 {copy_from_year} が見つかりません")

            conn.execute(
                "INSERT INTO ncd_versions (year, version, is_active, notes) VALUES (?, ?, 1, ?)",
                (year, version_name, f"NCD {year}年度版（{copy_from_year}年度からコピー）")
            )
            new_id = conn.execute(
                "SELECT id FROM ncd_versions WHERE year=?", (year,)
            ).fetchone()["id"]

            # フィールド定義コピー
            conn.execute(
                """INSERT INTO ncd_field_defs
                   (ncd_version_id, ncd_field_name, level, is_required,
                    db_table, db_column, converter, sort_order)
                   SELECT ?, ncd_field_name, level, is_required,
                          db_table, db_column, converter, sort_order
                   FROM ncd_field_defs WHERE ncd_version_id = ?""",
                (new_id, src["id"])
            )
        else:
            _seed_ncd_version(conn, year, version_name)
            new_id = conn.execute(
                "SELECT id FROM ncd_versions WHERE year=?", (year,)
            ).fetchone()["id"]

    return new_id


# ---------------------------------------------------------------------------
# 手動バックアップ
# ---------------------------------------------------------------------------
BACKUP_DIR = os.environ.get("UGI_BACKUP_DIR", "backups")


def backup_database(user_id=None, tag="manual"):
    """SQLite DBファイルのバックアップを作成する。

    Args:
        user_id: 実行ユーザーID（audit_log用）
        tag: バックアップタグ（"manual", "scheduled" 等）

    Returns:
        (success: bool, backup_path_or_error: str)
    """
    db_path = DB_PATH
    if not os.path.isfile(db_path):
        return False, f"DBファイルが見つかりません: {db_path}"

    os.makedirs(BACKUP_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"ugi_backup_{tag}_{timestamp}.db"
    backup_path = os.path.join(BACKUP_DIR, backup_name)

    try:
        # SQLite online backup API（安全なコピー）
        src = sqlite3.connect(db_path)
        dst = sqlite3.connect(backup_path)
        src.backup(dst)
        dst.close()
        src.close()

        # バックアップサイズ
        size_bytes = os.path.getsize(backup_path)

        # audit_log 記録
        if user_id:
            with get_db() as conn:
                log_audit(conn, user_id, "BACKUP", comment=f"{backup_name} ({size_bytes:,} bytes)")

        return True, backup_path

    except Exception as e:
        # 失敗時は不完全ファイルを削除
        if os.path.exists(backup_path):
            os.remove(backup_path)
        return False, str(e)


def list_backups(limit=20):
    """バックアップファイル一覧を返す（新しい順）。

    Returns:
        list[dict]: [{"filename": str, "size_bytes": int, "created": str}, ...]
    """
    if not os.path.isdir(BACKUP_DIR):
        return []

    backups = []
    for f in os.listdir(BACKUP_DIR):
        if f.startswith("ugi_backup_") and f.endswith(".db"):
            fpath = os.path.join(BACKUP_DIR, f)
            stat = os.stat(fpath)
            backups.append({
                "filename": f,
                "size_bytes": stat.st_size,
                "created": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            })

    backups.sort(key=lambda x: x["created"], reverse=True)
    return backups[:limit]


def delete_old_backups(keep_count=10):
    """古いバックアップを削除し、最新 keep_count 件のみ保持する。

    Returns:
        int: 削除したファイル数
    """
    all_backups = list_backups(limit=9999)
    if len(all_backups) <= keep_count:
        return 0

    to_delete = all_backups[keep_count:]
    deleted = 0
    for b in to_delete:
        fpath = os.path.join(BACKUP_DIR, b["filename"])
        try:
            os.remove(fpath)
            deleted += 1
        except OSError:
            pass
    return deleted


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    init_db()
