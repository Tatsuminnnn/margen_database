"""
上部消化管グループ 症例登録データベース v2
Streamlit Web Application

起動方法:
    streamlit run app.py
"""
import os
import streamlit as st
import pandas as pd
import hashlib
import json
from datetime import datetime, date, timedelta
from database import (
    init_db, get_db, authenticate, generate_study_id,
    log_audit, hash_password, upsert_record,
    encrypt_value, decrypt_value, ENCRYPTED_COLUMNS,
    create_outcome_snapshot, lock_existing_rows, unlock_rows,
    get_phase_reminders,
    backup_database, list_backups, delete_old_backups,
    get_ncd_versions, get_ncd_field_defs, add_ncd_version,
)
from csv_import import generate_import_template, validate_csv, import_csv_records
from required_fields import validate_phase1_submission, get_required_fields, get_requirement_matrix
from lab_reader import (
    extract_lab_values, check_vision_model, judge_lab_values,
    map_to_existing_fields, LAB_LABELS,
)
from codebook import get_codebook, populate_codebook, compute_stage, get_form_label, get_all_column_labels
from analytics import render_analytics_dashboard
from smart_query import ask as smart_ask, check_llm_connection, EXAMPLE_QUESTIONS

# ============================================================
# 初期設定
# ============================================================
st.set_page_config(
    page_title="上部消化管症例登録DB",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)
init_db()
populate_codebook()  # 起動時に CODEBOOK dict と DB を同期 (upsert)

STATUS_OPTIONS = {"draft": "📝 下書き", "submitted": "📤 提出済",
                  "verified": "✅ 確認済", "approved": "🔒 承認済"}
PHASE_STATUS = {"draft": "📝 下書き", "submitted": "📤 提出済", "approved": "🔒 承認済"}
PHASE_LABELS = {
    "phase1": "Phase 1（周術期）",
    "phase3": "Phase 3（術後3年）",
    "phase4": "Phase 4（術後5年）",
}

# Phase 1 対象テーブル（これらのタブは phase1_status で制御）
PHASE1_TABLES = {"patients", "tumor_preop", "neoadjuvant", "surgery",
                 "pathology", "lymph_nodes", "gist_detail"}
# Phase 3 対象テーブル（旧Phase2の内容 + Phase1以降のテーブル）
PHASE3_TABLES = {"adjuvant_chemo", "outcome", "palliative_chemo", "tumor_markers",
                 "radiation_therapy"}
# Phase 4 対象テーブル（Phase3と同じ範囲、スナップショット取得用）
PHASE4_TABLES = PHASE3_TABLES.copy()


# ============================================================
# 個人情報の暗号化/復号ヘルパー
# ============================================================
def _encrypt_patient_data(data_dict):
    """保存前に個人情報カラムを暗号化する。"""
    for col in ENCRYPTED_COLUMNS:
        if col in data_dict and data_dict[col] is not None:
            data_dict[col] = encrypt_value(data_dict[col])
    return data_dict

def _decrypt_patient_data(data_dict):
    """読み込み後に個人情報カラムを復号する。"""
    for col in ENCRYPTED_COLUMNS:
        if col in data_dict and data_dict[col] is not None:
            data_dict[col] = decrypt_value(data_dict[col])
    return data_dict


# ============================================================
# 通知ヘルパー
# ============================================================
def _get_unread_count(user_id):
    """未読通知件数を返す。"""
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM notifications WHERE user_id=? AND is_read=0",
            (user_id,)
        ).fetchone()
        return row[0] if row else 0

def _create_notification(conn, user_id, title, message="", link_page=None, link_study_id=None):
    """アプリ内通知を作成し、ユーザーの設定に応じて LINE / Email も送信する。"""
    # アプリ内通知
    conn.execute(
        "INSERT INTO notifications (user_id, title, message, link_page, link_study_id) "
        "VALUES (?,?,?,?,?)",
        (user_id, title, message, link_page, link_study_id)
    )
    # LINE / Email 外部通知
    try:
        ns = conn.execute(
            "SELECT line_user_id, email_address "
            "FROM notification_settings WHERE user_id = ?", (user_id,)
        ).fetchone()
        if ns:
            full_msg = f"{title}\n{message}" if message else title
            line_uid, email = ns[0], ns[1]
            if line_uid:
                _send_line_message(line_uid, full_msg)
            if email:
                try:
                    import smtplib, os as _os
                    from email.mime.text import MIMEText
                    smtp_host = _os.environ.get("UGI_SMTP_HOST", "")
                    smtp_user = _os.environ.get("UGI_SMTP_USER", "")
                    if smtp_host and smtp_user:
                        smtp_port = int(_os.environ.get("UGI_SMTP_PORT", "587"))
                        smtp_pass = _os.environ.get("UGI_SMTP_PASS", "")
                        from_addr = _os.environ.get("UGI_SMTP_FROM", smtp_user)
                        em = MIMEText(full_msg, "plain", "utf-8")
                        em["Subject"] = f"[UGI-DB] {title}"
                        em["From"] = from_addr
                        em["To"] = email
                        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as sv:
                            sv.starttls()
                            sv.login(smtp_user, smtp_pass)
                            sv.send_message(em)
                except Exception:
                    pass  # Email 送信失敗はサイレントに処理
    except Exception:
        pass  # 通知設定テーブル未存在などの場合はスキップ

def _send_line_message(user_id_line, message):
    """LINE Messaging API でプッシュメッセージを送信する。
    チャネルアクセストークンは環境変数 UGI_LINE_CHANNEL_TOKEN から取得。
    user_id_line:  送信先の LINE ユーザーID（U で始まる33文字）
    """
    import os as _os
    channel_token = _os.environ.get("UGI_LINE_CHANNEL_TOKEN", "")
    if not channel_token:
        return False
    try:
        import requests
        resp = requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {channel_token}",
            },
            json={
                "to": user_id_line,
                "messages": [{"type": "text", "text": message}],
            },
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False

# ============================================================
# カラーパレット（セクションカード用）
# ============================================================
CARD_COLORS = {
    "blue":   ("#1565C0", "#E3F2FD"),
    "green":  ("#2E7D32", "#E8F5E9"),
    "orange": ("#E65100", "#FFF3E0"),
    "purple": ("#6A1B9A", "#F3E5F5"),
    "red":    ("#C62828", "#FFEBEE"),
    "teal":   ("#00695C", "#E0F2F1"),
    "indigo": ("#283593", "#E8EAF6"),
    "brown":  ("#4E342E", "#EFEBE9"),
}

def section_card(title: str, color: str = "blue"):
    """セクション見出しを背景塗りつぶし（枠線なし）カードで表示。"""
    border, bg = CARD_COLORS.get(color, CARD_COLORS["blue"])
    st.markdown(
        f'<div style="background:{bg};'
        f'padding:10px 16px;border-radius:8px;margin:18px 0 8px 0;">'
        f'<span style="color:{border};font-weight:bold;font-size:15px;">{title}</span></div>',
        unsafe_allow_html=True,
    )

# ============================================================
# 参照テーブル表示ヘルパー
# ============================================================

def ref_table(title: str, headers: list, rows: list, note: str = None):
    """
    コンパクトな参照テーブルをインラインHTMLで表示する。
    入力フィールドの下や横の余白に定義・分類基準を示す用途。
    """
    ths = "".join(
        f'<th style="background:#e8ecf0;font-size:11px;padding:3px 6px;'
        f'text-align:left;border:1px solid #ccc;white-space:nowrap;">{h}</th>'
        for h in headers
    )
    body = ""
    for i, row in enumerate(rows):
        bg = "#fff" if i % 2 == 0 else "#f7f9fb"
        tds = "".join(
            f'<td style="background:{bg};font-size:11px;padding:3px 6px;'
            f'border:1px solid #ccc;vertical-align:top;line-height:1.35;">{c}</td>'
            for c in row
        )
        body += f"<tr>{tds}</tr>"
    title_html = (
        f'<div style="font-size:11px;font-weight:bold;color:#4a5568;'
        f'margin-bottom:4px;">{title}</div>' if title else ""
    )
    note_html = (
        f'<div style="font-size:10px;color:#999;margin-top:3px;">{note}</div>'
        if note else ""
    )
    st.markdown(
        f'<div style="background:#f0f4f8;border-radius:5px;padding:6px 8px;margin:4px 0 10px 0;">'
        f'{title_html}'
        f'<table style="border-collapse:collapse;width:100%;">'
        f'<thead><tr>{ths}</tr></thead>'
        f'<tbody>{body}</tbody>'
        f'</table>{note_html}</div>',
        unsafe_allow_html=True,
    )

# ============================================================
# UIヘルパー関数
# ============================================================

def selectbox_select(label, options_dict, key, default=None, include_blank=True, help_text=None):
    """コードブック辞書からセレクトボックスを生成。"""
    if include_blank:
        opts = ["---"] + list(options_dict.values())
        keys = [None] + list(options_dict.keys())
    else:
        opts = list(options_dict.values())
        keys = list(options_dict.keys())
    idx = keys.index(default) if default in keys else 0
    selected = st.selectbox(label, opts, index=idx, key=key, help=help_text)
    return keys[opts.index(selected)]


def selectbox_with_other(label, options_dict, key, other_key,
                         default=None, other_default=""):
    """「その他」選択時にテキスト入力が出る selectbox。"""
    val = selectbox_select(label, options_dict, key, default)
    other_val = None
    if val is not None:
        selected_label = options_dict.get(val, "")
        if "その他" in selected_label or "Other" in selected_label:
            other_val = st.text_input(f"{label}（詳細）", value=other_default, key=other_key)
    return val, other_val


def numeric_input(label, key, default=None, suffix="", min_val=None, max_val=None,
                  is_float=False, help_text=None):
    """
    NULL対応の数値入力。st.text_input ベースで、空欄=NULL, 0=0 を区別する。
    全角数字は自動で半角変換し赤警告を表示する。
    """
    _FULLWIDTH = str.maketrans('０１２３４５６７８９．－', '0123456789.-')
    display_label = f"{label} ({suffix})" if suffix else label
    default_str = "" if default is None else str(default)
    raw = st.text_input(display_label, value=default_str, key=key, help=help_text)
    if raw.strip() == "":
        return None
    converted = raw.translate(_FULLWIDTH)
    if converted != raw:
        st.markdown(
            '<p style="color:#e53935;font-size:12px;margin-top:-12px;">'
            '⚠ 全角文字が含まれています。半角数字で入力してください。</p>',
            unsafe_allow_html=True,
        )
        raw = converted
    try:
        return float(raw) if is_float else int(raw)
    except ValueError:
        st.markdown(
            f'<p style="color:#e53935;font-size:12px;margin-top:-12px;">'
            f'⚠ {label}: 半角数字で入力してください</p>',
            unsafe_allow_html=True,
        )
        return None


# ---- 占居部位 multiselect ヘルパー ----
# 長軸は表示順 E→U→M→L→D→残胃吻合部 で結合
_LOC_LONG_ORDER = {1: "U", 2: "M", 3: "L", 4: "T", 5: "E", 6: "D", 7: "残胃吻合部", 99: "不明"}
_LOC_LONG_SORT  = [5, 1, 2, 3, 4, 6, 7, 99]   # E > U > M > L > T > D > 残胃吻合部 > 不明
_LOC_SHORT_ORDER = {1: "小弯", 2: "大弯", 3: "前壁", 4: "後壁", 5: "全周", 99: "不明"}

def _parse_location_codes(stored_text, code_map):
    """DBに保存された文字列 → 選択済みラベルリストを保存順で復元。"""
    if not stored_text:
        return []
    stored = str(stored_text)
    # 保存文字列中の各コードの出現位置でソート → 選択順を復元
    found = []
    for code, label in code_map.items():
        short = label.split(" ")[0] if " " in label else label
        pos = stored.find(short)
        if pos == -1:
            pos = stored.find(label)
        if pos >= 0:
            found.append((pos, label))
    found.sort(key=lambda x: x[0])
    return [lab for _, lab in found]

def location_multiselect(label, code_map, sort_order, key, stored_value,
                         combine_fn=None):
    """占居部位用 multiselect。選択した順番で短縮表記を結合して返す。"""
    all_labels = [code_map[c] for c in sort_order if c in code_map]
    defaults = _parse_location_codes(stored_value, code_map)
    valid_defaults = [d for d in defaults if d in all_labels]
    selected = st.multiselect(label, all_labels, default=valid_defaults, key=key)
    if not selected:
        return None
    if combine_fn:
        return combine_fn(selected)
    # ユーザーの選択順（multiselect は選択順リストを返す）で連結
    parts = []
    for lab in selected:
        parts.append(lab.split(" ")[0] if " " in lab else lab)
    return "".join(parts)

def _combine_short(selected):
    """短軸: 全周が含まれていれば「全周」のみ返す。選択順を保持。"""
    labels = [s.split(" ")[0] if " " in s else s for s in selected]
    if "全周" in labels:
        return "全周"
    return ", ".join(labels)


def flag_group(label, flags, prefix, data, key_prefix, confirmed_key=None,
               show_allnone=True, help_dict=None):
    """
    3-state フラグ入力グループ。
    flags: [(column_suffix, display_label), ...]
    prefix: DB列名プレフィックス (例: "comor_")
    data: 既存データ dict
    confirmed_key: "すべて該当なし" チェックの session key
    show_allnone: False なら「すべて該当なし」チェックを非表示
    help_dict: {suffix: "ツールチップ文"} の辞書（任意）

    戻り値: {column_name: 0/1/2, ..., prefix+"confirmed": 0/1}
    """
    result = {}
    if label:
        st.markdown(f"**{label}**")

    confirmed_col = f"{prefix}confirmed"
    if show_allnone:
        existing_confirmed = data.get(confirmed_col, 0)
        all_none = st.checkbox(
            "すべて該当なし",
            value=bool(existing_confirmed),
            key=confirmed_key or f"{key_prefix}_confirmed"
        )
        result[confirmed_col] = 1 if all_none else 0
    else:
        all_none = False
        result[confirmed_col] = 0

    if not all_none:
        cols_per_row = 4
        for i in range(0, len(flags), cols_per_row):
            cols = st.columns(cols_per_row)
            for j, col in enumerate(cols):
                if i + j < len(flags):
                    suffix, flag_label = flags[i + j]
                    col_name = f"{prefix}{suffix}"
                    help_text = help_dict.get(suffix) if help_dict else None
                    with col:
                        val = st.checkbox(flag_label, value=bool(data.get(col_name, 0)),
                                          key=f"{key_prefix}_{suffix}",
                                          help=help_text)
                        result[col_name] = 1 if val else 0

        # その他
        other_col = f"{prefix}other"
        other_detail_col = f"{prefix}other_detail"
        if any(s == "other" for s, _ in flags):
            if result.get(other_col, 0):
                result[other_detail_col] = st.text_input(
                    f"{label} その他（詳細）",
                    value=data.get(other_detail_col, ""),
                    key=f"{key_prefix}_other_detail"
                )
    else:
        for suffix, _ in flags:
            result[f"{prefix}{suffix}"] = 0

    return result


def complication_group(comp_flags, data, surgery_date_str=None):
    """
    術後合併症入力グループ。各合併症に CDグレード + 発症日 + 処置内容 を付与。
    comp_flags: [(column_suffix, display_label), ...]
    data: 既存データ dict (surgery テーブル)
    戻り値: {comp_xxx: 0-7, comp_xxx_date: "YYYY-MM-DD",
             comp_xxx_tx: "処置内容", ...}
    """
    cd_opts = {0: "なし", 1: "I", 2: "II", 3: "IIIa", 4: "IIIb",
               5: "IVa", 6: "IVb", 7: "V(死亡)"}
    result = {}
    max_grade = 0

    # 手術日を date オブジェクトに変換（POD計算用）
    surgery_date_obj = None
    if surgery_date_str:
        try:
            surgery_date_obj = datetime.strptime(str(surgery_date_str), "%Y-%m-%d").date()
        except (ValueError, TypeError):
            pass

    for suffix, flag_label in comp_flags:
        col_name = f"comp_{suffix}"
        date_col = f"comp_{suffix}_date"
        tx_col   = f"comp_{suffix}_tx"
        existing_grade = data.get(col_name, 0) or 0

        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            grade = selectbox_select(
                flag_label, cd_opts, f"comp_{suffix}",
                default=existing_grade, include_blank=False)
        result[col_name] = grade if grade else 0

        if result[col_name] > 0:
            # 発症日（カレンダー選択）
            with col2:
                existing_date_str = data.get(date_col, "") or ""
                existing_date_val = None
                if existing_date_str:
                    try:
                        existing_date_val = datetime.strptime(
                            str(existing_date_str), "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        pass
                date_val = st.date_input(
                    "発症日", value=existing_date_val,
                    key=f"comp_{suffix}_dt", format="YYYY/MM/DD")
                result[date_col] = date_val.strftime("%Y-%m-%d") if date_val else None

            # POD 自動表示
            with col3:
                if surgery_date_obj and date_val:
                    pod = (date_val - surgery_date_obj).days
                    st.markdown(f"<br><b>POD {pod}</b>", unsafe_allow_html=True)

            # DGE / 膵液瘻: 専用グレード(ISGPS分類)
            if suffix in ("dge", "pancreatic_fistula"):
                _isgps_opts = {0: "なし", 1: "グレードA", 2: "グレードB", 3: "グレードC"}
                grade_col = f"comp_{suffix}_grade"
                result[grade_col] = selectbox_select(
                    f"{flag_label} グレード", _isgps_opts,
                    f"comp_{suffix}_gr", data.get(grade_col, 0) or 0,
                    include_blank=False)

            # 処置内容（自由記載）
            existing_tx = data.get(tx_col, "") or ""
            result[tx_col] = st.text_input(
                f"↳ {flag_label} の処置内容", value=existing_tx,
                key=f"comp_{suffix}_tx", placeholder="処置・治療の内容を記載")

            if result[col_name] > max_grade:
                max_grade = result[col_name]
        else:
            result[date_col] = None
            result[tx_col] = None

    # その他（detail）は別途処理
    if result.get("comp_other", 0) and result["comp_other"] > 0:
        result["comp_other_detail"] = st.text_input(
            "その他 合併症（詳細）", value=data.get("comp_other_detail", "") or "",
            key="comp_other_dtl")
    else:
        result["comp_other_detail"] = data.get("comp_other_detail")

    result["comp_confirmed"] = 1
    result["_max_cd_grade"] = max_grade
    return result


def calc_age(birthdate_str, surgery_date_str):
    """年齢計算"""
    if not birthdate_str or not surgery_date_str:
        return None
    try:
        bd = datetime.strptime(str(birthdate_str), "%Y-%m-%d")
        sd = datetime.strptime(str(surgery_date_str), "%Y-%m-%d")
        return sd.year - bd.year - ((sd.month, sd.day) < (bd.month, bd.day))
    except (ValueError, TypeError):
        return None


def load_patient_data(patient_db_id):
    """患者IDから全テーブルの既存データを読み込む。"""
    # ホワイトリスト: f-string SQL に渡すテーブル名を制限
    _ALLOWED_TABLES = frozenset([
        "patients", "tumor_preop", "neoadjuvant", "surgery",
        "pathology", "lymph_nodes", "gist_detail",
        "adjuvant_chemo", "outcome",
        "eso_tumor", "eso_surgery", "eso_course", "eso_lymph_nodes", "eso_pathology",
        "radiation_therapy", "palliative_chemo", "tumor_markers", "lab_results",
        "notifications", "audit_log",
    ])
    tables = [
        "patients", "tumor_preop", "neoadjuvant", "surgery",
        "pathology", "lymph_nodes", "gist_detail",
        "adjuvant_chemo", "outcome",
        "eso_tumor", "eso_surgery", "eso_course", "eso_lymph_nodes", "eso_pathology",
        "radiation_therapy",
    ]
    all_data = {}
    with get_db() as conn:
        for tbl in tables:
            assert tbl in _ALLOWED_TABLES, f"Invalid table: {tbl}"
            if tbl == "patients":
                row = conn.execute(f"SELECT * FROM {tbl} WHERE id=?", (patient_db_id,)).fetchone()
            else:
                row = conn.execute(f"SELECT * FROM {tbl} WHERE patient_id=?", (patient_db_id,)).fetchone()
            all_data[tbl] = dict(row) if row else {}
        rows = conn.execute(
            "SELECT * FROM palliative_chemo WHERE patient_id=? ORDER BY line_number",
            (patient_db_id,)
        ).fetchall()
        all_data["palliative_chemo"] = {r["line_number"]: dict(r) for r in rows}
    return all_data


# ============================================================
# 認証
# ============================================================
def login_page():
    st.markdown("## 🏥 上部消化管グループ 症例登録データベース")
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("### ログイン")
        username = st.text_input("ユーザー名", key="login_user")
        password = st.text_input("パスワード", type="password", key="login_pw")
        if st.button("ログイン", type="primary", use_container_width=True):
            result = authenticate(username, password)
            if result == "locked":
                st.error("🔒 ログイン試行回数の上限に達しました。5分後に再試行してください。")
            elif result:
                st.session_state.user = result
                st.rerun()
            else:
                st.error("ユーザー名またはパスワードが正しくありません")
        st.caption("初期ユーザー: admin / admin")


# ============================================================
# サイドバー
# ============================================================
def sidebar():
    menu_items = [
        "📋 症例一覧",
        "➕ 新規登録",
        "📊 進捗確認",
        "📈 サマリー分析",
        "📊 統計解析",
        "🔍 データ探索",
        "🤖 自然言語クエリ",
        "📤 データエクスポート",
        "🩸 検査値読取",
        "📜 監査ログ",
        "🔔 通知",
        "⚙️ マイページ",
    ]
    # 管理者専用メニュー
    if st.session_state.user.get("role") == "admin":
        menu_items += [
            "🗑️ データ管理",
            "👥 ユーザー管理",
        ]
    with st.sidebar:
        user = st.session_state.user
        # 通知バッジ
        unread = _get_unread_count(user["id"])
        badge = f" ({unread})" if unread > 0 else ""
        st.markdown(f"### 👤 {user['display_name']}{badge}")
        st.caption(f"権限: {user['role']}")

        # 新規症例登録ボタン（赤基調・目立つ位置）
        if st.button("新規症例登録", type="primary", use_container_width=True,
                      key="sidebar_new_case"):
            if "edit_study_id" in st.session_state:
                del st.session_state.edit_study_id
                st.session_state.pop("edit_loaded_updated_at", None)
            st.session_state._goto_page = "➕ 新規登録"
            st.rerun()

        st.markdown("---")

        # ページ遷移リクエストがあれば index で反映（key バインド不要）
        if "_goto_page" in st.session_state:
            goto = st.session_state.pop("_goto_page")
            idx = menu_items.index(goto) if goto in menu_items else 0
        elif "_current_page" in st.session_state:
            cur = st.session_state._current_page
            idx = menu_items.index(cur) if cur in menu_items else 0
        else:
            idx = 0

        page = st.radio("メニュー", menu_items, index=idx)
        st.session_state._current_page = page

        st.markdown("---")
        if st.button("ログアウト"):
            with get_db() as conn:
                log_audit(conn, user["id"], "LOGOUT")
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()
        return page


# ============================================================
# 症例一覧
# ============================================================
def case_list_page():
    st.markdown("## 📋 症例一覧")

    # --- 表示モード切替 ---
    view_mode = st.radio("表示モード", ["サマリー", "全データ（Excel風）"],
                         horizontal=True, key="case_list_view")

    # ================================================================
    # サマリーモード（従来の簡易一覧 + 行クリック編集）
    # ================================================================
    if view_mode == "サマリー":
        with get_db() as conn:
            df = pd.read_sql_query("""
                SELECT p.id, p.study_id, p.patient_id,
                       CASE p.sex WHEN 1 THEN '男' WHEN 2 THEN '女' END as sex_label,
                       p.surgery_date, p.data_status,
                       p.disease_category, p.disease_class,
                       s.op_procedure, o.vital_status, o.recurrence_yn
                FROM patients p
                LEFT JOIN surgery s ON p.id = s.patient_id
                LEFT JOIN outcome o ON p.id = o.patient_id
                WHERE p.is_deleted = 0
                ORDER BY p.surgery_date DESC
            """, conn)

        if df.empty:
            st.info("登録されている症例がありません。「新規登録」から症例を登録してください。")
            return

        DCAT = {1: "胃癌", 2: "食道癌"}
        dc = get_codebook("disease_class")
        vs = get_codebook("vital_status")

        col1, col2, col3 = st.columns(3)
        with col1:
            status_filter = st.multiselect("ステータス", list(STATUS_OPTIONS.values()), key="sf_sum")
        with col2:
            years = sorted(df["surgery_date"].dropna().str[:4].unique(), reverse=True)
            year_filter = st.multiselect("手術年", years, key="yf_sum")
        with col3:
            disease_filter = st.multiselect("疾患分類", list(DCAT.values()), key="df_sum")

        df["status_label"] = df["data_status"].map(STATUS_OPTIONS)
        df["category_label"] = df["disease_category"].map(DCAT)
        df["disease_label"] = df["disease_class"].map(dc)
        df["vital_label"] = df["vital_status"].map(vs)
        df["recurrence_label"] = df["recurrence_yn"].map({0: "なし", 1: "あり"})

        if status_filter:
            df = df[df["status_label"].isin(status_filter)]
        if year_filter:
            df = df[df["surgery_date"].str[:4].isin(year_filter)]
        if disease_filter:
            df = df[df["category_label"].isin(disease_filter)]

        total_count = len(df)
        PAGE_SIZE = 50
        total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
        if "case_list_page_num" not in st.session_state:
            st.session_state.case_list_page_num = 1
        current_page = st.session_state.case_list_page_num

        pg_col1, pg_col2, pg_col3, pg_col4 = st.columns([1, 1, 1, 3])
        with pg_col1:
            if st.button("◀ 前", key="pg_prev", disabled=(current_page <= 1)):
                st.session_state.case_list_page_num = max(1, current_page - 1)
                st.rerun()
        with pg_col2:
            st.markdown(f"**{current_page} / {total_pages}** （{total_count}件）")
        with pg_col3:
            if st.button("次 ▶", key="pg_next", disabled=(current_page >= total_pages)):
                st.session_state.case_list_page_num = min(total_pages, current_page + 1)
                st.rerun()

        start_idx = (current_page - 1) * PAGE_SIZE
        df_page = df.iloc[start_idx:start_idx + PAGE_SIZE]

        display_df = df_page[["study_id", "patient_id", "sex_label", "surgery_date",
                          "category_label", "disease_label", "vital_label",
                          "recurrence_label", "status_label"]].copy()
        display_df.columns = ["症例ID", "カルテNo", "性別", "手術日",
                               "疾患分類", "胃癌分類", "生存", "再発", "ステータス"]

        event = st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
        )

        if event and event.selection and event.selection.rows:
            selected_row_idx = event.selection.rows[0]
            selected_study_id = df_page.iloc[selected_row_idx]["study_id"]
            st.session_state.edit_study_id = selected_study_id
            st.session_state._goto_page = "➕ 新規登録"
            st.rerun()

    # ================================================================
    # 全データモード（Excel 風スクロール + フィルター）
    # ================================================================
    else:
        _join_tables = [
            ("tumor_preop", "tp"),
            ("neoadjuvant", "neo"),
            ("surgery", "s"),
            ("pathology", "pa"),
            ("lymph_nodes", "ln"),
            ("adjuvant_chemo", "ac"),
            ("outcome", "o"),
        ]
        # 全テーブルの全カラムを LEFT JOIN で取得
        select_parts = ["p.*"]
        join_parts = []
        for tbl, alias in _join_tables:
            join_parts.append(f"LEFT JOIN {tbl} {alias} ON p.id = {alias}.patient_id")
            select_parts.append(f"{alias}.*")

        query = f"SELECT {', '.join(select_parts)} FROM patients p {' '.join(join_parts)} WHERE p.is_deleted = 0 ORDER BY p.surgery_date DESC"

        with get_db() as conn:
            try:
                cursor = conn.execute(query)
                rows = cursor.fetchall()
                if rows:
                    col_names = [desc[0] for desc in cursor.description]
                    data = [dict(zip(col_names, r)) for r in rows]
                    df_full = pd.DataFrame(data)
                else:
                    df_full = pd.DataFrame()
            except Exception as e:
                st.error(f"データ取得エラー: {e}")
                return

        if df_full.empty:
            st.info("登録されている症例がありません。")
            return

        # 重複カラム名の除去（JOINで patient_id が複数テーブルに存在）
        df_full = df_full.loc[:, ~df_full.columns.duplicated()]

        # --- カラムラベル変換 ---
        col_labels = get_all_column_labels()

        rename_map = {}
        for c in df_full.columns:
            lbl = col_labels.get(c)
            if lbl and lbl != c:
                # 重複ラベルを避けるため、既に使われていれば元のカラム名を付記
                if lbl in rename_map.values():
                    rename_map[c] = f"{lbl}({c})"
                else:
                    rename_map[c] = lbl
            else:
                rename_map[c] = c
        df_display = df_full.rename(columns=rename_map)

        # --- フィルター UI ---
        st.markdown("#### フィルター")
        fcol1, fcol2, fcol3, fcol4 = st.columns(4)
        with fcol1:
            DCAT = {1: "胃癌", 2: "食道癌"}
            disease_category_col = rename_map.get("disease_category", "disease_category")
            df_display[disease_category_col] = df_full["disease_category"].map(DCAT)
            dcat_opts = [v for v in DCAT.values() if v in df_display[disease_category_col].values]
            dcat_filter = st.multiselect("疾患分類", dcat_opts, key="dcat_full")
        with fcol2:
            sdate_col = rename_map.get("surgery_date", "surgery_date")
            if sdate_col in df_display.columns:
                yr_vals = df_display[sdate_col].dropna().astype(str).str[:4].unique()
                yr_opts = sorted(yr_vals, reverse=True)
                yr_filter = st.multiselect("手術年", yr_opts, key="yr_full")
            else:
                yr_filter = []
        with fcol3:
            status_col = rename_map.get("data_status", "data_status")
            if status_col in df_display.columns:
                df_display[status_col] = df_full["data_status"].map(STATUS_OPTIONS)
                st_opts = [v for v in STATUS_OPTIONS.values() if v in df_display[status_col].values]
                st_filter = st.multiselect("ステータス", st_opts, key="st_full")
            else:
                st_filter = []
        with fcol4:
            keyword = st.text_input("キーワード検索", key="kw_full",
                                    placeholder="症例ID・カルテNo・術式など")

        # フィルター適用
        if dcat_filter:
            df_display = df_display[df_display[disease_category_col].isin(dcat_filter)]
        if yr_filter:
            df_display = df_display[df_display[sdate_col].astype(str).str[:4].isin(yr_filter)]
        if st_filter:
            df_display = df_display[df_display[status_col].isin(st_filter)]
        if keyword:
            mask = df_display.astype(str).apply(
                lambda row: row.str.contains(keyword, case=False, na=False).any(), axis=1)
            df_display = df_display[mask]

        # --- カラム選択 ---
        all_cols = list(df_display.columns)
        # デフォルトで表示する主要カラム
        _default_cols_raw = [
            "study_id", "patient_id", "sex", "surgery_date", "disease_category",
            "disease_class", "data_status",
            "c_depth", "c_ln_metastasis", "c_distant_metastasis", "c_stage",
            "op_approach", "op_procedure", "op_dissection",
            "op_time_min", "op_blood_loss_ml",
            "op_surgeon", "op_assistant1",
            "p_depth", "p_ln_metastasis", "p_distant_metastasis", "p_stage",
            "vital_status", "recurrence_yn",
        ]
        default_display = [rename_map.get(c, c) for c in _default_cols_raw if rename_map.get(c, c) in all_cols]

        with st.expander("📋 表示カラムを選択", expanded=False):
            selected_cols = st.multiselect(
                "表示するカラム（空 = 全カラム表示）",
                options=all_cols,
                default=default_display,
                key="col_select_full",
            )

        if selected_cols:
            df_show = df_display[selected_cols]
        else:
            df_show = df_display

        st.metric("該当症例数", f"{len(df_show)} 件 / {len(df_full)} 件中")

        # Excel風の横スクロール可能なテーブル
        st.dataframe(
            df_show,
            use_container_width=True,
            hide_index=True,
            height=600,
        )

        # --- CSV ダウンロード ---
        csv_data = df_show.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "📥 表示中のデータを CSV ダウンロード",
            data=csv_data,
            file_name="ugi_case_list.csv",
            mime="text/csv",
        )


# ============================================================
# 新規登録 / 編集 — メイン
# ============================================================
def case_entry_page():
    editing = hasattr(st.session_state, "edit_study_id") and st.session_state.get("edit_study_id")
    if editing:
        st.markdown(f"## ✏️ 症例編集: {st.session_state.edit_study_id}")
    else:
        st.markdown("## ➕ 新規症例登録")

    # 既存データ読み込み
    patient_db_id = None
    all_data = {}
    if editing:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM patients WHERE study_id=?",
                               (st.session_state.edit_study_id,)).fetchone()
            if row:
                patient_db_id = row["id"]
                all_data = load_patient_data(patient_db_id)
                # 楽観的ロック用: 読み込み時の updated_at を保持
                if "edit_loaded_updated_at" not in st.session_state:
                    st.session_state.edit_loaded_updated_at = row["updated_at"]

    p = all_data.get("patients", {})
    _decrypt_patient_data(p)  # 個人情報カラムを復号
    tp = all_data.get("tumor_preop", {})
    neo = all_data.get("neoadjuvant", {})
    surg = all_data.get("surgery", {})
    path = all_data.get("pathology", {})
    ln = all_data.get("lymph_nodes", {})
    adj = all_data.get("adjuvant_chemo", {})
    out = all_data.get("outcome", {})
    gist = all_data.get("gist_detail", {})
    pal_lines = all_data.get("palliative_chemo", {})
    rt = all_data.get("radiation_therapy", {})
    eso_t = all_data.get("eso_tumor", {})
    eso_s = all_data.get("eso_surgery", {})
    eso_pa = all_data.get("eso_pathology", {})
    eso_c = all_data.get("eso_course", {})
    eso_ln = all_data.get("eso_lymph_nodes", {})

    # ==========================================================
    # 疾患分類（タブの上に表示 ― タブ構成を決定する）
    # ==========================================================
    DISEASE_CATEGORY_OPTS = {1: "胃癌", 2: "食道癌"}
    disease_category = selectbox_select(
        "疾患分類", DISEASE_CATEGORY_OPTS, "disease_category",
        default=p.get("disease_category", 1), include_blank=False,
    )
    if disease_category is None:
        disease_category = 1

    is_gastric = (disease_category == 1)
    is_eso = (disease_category == 2)

    # 胃癌分類（9項目）は術前診断タブ内で選択。ここではDB値を読む
    disease_class = p.get("disease_class")
    is_gist = (disease_class == 4)

    # ==========================================================
    # タブ構成（疾患に応じて動的に変更）
    # ==========================================================
    tab_labels = ["👤 患者基本", "🔬 術前診断", "💊 術前療法",
                  "🔪 手術", "🔬 病理", "💉 薬物・放射線療法",
                  "📈 再発・予後", "🩸 血液検査"]
    # 食道専用タブは廃止 — 各タブ内にis_esoで追加フォーム表示

    tabs = st.tabs(tab_labels)
    save_data = {}  # table_name: {column: value}

    # ==========================================================
    # Tab 1: 患者基本情報
    # ==========================================================
    with tabs[0]:
        st.caption("＊胃癌取り扱い規約15版、胃癌治療ガイドライン第5版、RECIST1.1、CTCAE Ver.4.0に準拠しています。")
        st.caption("＊規約等改訂時は当データベースも更新いたします。")
        
        section_card("識別情報", "blue")
        patients_data = {"disease_category": disease_category}

        # 行1: 症例ID ／ 患者ID ／ NCD症例ID
        col1, col2, col3 = st.columns(3)
        with col1:
            if editing:
                patients_data["study_id"] = st.text_input("症例登録ID", value=p.get("study_id", ""), disabled=True)
            else:
                next_id = generate_study_id()
                patients_data["study_id"] = st.text_input("症例登録ID", value=next_id, key="new_study_id")
        with col2:
            patients_data["patient_id"] = st.text_input("患者ID（カルテ番号）", value=p.get("patient_id", ""), key="pid")
        with col3:
            patients_data["ncd_case_id"] = st.text_input("NCD症例ID", value=p.get("ncd_case_id", "") or "", key="ncd_id")

        # 行2: イニシャル ／ 性別
        col1, col2, col3 = st.columns(3)
        with col1:
            patients_data["initials"] = st.text_input("患者イニシャル", value=p.get("initials", ""), key="init")
            st.caption("例：山田太郎→YT")
        with col2:
            patients_data["sex"] = selectbox_select(get_form_label("sex"), get_codebook("sex"), "sex", p.get("sex"))

        # 行3: 生年月日 ／ 初診日 ／ 入院日
        col1, col2, col3 = st.columns(3)
        with col1:
            bd = p.get("birthdate")
            patients_data["birthdate"] = st.date_input(
                "生年月日",
                value=datetime.strptime(bd, "%Y-%m-%d").date() if bd else None,
                min_value=date(1900, 1, 1), max_value=date.today(),
                key="bd", format="YYYY/MM/DD"
            )
        with col2:
            fv = p.get("first_visit_date")
            patients_data["first_visit_date"] = st.date_input(
                "初診日", value=datetime.strptime(fv, "%Y-%m-%d").date() if fv else None,
                key="fvd", format="YYYY/MM/DD")

        section_card("身体所見", "blue")
        # 行1: 身長 ／ 入院時体重 ／ 退院時体重 ／ 入院時BMI
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            patients_data["height_cm"] = numeric_input("身長", "ht", p.get("height_cm"), "cm", is_float=True)
        with col2:
            patients_data["weight_admission"] = numeric_input("入院時体重", "wt", p.get("weight_admission"), "kg", is_float=True)
        with col3:
            patients_data["weight_discharge"] = numeric_input("退院時体重", "wtd", p.get("weight_discharge"), "kg", is_float=True)
        with col4:
            h = patients_data.get("height_cm")
            w = patients_data.get("weight_admission")
            if h and w and h > 0:
                bmi = w / ((h / 100) ** 2)
                st.metric("入院時BMI", f"{bmi:.1f}")
            else:
                st.metric("入院時BMI", "---")
        # 行2: 体重減少チェック
        patients_data["preop_weight_loss_10pct"] = st.checkbox(
            "術前6ヶ月で10%以上体重減少",
            value=bool(p.get("preop_weight_loss_10pct", 0)),
            key="wt_loss"
        )
        patients_data["preop_weight_loss_10pct"] = 1 if patients_data["preop_weight_loss_10pct"] else 0

        section_card("生活歴・PS・ASA", "blue")
        # 行1: 喫煙 ／ 飲酒
        col1, col2, col3 = st.columns(3)
        with col1:
            patients_data["smoking"] = selectbox_select(get_form_label("smoking"), get_codebook("smoking"), "smk", p.get("smoking"))
        with col2:
            patients_data["alcohol"] = selectbox_select(get_form_label("alcohol"), get_codebook("alcohol"), "alc", p.get("alcohol"))

        # 喫煙サブ（smoking ≠ 0 のとき展開）
        if patients_data.get("smoking") and patients_data["smoking"] != 0:
            smk_c1, smk_c2 = st.columns(2)
            with smk_c1:
                patients_data["smoking_type"] = selectbox_select(
                    "喫煙種別", {0: "紙巻", 1: "加熱式", 2: "両方"},
                    "smoking_type", p.get("smoking_type"))
            with smk_c2:
                patients_data["smoking_bi"] = st.number_input(
                    "Brinkman Index（BI = 本数/日 × 年数）",
                    min_value=0, max_value=9999, step=1,
                    value=int(p.get("smoking_bi") or 0),
                    key="smoking_bi")

        # 行2a: ADL ／ 法的判断能力
        col_adl, col_legal = st.columns(2)
        with col_adl:
            patients_data["adl_status"] = selectbox_select("ADLステータス", get_codebook("adl_status"), "adl", p.get("adl_status"))
            st.caption("NCD出力時には「術直前ADL」として使用されます。")
        with col_legal:
            patients_data["legal_capacity_admission"] = selectbox_select(
                "入院時の法的判断能力",
                {0: "代理人によって同意書サイン", 1: "患者自身によって同意書サイン"},
                "legal_cap", p.get("legal_capacity_admission"))

        # 行2b: PS (ECOG) ／ ASA-PS  ＋ 参照テーブル
        col_ps, col_asa = st.columns(2)
        with col_ps:
            patients_data["ps"] = selectbox_select("PS (ECOG)", get_codebook("ps"), "ps", p.get("ps"))
            ref_table("ECOG Performance Status", ["Grade", "定義"], [
                ["0", "全く問題なく活動できる。発病前と同じ日常生活が制限なく行える。"],
                ["1", "肉体的に激しい活動は制限されるが、歩行可能で、軽作業や座っての作業は行うことができる。"],
                ["2", "歩行可能で自分の身の回りのことはすべて可能。日中の50%以上はベッド外で過ごす。"],
                ["3", "限られた自分の身の回りのことしかできない。日中の50%以上をベッドか椅子で過ごす。"],
                ["4", "全く動けない。自分の周りのことを全くできず、完全にベッドか椅子で過ごす。"],
            ])
        with col_asa:
            patients_data["asa"] = selectbox_select("ASA-PS", get_codebook("asa"), "asa", p.get("asa"))
            ref_table("ASA-PS 分類", ["Class", "定義（例）"], [
                ["I", "正常健康患者（手術となる原因以外は）"],
                ["II", "軽度の全身性疾患（喫煙・常用飲酒・妊娠・肥満BMI30〜40・軽度の糖尿病・高血圧）"],
                ["III", "中〜高度の全身性疾患（心不全・COPD・極度肥満BMI≥40・活動期狭心症・3ヶ月以上前のMI・透析）"],
                ["IV", "生命を脅かす程度の全身性疾患（重症心不全・3ヶ月以内の心血管イベント・敗血症・DIC）"],
                ["V", "手術なしでは24時間以内に死亡が予測される瀕死患者（大動脈破裂・脳ヘルニア等）"],
            ])

        section_card("症状", "blue")
        # 「無症状(検診含む)」= すべて該当なしと同等
        sym_asymptomatic = st.checkbox(
            "無症状（検診含む）",
            value=bool(p.get("sym_asymptomatic", 0)),
            key="sym_asymptomatic")
        sym_result = {"sym_asymptomatic": 1 if sym_asymptomatic else 0}

        if not sym_asymptomatic:
            sym_flags = [
                ("epigastric_pain", "心窩部痛"),
                ("dysphagia", "嚥下困難"), ("weight_loss", "体重減少"),
                ("anemia", "貧血"), ("melena", "下血"), ("hematemesis", "吐血"),
                ("nausea_vomiting", "悪心・嘔吐"), ("abdominal_distension", "腹部膨満"),
                ("obstruction", "通過障害"), ("other", "その他"),
            ]
            _sym_sub = flag_group("", sym_flags, "sym_", p, "sym", show_allnone=False)
            sym_result.update(_sym_sub)
        else:
            # 無症状選択時は他の症状フラグを全て0
            for _s in ["epigastric_pain", "dysphagia", "weight_loss", "anemia",
                        "melena", "hematemesis", "nausea_vomiting",
                        "abdominal_distension", "obstruction", "other"]:
                sym_result[f"sym_{_s}"] = 0
            sym_result["sym_confirmed"] = 0
        patients_data.update(sym_result)

        section_card("併存疾患", "blue")
        st.caption("悪性腫瘍（血液疾患含む）に関しては重複癌に記載")

        # ── 「すべて該当なし」（基礎疾患+手術既往を包括） ──
        comor_confirmed_col = "comor_confirmed"
        existing_confirmed = p.get(comor_confirmed_col, 0)
        comor_all_none = st.checkbox(
            "すべて該当なし",
            value=bool(existing_confirmed),
            key="comor_confirmed"
        )
        comor_result = {comor_confirmed_col: 1 if comor_all_none else 0}

        if not comor_all_none:
            # ── 基礎疾患 ──
            comor_disease_flags = [
                ("hypertension", "高血圧"), ("cardiovascular", "心疾患"),
                ("cerebrovascular", "脳血管疾患"), ("respiratory", "呼吸器疾患"),
                ("renal", "腎疾患"), ("renal_dialysis", "透析"),
                ("hepatic", "肝疾患"), ("diabetes", "糖尿病"),
                ("endocrine", "内分泌疾患"), ("collagen", "膠原病"),
                ("hematologic", "血液疾患"), ("neurologic", "神経疾患"),
                ("psychiatric", "精神疾患"), ("other", "その他"),
            ]
            comor_disease_help = {
                "renal_dialysis": "血液透析・腹膜透析を含む",
                "endocrine": "糖尿病を除く（甲状腺・副腎等）",
            }
            cols_per_row = 4
            for i in range(0, len(comor_disease_flags), cols_per_row):
                cols = st.columns(cols_per_row)
                for j, col in enumerate(cols):
                    if i + j < len(comor_disease_flags):
                        suffix, flag_label = comor_disease_flags[i + j]
                        col_name = f"comor_{suffix}"
                        help_text = comor_disease_help.get(suffix)
                        with col:
                            val = st.checkbox(flag_label, value=bool(p.get(col_name, 0)),
                                              key=f"comor_{suffix}", help=help_text)
                            comor_result[col_name] = 1 if val else 0
            # その他 詳細
            if comor_result.get("comor_other", 0):
                comor_result["comor_other_detail"] = st.text_input(
                    "併存疾患 その他（詳細）",
                    value=p.get("comor_other_detail", ""),
                    key="comor_other_detail"
                )

            prior_flags = [
                ("prior_cardiac_surgery", "心臓外科手術既往"),
                ("prior_abdominal_surgery", "腹部手術既往"),
                ("preop_ventilator", "術前人工呼吸器管理下"),
            ]
            prior_help = {"preop_ventilator": "SAS目的のCPAPは除く"}
            pr_cols = st.columns(3)
            for i, (suffix, flag_label) in enumerate(prior_flags):
                col_name = f"comor_{suffix}"
                help_text = prior_help.get(suffix)
                with pr_cols[i]:
                    val = st.checkbox(flag_label, value=bool(p.get(col_name, 0)),
                                      key=f"comor_{suffix}", help=help_text)
                    comor_result[col_name] = 1 if val else 0

            # 腹部手術既往の詳細（チェック時のみ）
            if comor_result.get("comor_prior_abdominal_surgery"):
                patients_data["prior_abdominal_surgery_detail"] = st.text_input(
                    "↳ 腹部手術既往 詳細",
                    value=p.get("prior_abdominal_surgery_detail", "") or "",
                    key="prior_abd_surg_detail",
                    placeholder="例: 虫垂切除術(2015), 胆嚢摘出術(2020)")
            else:
                patients_data["prior_abdominal_surgery_detail"] = None
        else:
            # すべて該当なし → 全フラグ0
            for suffix, _ in [
                ("hypertension", ""), ("cardiovascular", ""), ("cerebrovascular", ""),
                ("respiratory", ""), ("renal", ""), ("renal_dialysis", ""),
                ("hepatic", ""), ("diabetes", ""), ("endocrine", ""), ("collagen", ""),
                ("hematologic", ""), ("neurologic", ""), ("psychiatric", ""),
                ("prior_cardiac_surgery", ""), ("prior_abdominal_surgery", ""),
                ("preop_ventilator", ""), ("other", ""),
            ]:
                comor_result[f"comor_{suffix}"] = 0
            patients_data["prior_abdominal_surgery_detail"] = None

        patients_data.update(comor_result)

        # ── 併存疾患サブカテゴリ展開 ──

        # 高血圧サブ → comor_ht_treatment（親 comor_hypertension は 0/1 のまま）
        if comor_result.get("comor_hypertension"):
            st.markdown("**↳ 高血圧 詳細**")
            ht_c1, _ = st.columns(2)
            with ht_c1:
                patients_data["comor_ht_treatment"] = selectbox_select(
                    "治療状況", {1: "未治療", 2: "治療中", 8: "詳細不明"},
                    "comor_ht_treatment", p.get("comor_ht_treatment"))

        # 心疾患サブ
        if comor_result.get("comor_cardiovascular"):
            st.markdown("**↳ 心疾患 詳細**")
            cv_sub = [
                ("mi", "心筋梗塞"), ("angina", "狭心症"),
                ("arrhythmia", "不整脈"), ("valvular", "弁膜症"),
                ("structural_vascular", "器質的血管疾患"), ("cv_unknown", "詳細不明"),
            ]
            cv_cols = st.columns(len(cv_sub))
            for i, (suffix, label) in enumerate(cv_sub):
                col_name = f"comor_{suffix}"
                with cv_cols[i]:
                    val = st.checkbox(label, value=bool(p.get(col_name, 0)),
                                      key=f"comor_cv_sub_{suffix}")
                    patients_data[col_name] = 1 if val else 0

        # 脳血管障害サブ
        if comor_result.get("comor_cerebrovascular"):
            st.markdown("**↳ 脳血管障害 詳細**")
            cb_sub = [
                ("ci_no_sequela", "脳梗塞（TIAまたは後遺症なし）"),
                ("ci_with_sequela", "脳梗塞（後遺症あり）"),
                ("cerebral_hemorrhage", "脳出血系"),
                ("cerebro_unknown", "詳細不明"),
            ]
            cb_cols = st.columns(len(cb_sub))
            for i, (suffix, label) in enumerate(cb_sub):
                col_name = f"comor_{suffix}"
                with cb_cols[i]:
                    val = st.checkbox(label, value=bool(p.get(col_name, 0)),
                                      key=f"comor_cerebro_sub_{suffix}")
                    patients_data[col_name] = 1 if val else 0

        # 呼吸器疾患サブ
        if comor_result.get("comor_respiratory"):
            st.markdown("**↳ 呼吸器疾患 詳細**")
            resp_sub = [
                ("copd", "COPD"), ("ild", "間質性肺炎"),
                ("asthma", "喘息"), ("resp_unknown", "その他・詳細不明"),
            ]
            resp_cols = st.columns(len(resp_sub))
            for i, (suffix, label) in enumerate(resp_sub):
                col_name = f"comor_{suffix}"
                with resp_cols[i]:
                    val = st.checkbox(label, value=bool(p.get(col_name, 0)),
                                      key=f"comor_resp_sub_{suffix}")
                    patients_data[col_name] = 1 if val else 0

        # 肝疾患サブ
        if comor_result.get("comor_hepatic"):
            st.markdown("**↳ 肝疾患 詳細**")
            hep_c1, hep_c2, hep_c3 = st.columns(3)
            with hep_c1:
                patients_data["comor_cirrhosis"] = selectbox_select(
                    "肝硬変", {0: "なし", 1: "Child A", 2: "Child B", 3: "Child C"},
                    "comor_cirrhosis", p.get("comor_cirrhosis"))
            with hep_c2:
                val = st.checkbox("門脈圧亢進", value=bool(p.get("comor_portal_htn", 0)),
                                  key="comor_hep_sub_portal")
                patients_data["comor_portal_htn"] = 1 if val else 0
            with hep_c3:
                patients_data["comor_hepatitis_virus"] = selectbox_select(
                    "ウイルス肝炎", {0: "なし", 1: "HBV", 2: "HCV", 3: "HBV+HCV"},
                    "comor_hepatitis_virus", p.get("comor_hepatitis_virus"))

        # 糖尿病サブ → comor_dm_treatment（親 comor_diabetes は 0/1 のまま）
        if comor_result.get("comor_diabetes"):
            st.markdown("**↳ 糖尿病 詳細**")
            dm_c1, _ = st.columns(2)
            with dm_c1:
                patients_data["comor_dm_treatment"] = selectbox_select(
                    "治療内容",
                    {0: "--", 1: "食事療法のみ", 2: "内服治療",
                     3: "インスリン(＋内服)", 4: "未治療"},
                    "comor_dm_treatment", p.get("comor_dm_treatment"))

        # 精神疾患サブ
        if comor_result.get("comor_psychiatric"):
            st.markdown("**↳ 精神疾患 詳細**")
            psy_sub = [
                ("dementia", "認知症"), ("mood_disorder", "気分障害"),
                ("schizophrenia", "統合失調症"), ("developmental", "発達知的障害"),
                ("psy_unknown", "詳細不明"),
            ]
            psy_cols = st.columns(len(psy_sub))
            for i, (suffix, label) in enumerate(psy_sub):
                col_name = f"comor_{suffix}"
                with psy_cols[i]:
                    val = st.checkbox(label, value=bool(p.get(col_name, 0)),
                                      key=f"comor_psy_sub_{suffix}")
                    patients_data[col_name] = 1 if val else 0
                    if suffix == "mood_disorder":
                        st.caption("躁鬱や双極性障害など")

        section_card("内服薬", "blue")
        st.caption("経口血糖降下薬，インスリン，降圧薬については併存疾患項目内に内包")
        med_flags = [
            ("antithrombotic", "抗血栓薬"),
            ("steroid_immunosup", "ステロイド/免疫抑制"), ("antineoplastic", "抗腫瘍薬"),
            ("thyroid", "甲状腺薬"), ("psychotropic", "向精神薬"),
            ("other", "その他"),
        ]
        med_result = flag_group("", med_flags, "med_", p, "med")
        patients_data.update(med_result)
        # ※癌家族歴セクション削除（解析に使用しないため）

        section_card("重複癌", "blue")
        # 重複癌 臓器リスト（チェックボックス選択形式）
        cancer_organ_flags = [
            ("oral_pharynx", "口腔または咽喉頭"), ("esophagus", "食道"),
            ("stomach", "胃"), ("colorectum", "大腸"),
            ("lung", "肺"), ("hepatobiliary", "肝胆道系"),
            ("pancreas", "膵臓"), ("breast", "乳腺"),
            ("urological", "泌尿器(腎・尿管・膀胱・前立腺・尿道)"),
            ("gynecological", "婦人科臓器(子宮・卵巣・膣)"),
            ("thyroid", "甲状腺"), ("nervous_system", "神経系臓器"),
            ("hematologic", "血液"), ("skin", "皮膚"),
            ("other", "その他"),
        ]

        dup_cancer_flags = [
            ("synchronous_cancer", "同時性重複癌"),
            ("metachronous_cancer", "異時性重複癌"),
        ]
        dup_cols = st.columns(2)
        for i, (suffix, label) in enumerate(dup_cancer_flags):
            col_name = f"{suffix}_yn"
            with dup_cols[i]:
                val = st.checkbox(label, value=bool(p.get(col_name, 0)),
                                  key=f"dup_{suffix}")
                patients_data[col_name] = 1 if val else 0

        if patients_data.get("synchronous_cancer_yn") == 1:
            st.markdown("**↳ 同時性重複癌 臓器**")
            sync_result = flag_group("", cancer_organ_flags, "sync_org_", p, "sync_org",
                                      show_allnone=False)
            patients_data.update(sync_result)

        if patients_data.get("metachronous_cancer_yn") == 1:
            st.markdown("**↳ 異時性重複癌 臓器**")
            meta_result = flag_group("", cancer_organ_flags, "meta_org_", p, "meta_org",
                                      show_allnone=False)
            patients_data.update(meta_result)

        save_data["patients"] = patients_data

    # ==========================================================
    # Tab 2: 術前診断
    # ==========================================================
    # 共通: ver_field_prefix と version_id を疾患に応じて設定
    ver_field_prefix = "gastric" if is_gastric else "eso"
    version_id = 1 if is_gastric else 3

    # 臨床的浸潤臓器フラグ（Tab 2 / Tab 3 で共用）
    c_inv_flags = [
        ("pancreas", "膵臓"), ("liver", "肝"), ("transverse_colon", "横行結腸"),
        ("spleen", "脾"), ("diaphragm", "横隔膜"),
        ("abdominal_wall", "腹壁"), ("adrenal", "副腎"),
        ("kidney", "腎"), ("small_intestine", "小腸"),
        ("retroperitoneum", "後腹膜"), ("transverse_mesocolon", "横行結腸間膜"),
        ("unknown", "不明"), ("other", "その他"),
    ]
    # 臨床的遠隔転移部位フラグ（Tab 2 / Tab 3 で共用）
    c_meta_flags = [
        ("lymph_node", "リンパ節 (LYM)"), ("skin", "皮膚 (SKI)"),
        ("lung", "肺 (PUL)"), ("marrow", "骨髄 (MAR)"),
        ("bone", "骨 (OSS)"), ("pleura", "胸膜 (PLE)"),
        ("brain", "脳 (BRA)"), ("meninges", "髄膜 (MEN)"),
        ("liver", "肝転移 (HEP)"), ("adrenal", "副腎転移 (ADR)"),
        ("cytology", "腹腔洗浄細胞診 (cy+)"), ("peritoneal", "腹膜転移 (PER)"),
        ("other", "その他（後腹膜癌症、卵巣転移を含む）"),
        ("unknown", "不明"),
    ]

# ==========================================================
    # Tab 2: 術前診断
# ==========================================================
    with tabs[1]:
        st.markdown("### 術前診断")
        tp_data = {}

        # ▼▼ (移動) 臨床的浸潤臓器・遠隔転移フラグの定義 ▼▼
        c_inv_flags = [
            ("pancreas", "膵臓"), ("liver", "肝"), ("transverse_colon", "横行結腸"),
            ("spleen", "脾"), ("diaphragm", "横隔膜"), ("esophagus", "食道"),
            ("duodenum", "十二指腸"), ("aorta", "大動脈"),
            ("abdominal_wall", "腹壁"), ("adrenal", "副腎"),
            ("kidney", "腎"), ("small_intestine", "小腸"),
            ("retroperitoneum", "後腹膜"), ("transverse_mesocolon", "横行結腸間膜"),
            ("unknown", "不明"), ("other", "その他"),
        ]
        c_meta_flags = [
            ("lymph_node", "リンパ節 (LYM)"), ("skin", "皮膚 (SKI)"),
            ("lung", "肺 (PUL)"), ("marrow", "骨髄 (MAR)"),
            ("bone", "骨 (OSS)"), ("pleura", "胸膜 (PLE)"),
            ("brain", "脳 (BRA)"), ("meninges", "髄膜 (MEN)"),
            ("liver", "肝転移 (HEP)"), ("adrenal", "副腎転移 (ADR)"),
            ("cytology", "腹腔洗浄細胞診 (cy+)"), ("peritoneal", "腹膜転移 (PER)"),
            ("other", "その他（後腹膜癌症、卵巣転移を含む）"),
            ("unknown", "不明"),
        ]

        # ── 胃癌分類・残胃の癌 ──
        if is_gastric:
            dc_opts = get_codebook("disease_class")
            disease_class = selectbox_select(
                "胃癌分類", dc_opts, "disease_class",
                default=p.get("disease_class"), include_blank=False)
            if disease_class is None:
                disease_class = 1
            patients_data["disease_class"] = disease_class
            is_gist = (disease_class == 4)

            # 悪性リンパ腫(5=B cell, 6=T cell, 7=その他)選択時のみ H. pylori 除菌を表示
            if disease_class in (5, 6, 7):
                col_hp, _, _ = st.columns(3)
                with col_hp:
                    patients_data["hp_eradication"] = selectbox_select(
                        "H. pylori 除菌", get_codebook("hp_eradication"), "hp", p.get("hp_eradication"))
                    st.caption("除菌療法の成否にかかわらず、除菌療法を行ったかどうかを記載")
            # 非リンパ腫時: hp_eradicationはsave_dataに含めない（既存値を維持）
        else:
            patients_data["disease_class"] = None
            is_gist = False
            disease_class = None

        ver_field_prefix = "gastric" if is_gastric else "eso"
        version_id = 1 if is_gastric else 3

        if disease_class == 3:
            st.write("")
            section_card("残胃の癌", "blue")
            col1, col2, col3 = st.columns(3)
            with col1:
                tp_data["remnant_initial_disease"], tp_data["remnant_initial_disease_other"] = \
                    selectbox_with_other("初回胃切除の病変", get_codebook("remnant_initial_disease"), "rem_dis", "rem_dis_oth",
                                         tp.get("remnant_initial_disease"), tp.get("remnant_initial_disease_other", ""))
            with col2:
                tp_data["remnant_interval_years"] = numeric_input("初回胃切除からの年数", "rem_yr", tp.get("remnant_interval_years"), "年", is_float=True)
            with col3:
                tp_data["remnant_location"] = selectbox_select(
                    "残胃の癌の存在部位", get_codebook("remnant_location"),
                    "rem_loc", tp.get("remnant_location"))
            tp_data["remnant_stomach_yn"] = 1
        else:
            tp_data["remnant_stomach_yn"] = 0

        # ── 腫瘍情報 ──
        st.write("")
        section_card("腫瘍情報", "blue")

        # ── 1. 腫瘍個数・占居部位 ──
        col1, _, _ = st.columns(3)
        with col1:
            tp_data["c_tumor_number"] = numeric_input("腫瘍個数", "tumor_n", tp.get("c_tumor_number"))

        col1, col2, _ = st.columns(3)
        with col1:
            tp_data["c_location_long"] = location_multiselect(
                "占居部位(長軸)", _LOC_LONG_ORDER, _LOC_LONG_SORT,
                "c_loc_long", path.get("c_location_long"))
        with col2:
            tp_data["c_location_short"] = location_multiselect(
                "占居部位(短軸)", _LOC_SHORT_ORDER, [1, 2, 3, 4, 5, 99],
                "c_loc_short", path.get("c_location_short"), combine_fn=_combine_short)
            
        # ── 2. 臨床肉眼型・腫瘍径 ──
        col1, col2_type0, _ = st.columns(3)
        with col1:
            tp_data["c_macroscopic_type"] = selectbox_select(get_form_label("c_macroscopic_type"), get_codebook("macroscopic_type"), "macro", tp.get("c_macroscopic_type"))
        with col2_type0:
            if tp_data.get("c_macroscopic_type") == 0:
                _type0_options = {v: lab for v, lab, _, _, _ in get_codebook("type0_subclass")}
                _existing_t0 = tp.get("c_type0_subclass") or ""
                _existing_t0_list = [int(x) for x in str(_existing_t0).split(",") if x.strip().isdigit()]
                _t0_selected = st.multiselect(
                    "Type 0 亜型",
                    options=list(_type0_options.keys()),
                    default=[v for v in _existing_t0_list if v in _type0_options],
                    format_func=lambda x: _type0_options[x],
                    key="c_type0sub_multi"
                )
                tp_data["c_type0_subclass"] = ",".join(str(v) for v in sorted(_t0_selected)) if _t0_selected else None

        col1, col2, _ = st.columns(3)
        with col1:
            tp_data["c_tumor_size_major_mm"] = numeric_input("腫瘍長径", "size_maj", tp.get("c_tumor_size_major_mm"), "mm")
        with col2:
            tp_data["c_tumor_size_minor_mm"] = numeric_input("腫瘍短径", "size_min", tp.get("c_tumor_size_minor_mm"), "mm")

        # GIST リスク分類表
        if is_gist:
            ref_table("Modified-Fletcher分類（GIST リスク分類）",
                ["リスク","腫瘍径(cm)","核分裂像(/50HPF)","原発部位"],
                [["超低リスク","≤2","≤5","—"],
                 ["低リスク","2.1〜5.0","≤5","—"],
                 ["中リスク","≤5 / 5.1〜10.0","6〜10 / ≤5","胃"],
                 ["中リスク","—","—","腫瘍破裂あり（胃）"],
                 ["高リスク",">10 / — / — / >5 / ≤5 / 5.1〜10.0",
                  "any / >10 / — / >5 / >5 / ≤5","—（一部 胃以外）"]],
                note="腫瘍破裂は部位に関わらず高リスク")

        # ── 3. 組織型 ──
        hist_field = "histology_gastric" if is_gastric else "histology_eso"
        col_hist, _ = st.columns(2)
        with col_hist:
            tp_data["c_histology1"] = selectbox_select("組織型", get_codebook(hist_field), "chist1", tp.get("c_histology1"))

        # ── EGJ関連（食道癌の場合） ──
        if is_eso:
            st.write("")
            col1, col2, col3 = st.columns(3)
            with col1:
                tp_data["c_location_egj"] = selectbox_select("EGJ Siewert分類", get_codebook("egj_siewert"), "egj_s", tp.get("c_location_egj"))
            with col2:
                tp_data["c_egj_distance_mm"] = numeric_input("EGJからの距離", "egj_dist", tp.get("c_egj_distance_mm"), "mm")
            with col3:
                tp_data["c_esophageal_invasion_mm"] = numeric_input("食道浸潤長", "eso_inv", tp.get("c_esophageal_invasion_mm"), "mm")

        # ── 4. cTNM / cStage ──
        # ── 行7: cT, cN, cM, cStage ──
        st.write("")
        section_card("臨床病期 (cTNM)", "blue")
        col_t, col_n, col_m, col_stage = st.columns([1, 1, 1, 1.5])
        st.caption("腫瘍が大網・小網内に浸潤しても漿膜に露出しない場合はT3とする。")
        st.caption("漿膜浸潤が大網・小網に波及する場合はT4bとはしない。")
        st.caption("横行結腸間膜への浸潤は間膜内の血管または間膜後面まで波及する場合にT4bとする。")
        
        st.caption("No.1~12および14vを胃の領域リンパ節とし、これ以外のリンパ節転移を認めた場合はM1とする。")
        st.caption("食道浸潤を有する場合はNo.19、20、110、111も領域リンパ節とする。")
        st.caption("残胃の癌で初回手術時に残胃と空腸が吻合してある場合、吻合部の空腸腸間膜リンパ節も領域リンパ節とする。")
        
        
        with col_t:
            tp_data["c_depth"] = selectbox_select(get_form_label("c_depth"), get_codebook(f"c_depth_{ver_field_prefix}", version_id), "ct", tp.get("c_depth"))
        with col_n:
            tp_data["c_ln_metastasis"] = selectbox_select(get_form_label("c_ln_metastasis"), get_codebook(f"c_ln_{ver_field_prefix}", version_id), "cn", tp.get("c_ln_metastasis"))
        with col_m:
            tp_data["c_distant_metastasis"] = selectbox_select(get_form_label("c_distant_metastasis"), get_codebook("distant_metastasis"), "cm", tp.get("c_distant_metastasis"))

        # ▼▼ T4b / M1 の判定 ▼▼
        ct_val = tp_data.get("c_depth")
        ct_codebook = get_codebook(f"c_depth_{ver_field_prefix}", version_id)
        is_t4b = False
        if ct_val is not None:
            ct_label = str(ct_codebook.get(ct_val, ""))
            if "T4b" in ct_label or "T4b" in str(ct_val):
                is_t4b = True

        cm_val = tp_data.get("c_distant_metastasis")
        is_m1 = False
        if cm_val is not None:
            cm_label = str(get_codebook("distant_metastasis").get(cm_val, ""))
            if "M1" in cm_label or "1" in str(cm_val):
                is_m1 = True

        # ▼▼ T4b または M1時：浸潤臓器、cP/cH、遠隔転移部位を横並びで表示 ▼▼
        if is_t4b or is_m1:
            st.write("")
            col_inv, col_ph, col_meta = st.columns([1.5, 1, 2])
            
            with col_inv:
                if is_t4b:
                    # show_allnone=False で「すべて該当無し」を非表示に
                    c_inv_result = flag_group("臨床的浸潤臓器 (cT4b)", c_inv_flags, "c_inv_", tp, "c_inv", show_allnone=False)
                    tp_data.update(c_inv_result)
            
            with col_ph:
                if is_m1:
                    st.markdown("##### cP / cH")
                    _p_help = "P1a: 胃,大網,小網,横行結腸間膜前葉,膵被膜,脾臓に限局 / P1b: 上腹部の腹膜(臍より頭側の壁側腹膜,横行結腸より頭側の臓側腹膜) / P1c: 中下腹部の腹膜 / P1x: 腹膜転移を認めるが分布不明"
                    tp_data["c_peritoneal"] = selectbox_select("c腹膜転移", get_codebook("peritoneal_status"), "c_peri", tp.get("c_peritoneal"), help_text=_p_help)
                    tp_data["c_liver_metastasis"] = selectbox_select("c肝転移", get_codebook("liver_metastasis_status"), "c_liver", tp.get("c_liver_metastasis"))
                    
            with col_meta:
                if is_m1:
                    # show_allnone=False で「すべて該当無し」を非表示に
                    meta_result = flag_group("臨床的遠隔転移部位 (cM1)", c_meta_flags, "c_meta_", tp, "c_meta", show_allnone=False)
                    tp_data.update(meta_result)

        # ▼▼ T4b以外時：自動でいいえ(0)をセット ▼▼
        if not is_t4b:
            for key, _ in c_inv_flags:
                tp_data[f"c_inv_{key}"] = 0

        # ▼▼ M0時：自動で陰性/いいえをセット ▼▼
        if not is_m1:
            tp_data["c_peritoneal"] = 0
            tp_data["c_liver_metastasis"] = 0
            for key, _ in c_meta_flags:
                tp_data[f"c_meta_{key}"] = 0

        # cStage 自動計算
        with col_stage:
            auto_cs = compute_stage(
                tp_data.get("c_depth"), tp_data.get("c_ln_metastasis"),
                tp_data.get("c_distant_metastasis"),
                is_gastric=is_gastric, context="clinical",
                p_peritoneal=tp_data.get("c_peritoneal"),
                p_liver=tp_data.get("c_liver_metastasis"))
            cs_default = auto_cs if auto_cs is not None else tp.get("c_stage")
            tp_data["c_stage"] = selectbox_select(get_form_label("c_stage"), get_codebook(f"c_stage_{ver_field_prefix}", version_id), "cs", cs_default)
            if auto_cs is not None:
                stage_label = get_codebook(f"c_stage_{ver_field_prefix}", version_id).get(auto_cs, "")
                st.caption(f"⚡ 自動算出: {stage_label}")

        # ── cTNM 参照テーブル（先生がカスタマイズされたもの） ──
        ref_col_n, ref_col_stage = st.columns(2)
        with ref_col_n:
            if is_gastric:
                ref_table("リンパ節転移の定義",
                    ["分類", "個数"],
                    [["NX","領域リンパ節転移の有無が不明"],
                     ["N0","転移なし"],["N1","1〜2個"],["N2","3〜6個"],
                     ["N3a","7〜15個"],["N3b","16個以上"]],
                    note="胃癌取扱い規約15版")
            else:
                ref_table("リンパ節転移の定義",
                    ["分類", "個数"],
                    [["NX","不明"],["N0","転移なし"],
                     ["N1","1〜2個"],["N2","3〜6個"],["N3","7個以上"]],
                    note="食道癌取扱い規約12版")
        with ref_col_stage:
            if is_gastric:
                ref_table("臨床的進行度分類（cStage）",
                    ["","N0","N1","M1"],
                    [["T1/T2","I","IIA","IVB"],
                     ["T3/T4a","IIB","III","IVB"],
                     ["T4b","IVA","IVA","IVB"]
                     ],
                    note="胃癌取扱い規約15版")
            else:
                ref_table("臨床的Stage分類（扁平上皮癌）",
                    ["","N0","N1","N2-3","M1b"],
                    [["T0/T1a","0","IIIA","IVB"],
                     ["T1b","I","II","IIIA","IVB"],
                     ["T2","II","IIIA","IIIA","IVB"],
                     ["T3r","II","IIIA","IIIA","IVB"],
                     ["T3br","IIIB","IIIB","IIIB","IVB"],
                     ["T4","IVA","IVA","IVA","IVB"],
                     ],
                    note="食道癌取扱い規約12版")
                ref_table("臨床的Stage分類（腺癌）",
                    ["","N0","N1-3","M1"],
                    [["T1/T2","I","IIA","IVB"],
                     ["T3/T4a","IIB","III","IVB"],
                     ["T4b","IVA","IVA","IVB"]],
                    note="食道癌取扱い規約12版（胃癌取扱い規約に準ずる）")
                
        # ── 食道癌 追加項目（疾患＝食道の場合に表示） ──
        if is_eso:
            eso_tumor_data = {}

            section_card("食道癌 追加評価", "teal")
            et1, et2, et3 = st.columns(3)
            with et1:
                eso_tumor_data["c_location_eso"] = selectbox_select(
                    "食道 腫瘍局在", get_codebook("eso_location"),
                    "eso_loc", default=eso_t.get("c_location_eso"))
                eso_tumor_data["c_macroscopic_type_eso"] = selectbox_select(
                    "肉眼型（病型分類）", get_codebook("eso_macroscopic_type"),
                    "eso_macro", default=eso_t.get("c_macroscopic_type_eso"))
                eso_tumor_data["c_multiple_cancer_eso"] = selectbox_select(
                    "多発癌", {0: "なし", 1: "あり"},
                    "eso_multi", default=eso_t.get("c_multiple_cancer_eso"))
            with et2:
                eso_tumor_data["c_depth_jce"] = selectbox_select(
                    "cT（食道癌取扱い規約）", get_codebook("c_depth_eso", 3),
                    "eso_cdepth_jce", default=eso_t.get("c_depth_jce"))
                eso_tumor_data["c_ln_jce"] = selectbox_select(
                    "cN（食道癌取扱い規約）", get_codebook("c_ln_eso", 3),
                    "eso_cln_jce", default=eso_t.get("c_ln_jce"))
            with et3:
                eso_tumor_data["c_distant_jce"] = selectbox_select(
                    "cM（遠隔転移）", {0: "cM0", 1: "cM1", 9: "cMX"},
                    "eso_cm_jce", default=eso_t.get("c_distant_jce"))
                eso_tumor_data["c_stage_jce"] = selectbox_select(
                    "cStage（食道癌取扱い規約）", get_codebook("c_stage_eso", 3),
                    "eso_cstage_jce", default=eso_t.get("c_stage_jce"))

            st.markdown("**UICC分類**")
            eu1, eu2, eu3 = st.columns(3)
            with eu1:
                eso_tumor_data["c_depth_uicc"] = selectbox_select(
                    "cT（UICC）", get_codebook("c_depth_eso", 3),
                    "eso_cdepth_uicc", default=eso_t.get("c_depth_uicc"))
            with eu2:
                eso_tumor_data["c_ln_uicc"] = selectbox_select(
                    "cN（UICC）", get_codebook("c_ln_eso", 3),
                    "eso_cln_uicc", default=eso_t.get("c_ln_uicc"))
                eso_tumor_data["c_distant_uicc"] = selectbox_select(
                    "cM（UICC）", {0: "cM0", 1: "cM1", 9: "cMX"},
                    "eso_cm_uicc", default=eso_t.get("c_distant_uicc"))
            with eu3:
                eso_tumor_data["c_stage_uicc"] = selectbox_select(
                    "cStage（UICC）", get_codebook("c_stage_eso", 3),
                    "eso_cstage_uicc", default=eso_t.get("c_stage_uicc"))

            st.markdown("**PET-CT**")
            ep1, ep2, ep3 = st.columns(3)
            with ep1:
                eso_tumor_data["c_pet_yn"] = selectbox_select(
                    "PET実施", {0: "なし", 1: "あり"},
                    "eso_pet_yn", default=eso_t.get("c_pet_yn"))
            with ep2:
                eso_tumor_data["c_pet_accumulation"] = selectbox_select(
                    "PET集積", {0: "なし", 1: "あり"},
                    "eso_pet_acc", default=eso_t.get("c_pet_accumulation"))
            with ep3:
                eso_tumor_data["c_pet_site"] = st.text_input(
                    "PET集積部位", value=eso_t.get("c_pet_site", "") or "",
                    key="eso_pet_site")

            eso_tumor_data["c_ln_detail"] = st.text_area(
                "リンパ節所見（転移疑い部位の詳細）",
                value=eso_t.get("c_ln_detail", "") or "",
                key="eso_ln_detail", height=80)

            save_data["eso_tumor"] = eso_tumor_data

        save_data["tumor_preop"] = tp_data

    # ==========================================================
    # Tab 3: 術前療法
    # ==========================================================
    with tabs[2]:
        st.markdown("### 術前療法")
        neo_data = {}
        neo_data["nac_yn"] = selectbox_select("術前療法", {0: "なし", 1: "あり"}, "nac_yn", neo.get("nac_yn"))

        if neo_data.get("nac_yn") == 1:
            reg_field = "nac_regimen_gastric" if is_gastric else "nac_regimen_eso"
            col1, col2, col3 = st.columns(3)
            with col1:
                neo_data["nac_regimen"], neo_data["nac_regimen_other"] = \
                    selectbox_with_other("レジメン", get_codebook(reg_field), "nac_reg", "nac_reg_oth",
                                         neo.get("nac_regimen"), neo.get("nac_regimen_other", ""))
                neo_data["nac_courses"] = numeric_input("コース数", "nac_c", neo.get("nac_courses"))
            with col2:
                nsd = neo.get("nac_start_date")
                neo_data["nac_start_date"] = st.date_input("開始日", value=datetime.strptime(nsd, "%Y-%m-%d").date() if nsd else None, key="nac_sd", format="YYYY/MM/DD")
                neo_data["nac_completion"] = selectbox_select("完遂", get_codebook("chemo_completion"), "nac_comp", neo.get("nac_completion"))
            with col3:
                neo_data["nac_adverse_event"] = st.text_area("有害事象 (自由記載)", value=neo.get("nac_adverse_event", "") or "", key="nac_ae")

            # --- 術前療法後診断（術前診断と同じ行レイアウト） ---
            section_card("術前療法後診断 (ycTNM)", "blue")
            st.caption("術前療法後の再評価所見を入力してください")

            # ── yc行1: 腫瘍個数 ──
            col1, _, _ = st.columns(3)
            with col1:
                neo_data["yc_tumor_number"] = numeric_input("腫瘍個数（治療後）", "yc_tumor_n", neo.get("yc_tumor_number"))

            # ── yc行2: 占居部位 長軸・短軸 ──
            col1, col2, _ = st.columns(3)
            with col1:
                neo_data["yc_location_long"] = location_multiselect(
                    "占居部位（長軸・治療後）", _LOC_LONG_ORDER, _LOC_LONG_SORT,
                    "yc_loc_long", neo.get("yc_location_long"))
            with col2:
                neo_data["yc_location_short"] = location_multiselect(
                    "占居部位（短軸・治療後）", _LOC_SHORT_ORDER, [1, 2, 3, 4, 5, 99],
                    "yc_loc_short", neo.get("yc_location_short"), combine_fn=_combine_short)

            # ── yc行3: 肉眼型 + Type 0 亜型 ──
            col1, col2_yct0, _ = st.columns(3)
            with col1:
                neo_data["yc_macroscopic_type"] = selectbox_select(
                    "肉眼型（治療後）", get_codebook("macroscopic_type"), "yc_macro", neo.get("yc_macroscopic_type"))
            with col2_yct0:
                if neo_data.get("yc_macroscopic_type") == 0:
                    _type0_options = {v: lab for v, lab, _, _, _ in get_codebook("type0_subclass")}
                    _existing_yct0 = neo.get("yc_type0_subclass") or ""
                    _existing_yct0_list = [int(x) for x in str(_existing_yct0).split(",") if x.strip().isdigit()]
                    _yct0_selected = st.multiselect(
                        "Type 0 亜型（治療後）",
                        options=list(_type0_options.keys()),
                        default=[v for v in _existing_yct0_list if v in _type0_options],
                        format_func=lambda x: _type0_options[x],
                        key="yc_type0sub_multi"
                    )
                    neo_data["yc_type0_subclass"] = ",".join(str(v) for v in sorted(_yct0_selected)) if _yct0_selected else None

            # ── yc行4: 腫瘍長径，腫瘍短径 ──
            col1, col2, _ = st.columns(3)
            with col1:
                neo_data["yc_tumor_size_major_mm"] = numeric_input("腫瘍長径（治療後）", "yc_size_maj", neo.get("yc_tumor_size_major_mm"), "mm")
            with col2:
                neo_data["yc_tumor_size_minor_mm"] = numeric_input("腫瘍短径（治療後）", "yc_size_min", neo.get("yc_tumor_size_minor_mm"), "mm")

            # ── yc行5: ycT, ycN, ycM, ycStage ──
            st.write("")
            section_card("術前療法後 臨床病期 (ycTNM)", "blue")
            col_yct, col_ycn, col_ycm, col_ycs = st.columns([1, 1, 1, 1.5])
            if is_gastric:
                st.caption("腫瘍が大網・小網内に浸潤しても漿膜に露出しない場合はT3とする。")
                st.caption("No.1~12および14vを胃の領域リンパ節とし、これ以外のリンパ節転移を認めた場合はM1とする。")

            with col_yct:
                _yc_t_field = f"yc_depth_{ver_field_prefix}" if is_gastric else f"c_depth_{ver_field_prefix}"
                neo_data["yc_depth"] = selectbox_select("ycT", get_codebook(_yc_t_field, version_id), "yct", neo.get("yc_depth"))
            with col_ycn:
                _yc_ln_field = f"yc_ln_{ver_field_prefix}" if is_gastric else f"c_ln_{ver_field_prefix}"
                neo_data["yc_ln_metastasis"] = selectbox_select("ycN", get_codebook(_yc_ln_field, version_id), "ycn", neo.get("yc_ln_metastasis"))
            with col_ycm:
                _yc_m_field = "yc_distant_metastasis" if is_gastric else "distant_metastasis"
                neo_data["yc_distant_metastasis"] = selectbox_select("ycM", get_codebook(_yc_m_field), "ycm", neo.get("yc_distant_metastasis"))

            # ▼▼ ycT4b / ycM1 判定 ▼▼
            yct_val = neo_data.get("yc_depth")
            yc_is_t4b = False
            if yct_val is not None:
                yct_label = str(get_codebook(_yc_t_field, version_id).get(yct_val, ""))
                if "T4b" in yct_label or "T4b" in str(yct_val):
                    yc_is_t4b = True

            yc_is_m1 = (neo_data.get("yc_distant_metastasis") == 1)

            # ▼▼ T4b または M1時：浸潤臓器、cP/cH、遠隔転移部位を横並びで表示 ▼▼
            if yc_is_t4b or yc_is_m1:
                st.write("")
                col_inv, col_ph, col_meta = st.columns([1.5, 1, 2])

                with col_inv:
                    if yc_is_t4b:
                        yc_inv_result = flag_group("浸潤臓器 (ycT4b)", c_inv_flags, "yc_inv_", neo, "yc_inv",
                                                    show_allnone=False)
                        neo_data.update(yc_inv_result)

                with col_ph:
                    if yc_is_m1:
                        st.markdown("##### ycP / ycH")
                        _p_help = "P1a: 胃,大網,小網,横行結腸間膜前葉,膵被膜,脾臓に限局 / P1b: 上腹部の腹膜(臍より頭側の壁側腹膜,横行結腸より頭側の臓側腹膜) / P1c: 中下腹部の腹膜 / P1x: 腹膜転移を認めるが分布不明"
                        neo_data["yc_peritoneal"] = selectbox_select("yc腹膜転移", get_codebook("peritoneal_status"), "yc_peri", neo.get("yc_peritoneal"), help_text=_p_help)
                        neo_data["yc_liver_metastasis"] = selectbox_select("yc肝転移", get_codebook("liver_metastasis_status"), "yc_liver", neo.get("yc_liver_metastasis"))

                with col_meta:
                    if yc_is_m1:
                        yc_meta_result = flag_group("遠隔転移部位 (ycM1)", c_meta_flags, "yc_meta_", neo, "yc_meta",
                                                     show_allnone=False)
                        neo_data.update(yc_meta_result)

            # ▼▼ ycT4b以外時：自動で0セット ▼▼
            if not yc_is_t4b:
                for key, _ in c_inv_flags:
                    neo_data[f"yc_inv_{key}"] = 0

            # ▼▼ ycM0時：自動で0セット ▼▼
            if not yc_is_m1:
                neo_data["yc_peritoneal"] = 0
                neo_data["yc_liver_metastasis"] = 0
                for key, _ in c_meta_flags:
                    neo_data[f"yc_meta_{key}"] = 0

            # ycStage 自動計算
            with col_ycs:
                auto_ycs = compute_stage(
                    neo_data.get("yc_depth"), neo_data.get("yc_ln_metastasis"),
                    neo_data.get("yc_distant_metastasis"),
                    is_gastric=is_gastric, context="clinical",
                    p_peritoneal=neo_data.get("yc_peritoneal"),
                    p_liver=neo_data.get("yc_liver_metastasis"))
                ycs_default = auto_ycs if auto_ycs is not None else neo.get("yc_stage")
                neo_data["yc_stage"] = selectbox_select("ycStage", get_codebook(f"c_stage_{ver_field_prefix}", version_id), "ycs", ycs_default)
                if auto_ycs is not None:
                    ycs_label = get_codebook(f"c_stage_{ver_field_prefix}", version_id).get(auto_ycs, "")
                    st.caption(f"⚡ 自動算出: {ycs_label}")

            # ── ycTNM 参照テーブル ──
            ref_yc_n, ref_yc_stage = st.columns(2)
            with ref_yc_n:
                if is_gastric:
                    ref_table("リンパ節転移の定義（胃癌）",
                        ["分類","個数"],
                        [["N0","転移なし"],["N1","1〜2個"],["N2","3〜6個"],
                         ["N3a","7〜15個"],["N3b","16個以上"]])
                else:
                    ref_table("リンパ節転移の定義（食道癌）",
                        ["分類","個数"],
                        [["N0","転移なし"],["N1","1〜2個"],
                         ["N2","3〜6個"],["N3","7個以上"]])
            with ref_yc_stage:
                if is_gastric:
                    ref_table("臨床的進行度分類（ycStage）",
                        ["","N0","N1"],
                        [["T1/T2","I","IIA"],["T3/T4a","IIB","III"],
                         ["T4b","IVA","IVA"],["Any T, M1","","IVB"]],
                        note="T4b→IVA, M1→IVB（取扱い規約15版）")
                else:
                    ref_table("臨床的進行度分類（食道癌）",
                        ["","N0","N1","N2","N3"],
                        [["T1a","0","I","II","II"],["T1b","I","II","II","III"],
                         ["T2","II","II","III","III"],["T3","II","III","III","IVA"],
                         ["T4a","III","IVA","IVA","IVA"],["T4b","IVA","IVA","IVA","IVA"]],
                        note="M1→IVB（取扱い規約12版）")

            # --- RECIST 評価（ycTNM の後に配置） ---
            section_card("RECIST 評価", "blue")
            st.caption("測定可能病変：5mmスライス以下のCTにて10mm以上の大きさであること。病的リンパ節はその短径が15mm以上であること。")
            st.caption("測定不能病変：測定可能病変以外。長径10mm未満の病変や短径10mm以上15mm未満の病的リンパ節も測定不能病変とする。")
            st.caption("標的病変　　：治療前に認められた測定可能病変のうち、一臓器につき最長径（リンパ節は短径）の大きい順に2個まで、合計5個までを標的病変として選択する。")
            st.caption("　　　　　　　すべての標的病変の最長径（リンパ節は短径）の和を算出し、ベースラインの径の和として記録する。")
            st.caption("非標的病変：標的病変以外の病的リンパ節を含むすべての病変を非標的病変とする。")
            st.caption("（※原発巣は非標的病変としてCTで評価。ただし測定が重要な場合は内視鏡検査またはX線による効果判定を行い、「胃原発巣の評価」に記載。）")

            col1, col2, col3 = st.columns(3)
            with col1:
                neo_data["recist_overall"] = selectbox_select("総合効果判定", get_codebook("recist_response"), "recist_ov", neo.get("recist_overall"))
                neo_data["recist_shrinkage_pct"] = numeric_input("標的病変縮小率", "recist_shrink", neo.get("recist_shrinkage_pct"), "%", is_float=True)
            with col2:
                neo_data["primary_shrinkage_pct"] = numeric_input("原発巣縮小率", "prim_shrink", neo.get("primary_shrinkage_pct"), "%", is_float=True)
                neo_data["primary_overall_response"] = selectbox_select("原発巣総合判定", get_codebook("recist_response"), "prim_resp", neo.get("primary_overall_response"))

            # ── 食道癌 yc追加評価（UICC + 内視鏡効果判定）──
            if is_eso:
                section_card("食道癌 術前療法後 追加評価", "teal")
                yc1, yc2, yc3 = st.columns(3)
                with yc1:
                    eso_tumor_data_neo = save_data.get("eso_tumor", {})
                    eso_tumor_data_neo["yc_depth_jce"] = selectbox_select(
                        "ycT（規約）", get_codebook("c_depth_eso", 3),
                        "eso_ycdepth_jce", default=eso_t.get("yc_depth_jce"))
                    eso_tumor_data_neo["yc_ln_jce"] = selectbox_select(
                        "ycN（規約）", get_codebook("c_ln_eso", 3),
                        "eso_ycln_jce", default=eso_t.get("yc_ln_jce"))
                with yc2:
                    eso_tumor_data_neo["yc_depth_uicc"] = selectbox_select(
                        "ycT（UICC）", get_codebook("c_depth_eso", 3),
                        "eso_ycdepth_uicc", default=eso_t.get("yc_depth_uicc"))
                    eso_tumor_data_neo["yc_ln_uicc"] = selectbox_select(
                        "ycN（UICC）", get_codebook("c_ln_eso", 3),
                        "eso_ycln_uicc", default=eso_t.get("yc_ln_uicc"))
                with yc3:
                    eso_tumor_data_neo["yc_stage_jce"] = selectbox_select(
                        "ycStage（規約）", get_codebook("c_stage_eso", 3),
                        "eso_ycstage_jce", default=eso_t.get("yc_stage_jce"))
                    eso_tumor_data_neo["yc_stage_uicc"] = selectbox_select(
                        "ycStage（UICC）", get_codebook("c_stage_eso", 3),
                        "eso_ycstage_uicc", default=eso_t.get("yc_stage_uicc"))
                eso_tumor_data_neo["nac_endoscopy_response"] = selectbox_select(
                    "NAC後内視鏡的効果判定",
                    {0: "CR", 1: "PR", 2: "SD", 3: "PD", 9: "判定不能"},
                    "eso_nac_resp", default=eso_t.get("nac_endoscopy_response"))
                # eso_tumorが術前診断タブで初期化済みならmerge、なければ新規
                if "eso_tumor" in save_data:
                    save_data["eso_tumor"].update(eso_tumor_data_neo)
                else:
                    save_data["eso_tumor"] = eso_tumor_data_neo

        save_data["neoadjuvant"] = neo_data

    # ==========================================================
    # Tab 4: 手術情報
    # ==========================================================
    tab_idx = 3
    with tabs[tab_idx]:
        st.markdown("### 手術情報")
        s_data = {}

        # --- 手術日・退院日（入院日は患者基本タブで入力） ---
        section_card("日程", "blue")
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            ad = p.get("admission_date")
            patients_data["admission_date"] = st.date_input(
                "入院日", value=datetime.strptime(ad, "%Y-%m-%d").date() if ad else None,
                key="adm_basic", format="YYYY/MM/DD")
        with col2:
            sd = p.get("surgery_date")
            patients_data["surgery_date"] = st.date_input(
                "手術日",
                value=datetime.strptime(sd, "%Y-%m-%d").date() if sd else None,
                key="sd", format="YYYY/MM/DD"
            )
        with col3:
            dd = p.get("discharge_date")
            patients_data["discharge_date"] = st.date_input(
                "退院日", value=datetime.strptime(dd, "%Y-%m-%d").date() if dd else None,
                key="disc", format="YYYY/MM/DD")
        with col4:
            _bd = patients_data.get("birthdate")
            _sd_val = patients_data.get("surgery_date")
            age = calc_age(str(_bd) if _bd else None, str(_sd_val) if _sd_val else None)
            if age is not None:
                st.metric("手術時年齢", f"{age} 歳")
        with col5:
            # 在院日数（自動計算: 手術日→退院日）
            _sd_v = patients_data.get("surgery_date")
            _dd_v = patients_data.get("discharge_date")
            if _sd_v and _dd_v and isinstance(_sd_v, date) and isinstance(_dd_v, date):
                pod = (_dd_v - _sd_v).days
                st.metric("術後在院日数", f"{pod} 日")

        # --- 執刀医・助手 ---
        section_card("術者", "blue")
        _SURGEON_OPTIONS = [
            "", "中出裕士", "國重智裕", "青木理子", "巽孝成", "曽我真弘",
            "助川正泰", "辻本成範", "宮尾晋太朗", "切畑屋友希", "松本壮平",
            "若月幸平", "他外科医", "研修医学生",
        ]
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            _surgeon_val = surg.get("op_surgeon", "") or ""
            _surgeon_idx = _SURGEON_OPTIONS.index(_surgeon_val) if _surgeon_val in _SURGEON_OPTIONS else 0
            s_data["op_surgeon"] = st.selectbox(
                "執刀医", options=_SURGEON_OPTIONS, index=_surgeon_idx, key="op_surgeon")
        with col2:
            _asst1_val = surg.get("op_assistant1", "") or ""
            _asst1_idx = _SURGEON_OPTIONS.index(_asst1_val) if _asst1_val in _SURGEON_OPTIONS else 0
            s_data["op_assistant1"] = st.selectbox(
                "第1助手", options=_SURGEON_OPTIONS, index=_asst1_idx, key="op_asst1")
        with col3:
            _asst2_val = surg.get("op_assistant2", "") or ""
            _asst2_idx = _SURGEON_OPTIONS.index(_asst2_val) if _asst2_val in _SURGEON_OPTIONS else 0
            s_data["op_assistant2"] = st.selectbox(
                "第2助手", options=_SURGEON_OPTIONS, index=_asst2_idx, key="op_asst2")
        with col4:
            _scop_val = surg.get("op_scopist", "") or ""
            _scop_idx = _SURGEON_OPTIONS.index(_scop_val) if _scop_val in _SURGEON_OPTIONS else 0
            s_data["op_scopist"] = st.selectbox(
                "スコピスト", options=_SURGEON_OPTIONS, index=_scop_idx, key="op_scopist")

        section_card("麻酔", "blue")
        _emerg_col, _ = st.columns(2)
        with _emerg_col:
            s_data["op_emergency"] = selectbox_select("予定/緊急", get_codebook("op_emergency"), "emerg", surg.get("op_emergency"))
        anest_flags = [
            ("general", "全身麻酔"), ("epidural", "硬膜外麻酔"),
            ("ivpca", "IVPCA"), ("spinal", "脊椎麻酔"),
            ("local", "局所麻酔"), ("anest_other", "その他"),
        ]
        anest_cols = st.columns(len(anest_flags))
        for i, (suffix, label) in enumerate(anest_flags):
            col_name = f"anest_{suffix}"
            with anest_cols[i]:
                val = st.checkbox(label, value=bool(surg.get(col_name, 0)),
                                  key=f"anest_flag_{suffix}")
                s_data[col_name] = 1 if val else 0

        section_card("術式", "blue")
        col1, col2 = st.columns(2)
        with col1:
            s_data["op_approach"] = selectbox_select(get_form_label("op_approach"), get_codebook("op_approach"), "approach", surg.get("op_approach"))
        with col2:
            s_data["op_completion"] = selectbox_select("完遂", get_codebook("op_completion"), "op_comp", surg.get("op_completion"))
            if s_data.get("op_completion") == 2:
                s_data["op_conversion_yn"] = 1
            else:
                s_data["op_conversion_yn"] = 0

        proc_field = "op_procedure_gastric" if is_gastric else "op_procedure_eso"
        diss_field = "op_dissection_gastric" if is_gastric else "op_dissection_eso"
        recon_field = "op_reconstruction_gastric" if is_gastric else "op_reconstruction_eso"

        col1, col2, col3 = st.columns(3)
        with col1:
            s_data["op_procedure"], s_data["op_procedure_other"] = \
                selectbox_with_other("術式", get_codebook(proc_field), "proc", "proc_oth",
                                     surg.get("op_procedure"), surg.get("op_procedure_other", ""))
        with col2:
            _diss_help = "残胃癌の場合は規定がないので不明" if is_gastric else None
            s_data["op_dissection"] = selectbox_select("郭清", get_codebook(diss_field), "diss", surg.get("op_dissection"), help_text=_diss_help)
        with col3:
            s_data["op_reconstruction"], s_data["op_reconstruction_other"] = \
                selectbox_with_other("再建", get_codebook(recon_field), "recon", "recon_oth",
                                     surg.get("op_reconstruction"), surg.get("op_reconstruction_other", ""))

        s_data["op_anastomosis_method"], s_data["op_anastomosis_method_other"] = \
            selectbox_with_other("吻合法", get_codebook("op_anastomosis_method"), "anast", "anast_oth",
                                 surg.get("op_anastomosis_method"), surg.get("op_anastomosis_method_other", ""))

        if is_eso:
            col1, col2 = st.columns(2)
            with col1:
                s_data["op_peristalsis_direction"] = selectbox_select(
                    "蠕動方向", get_codebook("op_peristalsis_direction"), "perist", surg.get("op_peristalsis_direction"))
            with col2:
                s_data["op_reconstruction_route"] = selectbox_select(
                    "再建経路", get_codebook("op_reconstruction_route"), "recon_route", surg.get("op_reconstruction_route"))

        # 時間・出血量・輸液量・尿量
        col1, col2, col3 = st.columns(3)
        with col1:
            s_data["op_time_min"] = numeric_input("手術時間", "optime", surg.get("op_time_min"), "分")
        with col2:
            s_data["op_blood_loss_ml"] = numeric_input("出血量", "blood", surg.get("op_blood_loss_ml"), "mL")
        with col3:
            if s_data.get("op_approach") == 7:  # ロボット支援
                s_data["op_console_time_min"] = numeric_input("コンソール時間", "console", surg.get("op_console_time_min"), "分")

        _fl_col1, _fl_col2, _ = st.columns(3)
        with _fl_col1:
            s_data["op_fluid_volume_ml"] = numeric_input("輸液量", "fluid_vol", surg.get("op_fluid_volume_ml"), "mL")
        with _fl_col2:
            s_data["op_urine_output_ml"] = numeric_input("尿量", "urine_out", surg.get("op_urine_output_ml"), "mL")

        # --- CRF追加: 使用機器・腹腔鏡/ロボット詳細・JSES ---
        # op_approach codes: 6=腹腔鏡/補助, 7=ロボット
        _approach = s_data.get("op_approach")
        _ENDO_APPROACHES = (6, 7)  # 内視鏡外科系
        _LAP_APPROACHES = (6,)     # 腹腔鏡系（ロボット以外）
        if _approach in _ENDO_APPROACHES:
            section_card("内視鏡外科 詳細", "blue")
            _endo_cols = st.columns(3)
            with _endo_cols[0]:
                s_data["surgeon_jses_certification"] = selectbox_select(
                    "術者JSES技術認定医", {0: "なし", 1: "あり"},
                    "jses_cert", surg.get("surgeon_jses_certification"))
            if _approach == 7:  # ロボット支援
                with _endo_cols[1]:
                    s_data["robot_system_type"], s_data["robot_system_other"] = \
                        selectbox_with_other("使用機器", get_codebook("robot_system_type"),
                                             "robot_sys", "robot_sys_oth",
                                             surg.get("robot_system_type"), surg.get("robot_system_other", ""))
                with _endo_cols[2]:
                    s_data["robot_detail_type"], s_data["robot_detail_other"] = \
                        selectbox_with_other("ロボット手術詳細", get_codebook("robot_detail_type"),
                                             "robot_det", "robot_det_oth",
                                             surg.get("robot_detail_type"), surg.get("robot_detail_other", ""))
            elif _approach in _LAP_APPROACHES:  # 腹腔鏡系
                with _endo_cols[1]:
                    s_data["lap_detail_type"] = selectbox_select(
                        "腹腔鏡詳細", get_codebook("lap_detail_type"),
                        "lap_det", surg.get("lap_detail_type"))

        # --- 術中有害事象・損傷・併施手術 ---
        section_card("術中イベント", "blue")
        _ev1, _ev2, _ev3 = st.columns(3)
        with _ev1:
            _adv_options = {
                0: "なし", 1: "あり（肺梗塞）", 2: "あり（肺塞栓症）",
                3: "あり（心筋梗塞）", 4: "あり（脳梗塞）", 5: "あり（その他）",
            }
            s_data["op_adverse_event_yn"] = selectbox_select(
                "術中有害事象", _adv_options,
                "adv_ev", surg.get("op_adverse_event_yn"))
            if s_data.get("op_adverse_event_yn") == 5:  # その他
                s_data["op_adverse_event_detail"] = st.text_input(
                    "有害事象詳細", value=surg.get("op_adverse_event_detail", "") or "",
                    key="adv_ev_det")
        with _ev2:
            _injury_options = {
                0: "なし", 1: "膵", 2: "脾臓", 3: "胆管-総胆管",
                4: "門脈", 5: "臓器の主要な動脈", 6: "臓器の主要な静脈",
                7: "食道", 8: "十二指腸", 9: "空腸", 10: "回腸", 11: "結腸",
            }
            s_data["op_intra_injury_yn"] = selectbox_select(
                "術中損傷", _injury_options,
                "intra_inj", surg.get("op_intra_injury_yn"))
        with _ev3:
            _conc_options = {
                0: "なし", 1: "腸瘻造設", 2: "裂孔ヘルニア修復",
                3: "中心静脈栄養カテーテル・ポート留置術", 4: "その他",
            }
            s_data["concurrent_procedure_yn"] = selectbox_select(
                "併施手術", _conc_options,
                "conc_proc", surg.get("concurrent_procedure_yn"))
            if s_data.get("concurrent_procedure_yn") == 4:  # その他
                s_data["concurrent_procedure_detail"] = st.text_input(
                    "併施手術詳細", value=surg.get("concurrent_procedure_detail", "") or "",
                    key="conc_proc_det")

        # --- 合併切除 ---
        _comb_yn = selectbox_select(
            "合併切除臓器", {0: "なし", 1: "あり"},
            "comb_yn", surg.get("comb_yn"))
        s_data["comb_yn"] = _comb_yn

        if _comb_yn == 1:
            comb_flags = [
                ("distal_pancreatectomy", "膵尾側"),
                ("splenectomy", "脾"), ("transverse_colectomy", "横行結腸"),
                ("transverse_mesocolon", "横行結腸間膜"), ("diaphragm", "横隔膜"),
                ("thoracic_esophagus", "胸部食道"), ("partial_hepatectomy", "肝"),
                ("cholecystectomy", "胆嚢"), ("adrenalectomy", "副腎"),
                ("kidney", "腎"), ("small_intestine", "小腸"),
                ("abdominal_wall", "腹壁"), ("ovary", "卵巣"),
                ("portal_vein", "門脈"), ("appleby", "Appleby手術"),
                ("pancreatoduodenectomy", "膵頭十二指腸切除"),
                ("other", "その他"),
            ]
            comb_result = flag_group("", comb_flags, "comb_", surg, "comb",
                                      show_allnone=False)
            s_data.update(comb_result)
        else:
            s_data["comb_confirmed"] = 0

        # ── 食道癌 手術追加項目 ──
        if is_eso:
            eso_surgery_data = {}
            section_card("食道手術 追加情報", "teal")
            es1, es2, es3 = st.columns(3)
            with es1:
                eso_surgery_data["op_type"] = selectbox_select(
                    "手術区分", {1: "根治手術", 2: "姑息手術", 3: "審査手術"},
                    "eso_optype", default=eso_s.get("op_type"))
                eso_surgery_data["op_surgery_type"] = selectbox_select(
                    "術式区分", get_codebook("op_procedure_eso", 3),
                    "eso_surgtype", default=eso_s.get("op_surgery_type"))
                eso_surgery_data["op_surgery_type_other"] = st.text_input(
                    "術式（その他）",
                    value=eso_s.get("op_surgery_type_other", "") or "",
                    key="eso_surgtype_other")
            with es2:
                eso_surgery_data["op_endoscopic"] = selectbox_select(
                    "鏡視下手術",
                    {0: "なし", 1: "胸腔鏡", 2: "腹腔鏡", 3: "胸腔鏡+腹腔鏡",
                     4: "ロボット支援（胸腔）", 5: "ロボット支援（腹腔）",
                     6: "ロボット支援（胸腔+腹腔）"},
                    "eso_endoscopic", default=eso_s.get("op_endoscopic"))
                eso_surgery_data["op_conversion_detail"] = selectbox_select(
                    "開胸/開腹移行",
                    {0: "なし", 1: "開胸移行", 2: "開腹移行", 3: "両方"},
                    "eso_conv", default=eso_s.get("op_conversion_detail"))
                eso_surgery_data["op_conversion_reason"] = st.text_input(
                    "移行理由",
                    value=eso_s.get("op_conversion_reason", "") or "",
                    key="eso_conv_reason")
            with es3:
                eso_surgery_data["op_resection_extent"] = selectbox_select(
                    "切除範囲",
                    {1: "食道亜全摘", 2: "食道部分切除", 3: "食道全摘",
                     4: "下部食道+噴門側胃切除", 9: "その他"},
                    "eso_resect", default=eso_s.get("op_resection_extent"))
                eso_surgery_data["op_dissection_field"] = selectbox_select(
                    "郭清度(D)", get_codebook("op_dissection_eso"),
                    "eso_dissect_field", default=eso_s.get("op_dissection_field"))

            st.markdown("**再建**")
            er1, er2, er3 = st.columns(3)
            with er1:
                eso_surgery_data["op_reconstruction_route_eso"] = selectbox_select(
                    "再建経路", get_codebook("op_reconstruction_route"),
                    "eso_recon_route", default=eso_s.get("op_reconstruction_route_eso"))
            with er2:
                eso_surgery_data["op_reconstruction_organ"] = selectbox_select(
                    "再建臓器", get_codebook("op_reconstruction_eso"),
                    "eso_recon_organ", default=eso_s.get("op_reconstruction_organ"))
            with er3:
                eso_surgery_data["op_anastomosis_site"] = selectbox_select(
                    "吻合部位", get_codebook("eso_anastomosis_site"),
                    "eso_anast_site", default=eso_s.get("op_anastomosis_site"))

            st.markdown("**手術時間詳細**")
            et1, et2, et3 = st.columns(3)
            with et1:
                eso_surgery_data["op_anesthesia_time_min"] = numeric_input(
                    "麻酔時間", "eso_anesth_time",
                    default=eso_s.get("op_anesthesia_time_min"), suffix="分")
            with et2:
                eso_surgery_data["op_thoracic_time_min"] = numeric_input(
                    "胸腔操作時間", "eso_thorac_time",
                    default=eso_s.get("op_thoracic_time_min"), suffix="分")
            with et3:
                eso_surgery_data["op_thoracic_blood_loss_ml"] = numeric_input(
                    "胸腔内出血量", "eso_thorac_bl",
                    default=eso_s.get("op_thoracic_blood_loss_ml"), suffix="mL")

            eso_surgery_data["op_surgeons"] = st.text_input(
                "術者（自由記載）",
                value=eso_s.get("op_surgeons", "") or "",
                key="eso_surgeons")

            st.markdown("**CRF追加: 胸腔操作・再建**")
            _esc1, _esc2, _esc3 = st.columns(3)
            with _esc1:
                eso_surgery_data["thoracic_position"] = selectbox_select(
                    "胸腔操作体位", get_codebook("thoracic_position"),
                    "eso_thorac_pos", default=eso_s.get("thoracic_position"))
            with _esc2:
                eso_surgery_data["thoracoscope_detail_type"], eso_surgery_data["thoracoscope_detail_other"] = \
                    selectbox_with_other("胸腔鏡詳細", get_codebook("thoracoscope_detail_type"),
                                         "eso_thorac_det", "eso_thorac_det_oth",
                                         eso_s.get("thoracoscope_detail_type"), eso_s.get("thoracoscope_detail_other", ""))
            with _esc3:
                eso_surgery_data["reconstruction_stage"] = selectbox_select(
                    "一期/二期再建", {1: "一期", 2: "二期"},
                    "eso_recon_stage", default=eso_s.get("reconstruction_stage"))
            _esc4, _ = st.columns(2)
            with _esc4:
                eso_surgery_data["op_abdominal_time_min"] = numeric_input(
                    "腹部操作時間", "eso_abd_time",
                    default=eso_s.get("op_abdominal_time_min"), suffix="分")

            # 裂孔ヘルニア関連
            st.markdown("**裂孔ヘルニア関連**")
            hh1, hh2, hh3 = st.columns(3)
            with hh1:
                eso_surgery_data["hiatal_hernia_yn"] = selectbox_select(
                    "裂孔ヘルニア", {0: "なし", 1: "あり"},
                    "eso_hh_yn", default=eso_s.get("hiatal_hernia_yn"))
            with hh2:
                eso_surgery_data["hiatal_hernia_type"] = selectbox_select(
                    "ヘルニア型",
                    {1: "I型（滑脱型）", 2: "II型（傍食道型）",
                     3: "III型（混合型）", 4: "IV型（巨大）"},
                    "eso_hh_type", default=eso_s.get("hiatal_hernia_type"))
            with hh3:
                eso_surgery_data["gerd_la"] = selectbox_select(
                    "GERD (LA分類)",
                    {0: "なし", 1: "Grade A", 2: "Grade B",
                     3: "Grade C", 4: "Grade D"},
                    "eso_gerd_la", default=eso_s.get("gerd_la"))
            hh4, hh5, hh6 = st.columns(3)
            with hh4:
                eso_surgery_data["fundoplication"] = selectbox_select(
                    "噴門形成術",
                    {0: "なし", 1: "Nissen", 2: "Toupet", 3: "Dor", 9: "その他"},
                    "eso_fundo", default=eso_s.get("fundoplication"))
            with hh5:
                eso_surgery_data["vagus_nerve"] = selectbox_select(
                    "迷走神経", {0: "温存", 1: "切離"},
                    "eso_vagus", default=eso_s.get("vagus_nerve"))
            with hh6:
                eso_surgery_data["hiatal_hernia_gate_mm"] = numeric_input(
                    "ヘルニア門径", "eso_hh_gate",
                    default=eso_s.get("hiatal_hernia_gate_mm"), suffix="mm")

            save_data["eso_surgery"] = eso_surgery_data

        # --- 術後転帰（術後合併症の上に配置） ---
        section_card("術後転帰", "blue")

        # --- 輸血（術前・術中・術後） 横4列統一レイアウト ---
        # 術前輸血（72h以内）
        trf_pre1, trf_pre2, trf_pre3, trf_pre4 = st.columns(4)
        with trf_pre1:
            s_data["op_transfusion_preop"] = selectbox_select(
                "術前輸血（72h以内）", {0: "なし", 1: "あり"},
                "trf_preop", surg.get("op_transfusion_preop"))
        if s_data.get("op_transfusion_preop") == 1:
            with trf_pre2:
                s_data["op_transfusion_preop_rbc"] = st.number_input(
                    "術前 RBC（単位）", min_value=0, max_value=100, step=1,
                    value=int(surg.get("op_transfusion_preop_rbc") or 0), key="trf_preop_rbc")
            with trf_pre3:
                s_data["op_transfusion_preop_ffp"] = st.number_input(
                    "術前 FFP（単位）", min_value=0, max_value=100, step=1,
                    value=int(surg.get("op_transfusion_preop_ffp") or 0), key="trf_preop_ffp")
            with trf_pre4:
                s_data["op_transfusion_preop_pc"] = st.number_input(
                    "術前 PC（単位）", min_value=0, max_value=100, step=1,
                    value=int(surg.get("op_transfusion_preop_pc") or 0), key="trf_preop_pc")

        # 術中輸血
        trf_in1, trf_in2, trf_in3, trf_in4 = st.columns(4)
        with trf_in1:
            s_data["op_transfusion_intra"] = selectbox_select(
                "術中輸血", get_codebook("transfusion_yn"), "trf_in", surg.get("op_transfusion_intra"))
        if s_data.get("op_transfusion_intra") == 1:
            with trf_in2:
                s_data["op_transfusion_intra_rbc"] = st.number_input(
                    "術中 RBC（単位）", min_value=0, max_value=100, step=1,
                    value=int(surg.get("op_transfusion_intra_rbc") or 0), key="trf_in_rbc")
            with trf_in3:
                s_data["op_transfusion_intra_ffp"] = st.number_input(
                    "術中 FFP（単位）", min_value=0, max_value=100, step=1,
                    value=int(surg.get("op_transfusion_intra_ffp") or 0), key="trf_in_ffp")
            with trf_in4:
                s_data["op_transfusion_intra_pc"] = st.number_input(
                    "術中 PC（単位）", min_value=0, max_value=100, step=1,
                    value=int(surg.get("op_transfusion_intra_pc") or 0), key="trf_in_pc")
            _auto_intra_col, _ = st.columns(2)
            with _auto_intra_col:
                s_data["op_transfusion_intra_autologous"] = st.number_input(
                    "術中 自己血（単位）", min_value=0, max_value=100, step=1,
                    value=int(surg.get("op_transfusion_intra_autologous") or 0),
                    key="trf_in_auto", help="セルセーバー含む")

        # 術後輸血
        trf_po1, trf_po2, trf_po3, trf_po4 = st.columns(4)
        with trf_po1:
            s_data["op_transfusion_post"] = selectbox_select(
                "術後輸血", get_codebook("transfusion_yn"), "trf_post", surg.get("op_transfusion_post"))
        if s_data.get("op_transfusion_post") == 1:
            with trf_po2:
                s_data["op_transfusion_post_rbc"] = st.number_input(
                    "術後 RBC（単位）", min_value=0, max_value=100, step=1,
                    value=int(surg.get("op_transfusion_post_rbc") or 0), key="trf_post_rbc")
            with trf_po3:
                s_data["op_transfusion_post_ffp"] = st.number_input(
                    "術後 FFP（単位）", min_value=0, max_value=100, step=1,
                    value=int(surg.get("op_transfusion_post_ffp") or 0), key="trf_post_ffp")
            with trf_po4:
                s_data["op_transfusion_post_pc"] = st.number_input(
                    "術後 PC（単位）", min_value=0, max_value=100, step=1,
                    value=int(surg.get("op_transfusion_post_pc") or 0), key="trf_post_pc")

        if s_data.get("op_transfusion_post") == 1:
            _auto_post_col, _ = st.columns(2)
            with _auto_post_col:
                s_data["op_transfusion_post_autologous"] = st.number_input(
                    "術後 自己血（単位）", min_value=0, max_value=100, step=1,
                    value=int(surg.get("op_transfusion_post_autologous") or 0),
                    key="trf_post_auto", help="セルセーバー含む")

        # 空白行（再手術セクションとの区切り）
        st.markdown("")

        col1, col2, _, _ = st.columns(4)
        with col1:
            s_data["op_reop_yn"] = selectbox_select("再手術", {0: "なし", 1: "あり"}, "reop", surg.get("op_reop_yn"))
            if s_data.get("op_reop_yn") == 1:
                s_data["op_reop_30d"] = selectbox_select("30日以内再手術", {0: "いいえ", 1: "はい"}, "reop30", surg.get("op_reop_30d"))
        with col2:
            s_data["readmission_30d"] = selectbox_select("30日以内再入院", {0: "なし", 1: "あり"}, "readm", surg.get("readmission_30d"))
            st.caption("※退院後30日以内の予定しない再入院")
            if s_data.get("readmission_30d") == 1:
                s_data["readmission_30d_reason"] = st.text_input("再入院理由", value=surg.get("readmission_30d_reason", "") or "", key="readm_rsn")

        col1, _ = st.columns(2)
        with col1:
            s_data["mortality_inhospital"] = selectbox_select("在院死亡", get_codebook("mortality_inhospital"), "mort_hosp", surg.get("mortality_inhospital"))

        col1, _ = st.columns(2)
        with col1:
            patients_data["discharge_destination"] = selectbox_select(
                "退院先", get_codebook("discharge_destination"), "dischg_dest", p.get("discharge_destination"))

        # --- 術後合併症 ---
            # ── 食道 周術期経過 ──
            eso_course_data = {}
            section_card("食道 周術期経過", "teal")
            with st.expander("📋 食事・ドレーン・ICU", expanded=False):
                st.markdown("**食事開始**")
                mc1, mc2, mc3 = st.columns(3)
                with mc1:
                    eso_course_data["meal_water_date"] = st.text_input(
                        "飲水開始日", value=eso_c.get("meal_water_date", "") or "",
                        key="eso_water", placeholder="YYYY-MM-DD")
                    eso_course_data["meal_liquid_date"] = st.text_input(
                        "流動食開始日", value=eso_c.get("meal_liquid_date", "") or "",
                        key="eso_liquid", placeholder="YYYY-MM-DD")
                with mc2:
                    eso_course_data["meal_3bu_date"] = st.text_input(
                        "3分粥開始日", value=eso_c.get("meal_3bu_date", "") or "",
                        key="eso_3bu", placeholder="YYYY-MM-DD")
                    eso_course_data["meal_5bu_date"] = st.text_input(
                        "5分粥開始日", value=eso_c.get("meal_5bu_date", "") or "",
                        key="eso_5bu", placeholder="YYYY-MM-DD")
                with mc3:
                    eso_course_data["meal_zenkayu_date"] = st.text_input(
                        "全粥開始日", value=eso_c.get("meal_zenkayu_date", "") or "",
                        key="eso_zenkayu", placeholder="YYYY-MM-DD")
                    eso_course_data["icu_discharge_date"] = st.text_input(
                        "ICU退室日", value=eso_c.get("icu_discharge_date", "") or "",
                        key="eso_icu_dc", placeholder="YYYY-MM-DD")

                st.markdown("**食事中断 → 再開**")
                mc4, mc5, mc6 = st.columns(3)
                with mc4:
                    eso_course_data["npo_date"] = st.text_input(
                        "絶食日", value=eso_c.get("npo_date", "") or "",
                        key="eso_npo", placeholder="YYYY-MM-DD")
                    eso_course_data["meal_water_date2"] = st.text_input(
                        "飲水再開日", value=eso_c.get("meal_water_date2", "") or "",
                        key="eso_water2", placeholder="YYYY-MM-DD")
                with mc5:
                    eso_course_data["meal_liquid_date2"] = st.text_input(
                        "流動食再開日", value=eso_c.get("meal_liquid_date2", "") or "",
                        key="eso_liquid2", placeholder="YYYY-MM-DD")
                    eso_course_data["meal_3bu_date2"] = st.text_input(
                        "3分粥再開日", value=eso_c.get("meal_3bu_date2", "") or "",
                        key="eso_3bu2", placeholder="YYYY-MM-DD")
                with mc6:
                    eso_course_data["meal_5bu_date2"] = st.text_input(
                        "5分粥再開日", value=eso_c.get("meal_5bu_date2", "") or "",
                        key="eso_5bu2", placeholder="YYYY-MM-DD")
                    eso_course_data["meal_zenkayu_date2"] = st.text_input(
                        "全粥再開日", value=eso_c.get("meal_zenkayu_date2", "") or "",
                        key="eso_zenkayu2", placeholder="YYYY-MM-DD")

                st.markdown("**ドレーン抜去日**")
                dr1, dr2, dr3 = st.columns(3)
                with dr1:
                    eso_course_data["drain_left_chest_date"] = st.text_input(
                        "左胸腔ドレーン抜去日",
                        value=eso_c.get("drain_left_chest_date", "") or "",
                        key="eso_drain_l", placeholder="YYYY-MM-DD")
                with dr2:
                    eso_course_data["drain_right_chest_date"] = st.text_input(
                        "右胸腔ドレーン抜去日",
                        value=eso_c.get("drain_right_chest_date", "") or "",
                        key="eso_drain_r", placeholder="YYYY-MM-DD")
                with dr3:
                    eso_course_data["drain_neck_date"] = st.text_input(
                        "頸部ドレーン抜去日",
                        value=eso_c.get("drain_neck_date", "") or "",
                        key="eso_drain_neck", placeholder="YYYY-MM-DD")

                do1, do2 = st.columns(2)
                with do1:
                    eso_course_data["drain_other1"] = st.text_input(
                        "その他ドレーン1（名称）",
                        value=eso_c.get("drain_other1", "") or "",
                        key="eso_drain_o1")
                    eso_course_data["drain_other1_date"] = st.text_input(
                        "抜去日",
                        value=eso_c.get("drain_other1_date", "") or "",
                        key="eso_drain_o1d", placeholder="YYYY-MM-DD")
                with do2:
                    eso_course_data["drain_other2"] = st.text_input(
                        "その他ドレーン2（名称）",
                        value=eso_c.get("drain_other2", "") or "",
                        key="eso_drain_o2")
                    eso_course_data["drain_other2_date"] = st.text_input(
                        "抜去日",
                        value=eso_c.get("drain_other2_date", "") or "",
                        key="eso_drain_o2d", placeholder="YYYY-MM-DD")

                st.markdown("**経腸栄養・ICU・再入院**")
                tf1, tf2, tf3 = st.columns(3)
                with tf1:
                    eso_course_data["tube_feeding_yn"] = selectbox_select(
                        "経腸栄養", {0: "なし", 1: "あり"},
                        "eso_tube_yn", default=eso_c.get("tube_feeding_yn"))
                    eso_course_data["tube_feeding_start"] = st.text_input(
                        "経腸栄養開始日",
                        value=eso_c.get("tube_feeding_start", "") or "",
                        key="eso_tube_start", placeholder="YYYY-MM-DD")
                    eso_course_data["tube_feeding_end"] = st.text_input(
                        "経腸栄養終了日",
                        value=eso_c.get("tube_feeding_end", "") or "",
                        key="eso_tube_end", placeholder="YYYY-MM-DD")
                with tf2:
                    eso_course_data["icu_type"] = selectbox_select(
                        "ICU種別",
                        {0: "入室なし", 1: "ICU", 2: "HCU", 3: "ICU→HCU"},
                        "eso_icu_type", default=eso_c.get("icu_type"))
                    eso_course_data["reintubation_yn"] = selectbox_select(
                        "再挿管", {0: "なし", 1: "あり"},
                        "eso_reintub", default=eso_c.get("reintubation_yn"))
                with tf3:
                    eso_course_data["readmission_yn"] = selectbox_select(
                        "再入院", {0: "なし", 1: "あり"},
                        "eso_readmit", default=eso_c.get("readmission_yn"))
                    eso_course_data["readmission_reason"] = st.text_input(
                        "再入院理由",
                        value=eso_c.get("readmission_reason", "") or "",
                        key="eso_readmit_reason")

                st.markdown("**再手術**")
                ro1, ro2 = st.columns(2)
                with ro1:
                    eso_course_data["reop_date"] = st.text_input(
                        "再手術日1",
                        value=eso_c.get("reop_date", "") or "",
                        key="eso_reop1", placeholder="YYYY-MM-DD")
                    eso_course_data["reop2_date"] = st.text_input(
                        "再手術日2",
                        value=eso_c.get("reop2_date", "") or "",
                        key="eso_reop2", placeholder="YYYY-MM-DD")
                with ro2:
                    eso_course_data["reop_detail"] = st.text_area(
                        "再手術詳細",
                        value=eso_c.get("reop_detail", "") or "",
                        key="eso_reop_detail", height=80)

                st.markdown("**吻合部狭窄**")
                sx1, sx2, sx3 = st.columns(3)
                with sx1:
                    eso_course_data["stricture_yn"] = selectbox_select(
                        "吻合部狭窄", {0: "なし", 1: "あり"},
                        "eso_stricture", default=eso_c.get("stricture_yn"))
                with sx2:
                    eso_course_data["stricture_first_date"] = st.text_input(
                        "初回拡張日",
                        value=eso_c.get("stricture_first_date", "") or "",
                        key="eso_strict_date", placeholder="YYYY-MM-DD")
                with sx3:
                    eso_course_data["stricture_count"] = numeric_input(
                        "拡張回数", "eso_strict_cnt",
                        default=eso_c.get("stricture_count"))

                eso_course_data["course_notes"] = st.text_area(
                    "経過メモ",
                    value=eso_c.get("course_notes", "") or "",
                    key="eso_course_notes", height=80)

            save_data["eso_course"] = eso_course_data

        section_card("術後合併症", "red")
        s_data["op_complication_yn"] = selectbox_select("術後合併症", {0: "なし", 1: "あり"}, "comp_yn", surg.get("op_complication_yn"))

        if s_data.get("op_complication_yn") == 1:
            ref_table("Clavien-Dindo 分類",
                ["Grade","定義"],
                [["I","正常な術後経過からの逸脱。薬物療法・外科的治療・内視鏡的治療・IVR治療を要さないもの"],
                 ["II","制吐剤・解熱薬・鎮痛薬・利尿薬以外の薬物療法を要する。輸血・中心静脈栄養を含む"],
                 ["IIIa","全身麻酔を要さない外科的治療・内視鏡的治療・IVR治療"],
                 ["IIIb","全身麻酔を要する外科的治療・内視鏡的治療・IVR治療"],
                 ["IVa","ICU管理を要する単一臓器不全（中枢神経系含む）"],
                 ["IVb","ICU管理を要する多臓器不全"],
                 ["V","患者の死亡"]])

            st.markdown("**各合併症の CDグレード を選択（なし以外を選択すると発症日入力欄が出ます）**")

            comp_flags = [
                ("ssi", "SSI（手術部位感染）"), ("wound_dehiscence", "創離開"),
                ("intra_abd_abscess", "腹腔内膿瘍"), ("bleeding", "術後出血"),
                ("ileus", "イレウス"), ("dvt_pe", "DVT/PE"),
                ("pneumonia", "肺炎"), ("atelectasis", "無気肺"),
                ("uti", "尿路感染"), ("delirium", "せん妄"),
                ("cardiac", "心合併症"), ("dge", "胃内容排出遅延(DGE)"),
                ("perforation", "穿孔"), ("cholelithiasis", "胆石"),
                ("anastomotic_leak", "縫合不全"), ("anastomotic_stricture", "吻合部狭窄"),
                ("anastomotic_bleeding", "吻合部出血"), ("pancreatic_fistula", "膵液瘻"),
                ("bile_leak", "胆汁漏"), ("duodenal_stump_leak", "十二指腸断端瘻"),
                ("rln_palsy", "反回神経麻痺"), ("chylothorax", "乳び胸"),
                ("empyema", "膿胸"), ("pneumothorax", "気胸"),
                ("ards", "ARDS"), ("dic", "DIC"),
                ("sepsis", "敗血症"), ("renal_failure", "腎不全"),
                ("hepatic_failure", "肝不全"), ("other", "その他"),
            ]
            op_date = p.get("surgery_date") or patients_data.get("surgery_date")
            comp_result = complication_group(comp_flags, surg, op_date)
            auto_max = comp_result.pop("_max_cd_grade", 0)
            s_data.update(comp_result)

            # 最大CDグレード自動算出＋表示
            s_data["op_cd_grade_max"] = auto_max
            if auto_max > 0:
                cd_labels = {1: "I", 2: "II", 3: "IIIa", 4: "IIIb",
                             5: "IVa", 6: "IVb", 7: "V"}
                st.info(f"⚡ 最大 Clavien-Dindo: **Grade {cd_labels.get(auto_max, auto_max)}** （自動算出）")
        else:
            s_data["op_cd_grade_max"] = 0


        save_data["surgery"] = s_data

    # ==========================================================
    # Tab: 病理（リンパ節を統合）
    # ==========================================================
    path_tab_idx = 4
    with tabs[path_tab_idx]:
        st.markdown("### 病理診断")
        pa_data = {}
        hist_field = "histology_gastric" if is_gastric else "histology_eso"

        # ▼▼ (移動) 浸潤臓器・遠隔転移フラグの定義 ▼▼
        p_inv_flags = [
            ("pancreas", "膵臓"), ("liver", "肝"), ("transverse_colon", "横行結腸"),
            ("spleen", "脾"), ("diaphragm", "横隔膜"),
            ("abdominal_wall", "腹壁"), ("adrenal", "副腎"),
            ("kidney", "腎"), ("small_intestine", "小腸"),
            ("retroperitoneum", "後腹膜"), ("transverse_mesocolon", "横行結腸間膜"),
            ("unknown", "不明"), ("other", "その他"),
        ]
        p_meta_flags = [
            ("peritoneal", "腹膜"), ("liver", "肝"), ("lung", "肺"),
            ("lymph_node", "遠隔LN"), ("bone", "骨"), ("brain", "脳"),
            ("ovary", "卵巣"), ("adrenal", "副腎"), ("pleura", "胸膜"),
            ("skin", "皮膚"), ("marrow", "骨髄"), ("meninges", "髄膜"),
            ("cytology", "洗浄細胞診"), ("other", "その他"),
        ]

        # ── 1. 腫瘍個数・占居部位 ──
        col1, col2, col3 = st.columns(3)
        with col1:
            pa_data["p_tumor_number"] = numeric_input("腫瘍個数(病理)", "p_tumor_n", path.get("p_tumor_number"))

        col1, col2, _ = st.columns(3)
        with col1:
            pa_data["p_location_long"] = location_multiselect(
                "病理占居部位(長軸)", _LOC_LONG_ORDER, _LOC_LONG_SORT,
                "p_loc_long", path.get("p_location_long"))
        with col2:
            pa_data["p_location_short"] = location_multiselect(
                "病理占居部位(短軸)", _LOC_SHORT_ORDER, [1, 2, 3, 4, 5, 99],
                "p_loc_short", path.get("p_location_short"), combine_fn=_combine_short)

        # ── 2. 病理肉眼型・腫瘍径 ──
        col1, col2_pt0, _ = st.columns(3)
        with col1:
            pa_data["p_macroscopic_type"] = selectbox_select("病理肉眼型", get_codebook("macroscopic_type"), "p_macro", path.get("p_macroscopic_type"))
        with col2_pt0:
            if pa_data.get("p_macroscopic_type") == 0:
                _pt0_options = {v: lab for v, lab, _, _, _ in get_codebook("type0_subclass")}
                _existing_pt0 = path.get("p_type0_subclass") or ""
                _existing_pt0_list = [int(x) for x in str(_existing_pt0).split(",") if x.strip().isdigit()]
                _pt0_selected = st.multiselect(
                    "Type 0 亜型（病理）",
                    options=list(_pt0_options.keys()),
                    default=[v for v in _existing_pt0_list if v in _pt0_options],
                    format_func=lambda x: _pt0_options[x],
                    key="p_type0sub_multi"
                )
                pa_data["p_type0_subclass"] = ",".join(str(v) for v in sorted(_pt0_selected)) if _pt0_selected else None

        col1, col2, col3 = st.columns(3)
        with col1:
            pa_data["p_size_major_mm"] = numeric_input("腫瘍長径(病理)", "p_size_maj", path.get("p_size_major_mm"), "mm")
        with col2:
            pa_data["p_size_minor_mm"] = numeric_input("腫瘍短径(病理)", "p_size_min", path.get("p_size_minor_mm"), "mm")

        # ── 3. 組織型 ──
        col_phist, _ = st.columns(2)
        with col_phist:
            pa_data["p_histology1"] = selectbox_select("組織型", get_codebook(hist_field), "ph1", path.get("p_histology1"))

        # ── EGJ条件表示（食道癌の場合） ──
        if is_eso:
            st.write("")
            col1, col2, col3 = st.columns(3)
            with col1:
                pa_data["p_location_egj"] = selectbox_select("病理 EGJ Siewert", get_codebook("egj_siewert"), "p_egj_s", path.get("p_location_egj"))
            with col2:
                pa_data["p_egj_distance_mm"] = numeric_input("病理 EGJ距離", "p_egj_dist", path.get("p_egj_distance_mm"), "mm")
            with col3:
                pa_data["p_esoph_invasion_mm"] = numeric_input("病理 食道浸潤長", "p_eso_inv", path.get("p_esoph_invasion_mm"), "mm")
            # CRF追加: 接合部癌フラグ・腫瘍中心位置
            _egj1, _egj2 = st.columns(2)
            with _egj1:
                pa_data["egj_cancer_yn"] = selectbox_select(
                    "接合部癌", {0: "いいえ", 1: "はい"},
                    "egj_cancer", path.get("egj_cancer_yn"))
            with _egj2:
                if pa_data.get("egj_cancer_yn") == 1:
                    pa_data["egj_center_position"] = selectbox_select(
                        "腫瘍中心位置", get_codebook("egj_center_position"),
                        "egj_center", path.get("egj_center_position"))

        # ── 4. pTNM / P / H / CY / pStage ──
        st.write("")
        section_card("病理病期 (pTNM)", "blue")
        col_t, col_n, col_cy, col_m, col_stage = st.columns([1, 1, 1, 1, 1.5])
        
        with col_t:
            pa_data["p_depth"] = selectbox_select(get_form_label("p_depth"), get_codebook(f"p_depth_{ver_field_prefix}", version_id), "pt", path.get("p_depth"))
        with col_n:
            pa_data["p_ln_metastasis"] = selectbox_select(get_form_label("p_ln_metastasis"), get_codebook(f"p_ln_{ver_field_prefix}", version_id), "pn", path.get("p_ln_metastasis"))
        with col_cy:
            pa_data["p_cytology"] = selectbox_select("CY", get_codebook("cytology"), "p_cy", path.get("p_cytology"))
        with col_m:
            pa_data["p_distant_metastasis"] = selectbox_select("pM", get_codebook("p_distant_metastasis"), "pm_dist", path.get("p_distant_metastasis"))

        # ▼▼ T4b / M1 の判定 ▼▼
        pt_val = pa_data.get("p_depth")
        is_t4b = False
        if pt_val is not None:
            pt_label = str(get_codebook(f"p_depth_{ver_field_prefix}", version_id).get(pt_val, ""))
            if "T4b" in pt_label or "T4b" in str(pt_val):
                is_t4b = True

        pm_val = pa_data.get("p_distant_metastasis")
        is_m1 = False
        if pm_val is not None:
            pm_label = str(get_codebook("p_distant_metastasis").get(pm_val, ""))
            if "M1" in pm_label or "1" in str(pm_val):
                is_m1 = True

        # ▼▼ T4b または M1時：浸潤臓器、P/H、遠隔転移部位を横並びで表示 ▼▼
        if is_t4b or is_m1:
            st.write("")
            col_inv, col_ph, col_meta = st.columns([1.5, 1, 2])
            
            with col_inv:
                if is_t4b:
                    p_inv_result = flag_group("病理的浸潤臓器 (pT4b)", p_inv_flags, "p_inv_", path, "p_inv", show_allnone=False)
                    pa_data.update(p_inv_result)
            
            with col_ph:
                if is_m1:
                    st.markdown("##### P / H")
                    _p_help = "P1a: 胃,大網,小網,横行結腸間膜前葉,膵被膜,脾臓に限局 / P1b: 上腹部の腹膜(臍より頭側の壁側腹膜,横行結腸より頭側の臓側腹膜) / P1c: 中下腹部の腹膜 / P1x: 腹膜転移を認めるが分布不明"
                    pa_data["p_peritoneal"] = selectbox_select("P (腹膜転移)", get_codebook("p_peritoneal_status"), "p_peri", path.get("p_peritoneal"), help_text=_p_help)
                    pa_data["p_liver"] = selectbox_select("H (肝転移)", get_codebook("p_liver_metastasis_status"), "p_liver", path.get("p_liver"))
                    
            with col_meta:
                if is_m1:
                    p_meta_result = flag_group("病理的遠隔転移部位", p_meta_flags, "p_meta_", path, "p_meta", show_allnone=False)
                    pa_data.update(p_meta_result)

        # ▼▼ T4b以外時：自動でいいえ(0)をセット ▼▼
        if not is_t4b:
            for key, _ in p_inv_flags:
                pa_data[f"p_inv_{key}"] = 0

        # ▼▼ M0時：自動で陰性/いいえをセット ▼▼
        if not is_m1:
            pa_data["p_peritoneal"] = 0
            pa_data["p_liver"] = 0
            for key, _ in p_meta_flags:
                pa_data[f"p_meta_{key}"] = 0

        # pStage 自動計算
        with col_stage:
            auto_ps = compute_stage(
                pa_data.get("p_depth"), pa_data.get("p_ln_metastasis"),
                pa_data.get("p_distant_metastasis"),
                is_gastric=is_gastric, context="pathological",
                p_peritoneal=pa_data.get("p_peritoneal"),
                p_liver=pa_data.get("p_liver"),
                p_cytology=pa_data.get("p_cytology"))
            ps_default = auto_ps if auto_ps is not None else path.get("p_stage")
            pa_data["p_stage"] = selectbox_select(get_form_label("p_stage"), get_codebook(f"p_stage_{ver_field_prefix}", version_id), "pstg", ps_default)
            if auto_ps is not None:
                ps_label = get_codebook(f"p_stage_{ver_field_prefix}", version_id).get(auto_ps, "")
                st.caption(f"⚡ 自動算出: {ps_label}")

        # pTNM 参照テーブルの描画はそのまま維持
        ref_pn_col, ref_ps_col = st.columns(2)
        with ref_pn_col:
            if is_gastric:
                ref_table("リンパ節転移の定義（pN・胃癌）",
                    ["分類","個数"],
                    [["NX","不明"],["N0","転移なし"],["N1","1〜2個"],
                     ["N2","3〜6個"],["N3a","7〜15個"],["N3b","16個以上"]])
            else:
                ref_table("リンパ節転移の定義（pN・扁平上皮癌）",
                    ["分類","個数"],
                    [["NX","不明"],["N0","転移なし"],["N1","1〜2個"],
                     ["N2","3〜6個"],["N3","7個以上"]])
                
                ref_table("リンパ節転移の定義（pN・腺癌）",
                    ["分類","個数"],
                    [["NX","不明"],["N0","転移なし"],["N1","1〜2個"],
                     ["N2","3〜6個"],["N3a","7〜15個"],["N3b","16個以上"]],
                    note="胃癌取扱い規約に準ずる")

        with ref_ps_col:
            if is_gastric:
                ref_table("病理病期分類（pStage",
                    ["","N0","N1","N2","N3a","N3b","M1"],
                    [["T1a/T1b","IA","IB","IIA","IIB","IIIB","IV"],
                     ["T2","IB","IIA","IIB","IIIA","IIIB","IV"],
                     ["T3","IIA","IIB","IIIA","IIIB","IIIC","IV"],
                     ["T4a","IIB","IIIA","IIIA","IIIB","IIIC","IV"],
                     ["T4b","IIIA","IIIB","IIIB","IIIC","IIIC","IV"]],
                    note="胃癌取扱い規約15版")
            else:
                ref_table("病理分類（pStage・扁平上皮癌）",
                    ["","N0","N1","N2","N3/M1a","M1b"],
                    [["T0","0","IIA","IIA","IIIA","IVB"],
                     ["T1a","0","IIA","IIB","IIIA","IVB"],
                     ["T1b","I","IIA","IIIA","IIIA","IVB"],
                     ["T2","IIA","IIB","IIIA","IIIB","IVB"],
                     ["T3","IIB","IIIA","IIIB","IVA","IVB"],
                     ["T4a","IIIB","IIIB","IVA","IVA","IVB"],
                     ["T4b","IVA","IVA","IVA","IVA","IVB"]],
                    note="食道癌取扱い規約12版")

                ref_table("病理分類（pStage・腺癌）",
                    ["","N0","N1","N2","N3a","N3b","M1"],
                    [["T1","IA","IB","IIA","IIB","IIIB","IV"],
                     ["T2","IB","IIA","IIB","IIIA","IIIB","IV"],
                     ["T3","IIA","IIB","IIIA","IIIB","IIIC","IV"],
                     ["T4a","IIB","IIIA","IIIA","IIIB","IIIC","IV"],
                     ["T4b","IIIA","IIIB","IIIB","IIIC","IIIC","IV"]],
                    note="食道癌取扱い規約12版（胃癌取扱い規約に準ずる）")

        # ── 5. 脈管侵襲・断端・腫瘍遺残 ──
        st.write("")
        section_card("脈管侵襲・断端・腫瘍遺残", "blue")
        col1, col2, col3 = st.columns(3)
        with col1:
            pa_data["p_inf"] = selectbox_select(get_form_label("p_inf"), get_codebook("inf_pattern"), "p_inf", path.get("p_inf"))
        with col2:
            pa_data["p_ly"] = selectbox_select(get_form_label("p_ly"), get_codebook("lymphatic_invasion"), "p_ly", path.get("p_ly"))
        with col3:
            pa_data["p_v"] = selectbox_select(get_form_label("p_v"), get_codebook("venous_invasion"), "p_v", path.get("p_v"))

        col1, col2, col3 = st.columns(3)
        with col1:
            pa_data["p_pm"] = selectbox_select("PM", get_codebook("pm_status"), "p_pm", path.get("p_pm"),
                help_text="PM1選択時のみ断端距離(mm)を入力。PMX=距離測定不能")
        with col2:
            if pa_data.get("p_pm") == 1:
                pa_data["p_pm_mm"] = numeric_input("PM(mm)", "p_pm_mm", path.get("p_pm_mm"), "mm", is_float=True)
            else:
                pa_data["p_pm_mm"] = None

        col1, col2, col3 = st.columns(3)
        with col1:
            pa_data["p_dm"] = selectbox_select("DM", get_codebook("dm_status"), "p_dm", path.get("p_dm"),
                help_text="DM1選択時のみ断端距離(mm)を入力。DMX=距離測定不能")
        with col2:
            if pa_data.get("p_dm") == 1:
                pa_data["p_dm_mm"] = numeric_input("DM(mm)", "p_dm_mm", path.get("p_dm_mm"), "mm", is_float=True)
            else:
                pa_data["p_dm_mm"] = None

        col1, col2, col3 = st.columns(3)
        with col1:
            _resid_help = "R0: 癌の遺残がない / R1: 癌の顕微鏡的遺残がある / R2: 癌の肉眼的遺残がある / RX: 癌の遺残が評価できない / 不明: XX"
            pa_data["p_residual_tumor"] = selectbox_select("腫瘍遺残", get_codebook("residual_tumor"), "resid", path.get("p_residual_tumor"), help_text=_resid_help)

        # ── 6. 組織学的化療効果 ──
        st.write("")
        section_card("組織学的効果判定", "blue")
        
        # 左側に2つの入力欄を縦並び、右側に表を配置
        col_eff_left, col_eff_right = st.columns([1, 1])
        
        with col_eff_left:
            pa_data["p_chemo_effect"] = selectbox_select("薬物放射線治療の効果判定基準", get_codebook("chemo_effect_pathologic"), "p_chemo_eff", path.get("p_chemo_effect"))
            pa_data["p_ln_chemo_effect_text"] = st.text_input(
                "リンパ節転移巣における治療効果", value=path.get("p_ln_chemo_effect_text", "") or "",
                key="p_ln_eff_text", help="癌細胞の消失・壊死・変性の有無などを記載")
            
        with col_eff_right:
            ref_table("胃原発巣の効果判定基準",
                ["判定","基準"],
                [["eCR","悪性所見の消失かつ、生検にて癌細胞が確認されない"],
                 ["ePR","著明な縮小（一方向測定として2/3以下、面積として1/2以下、体積として1/3以下）"],
                 ["eSD","eCR、ePR、ePDに含まれない場合"],
                 ["ePD","明らかな増大"]])
        
        # ── 7. バイオマーカー ──
        st.write("")
        section_card("バイオマーカー", "blue")
        col1, col2, col3 = st.columns(3)
        with col1:
            pa_data["msi_status"] = selectbox_select("MSI", get_codebook("msi_status"), "msi", path.get("msi_status"))
            pa_data["her2_status"] = selectbox_select("HER2", get_codebook("her2_status"), "her2", path.get("her2_status"))
        with col2:
            pa_data["pdl1_status"] = selectbox_select("PD-L1", get_codebook("pdl1_status"), "pdl1", path.get("pdl1_status"))
            pa_data["pdl1_cps"] = numeric_input("PD-L1 CPS", "cps", path.get("pdl1_cps"), is_float=True)
            pa_data["pdl1_tps"] = numeric_input("PD-L1 TPS", "tps", path.get("pdl1_tps"), "%", is_float=True)
        with col3:
            pa_data["claudin18_status"] = selectbox_select("Claudin 18.2", get_codebook("claudin18_status"), "cldn18", path.get("claudin18_status"))
            pa_data["fgfr2b_status"] = selectbox_select("FGFR2b", get_codebook("fgfr2b_status"), "fgfr2b", path.get("fgfr2b_status"))
            pa_data["ebv_status"] = selectbox_select("EBV", get_codebook("ebv_status"), "ebv", path.get("ebv_status"))

        # ── 食道癌 病理追加項目 ──
        if is_eso:
            eso_path_data = {}
            section_card("食道病理 追加情報", "teal")
            ep1, ep2, ep3 = st.columns(3)
            with ep1:
                eso_path_data["p_pretreatment"] = selectbox_select(
                    "術前治療",
                    {0: "なし", 1: "化学療法", 2: "放射線化学療法", 3: "放射線療法"},
                    "eso_p_pretx", default=eso_pa.get("p_pretreatment"))
                eso_path_data["p_depth_jce"] = selectbox_select(
                    "pT（食道癌取扱い規約）", get_codebook("p_depth_eso", 3),
                    "eso_pdepth_jce", default=eso_pa.get("p_depth_jce"))
                eso_path_data["p_ln_jce"] = selectbox_select(
                    "pN（食道癌取扱い規約）", get_codebook("p_ln_eso", 3),
                    "eso_pln_jce", default=eso_pa.get("p_ln_jce"))
            with ep2:
                eso_path_data["p_depth_uicc"] = selectbox_select(
                    "pT（UICC）", get_codebook("p_depth_eso", 3),
                    "eso_pdepth_uicc", default=eso_pa.get("p_depth_uicc"))
                eso_path_data["p_ln_uicc"] = selectbox_select(
                    "pN（UICC）", get_codebook("p_ln_eso", 3),
                    "eso_pln_uicc", default=eso_pa.get("p_ln_uicc"))
                eso_path_data["p_stage_jce"] = selectbox_select(
                    "pStage（規約）", get_codebook("p_stage_eso", 3),
                    "eso_pstage_jce", default=eso_pa.get("p_stage_jce"))
            with ep3:
                eso_path_data["p_stage_uicc"] = selectbox_select(
                    "pStage（UICC）", get_codebook("p_stage_eso", 3),
                    "eso_pstage_uicc", default=eso_pa.get("p_stage_uicc"))
                eso_path_data["p_rm"] = selectbox_select(
                    "断端 (RM)",
                    {0: "陰性", 1: "陽性（口側）", 2: "陽性（肛門側）", 3: "陽性（両側）"},
                    "eso_rm", default=eso_pa.get("p_rm"))
                eso_path_data["p_rm_mm"] = numeric_input(
                    "断端距離", "eso_rm_mm",
                    default=eso_pa.get("p_rm_mm"), suffix="mm")

            ep4, ep5, ep6 = st.columns(3)
            with ep4:
                eso_path_data["p_im_eso"] = selectbox_select(
                    "食道IM", {0: "なし", 1: "あり"},
                    "eso_im_eso", default=eso_pa.get("p_im_eso"))
            with ep5:
                eso_path_data["p_im_stomach"] = selectbox_select(
                    "胃IM", {0: "なし", 1: "あり"},
                    "eso_im_stomach", default=eso_pa.get("p_im_stomach"))
            with ep6:
                eso_path_data["p_multiple_cancer_eso"] = selectbox_select(
                    "病理多発癌", {0: "なし", 1: "あり"},
                    "eso_p_multi", default=eso_pa.get("p_multiple_cancer_eso"))

            ep7, ep8, ep9 = st.columns(3)
            with ep7:
                eso_path_data["p_curability"] = selectbox_select(
                    "根治度", {0: "R0", 1: "R1", 2: "R2"},
                    "eso_curab", default=eso_pa.get("p_curability"))
            with ep8:
                eso_path_data["p_residual_factor"] = st.text_input(
                    "残存腫瘍詳細",
                    value=eso_pa.get("p_residual_factor", "") or "",
                    key="eso_residual_detail")
            with ep9:
                eso_path_data["p_treatment_effect"] = selectbox_select(
                    "組織学的治療効果判定", get_codebook("eso_treatment_effect"),
                    "eso_tx_effect", default=eso_pa.get("p_treatment_effect"))

            save_data["eso_pathology"] = eso_path_data

        save_data["pathology"] = pa_data

        # ==========================================================
        # リンパ節（病理タブの下部に統合）
        # ==========================================================
        # ==========================================================
        # 統合リンパ節入力（食道領域 + 胃領域）
        # ==========================================================
        section_card("🔗 リンパ節部位別", "teal")

        # --- ステーション定義 ---
        ESO_LN_CERVICAL = [
            ("100L", "#100L 頸部傍食道左"), ("100R", "#100R 頸部傍食道右"),
            ("101L", "#101L 頸部傍気管左"), ("101R", "#101R 頸部傍気管右"),
            ("102midL", "#102midL 深頸(中)左"), ("102midR", "#102midR 深頸(中)右"),
            ("102upL", "#102upL 深頸(上)左"), ("102upR", "#102upR 深頸(上)右"),
            ("103", "#103 咽頭後方"), ("104L", "#104L 鎖骨上左"), ("104R", "#104R 鎖骨上右"),
        ]
        ESO_LN_THORACIC = [
            ("105", "#105 上部胸部傍食道"),
            ("106recL", "#106recL 反回神経左"), ("106recR", "#106recR 反回神経右"),
            ("106pre", "#106pre 気管前"), ("106tbL", "#106tbL 気管気管支左"), ("106tbR", "#106tbR 気管気管支右"),
            ("107", "#107 気管分岐下"),
            ("108", "#108 中部胸部傍食道"), ("109L", "#109L 主気管支左"), ("109R", "#109R 主気管支右"),
            ("110", "#110 下部胸部傍食道"), ("111", "#111 食道裂孔"),
            ("112aoA", "#112aoA 後縦隔A"), ("112aoP", "#112aoP 後縦隔P"),
            ("112pulL", "#112pulL 肺門左"), ("112pulR", "#112pulR 肺門右"),
            ("113", "#113 ligamentum arteriosum"), ("114", "#114 前縦隔"),
        ]
        GASTRIC_LN_STATIONS = [
            ("1", "No.1"), ("2", "No.2"), ("3a", "No.3a"), ("3b", "No.3b"),
            ("4sa", "No.4sa"), ("4sb", "No.4sb"), ("4d", "No.4d"),
            ("5", "No.5"), ("6", "No.6"), ("7", "No.7"),
            ("8a", "No.8a"), ("8p", "No.8p"), ("9", "No.9"),
            ("10", "No.10"), ("11p", "No.11p"), ("11d", "No.11d"),
            ("12a", "No.12a"), ("14v", "No.14v"), ("16", "No.16"),
            ("19", "No.19"), ("20", "No.20"),
        ]

        ln_data = {}
        eso_ln_data = {}
        total_m, total_l = 0, 0

        def _render_ln_station_rows(stations, data_dict, existing_data, key_prefix):
            """リンパ節ステーション行を描画するヘルパー（コンパクト版）"""
            nonlocal total_m, total_l
            # ヘッダー
            hc = st.columns([2.5, 1.2, 1.2])
            hc[0].markdown("**ステーション**")
            hc[1].markdown("**転移(M)**")
            hc[2].markdown("**郭清(L)**")
            for code, label in stations:
                cc = st.columns([2.5, 1.2, 1.2])
                m_key = f"ln_{code}_m"
                l_key = f"ln_{code}_l"
                cc[0].markdown(
                    f'<p style="margin:4px 0;line-height:38px;font-weight:600;">{label}</p>',
                    unsafe_allow_html=True)
                with cc[1]:
                    m_val = numeric_input("", f"{key_prefix}_m_{code}",
                                          existing_data.get(m_key, 0), min_val=0)
                    data_dict[m_key] = m_val
                    if m_val:
                        total_m += m_val
                with cc[2]:
                    l_val = numeric_input("", f"{key_prefix}_l_{code}",
                                          existing_data.get(l_key, 0), min_val=0)
                    data_dict[l_key] = l_val
                    if l_val:
                        total_l += l_val

        # ── 食道（頸部〜胸部）領域リンパ節 ──
        if is_eso:
            with st.expander("🫁 食道（頸部〜胸部）領域リンパ節", expanded=False):
                st.caption("食道癌取扱い規約 第12版 — 頸部・胸部リンパ節ステーション")
                st.markdown("##### 頸部 (#100–#104)")
                _render_ln_station_rows(ESO_LN_CERVICAL, eso_ln_data, eso_ln, "eso_cerv")
                st.divider()
                st.markdown("##### 胸部 (#105–#114)")
                _render_ln_station_rows(ESO_LN_THORACIC, eso_ln_data, eso_ln, "eso_thor")

        # ── 胃領域リンパ節（腹部 No.1–20）──
        with st.expander("🔴 胃領域リンパ節（腹部）", expanded=not is_eso):
            st.caption("胃癌取扱い規約 — 腹部リンパ節ステーション")
            _render_ln_station_rows(GASTRIC_LN_STATIONS, ln_data, ln, "gln")

        # ── 節外転移（共通・1つのみ）──
        st.markdown("##### 節外転移")
        _xc1, _xc2, _xsep, _xc3, _ = st.columns([3, 1, 0.2, 1, 2])
        with _xc1:
            st.markdown("**extranodal**")
        with _xc2:
            _xm = numeric_input("", "ln_m_extranodal",
                                ln.get("ln_extranodal_m", 0), min_val=0)
            ln_data["ln_extranodal_m"] = _xm
            if _xm:
                total_m += _xm
        with _xsep:
            st.markdown(
                '<p style="text-align:center;font-size:18px;margin-top:28px;">/</p>',
                unsafe_allow_html=True)
        with _xc3:
            _xl = numeric_input("", "ln_l_extranodal",
                                ln.get("ln_extranodal_l", 0), min_val=0)
            ln_data["ln_extranodal_l"] = _xl
            if _xl:
                total_l += _xl

        # ── 合計表示 ──
        col1, col2 = st.columns(2)
        with col1:
            st.metric("転移合計 (全領域)", total_m)
        with col2:
            st.metric("郭清合計 (全領域)", total_l)

        save_data["lymph_nodes"] = ln_data
        if is_eso:
            save_data["eso_lymph_nodes"] = eso_ln_data

    # ==========================================================
    # Tab: 薬物・放射線療法（旧：化学療法/RT）
    # ==========================================================
    chemo_tab_idx = 5
    with tabs[chemo_tab_idx]:
        st.markdown("### 術後補助化学療法")
        adj_data = {}
        adj_data["adj_yn"] = selectbox_select("術後補助化学療法", {0: "なし", 1: "あり"}, "adj_yn", adj.get("adj_yn"))

        if adj_data.get("adj_yn") == 1:
            reg_field = "adj_regimen_gastric" if is_gastric else "adj_regimen_eso"
            col1, col2, col3 = st.columns(3)
            with col1:
                asd = adj.get("adj_start_date")
                adj_data["adj_start_date"] = st.date_input("開始日", value=datetime.strptime(asd, "%Y-%m-%d").date() if asd else None, key="adj_sd", format="YYYY/MM/DD")
                adj_data["adj_regimen"], adj_data["adj_regimen_other"] = \
                    selectbox_with_other("レジメン", get_codebook(reg_field), "adj_reg", "adj_reg_oth",
                                         adj.get("adj_regimen"), adj.get("adj_regimen_other", ""))
            with col2:
                adj_data["adj_courses"] = numeric_input("コース数", "adj_c", adj.get("adj_courses"))
                adj_data["adj_completion"] = selectbox_select("完遂", get_codebook("chemo_completion"), "adj_comp", adj.get("adj_completion"))
            with col3:
                adj_data["adj_adverse_event"] = st.text_area("有害事象", value=adj.get("adj_adverse_event", "") or "", key="adj_ae")

        save_data["adjuvant_chemo"] = adj_data

        # --- Palliative chemo ---
        st.markdown("---")
        st.markdown("### 再発後/切除不能化学療法 (1st-5th line)")
        pal_data_all = {}
        pal_yn = selectbox_select("化学療法", {0: "なし", 1: "あり"}, "pal_yn",
                                   1 if pal_lines else 0)

        if pal_yn == 1:
            for line in range(1, 6):
                ordinal = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th", 5: "5th"}[line]
                existing_line = pal_lines.get(line, {})
                with st.expander(f"{ordinal} line", expanded=(line <= 2)):
                    pld = {}
                    pld["line_number"] = line
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        pld["regimen"], pld["regimen_other"] = \
                            selectbox_with_other("レジメン", get_codebook("pal_regimen_gastric"), f"pal{line}_reg", f"pal{line}_reg_oth",
                                                 existing_line.get("regimen"), existing_line.get("regimen_other", ""))
                    with col2:
                        pld["courses"] = numeric_input("コース数", f"pal{line}_c", existing_line.get("courses"))
                    with col3:
                        pld["adverse_event"] = st.text_input("有害事象", value=existing_line.get("adverse_event", "") or "", key=f"pal{line}_ae")
                    pal_data_all[line] = pld

        save_data["palliative_chemo"] = pal_data_all

        # --- RT ---
        st.markdown("---")
        st.markdown("### 放射線療法")
        rt_data = {}
        rt_yn = selectbox_select("放射線療法", {0: "なし", 1: "あり"}, "rt_yn",
                                  1 if rt.get("rt_intent") else 0)
        if rt_yn == 1:
            col1, col2, col3 = st.columns(3)
            with col1:
                rt_data["rt_intent"] = selectbox_select("目的", get_codebook("rt_intent"), "rt_int", rt.get("rt_intent"))
                rt_data["rt_modality"] = selectbox_select("照射法", get_codebook("rt_modality"), "rt_mod", rt.get("rt_modality"))
            with col2:
                rt_data["rt_total_dose_gy"] = numeric_input("総線量", "rt_dose", rt.get("rt_total_dose_gy"), "Gy", is_float=True)
                rt_data["rt_fractions"] = numeric_input("分割回数", "rt_frac", rt.get("rt_fractions"))
            with col3:
                rsd = rt.get("rt_start_date")
                rt_data["rt_start_date"] = st.date_input("開始日", value=datetime.strptime(rsd, "%Y-%m-%d").date() if rsd else None, key="rt_sd", format="YYYY/MM/DD")
                red = rt.get("rt_end_date")
                rt_data["rt_end_date"] = st.date_input("終了日", value=datetime.strptime(red, "%Y-%m-%d").date() if red else None, key="rt_ed", format="YYYY/MM/DD")

        save_data["radiation_therapy"] = rt_data

    # ==========================================================
    # Tab: 再発・予後
    # ==========================================================
    outcome_tab_idx = 6
    with tabs[outcome_tab_idx]:
        st.markdown("### 再発・予後情報")
        o_data = {}

        col1, col2 = st.columns(2)
        with col1:
            o_data["recurrence_yn"] = selectbox_select("再発有無", get_codebook("recurrence_yn"), "rec_yn", out.get("recurrence_yn"))
            if o_data.get("recurrence_yn") == 1:
                rd = out.get("recurrence_date")
                o_data["recurrence_date"] = st.date_input("再発確認日", value=datetime.strptime(rd, "%Y-%m-%d").date() if rd else None, key="rec_date", format="YYYY/MM/DD")

                rec_site_flags = [
                    ("peritoneal", "腹膜"), ("liver", "肝"), ("lung", "肺"),
                    ("lymph_node", "リンパ節"), ("bone", "骨"), ("brain", "脳"),
                    ("ovary", "卵巣"), ("adrenal", "副腎"),
                    ("local", "局所再発"), ("remnant_stomach", "残胃"),
                    ("other", "その他"),
                ]
                rec_result = flag_group("再発部位", rec_site_flags, "rec_", out, "rec")
                o_data.update(rec_result)

        with col2:
            o_data["vital_status"] = selectbox_select("生死", get_codebook("vital_status"), "vital", out.get("vital_status"))
            lad = out.get("last_alive_date")
            o_data["last_alive_date"] = st.date_input("最終生存確認日", value=datetime.strptime(lad, "%Y-%m-%d").date() if lad else None, key="last_alive", format="YYYY/MM/DD")

            if o_data.get("vital_status") in [2, 3, 4, 5]:
                ddd = out.get("death_date")
                o_data["death_date"] = st.date_input("死亡日", value=datetime.strptime(ddd, "%Y-%m-%d").date() if ddd else None, key="death_d", format="YYYY/MM/DD")
                o_data["death_cause"] = selectbox_select("死因", get_codebook("death_cause"), "death_cause", out.get("death_cause"))
                o_data["death_cause_detail"] = selectbox_select(
                    "死因詳細", get_codebook("death_cause_detail"),
                    "death_cause_det", out.get("death_cause_detail"))

            # 術後30日/90日転帰は手術日・死亡日から自動計算するため入力欄は不要

        # OS/RFS 自動計算表示
        surgery_date_val = patients_data.get("surgery_date")
        last_alive_val = o_data.get("last_alive_date")
        if surgery_date_val and last_alive_val:
            try:
                s_d = surgery_date_val if isinstance(surgery_date_val, date) else datetime.strptime(str(surgery_date_val), "%Y-%m-%d").date()
                if o_data.get("vital_status") in [2, 3, 4, 5] and o_data.get("death_date"):
                    end_d = o_data["death_date"] if isinstance(o_data["death_date"], date) else datetime.strptime(str(o_data["death_date"]), "%Y-%m-%d").date()
                else:
                    end_d = last_alive_val if isinstance(last_alive_val, date) else datetime.strptime(str(last_alive_val), "%Y-%m-%d").date()
                os_days = (end_d - s_d).days
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("全生存期間", f"{os_days} 日 ({os_days / 30.44:.1f} 月)")
                with col2:
                    if o_data.get("recurrence_yn") == 1 and o_data.get("recurrence_date"):
                        r_d = o_data["recurrence_date"] if isinstance(o_data["recurrence_date"], date) else datetime.strptime(str(o_data["recurrence_date"]), "%Y-%m-%d").date()
                        rfs_days = (r_d - s_d).days
                    else:
                        rfs_days = os_days
                    st.metric("無再発生存期間", f"{rfs_days} 日 ({rfs_days / 30.44:.1f} 月)")
            except Exception:
                pass

        save_data["outcome"] = o_data

    # ==========================================================
    # GIST専用（disease_class==99 でGISTを選んだ場合も含む）
    # ==========================================================
    # GISTは疾患分類から削除されたが、将来の拡張のためコードは残す

    # ==========================================================
    # Tab: 腫瘍マーカー
    # ==========================================================
    tm_tab_idx = 7
    with tabs[tm_tab_idx]:
        st.markdown("### 腫瘍マーカー")
        st.caption("術後初回外来時（ベースライン）と再発確認時のみ記録")

        # 既存データ読み込み
        existing_markers = []
        if editing and patient_db_id:
            with get_db() as conn:
                existing_markers = conn.execute(
                    "SELECT * FROM tumor_markers WHERE patient_id=? ORDER BY measurement_date",
                    (patient_db_id,)
                ).fetchall()
                existing_markers = [dict(r) for r in existing_markers]

        # --- 既存レコード表示 ---
        if existing_markers:
            for i, mk in enumerate(existing_markers):
                timing_label = "ベースライン" if mk["timing"] == "baseline" else "再発時"
                st.markdown(f"**{timing_label}** ({mk.get('measurement_date', '日付未設定')})")
                if is_gastric:
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("CEA", f"{mk.get('cea', '-')}")
                    c2.metric("CA19-9", f"{mk.get('ca199', '-')}")
                    c3.metric("CA125", f"{mk.get('ca125', '-')}")
                    c4.metric("AFP", f"{mk.get('afp', '-')}")
                else:
                    c1, c2, c3, c4, c5 = st.columns(5)
                    c1.metric("p53抗体", f"{mk.get('p53_antibody', '-')}")
                    c2.metric("シフラ", f"{mk.get('cyfra', '-')}")
                    c3.metric("SCC", f"{mk.get('scc_ag', '-')}")
                    c4.metric("AFP", f"{mk.get('afp', '-')}")
                    c5.metric("KL-6", f"{mk.get('kl6', '-')}")
                st.markdown("---")

        # --- 新規マーカー入力 ---
        section_card("新規マーカー入力", "blue")
        tc1, tc2 = st.columns([1, 2])
        with tc1:
            tm_timing = st.selectbox("記録タイミング", ["baseline", "recurrence"],
                format_func=lambda x: "術後初回（ベースライン）" if x == "baseline" else "再発確認時",
                key="tm_timing")
            tm_date = st.date_input("測定日", value=None, key="tm_date", format="YYYY/MM/DD")

        with tc2:
            if is_gastric:
                mc1, mc2, mc3, mc4 = st.columns(4)
                tm_cea = mc1.number_input("CEA (ng/mL)", min_value=0.0, value=None, key="tm_cea", format="%.2f")
                tm_ca199 = mc2.number_input("CA19-9 (U/mL)", min_value=0.0, value=None, key="tm_ca199", format="%.2f")
                tm_ca125 = mc3.number_input("CA125 (U/mL)", min_value=0.0, value=None, key="tm_ca125", format="%.2f")
                tm_afp = mc4.number_input("AFP (ng/mL)", min_value=0.0, value=None, key="tm_afp", format="%.2f")
            else:
                mc1, mc2, mc3, mc4, mc5 = st.columns(5)
                tm_p53 = mc1.number_input("p53抗体 (U/mL)", min_value=0.0, value=None, key="tm_p53", format="%.2f")
                tm_cyfra = mc2.number_input("シフラ (ng/mL)", min_value=0.0, value=None, key="tm_cyfra", format="%.2f")
                tm_scc = mc3.number_input("SCC (ng/mL)", min_value=0.0, value=None, key="tm_scc", format="%.2f")
                tm_afp = mc4.number_input("AFP (ng/mL)", min_value=0.0, value=None, key="tm_afp_e", format="%.2f")
                tm_kl6 = mc5.number_input("KL-6 (U/mL)", min_value=0.0, value=None, key="tm_kl6", format="%.2f")

        tm_notes = st.text_input("備考", key="tm_notes", placeholder="特記事項")

        if st.button("腫瘍マーカーを追加保存", key="tm_save") and editing and patient_db_id:
            tm_data = {
                "patient_id": patient_db_id,
                "timing": tm_timing,
                "measurement_date": tm_date.strftime("%Y-%m-%d") if tm_date else None,
                "notes": tm_notes or None,
                "created_by": st.session_state.user["id"],
            }
            if is_gastric:
                tm_data.update({"cea": tm_cea, "ca199": tm_ca199, "ca125": tm_ca125, "afp": tm_afp})
            else:
                tm_data.update({"p53_antibody": tm_p53, "cyfra": tm_cyfra, "scc_ag": tm_scc,
                                "afp": tm_afp, "kl6": tm_kl6})
            with get_db() as conn:
                # カラム名ホワイトリスト検証
                _TM_ALLOWED_COLS = frozenset([
                    "patient_id", "timing", "timing_date",
                    "cea", "ca199", "ca125", "afp",
                    "p53_antibody", "cyfra", "scc_ag", "kl6",
                ])
                for k in tm_data.keys():
                    assert k in _TM_ALLOWED_COLS, f"Invalid column: {k}"
                cols_str = ", ".join(tm_data.keys())
                placeholders = ", ".join(["?"] * len(tm_data))
                conn.execute(f"INSERT INTO tumor_markers ({cols_str}) VALUES ({placeholders})",
                             list(tm_data.values()))
                log_audit(conn, st.session_state.user["id"], "INSERT", "tumor_markers", patient_db_id)
            st.success("✅ 腫瘍マーカーを保存しました")
            st.rerun()

        # --- 検査読み取り (lab_results) からの検査値履歴 ---
        if editing and patient_db_id:
            st.markdown("---")
            st.markdown("### 検査値履歴（検査読み取りデータ）")
            st.caption("「検査読み取り」ページで OCR 保存した血液検査データの腫瘍マーカー抜粋")
            with get_db() as conn:
                lab_rows = conn.execute(
                    "SELECT * FROM lab_results WHERE patient_id = ? ORDER BY sample_date DESC, created_at DESC",
                    (patient_db_id,),
                ).fetchall()
            if lab_rows:
                _tm_cols = ["cea_lab", "ca199_lab", "afp_lab", "ca125_lab"]
                _lab_tm_labels = {"cea_lab": "CEA", "ca199_lab": "CA19-9", "afp_lab": "AFP", "ca125_lab": "CA125"}
                _timing_map = {"preop": "術前", "postop": "術後", "recurrence": "再発時"}
                lab_tm_data = []
                for r in lab_rows:
                    rd = dict(r)
                    # 腫瘍マーカーが1つでも入っていれば表示
                    if any(rd.get(c) is not None for c in _tm_cols):
                        row_d = {
                            "採取日": rd.get("sample_date", "-"),
                            "タイミング": _timing_map.get(rd.get("timing"), rd.get("timing", "-")),
                        }
                        for c in _tm_cols:
                            row_d[_lab_tm_labels[c]] = rd.get(c)
                        row_d["ソース"] = rd.get("source_type", "-")
                        lab_tm_data.append(row_d)

                if lab_tm_data:
                    st.dataframe(pd.DataFrame(lab_tm_data), use_container_width=True, hide_index=True)
                else:
                    st.info("腫瘍マーカーが含まれる検査データはまだありません。")

                # 全検査値の詳細一覧（折りたたみ）
                with st.expander("📊 全検査値一覧（CBC・生化学含む）"):
                    from lab_reader import LAB_LABELS as _LR_LABELS
                    _display_cols = ["sample_date", "timing", "source_type",
                                     "wbc", "rbc", "hgb", "hct", "plt",
                                     "tp", "alb", "t_bil", "ast", "alt", "crp",
                                     "cea_lab", "ca199_lab", "afp_lab", "ca125_lab",
                                     "created_at"]
                    _col_rename = {c: _LR_LABELS.get(c, c) for c in _display_cols}
                    _col_rename.update({"sample_date": "採取日", "timing": "タイミング",
                                        "source_type": "ソース", "created_at": "登録日時"})
                    full_data = []
                    for r in lab_rows:
                        rd = dict(r)
                        full_data.append({_col_rename.get(c, c): rd.get(c) for c in _display_cols})
                    df_lab = pd.DataFrame(full_data)
                    if "タイミング" in df_lab.columns:
                        df_lab["タイミング"] = df_lab["タイミング"].map(
                            lambda x: _timing_map.get(x, x) if isinstance(x, str) else x)
                    st.dataframe(df_lab, use_container_width=True, hide_index=True)
            else:
                st.info("この患者の検査値記録はまだありません。「検査読み取り」ページからOCRで取り込めます。")

    # ==========================================================
    # Phase承認フロー & 保存ボタン
    # ==========================================================
    st.markdown("---")
    user_role = st.session_state.user.get("role", "entry")
    current_status = p.get("data_status", "draft") if editing else "draft"

    # Phase ステータス
    p1_status = p.get("phase1_status", "draft") if editing else "draft"
    p3_status = p.get("phase3_status", "draft") if editing else "draft"
    p4_status = p.get("phase4_status", "draft") if editing else "draft"

    if editing:
        st.markdown("#### 承認ステータス")
        sc1, sc2, sc3 = st.columns(3)
        sc1.markdown(f"**Phase 1（周術期）**: {PHASE_STATUS.get(p1_status, p1_status)}")
        sc2.markdown(f"**Phase 3（術後3年）**: {PHASE_STATUS.get(p3_status, p3_status)}")
        sc3.markdown(f"**Phase 4（術後5年）**: {PHASE_STATUS.get(p4_status, p4_status)}")

    # --- Phase 操作ボタン ---
    save_btn = False
    phase_action = None   # ("phase1", "submit") / ("phase1", "approve") etc.
    return_comment = ""

    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    with col1:
        save_btn = st.button("💾 保存（下書き）", type="primary", use_container_width=True)
    with col4:
        if editing:
            if st.button("🗑️ 編集をキャンセル", use_container_width=True):
                del st.session_state.edit_study_id
                st.session_state.pop("edit_loaded_updated_at", None)
                st.rerun()

    if editing:
        with st.expander("📤 Phase 提出・承認・差戻し", expanded=False):
            pc1, pc2, pc3 = st.columns(3)
            # --- Phase 1 ---
            with pc1:
                st.markdown("**Phase 1（周術期）**")
                if p1_status == "draft":
                    if st.button("📤 Phase 1 提出", key="p1_submit", use_container_width=True):
                        # 術式-必須項目チェック
                        can_submit, missing = validate_phase1_submission(patient_db_id)
                        if can_submit:
                            phase_action = ("phase1", "submit")
                        else:
                            st.error(f"❌ Phase 1 提出不可: {len(missing)} 件の必須項目が未入力です")
                            for m in missing:
                                st.caption(f"  • {m['label']}")
                            phase_action = None
                elif p1_status == "submitted":
                    if user_role in ("reviewer", "admin"):
                        if st.button("✅ Phase 1 承認", key="p1_approve", use_container_width=True):
                            phase_action = ("phase1", "approve")
                        return_comment = st.text_input("差戻しコメント", key="p1_comment", placeholder="Phase 1")
                        if st.button("↩️ Phase 1 差戻し", key="p1_return", use_container_width=True):
                            phase_action = ("phase1", "return")
                elif p1_status == "approved":
                    st.success("✅ Phase 1 承認済（ロック中）")
                    if user_role == "admin":
                        if st.button("🔓 Phase 1 アンロック", key="p1_unlock", use_container_width=True):
                            phase_action = ("phase1", "unlock")
            # --- Phase 3（術後3年） ---
            with pc2:
                st.markdown("**Phase 3（術後3年）**")
                if p3_status == "draft":
                    if st.button("📤 Phase 3 提出", key="p3_submit", use_container_width=True):
                        phase_action = ("phase3", "submit")
                elif p3_status == "submitted":
                    if user_role in ("reviewer", "admin"):
                        if st.button("✅ Phase 3 承認", key="p3_approve", use_container_width=True):
                            phase_action = ("phase3", "approve")
                        return_comment = st.text_input("差戻しコメント", key="p3_comment", placeholder="Phase 3")
                        if st.button("↩️ Phase 3 差戻し", key="p3_return", use_container_width=True):
                            phase_action = ("phase3", "return")
                elif p3_status == "approved":
                    st.success("✅ Phase 3 承認済（ロック中）")
                    if user_role == "admin":
                        if st.button("🔓 Phase 3 アンロック", key="p3_unlock", use_container_width=True):
                            phase_action = ("phase3", "unlock")
            # --- Phase 4（術後5年） ---
            with pc3:
                st.markdown("**Phase 4（術後5年）**")
                if p4_status == "draft":
                    if st.button("📤 Phase 4 提出", key="p4_submit", use_container_width=True):
                        phase_action = ("phase4", "submit")
                elif p4_status == "submitted":
                    if user_role in ("reviewer", "admin"):
                        if st.button("✅ Phase 4 承認", key="p4_approve", use_container_width=True):
                            phase_action = ("phase4", "approve")
                        return_comment = st.text_input("差戻しコメント", key="p4_comment", placeholder="Phase 4") if not return_comment else return_comment
                        if st.button("↩️ Phase 4 差戻し", key="p4_return", use_container_width=True):
                            phase_action = ("phase4", "return")
                elif p4_status == "approved":
                    st.success("✅ Phase 4 承認済（ロック中）")
                    if user_role == "admin":
                        if st.button("🔓 Phase 4 アンロック", key="p4_unlock", use_container_width=True):
                            phase_action = ("phase4", "unlock")

    # ステータス遷移の決定（後方互換: data_status も更新）
    status_action = None
    if save_btn:
        status_action = current_status if current_status else "draft"
        # Phase 1 が承認済の場合、Phase 1 対象テーブルは書き込み不可（管理者以外）
        if p1_status == "approved" and user_role != "admin":
            # Phase 1 テーブルのデータは保存をスキップ（Phase 3/4 対象のみ保存）
            for tbl in PHASE1_TABLES:
                if tbl != "patients":
                    save_data.pop(tbl, None)
        # Phase 3 が承認済の場合、Phase 3 対象テーブルの既存レコードはロック
        if p3_status == "approved" and user_role != "admin":
            for tbl in PHASE3_TABLES:
                if tbl not in ("palliative_chemo", "tumor_markers"):
                    save_data.pop(tbl, None)
        # Phase 4 が承認済の場合は管理者のみ編集可
        if p4_status == "approved" and user_role != "admin":
            st.error("Phase 4 承認済の症例は管理者のみ編集できます")
            status_action = None

    # ── フラグセクション未入力バリデーション ──
    _flag_warnings = []
    if save_btn:
        _pd = save_data.get("patients", {})

        # 各フラグセクションを検査: フラグ全OFF かつ confirmed=0 → 未確認
        # 症状は sym_asymptomatic=1 で確認済み扱い
        _flag_section_defs = [
            ("症状", "sym_", [
                "asymptomatic", "epigastric_pain", "dysphagia", "weight_loss",
                "anemia", "melena", "hematemesis", "nausea_vomiting",
                "abdominal_distension", "obstruction", "other",
            ]),
            ("併存疾患", "comor_", [
                "hypertension", "cardiovascular", "cerebrovascular", "respiratory",
                "renal", "renal_dialysis", "hepatic", "diabetes", "endocrine",
                "collagen", "hematologic", "neurologic", "psychiatric",
                "prior_cardiac_surgery", "prior_abdominal_surgery", "preop_ventilator",
                "other",
            ]),
            ("内服薬", "med_", [
                "antithrombotic", "steroid_immunosup", "antineoplastic",
                "thyroid", "psychotropic", "other",
            ]),
        ]
        for sec_label, prefix, suffixes in _flag_section_defs:
            any_on = any(_pd.get(f"{prefix}{s}", 0) for s in suffixes)
            confirmed = _pd.get(f"{prefix}confirmed", 0)
            if not any_on and not confirmed:
                _flag_warnings.append(sec_label)

        if _flag_warnings:
            st.warning("⚠️ 以下のセクションが未確認です")
            for _sw in _flag_warnings:
                st.caption(f"　• **{_sw}**: 該当項目をチェック、または「すべて該当なし」をチェックしてください")

    if status_action is not None:
        status = status_action
        try:
            # 日付フィールドを文字列に変換
            date_fields_to_convert = [
                "birthdate", "first_visit_date", "admission_date",
                "surgery_date", "discharge_date",
            ]
            for f in date_fields_to_convert:
                val = save_data.get("patients", {}).get(f)
                if val and isinstance(val, date):
                    save_data["patients"][f] = val.strftime("%Y-%m-%d")

            # neoadjuvant date
            for f in ["nac_start_date"]:
                val = save_data.get("neoadjuvant", {}).get(f)
                if val and isinstance(val, date):
                    save_data["neoadjuvant"][f] = val.strftime("%Y-%m-%d")

            # adj date
            for f in ["adj_start_date"]:
                val = save_data.get("adjuvant_chemo", {}).get(f)
                if val and isinstance(val, date):
                    save_data["adjuvant_chemo"][f] = val.strftime("%Y-%m-%d")

            # outcome dates
            for f in ["recurrence_date", "last_alive_date", "death_date"]:
                val = save_data.get("outcome", {}).get(f)
                if val and isinstance(val, date):
                    save_data["outcome"][f] = val.strftime("%Y-%m-%d")

            # rt dates
            for f in ["rt_start_date", "rt_end_date"]:
                val = save_data.get("radiation_therapy", {}).get(f)
                if val and isinstance(val, date):
                    save_data["radiation_therapy"][f] = val.strftime("%Y-%m-%d")

            save_data["patients"]["data_status"] = status
            # 個人情報カラムの暗号化
            _encrypt_patient_data(save_data.get("patients", {}))
            user_id = st.session_state.user["id"]

            with get_db() as conn:
                # 楽観的ロック: 他ユーザーによる同時更新を検出
                if editing and patient_db_id:
                    expected_ts = st.session_state.get("edit_loaded_updated_at")
                    if expected_ts:
                        current_row = conn.execute(
                            "SELECT updated_at FROM patients WHERE id=?",
                            (patient_db_id,)
                        ).fetchone()
                        if current_row and current_row["updated_at"] and \
                           str(current_row["updated_at"]) != str(expected_ts):
                            st.error(
                                "⚠️ 同時編集検出: この症例は他のユーザーに更新されています。\n"
                                "ページを再読み込みして最新データを取得してください。"
                            )
                            raise Exception("OptimisticLockConflict")

                # 1. patients テーブル
                p_cols = save_data["patients"]
                if editing and patient_db_id:
                    p_cols.pop("study_id", None)
                    set_clause = ", ".join(f"{k}=?" for k in p_cols.keys())
                    vals = list(p_cols.values()) + [user_id, patient_db_id]
                    conn.execute(
                        f"UPDATE patients SET {set_clause}, updated_by=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                        vals
                    )
                    log_audit(conn, user_id, "UPDATE", "patients", patient_db_id)
                else:
                    p_cols["created_by"] = user_id
                    p_cols["updated_by"] = user_id
                    cols_str = ", ".join(p_cols.keys())
                    placeholders = ", ".join(["?"] * len(p_cols))
                    cursor = conn.execute(
                        f"INSERT INTO patients ({cols_str}) VALUES ({placeholders})",
                        list(p_cols.values())
                    )
                    patient_db_id = cursor.lastrowid
                    log_audit(conn, user_id, "INSERT", "patients", patient_db_id)

                # 2. 1:1 テーブル群を upsert
                one_to_one_tables = [
                    "tumor_preop", "neoadjuvant", "surgery",
                    "pathology", "lymph_nodes", "adjuvant_chemo",
                    "outcome", "radiation_therapy",
                    "eso_tumor", "eso_surgery", "eso_pathology",
                    "eso_course", "eso_lymph_nodes",
                ]

                for tbl in one_to_one_tables:
                    tbl_data = save_data.get(tbl, {})
                    if not tbl_data:
                        continue
                    tbl_data["patient_id"] = patient_db_id

                    existing = conn.execute(
                        f"SELECT id FROM {tbl} WHERE patient_id=?", (patient_db_id,)
                    ).fetchone()

                    if existing:
                        tbl_data.pop("patient_id", None)
                        set_clause = ", ".join(f"{k}=?" for k in tbl_data.keys())
                        conn.execute(
                            f"UPDATE {tbl} SET {set_clause}, updated_at=CURRENT_TIMESTAMP WHERE patient_id=?",
                            list(tbl_data.values()) + [patient_db_id]
                        )
                        log_audit(conn, user_id, "UPDATE", tbl, patient_db_id)
                    else:
                        cols_str = ", ".join(tbl_data.keys())
                        placeholders = ", ".join(["?"] * len(tbl_data))
                        conn.execute(
                            f"INSERT INTO {tbl} ({cols_str}) VALUES ({placeholders})",
                            list(tbl_data.values())
                        )
                        log_audit(conn, user_id, "INSERT", tbl, patient_db_id)

                # 3. palliative_chemo (1:N)
                pal_data = save_data.get("palliative_chemo", {})
                if pal_data:
                    conn.execute("DELETE FROM palliative_chemo WHERE patient_id=?", (patient_db_id,))
                    for line_num, line_data in pal_data.items():
                        if line_data.get("regimen") is not None:
                            line_data["patient_id"] = patient_db_id
                            cols_str = ", ".join(line_data.keys())
                            placeholders = ", ".join(["?"] * len(line_data))
                            conn.execute(
                                f"INSERT INTO palliative_chemo ({cols_str}) VALUES ({placeholders})",
                                list(line_data.values())
                            )
                    log_audit(conn, user_id, "UPSERT", "palliative_chemo", patient_db_id)

            _study_id_display = save_data['patients'].get('study_id', patients_data.get('study_id'))
            if _flag_warnings:
                st.warning(f"⚠️ 症例 {_study_id_display} を保存しました（未確認 {len(_flag_warnings)} セクション）")
            else:
                st.success(f"✅ 症例 {_study_id_display} を保存しました")
            # 楽観的ロック: 保存成功後に updated_at を更新
            st.session_state.pop("edit_loaded_updated_at", None)
            if not editing:
                st.session_state.edit_study_id = patients_data.get("study_id")
        except Exception as e:
            if "OptimisticLockConflict" not in str(e):
                st.error(f"❌ 保存エラー: {e}")
                import traceback
                st.code(traceback.format_exc())

    # --- Phase 承認アクション処理 ---
    if phase_action and editing and patient_db_id:
        phase_name, action = phase_action
        user_id = st.session_state.user["id"]
        now_ts = datetime.now().isoformat()
        study_id_str = p.get("study_id", "")
        try:
            with get_db() as conn:
                if phase_name in ("phase1", "phase3", "phase4"):
                    col_prefix = phase_name
                    if action == "submit":
                        conn.execute(
                            f"UPDATE patients SET {col_prefix}_status='submitted', "
                            f"{col_prefix}_submitted_at=?, {col_prefix}_submitted_by=? WHERE id=?",
                            (now_ts, user_id, patient_db_id))
                        log_audit(conn, user_id, "SUBMIT", "patients", patient_db_id, phase=phase_name)
                        # 確認者への通知
                        reviewers = conn.execute(
                            "SELECT id FROM users WHERE role IN ('reviewer','admin') AND is_active=1"
                        ).fetchall()
                        for r in reviewers:
                            _create_notification(conn, r["id"],
                                f"症例 {study_id_str} の {PHASE_LABELS[phase_name]} が提出されました",
                                link_page="➕ 新規登録", link_study_id=study_id_str)
                        st.success(f"📤 {PHASE_LABELS[phase_name]} を提出しました")
                    elif action == "approve":
                        conn.execute(
                            f"UPDATE patients SET {col_prefix}_status='approved', "
                            f"{col_prefix}_approved_at=?, {col_prefix}_approved_by=? WHERE id=?",
                            (now_ts, user_id, patient_db_id))
                        log_audit(conn, user_id, "APPROVE", "patients", patient_db_id, phase=phase_name)
                        # Phase3/4 承認時: outcome スナップショット + 1:Nテーブルロック
                        if phase_name in ("phase3", "phase4"):
                            create_outcome_snapshot(conn, patient_db_id, phase_name, user_id)
                            lock_existing_rows(conn, "palliative_chemo", patient_db_id, phase_name)
                            lock_existing_rows(conn, "tumor_markers", patient_db_id, phase_name)
                        # 入力者への通知
                        creator_id = p.get("created_by")
                        if creator_id:
                            _create_notification(conn, creator_id,
                                f"症例 {study_id_str} の {PHASE_LABELS[phase_name]} が承認されました",
                                link_page="➕ 新規登録", link_study_id=study_id_str)
                        st.success(f"✅ {PHASE_LABELS[phase_name]} を承認しました")
                    elif action == "return":
                        conn.execute(
                            f"UPDATE patients SET {col_prefix}_status='draft', "
                            f"{col_prefix}_submitted_at=NULL, {col_prefix}_submitted_by=NULL WHERE id=?",
                            (patient_db_id,))
                        log_audit(conn, user_id, "RETURN", "patients", patient_db_id,
                                  phase=phase_name, comment=return_comment)
                        creator_id = p.get("created_by")
                        if creator_id:
                            _create_notification(conn, creator_id,
                                f"症例 {study_id_str} の {PHASE_LABELS[phase_name]} が差し戻されました: {return_comment}",
                                link_page="➕ 新規登録", link_study_id=study_id_str)
                        st.warning(f"↩️ {PHASE_LABELS[phase_name]} を差し戻しました")
                    elif action == "unlock":
                        conn.execute(
                            f"UPDATE patients SET {col_prefix}_status='draft', "
                            f"{col_prefix}_approved_at=NULL, {col_prefix}_approved_by=NULL WHERE id=?",
                            (patient_db_id,))
                        # Phase3/4 アンロック時: 1:Nテーブルロック解除
                        if phase_name in ("phase3", "phase4"):
                            unlock_rows(conn, "palliative_chemo", patient_db_id, phase_name)
                            unlock_rows(conn, "tumor_markers", patient_db_id, phase_name)
                        log_audit(conn, user_id, "UNLOCK", "patients", patient_db_id, phase=phase_name)
                        st.info(f"🔓 {PHASE_LABELS[phase_name]} をアンロックしました")
                # （旧 final 承認は廃止 — Phase3/4 で代替）
            st.rerun()
        except Exception as e:
            st.error(f"❌ Phase操作エラー: {e}")

    # ==========================================================
    # 症例編集履歴（この症例をいつ誰が触ったか）
    # ==========================================================
    if editing and patient_db_id:
        st.markdown("---")
        with st.expander("📝 この症例の編集履歴", expanded=False):
            with get_db() as conn:
                hist_df = pd.read_sql_query("""
                    SELECT al.timestamp as 日時,
                           u.display_name as ユーザー,
                           al.action as 操作,
                           al.table_name as テーブル,
                           al.field_name as 項目,
                           al.old_value as 変更前,
                           al.new_value as 変更後
                    FROM audit_log al
                    LEFT JOIN users u ON al.user_id = u.id
                    WHERE al.record_id = ?
                    ORDER BY al.timestamp DESC
                """, conn, params=[patient_db_id])

            if hist_df.empty:
                st.info("この症例の編集履歴はありません")
            else:
                # サマリー行
                unique_editors = hist_df["ユーザー"].nunique()
                total_edits = len(hist_df)
                first_edit = hist_df["日時"].iloc[-1] if len(hist_df) > 0 else ""
                last_edit = hist_df["日時"].iloc[0] if len(hist_df) > 0 else ""

                sc1, sc2, sc3, sc4 = st.columns(4)
                sc1.metric("総操作回数", total_edits)
                sc2.metric("編集者数", unique_editors)
                sc3.metric("初回登録", str(first_edit)[:16])
                sc4.metric("最終更新", str(last_edit)[:16])

                st.dataframe(hist_df, use_container_width=True, hide_index=True,
                              column_config={
                                  "日時": st.column_config.TextColumn("日時", width="medium"),
                              })


# ============================================================
# 進捗確認（ステータス別集計・一覧）
# ============================================================
def progress_page():
    st.markdown("## 📊 進捗確認")

    with get_db() as conn:
        df = pd.read_sql_query("""
            SELECT p.id, p.study_id, p.patient_id, p.surgery_date,
                   p.data_status, p.disease_category, p.disease_class, p.updated_at
            FROM patients p
            WHERE p.is_deleted = 0
            ORDER BY p.updated_at DESC
        """, conn)

    if df.empty:
        st.info("登録されている症例がありません。")
        return

    DCAT = {1: "胃癌", 2: "食道癌"}
    df["disease_label"] = df["disease_category"].map(DCAT)
    df["status_label"] = df["data_status"].map(STATUS_OPTIONS)

    # ステータス別集計
    st.markdown("### ステータス別集計")
    status_counts = df["data_status"].value_counts()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        n_draft = int(status_counts.get("draft", 0))
        st.metric("📝 下書き", f"{n_draft} 件")
    with col2:
        n_submitted = int(status_counts.get("submitted", 0))
        st.metric("📤 提出済", f"{n_submitted} 件")
    with col3:
        n_verified = int(status_counts.get("verified", 0))
        st.metric("✅ 確認済", f"{n_verified} 件")
    with col4:
        n_approved = int(status_counts.get("approved", 0))
        st.metric("🔒 承認済", f"{n_approved} 件")

    st.metric("合計症例数", f"{len(df)} 件")

    # 進捗バー
    total = len(df)
    if total > 0:
        approved_pct = n_approved / total
        verified_pct = n_verified / total
        submitted_pct = n_submitted / total
        st.markdown("#### 承認進捗")
        st.progress(approved_pct, text=f"承認済: {n_approved}/{total} ({approved_pct:.0%})")
        st.markdown("#### 確認進捗")
        st.progress((n_approved + n_verified) / total,
                     text=f"確認済以上: {n_approved + n_verified}/{total} ({(n_approved + n_verified) / total:.0%})")

    # 疾患別集計
    st.markdown("### 疾患別集計")
    disease_status = df.groupby(["disease_label", "status_label"]).size().unstack(fill_value=0)
    st.dataframe(disease_status, use_container_width=True)

    # 手術年別集計
    df["surgery_year"] = df["surgery_date"].str[:4]
    year_status = df.groupby(["surgery_year", "status_label"]).size().unstack(fill_value=0)
    if not year_status.empty:
        st.markdown("### 手術年別集計")
        st.dataframe(year_status, use_container_width=True)

    # 下書き一覧（要対応）
    st.markdown("### 📝 下書き症例（要入力完了）")
    drafts = df[df["data_status"] == "draft"][["study_id", "patient_id", "surgery_date",
                                                 "disease_label", "updated_at"]].copy()
    drafts.columns = ["症例ID", "カルテNo", "手術日", "疾患", "最終更新"]
    if drafts.empty:
        st.success("下書き症例はありません")
    else:
        st.dataframe(drafts, use_container_width=True, hide_index=True)


# ============================================================
# サマリー分析（analytics.py に委譲）
# ============================================================
def summary_analysis_page():
    render_analytics_dashboard()


def statistical_analysis_standalone_page():
    """統計解析 — 独立ページ（タブ切替問題回避）"""
    from statistical_analysis import render_statistical_analysis
    st.title("📊 統計解析")
    from analytics import _load_analysis_df
    df = _load_analysis_df()
    if df is not None and not df.empty:
        render_statistical_analysis(df)
    else:
        st.info("解析対象のデータがありません。症例を登録してください。")


def data_explore_standalone_page():
    """データ探索 — 独立ページ（タブ切替問題回避）"""
    from analytics import render_data_exploration, _load_analysis_df
    st.title("🔍 データ探索")
    df = _load_analysis_df()
    if df is not None and not df.empty:
        render_data_exploration(df)
    else:
        st.info("解析対象のデータがありません。症例を登録してください。")


# ============================================================
# データエクスポート — 全カラム日本語マッピング
# ============================================================
# codebook.COLUMN_LABELS を単一ソースとして使用
COLUMN_JP = get_all_column_labels()

# コードブック値をラベルに変換する対象フィールド
CODEBOOK_DECODE_FIELDS = [
    "sex", "smoking", "alcohol", "ps", "asa", "adl_status",
    "disease_class", "discharge_destination",
    "hp_eradication", "preop_weight_loss_10pct",
    "remnant_stomach_yn", "remnant_initial_disease", "remnant_location",
    "c_tumor_number", "c_location_egj", "c_macroscopic_type", "c_type0_subclass",
    "c_histology", "c_histology1", "c_histology2", "c_histology3",
    "c_depth", "c_ln_metastasis", "c_distant_metastasis",
    "c_peritoneal", "c_liver_metastasis", "c_stage",
    "nac_yn", "nac_regimen", "nac_completion",
    "recist_target_response", "recist_nontarget_response",
    "recist_new_lesion", "recist_overall",
    "primary_elevation", "primary_depression", "primary_stenosis",
    "primary_overall_response",
    "op_emergency", "op_anesthesia_type", "op_approach", "op_completion",
    "op_conversion_yn", "op_procedure", "op_dissection",
    "op_reconstruction", "op_anastomosis_method",
    "op_peristalsis_direction", "op_reconstruction_route",
    "op_transfusion_intra", "op_transfusion_post",
    "op_reop_yn", "op_reop_30d", "readmission_30d",
    "op_complication_yn", "op_cd_grade_max",
    "p_macroscopic_type", "p_type0_subclass",
    "p_histology1", "p_histology2", "p_histology3",
    "p_depth", "p_inf", "p_ly", "p_v", "p_pm", "p_dm",
    "p_ln_metastasis", "p_distant_metastasis",
    "p_peritoneal", "p_cytology", "p_liver", "p_stage",
    "p_residual_tumor", "p_chemo_effect", "p_ln_chemo_effect",
    "msi_status", "her2_status", "pdl1_status",
    "claudin18_status", "fgfr2b_status", "ebv_status",
    "gist_kit", "gist_cd34", "gist_desmin", "gist_s100",
    "gist_mitosis", "gist_rupture", "gist_fletcher",
    "adj_yn", "adj_regimen", "adj_completion",
    "mortality_30d", "mortality_inhospital",
    "recurrence_yn", "vital_status", "death_cause",
    # v7.0: NCD codebook検証で追加
    "death_cause_detail",
    "eso_anastomosis_site", "eso_macroscopic_type", "eso_treatment_effect",
    "robot_system_type", "lap_detail_type", "robot_detail_type",
    "thoracic_position", "thoracoscope_detail_type", "egj_center_position",
]


def _build_export_query():
    """全テーブルを結合するエクスポートSQLを組み立てる（重複カラム除去）。"""
    _tables = [
        ("p",   "patients"),
        ("tp",  "tumor_preop"),
        ("neo", "neoadjuvant"),
        ("s",   "surgery"),
        ("pa",  "pathology"),
        ("gd",  "gist_detail"),
        ("adj", "adjuvant_chemo"),
        ("o",   "outcome"),
        ("et",  "eso_tumor"),
        ("es",  "eso_surgery"),
        ("ep",  "eso_pathology"),
        ("ec",  "eso_course"),
        ("rt",  "radiation_therapy"),
    ]
    cols_parts = []
    with get_db() as conn:
        for alias, tbl in _tables:
            assert tbl in _ALLOWED_TABLES, f"Invalid table: {tbl}"
            info = conn.execute(f"PRAGMA table_info({tbl})").fetchall()
            for row in info:
                col_name = row["name"]
                if alias == "p":
                    if col_name == "id":
                        cols_parts.append("p.id AS _db_id")
                    else:
                        cols_parts.append(f"p.{col_name}")
                else:
                    if col_name in ("id", "patient_id", "updated_at"):
                        continue
                    cols_parts.append(f"{alias}.{col_name}")
    select_clause = ",\n       ".join(cols_parts)
    return f"""SELECT {select_clause}
FROM patients p
LEFT JOIN tumor_preop tp ON p.id = tp.patient_id
LEFT JOIN neoadjuvant neo ON p.id = neo.patient_id
LEFT JOIN surgery s ON p.id = s.patient_id
LEFT JOIN pathology pa ON p.id = pa.patient_id
LEFT JOIN gist_detail gd ON p.id = gd.patient_id
LEFT JOIN adjuvant_chemo adj ON p.id = adj.patient_id
LEFT JOIN outcome o ON p.id = o.patient_id
LEFT JOIN eso_tumor et ON p.id = et.patient_id
LEFT JOIN eso_surgery es ON p.id = es.patient_id
LEFT JOIN eso_pathology ep ON p.id = ep.patient_id
LEFT JOIN eso_course ec ON p.id = ec.patient_id
LEFT JOIN radiation_therapy rt ON p.id = rt.patient_id
WHERE p.is_deleted = 0
ORDER BY p.surgery_date DESC"""


def _decode_codebook_values(df):
    """整数コード値をコードブックの日本語ラベルに変換する。"""
    for field in CODEBOOK_DECODE_FIELDS:
        if field not in df.columns:
            continue
        cb = get_codebook(field)
        if not cb:
            continue
        df[field] = df[field].map(lambda v, _cb=cb: _cb.get(v, v) if pd.notna(v) else v)
    # 組織型カラム: DB列名とcodebookフィールド名が異なるため個別処理
    histology_cols = [c for c in df.columns if c in (
        "c_histology", "c_histology1", "c_histology2", "c_histology3",
        "p_histology1", "p_histology2", "p_histology3", "yc_histology")]
    if histology_cols:
        cb_g = get_codebook("histology_gastric")
        cb_e = get_codebook("histology_eso")
        cb_merged = {**cb_g, **cb_e}
        for col in histology_cols:
            if col in df.columns:
                df[col] = df[col].map(
                    lambda v, _m=cb_merged: _m.get(v, v) if pd.notna(v) else v)
    # disease_category は codebook に無いので直接変換
    if "disease_category" in df.columns:
        dcat = {1: "胃癌", 2: "食道癌"}
        df["disease_category"] = df["disease_category"].map(
            lambda v, _m=dcat: _m.get(v, v) if pd.notna(v) else v)
    # data_status
    if "data_status" in df.columns:
        smap = {"draft": "下書き", "submitted": "提出済",
                "verified": "確認済", "approved": "承認済"}
        df["data_status"] = df["data_status"].map(
            lambda v, _m=smap: _m.get(v, v) if pd.notna(v) else v)
    # comp_* のCDグレード値 → ラベル変換
    cd_label = {0: "なし", 1: "Grade I", 2: "Grade II", 3: "Grade IIIa",
                4: "Grade IIIb", 5: "Grade IVa", 6: "Grade IVb", 7: "Grade V"}
    for col in df.columns:
        if col.startswith("comp_") and not col.endswith(("_date", "_detail", "_confirmed")):
            df[col] = df[col].map(lambda v, _m=cd_label: _m.get(v, v) if pd.notna(v) else v)
    return df


def export_page():
    st.markdown("## 📤 データエクスポート / インポート")

    tab_export, tab_import = st.tabs(["📤 エクスポート", "📥 CSVインポート"])

    with tab_import:
        _csv_import_tab()

    with tab_export:
        _export_tab()


def _export_tab():
    # --- フィルター ---
    with st.expander("🔍 エクスポート条件", expanded=True):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            export_format = st.radio("形式", ["Excel (.xlsx)", "CSV"], horizontal=True)
            anonymize = st.checkbox("匿名化エクスポート（患者ID・イニシャル除去）", value=True)
        with fc2:
            header_lang = st.radio("項目名の出力形式",
                                   ["日本語", "両方（日本語 [英語]）", "英語（DB列名そのまま）"])
            decode_values = st.checkbox("コード値を日本語ラベルに変換", value=True)
        with fc3:
            # Phase承認状態フィルター
            approval_filter = st.radio("承認状態",
                ["全データ（下書き含む）", "提出済み以上", "Phase 1 承認済みのみ"], index=0)
            # 疾患分類フィルター
            disease_filter = st.multiselect("疾患分類",
                ["胃癌", "食道癌"], default=["胃癌", "食道癌"], key="exp_disease")
        # 手術年
        yc1, yc2 = st.columns(2)
        with yc1:
            year_from = st.number_input("手術年（開始）", min_value=2000, max_value=2030,
                                         value=2020, key="exp_yr_from")
        with yc2:
            year_to = st.number_input("手術年（終了）", min_value=2000, max_value=2030,
                                       value=datetime.now().year, key="exp_yr_to")

    if st.button("エクスポート実行", type="primary"):
        with st.spinner("データ取得中..."):
            sql = _build_export_query()
            with get_db() as conn:
                df = pd.read_sql_query(sql, conn)

            if df.empty:
                st.warning("エクスポートするデータがありません")
                return

            # --- フィルタリング適用 ---
            # 承認状態
            if approval_filter == "提出済み以上" and "phase1_status" in df.columns:
                df = df[df["phase1_status"].isin(["submitted", "approved"])]
            elif approval_filter == "Phase 1 承認済みのみ" and "phase1_status" in df.columns:
                df = df[df["phase1_status"] == "approved"]

            # 疾患分類
            if "disease_category" in df.columns:
                dcat_map = {"胃癌": 1, "食道癌": 2}
                allowed = [dcat_map[d] for d in disease_filter if d in dcat_map]
                if allowed:
                    df = df[df["disease_category"].isin(allowed)]

            # 手術年
            if "surgery_date" in df.columns:
                df["_sy"] = pd.to_datetime(df["surgery_date"], errors="coerce").dt.year
                df = df[(df["_sy"] >= year_from) & (df["_sy"] <= year_to) | df["_sy"].isna()]
                df.drop(columns=["_sy"], inplace=True)

            if df.empty:
                st.warning("フィルター条件に該当するデータがありません")
                return

            # 暗号化カラムの復号
            for col in ENCRYPTED_COLUMNS:
                if col in df.columns:
                    df[col] = df[col].apply(decrypt_value)

            # 監査ログ（フィルタ条件記録）
            filter_info = json.dumps({
                "approval": approval_filter, "disease": disease_filter,
                "year": f"{year_from}-{year_to}", "anonymize": anonymize
            }, ensure_ascii=False)
            with get_db() as conn:
                log_audit(conn, st.session_state.user["id"], "EXPORT", "all", None,
                          export_filter=filter_info, export_count=len(df))

        # 内部ID列除去
        for col in ["_db_id", "classification_version_id", "created_by", "updated_by"]:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        if anonymize:
            for col in ["patient_id", "initials", "birthdate", "ncd_case_id"]:
                if col in df.columns:
                    df.drop(columns=[col], inplace=True)

        if decode_values:
            df = _decode_codebook_values(df)

        if header_lang == "日本語":
            df.rename(columns=COLUMN_JP, inplace=True)
        elif header_lang == "両方（日本語 [英語]）":
            rename_dict = {k: f"{v} [{k}]" for k, v in COLUMN_JP.items()
                           if k in df.columns}
            df.rename(columns=rename_dict, inplace=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        st.info(f"📊 {len(df)} 件 × {len(df.columns)} 列")

        if export_format == "Excel (.xlsx)":
            path = f"/tmp/ugi_export_{timestamp}.xlsx"
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="全データ")
            with open(path, "rb") as f:
                st.download_button("📥 Excelダウンロード", f,
                                   f"ugi_export_{timestamp}.xlsx",
                                   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            csv_data = df.to_csv(index=False).encode("utf-8-sig")
            st.download_button("📥 CSVダウンロード", csv_data,
                               f"ugi_export_{timestamp}.csv", "text/csv")

        st.success(f"✅ {len(df)} 件のデータをエクスポートしました")

    # --- NCD エクスポート ---
    st.markdown("---")
    st.markdown("### 🏥 NCD CSV エクスポート")
    st.caption("NCD (National Clinical Database) 登録用CSVを出力します。未入力項目や「詳細不明」項目は警告として表示されます。")

    ncd_col1, ncd_col2 = st.columns(2)
    with ncd_col1:
        ncd_approval = st.radio("NCD出力対象",
            ["Phase 1 承認済みのみ", "提出済み以上", "全データ"],
            index=0, key="ncd_approval_filter")
    with ncd_col2:
        ncd_year_from = st.number_input("手術年（開始）", min_value=2000, max_value=2030,
                                         value=datetime.now().year, key="ncd_yr_from")
        ncd_year_to = st.number_input("手術年（終了）", min_value=2000, max_value=2030,
                                       value=datetime.now().year, key="ncd_yr_to")

    # NCD エクスポート用パスワード（AES-256 ZIP 暗号化に使用）
    ncd_zip_pw = st.text_input(
        "ZIP パスワード（AES-256暗号化）", type="password", key="ncd_zip_pw",
        help="ダウンロードCSVをパスワード付きZIPで暗号化します。空欄の場合は平文CSVになります。")

    if st.button("NCD CSV エクスポート", type="secondary", key="ncd_export_btn"):
        from ncd_export import export_ncd_csv
        with st.spinner("NCD CSV 生成中..."):
            # 対象患者IDを取得
            with get_db() as conn:
                where_parts = ["p.is_deleted = 0"]
                if ncd_approval == "Phase 1 承認済みのみ":
                    where_parts.append("p.phase1_status = 'approved'")
                elif ncd_approval == "提出済み以上":
                    where_parts.append("p.phase1_status IN ('submitted', 'approved')")
                where_parts.append(
                    "CAST(strftime('%Y', p.surgery_date) AS INTEGER) BETWEEN ? AND ?"
                )
                where_sql = " AND ".join(where_parts)
                pids = [r["id"] for r in conn.execute(
                    f"SELECT id FROM patients p WHERE {where_sql}",
                    (int(ncd_year_from), int(ncd_year_to))
                ).fetchall()]

            if not pids:
                st.warning("対象症例がありません")
            else:
                csv_bytes, warnings = export_ncd_csv(patient_ids=pids)

                # 警告表示
                if warnings:
                    with st.expander(f"⚠️ NCD登録前確認事項（{len(warnings)}件）", expanded=True):
                        warn_df = pd.DataFrame(warnings)
                        st.dataframe(warn_df, use_container_width=True)
                else:
                    st.success("✅ 全項目入力済み — 警告なし")

                timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                csv_filename = f"ncd_export_{timestamp}.csv"

                # パスワード付きZIP暗号化
                if ncd_zip_pw:
                    import io as _io
                    try:
                        import pyzipper
                        zip_buffer = _io.BytesIO()
                        with pyzipper.AESZipFile(
                            zip_buffer, "w",
                            compression=pyzipper.ZIP_LZMA,
                            encryption=pyzipper.WZ_AES,
                        ) as zf:
                            zf.setpassword(ncd_zip_pw.encode())
                            zf.writestr(csv_filename, csv_bytes)
                        zip_data = zip_buffer.getvalue()
                        st.download_button(
                            f"📥 NCD CSV ダウンロード（{len(pids)}件・AES-256暗号化ZIP）",
                            zip_data,
                            f"ncd_export_{timestamp}.zip",
                            "application/zip",
                            key="ncd_dl_btn"
                        )
                        st.info("🔒 AES-256 暗号化ZIPで出力されます。解凍時にパスワードが必要です。")
                    except ImportError:
                        st.warning("pyzipper未インストールのため平文CSVで出力します。`pip install pyzipper` で暗号化ZIPが利用可能になります。")
                        st.download_button(
                            f"📥 NCD CSV ダウンロード（{len(pids)}件）",
                            csv_bytes, csv_filename, "text/csv", key="ncd_dl_btn"
                        )
                else:
                    st.download_button(
                        f"📥 NCD CSV ダウンロード（{len(pids)}件）",
                        csv_bytes, csv_filename, "text/csv", key="ncd_dl_btn"
                    )

                # 監査ログ
                with get_db() as conn:
                    log_audit(conn, st.session_state.user["id"], "NCD_EXPORT",
                              "patients", None,
                              export_filter=f"approval={ncd_approval},year={ncd_year_from}-{ncd_year_to}",
                              export_count=len(pids))

                st.info(f"📊 {len(pids)} 件をNCD形式で出力しました（警告 {len(warnings)} 件）")


# ============================================================
# CSVインポート UI
# ============================================================
def _csv_import_tab():
    """CSVインポートタブの内容。"""

    st.markdown("### 📥 CSV一括インポート")
    st.info(
        "CSVファイルから複数の症例データを一括登録できます。\n"
        "テンプレートをダウンロードし、データを記入してアップロードしてください。"
    )

    # --- テンプレートダウンロード ---
    section_card("1. テンプレートダウンロード", "blue")
    tc1, tc2 = st.columns(2)
    with tc1:
        tmpl_disease = st.radio(
            "疾患分類", ["胃癌", "食道癌", "全カラム"],
            horizontal=True, key="import_tmpl_disease"
        )
    with tc2:
        disease_map = {"胃癌": 1, "食道癌": 2, "全カラム": None}
        tmpl_csv = generate_import_template(disease_category=disease_map[tmpl_disease])
        st.download_button(
            f"📥 テンプレートCSV（{tmpl_disease}）",
            tmpl_csv.encode("utf-8"),
            f"import_template_{tmpl_disease}.csv",
            "text/csv",
            key="dl_import_tmpl"
        )
        st.caption("1行目=日本語ヘッダー, 2行目=DB列名, 3行目以降=データ")

    st.markdown("---")

    # --- CSVアップロード ---
    section_card("2. CSVアップロード & バリデーション", "green")

    ic1, ic2 = st.columns(2)
    with ic1:
        import_disease = st.radio(
            "登録する疾患分類", ["胃癌", "食道癌"],
            horizontal=True, key="import_disease_cat"
        )
    with ic2:
        uploaded = st.file_uploader(
            "CSVファイルを選択", type=["csv"],
            key="csv_import_file"
        )

    if uploaded is not None:
        try:
            raw = uploaded.read()
            # エンコーディング検出
            for enc in ["utf-8-sig", "utf-8", "shift_jis", "cp932"]:
                try:
                    csv_text = raw.decode(enc)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            else:
                st.error("❌ ファイルのエンコーディングを判定できません（UTF-8 / Shift_JIS で保存してください）")
                return

            rows, errors, warnings = validate_csv(csv_text)

            # 結果表示
            st.markdown(f"**検出データ: {len(rows)} 行**")

            if errors:
                st.error(f"❌ エラー {len(errors)} 件 — 修正が必要です")
                with st.expander(f"エラー一覧（{len(errors)} 件）", expanded=True):
                    for e in errors:
                        st.markdown(f"- {e}")

            if warnings:
                st.warning(f"⚠️ 警告 {len(warnings)} 件 — 確認推奨")
                with st.expander(f"警告一覧（{len(warnings)} 件）"):
                    for w in warnings:
                        st.markdown(f"- {w}")

            if not errors and rows:
                st.success(f"✅ バリデーション通過 — {len(rows)} 件のインポート準備完了")

                # プレビュー
                with st.expander("データプレビュー（先頭5行）"):
                    import pandas as _pd
                    preview_df = _pd.DataFrame(rows[:5])
                    st.dataframe(preview_df, use_container_width=True, hide_index=True)

                st.markdown("---")
                section_card("3. インポート実行", "orange")

                st.warning(
                    f"⚠️ {len(rows)} 件を「{import_disease}」として新規登録します。\n"
                    "この操作は取り消せません。"
                )

                # 確認チェックボックス
                confirm = st.checkbox(
                    f"上記 {len(rows)} 件のデータを登録することに同意します",
                    key="import_confirm"
                )

                if confirm and st.button("🚀 インポート実行", type="primary", key="btn_do_import"):
                    disease_cat = 1 if import_disease == "胃癌" else 2
                    with st.spinner(f"インポート中... ({len(rows)} 件)"):
                        success_count, import_errors = import_csv_records(
                            rows,
                            user_id=st.session_state.user["id"],
                            disease_category=disease_cat
                        )

                    if success_count > 0:
                        st.success(f"✅ {success_count} 件のインポートが完了しました")
                        st.balloons()

                    if import_errors:
                        st.error(f"❌ {len(import_errors)} 件のエラーが発生しました")
                        with st.expander("インポートエラー詳細"):
                            for ie in import_errors:
                                st.markdown(f"- {ie}")

            elif not errors and not rows:
                st.warning("インポート可能なデータ行がありません")

        except Exception as e:
            st.error(f"❌ ファイル読み込みエラー: {e}")


# ============================================================
# 🩸 検査値読取ページ
# ============================================================
def lab_reader_page():
    st.markdown("## 🩸 血液検査値 読み取り")
    st.caption("基準値: JCCLS 共用基準範囲 2020年版（男性基準）")

    user = st.session_state.get("user", {})

    # ---- 1. Vision モデル接続チェック ----
    ok, msg = check_vision_model()
    if ok:
        st.success(msg)
    else:
        st.error(msg)
        st.info(
            "Ollama で Vision モデルを起動してください。\n\n"
            "```bash\n"
            "ollama pull llama3.2-vision\n"
            "ollama serve\n"
            "```"
        )
        st.stop()

    # ---- 2. 画像アップロード（最初に） ----
    st.markdown("### 検査結果画像")
    uploaded = st.file_uploader(
        "電子カルテの検査結果スクリーンショットをアップロード",
        type=["png", "jpg", "jpeg", "bmp", "webp"],
    )

    if uploaded:
        st.image(uploaded, caption="アップロード画像", use_container_width=True)

    # ---- 3. 読み取り実行 ----
    if uploaded and st.button("🔍 読み取り実行", type="primary"):
        image_bytes = uploaded.getvalue()
        # ページ離脱防止: 処理中は警告を表示し、ブラウザの離脱も抑止
        _ocr_warn = st.warning(
            "🔄 **OCR 処理中です。このページから移動しないでください。**\n\n"
            "ページ移動やサイドバー操作を行うと処理が中断されます。"
        )
        import streamlit.components.v1 as components
        components.html(
            """<script>
            window.onbeforeunload = function(e) {
                e.preventDefault();
                e.returnValue = 'OCR処理中です。ページを離れますか？';
                return e.returnValue;
            };
            </script>""",
            height=0,
        )
        with st.spinner("Ollama Vision で検査値を読み取り中...（最大3分）"):
            result = extract_lab_values(image_bytes)
        _ocr_warn.empty()  # 処理完了後に警告を消す

        if result["errors"]:
            for err in result["errors"]:
                st.warning(f"⚠️ {err}")

        values = result.get("values", {})
        if not values:
            st.error("検査値を抽出できませんでした。画像を確認してください。")
            st.stop()

        # セッションに保存
        st.session_state["lab_extracted"] = values
        st.session_state["lab_raw_text"] = result.get("raw_text", "")
        st.session_state["lab_judgments"] = judge_lab_values(values)
        st.rerun()

    # ---- 4. 結果表示・編集・保存 ----
    if "lab_extracted" not in st.session_state:
        return

    values = st.session_state["lab_extracted"]
    judgments = st.session_state.get("lab_judgments", [])

    st.markdown("---")
    st.markdown("### 読み取り結果")

    # ---- 4a. OCR から抽出した患者ID・採取日を表示＆編集 ----
    ocr_pid = values.pop("patient_id_ocr", None)
    ocr_date = values.pop("sample_date", None)

    st.markdown("**患者情報・採取日**")
    info_c1, info_c2, info_c3 = st.columns(3)
    with info_c1:
        input_pid = st.text_input(
            "患者ID（カルテ番号）",
            value=ocr_pid or "",
            key="lab_patient_id_input",
            help="画像から自動取得。手動で修正可能。")
        if ocr_pid:
            st.caption(f"📷 画像から取得: {ocr_pid}")

    with info_c2:
        # 日付のパース
        default_date = None
        if ocr_date:
            try:
                default_date = date.fromisoformat(ocr_date.replace("/", "-")[:10])
            except (ValueError, AttributeError):
                pass
        sample_date = st.date_input("採血日", value=default_date, key="lab_sample_date")
        if ocr_date:
            st.caption(f"📷 画像から取得: {ocr_date}")

    with info_c3:
        timing = st.selectbox(
            "検査タイミング",
            ["preop", "postop", "recurrence"],
            format_func=lambda x: {"preop": "術前", "postop": "術後", "recurrence": "再発時"}[x],
            key="lab_timing",
        )

    # 患者IDで DB 検索してマッチ表示
    db_patient_pk = None  # patients.id (内部PK)
    if input_pid and input_pid.strip():
        with get_db() as conn:
            matched = conn.execute(
                "SELECT id, study_id, patient_id, sex, surgery_date "
                "FROM patients WHERE patient_id = ? AND is_deleted = 0",
                (input_pid.strip(),)
            ).fetchone()
        if matched:
            db_patient_pk = matched["id"]
            st.success(
                f"✅ 患者マッチ: {matched['study_id']}  "
                f"（内部ID:{matched['id']}  手術日:{matched['surgery_date'] or '未定'}）"
            )
        else:
            st.warning(f"⚠️ 患者ID「{input_pid.strip()}」に一致する登録症例が見つかりません。保存時は紐づけなしで登録されます。")

    st.caption("値を修正してから保存できます。")

    # 基準値判定をdict化
    judge_map = {j["col"]: j for j in judgments}

    # カテゴリごとに表示
    _LAB_CATEGORIES = [
        ("血算 (CBC)", ["wbc", "rbc", "hgb", "hct", "plt", "mcv", "mch", "mchc",
                        "neut_pct", "lymph_pct", "mono_pct", "eosin_pct", "baso_pct"]),
        ("生化学", ["tp", "alb", "t_bil", "ast", "alt", "ldh", "alp", "ggt",
                    "che", "bun", "cre", "egfr", "na", "k", "cl",
                    "crp", "amy", "ck", "glu", "hba1c"]),
        ("栄養・凝固", ["prealb", "cholinesterase", "pt_inr", "aptt",
                        "fibrinogen", "d_dimer"]),
        ("腫瘍マーカー", ["cea_lab", "ca199_lab", "afp_lab", "ca125_lab"]),
    ]

    edited_values = {}
    for cat_name, cols in _LAB_CATEGORIES:
        cat_cols = [c for c in cols if c in values]
        if not cat_cols:
            continue

        st.markdown(f"**{cat_name}**")
        col_chunks = [cat_cols[i:i+4] for i in range(0, len(cat_cols), 4)]
        for chunk in col_chunks:
            ui_cols = st.columns(4)
            for i, col in enumerate(chunk):
                label = LAB_LABELS.get(col, col)
                j = judge_map.get(col, {})
                status = j.get("status", "unknown")
                range_str = j.get("range", "")

                if status == "high":
                    indicator = "🔴"
                elif status == "low":
                    indicator = "🔵"
                elif status == "normal":
                    indicator = "🟢"
                else:
                    indicator = "⚪"

                with ui_cols[i]:
                    v = st.number_input(
                        f"{indicator} {label}",
                        value=float(values[col]) if isinstance(values[col], (int, float)) else 0.0,
                        format="%.2f",
                        key=f"lab_edit_{col}",
                        help=f"基準値: {range_str}" if range_str else None,
                    )
                    edited_values[col] = v

    # ---- 既存フィールドへの自動マッピング候補 ----
    auto_map = map_to_existing_fields(edited_values)
    if auto_map and db_patient_pk:
        st.markdown("### 既存フィールド自動反映候補")
        for tbl, fields in auto_map.items():
            for fld, val in fields.items():
                st.info(f"📌 {tbl}.{fld} ← {val}")
        apply_auto = st.checkbox("上記の自動反映も同時に行う", value=False)
    else:
        apply_auto = False

    # ---- 保存ボタン ----
    st.markdown("---")
    c1, c2 = st.columns(2)

    with c1:
        if st.button("💾 検査値を保存", type="primary"):
            save_data = dict(edited_values)
            save_data["timing"] = timing
            if sample_date:
                save_data["sample_date"] = sample_date.isoformat()
            save_data["source_type"] = "ocr"
            save_data["raw_ocr_text"] = st.session_state.get("lab_raw_text", "")
            save_data["created_by"] = user.get("id")
            if input_pid and input_pid.strip():
                save_data["notes"] = f"患者ID(カルテ番号):{input_pid.strip()}"

            try:
                with get_db() as conn:
                    if db_patient_pk:
                        save_data["patient_id"] = db_patient_pk
                    # lab_results は1患者に複数行OKなので INSERT
                    cols_list = list(save_data.keys())
                    placeholders = ", ".join(["?"] * len(cols_list))
                    col_str = ", ".join(cols_list)
                    conn.execute(
                        f"INSERT INTO lab_results ({col_str}) VALUES ({placeholders})",
                        [save_data[c] for c in cols_list],
                    )
                    # 自動反映
                    if apply_auto and db_patient_pk:
                        for tbl, fields in auto_map.items():
                            if tbl == "patients":
                                sets = ", ".join(f"{k} = ?" for k in fields)
                                conn.execute(
                                    f"UPDATE patients SET {sets}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                                    list(fields.values()) + [db_patient_pk],
                                )
                            else:
                                upsert_record(conn, tbl, db_patient_pk, fields,
                                              user_id=user.get("id"))

                    log_audit(conn, user.get("id"), "lab_import",
                              table_name="lab_results", record_id=db_patient_pk,
                              comment=f"OCR検査値取込 pid={input_pid} timing={timing} items={len(edited_values)}")

                st.success(f"✅ 検査値を保存しました（{len(edited_values)} 項目）")
                for k in ["lab_extracted", "lab_raw_text", "lab_judgments"]:
                    st.session_state.pop(k, None)
                st.rerun()

            except Exception as e:
                st.error(f"❌ 保存エラー: {e}")

    with c2:
        if st.button("🗑️ 読み取り結果をクリア"):
            for k in ["lab_extracted", "lab_raw_text", "lab_judgments"]:
                st.session_state.pop(k, None)
            st.rerun()

    # ---- デバッグ: LLM生出力 ----
    with st.expander("🔧 LLM 生出力（デバッグ用）"):
        st.code(st.session_state.get("lab_raw_text", ""), language="json")

    # ---- 5. 過去の検査値一覧 ----
    if db_patient_pk:
        st.markdown("---")
        st.markdown("### 過去の検査値記録")
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM lab_results WHERE patient_id = ? ORDER BY sample_date DESC, created_at DESC",
                (db_patient_pk,),
            ).fetchall()

        if rows:
            display_cols = ["id", "timing", "sample_date", "source_type",
                            "wbc", "hgb", "plt", "alb", "crp", "cea_lab", "created_at"]
            data = []
            for r in rows:
                row_dict = dict(r)
                data.append({c: row_dict.get(c) for c in display_cols})
            df = pd.DataFrame(data)
            timing_map = {"preop": "術前", "postop": "術後", "recurrence": "再発時"}
            df["timing"] = df["timing"].map(lambda x: timing_map.get(x, x))
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("この患者の検査値記録はまだありません。")


# ============================================================
# 監査ログ（改良版）
# ============================================================
def audit_page():
    st.markdown("## 📜 監査ログ（Audit Trail）")

    # --- サマリー統計 ---
    with get_db() as conn:
        stats = conn.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT record_id) as unique_cases,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest
            FROM audit_log
        """).fetchone()
        today_count = (conn.execute(
            "SELECT COUNT(*) FROM audit_log WHERE DATE(timestamp) = DATE('now')"
        ).fetchone() or (0,))[0]
        week_count = (conn.execute(
            "SELECT COUNT(*) FROM audit_log WHERE timestamp >= datetime('now', '-7 days')"
        ).fetchone() or (0,))[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("総ログ件数", f"{stats['total']:,}")
    c2.metric("本日の操作", f"{today_count:,}")
    c3.metric("直近7日", f"{week_count:,}")
    c4.metric("関係ユーザー数", f"{stats['unique_users']}")

    st.markdown("---")

    # --- フィルター ---
    with st.expander("🔍 フィルター", expanded=True):
        fc1, fc2, fc3, fc4 = st.columns(4)

        with fc1:
            # 日付範囲
            date_from = st.date_input("開始日", value=None, key="audit_from",
                                       format="YYYY/MM/DD")
            date_to = st.date_input("終了日", value=None, key="audit_to",
                                     format="YYYY/MM/DD")

        with fc2:
            # ユーザーフィルター
            with get_db() as conn:
                user_rows = conn.execute(
                    "SELECT DISTINCT u.display_name FROM audit_log al "
                    "JOIN users u ON al.user_id = u.id ORDER BY u.display_name"
                ).fetchall()
            user_options = ["すべて"] + [r["display_name"] for r in user_rows]
            sel_user = st.selectbox("ユーザー", user_options, key="audit_user")

        with fc3:
            # 操作種別フィルター
            action_options = ["すべて", "INSERT", "UPDATE", "UPSERT", "DELETE", "BULK_DELETE"]
            sel_action = st.selectbox("操作種別", action_options, key="audit_action")

        with fc4:
            # テーブルフィルター
            with get_db() as conn:
                tbl_rows = conn.execute(
                    "SELECT DISTINCT table_name FROM audit_log WHERE table_name IS NOT NULL ORDER BY table_name"
                ).fetchall()
            table_options = ["すべて"] + [r["table_name"] for r in tbl_rows]
            sel_table = st.selectbox("テーブル", table_options, key="audit_table")

        # 症例ID検索
        fc5, fc6, _ = st.columns([1, 1, 2])
        with fc5:
            search_record_id = st.text_input("症例ID (record_id)", key="audit_rid",
                                              placeholder="例: 42")
        with fc6:
            max_rows = st.number_input("表示件数上限", min_value=50, max_value=5000,
                                        value=500, step=50, key="audit_limit")

    # --- SQLクエリ構築 ---
    where_clauses = []
    params = []

    if date_from:
        where_clauses.append("DATE(al.timestamp) >= ?")
        params.append(date_from.strftime("%Y-%m-%d"))
    if date_to:
        where_clauses.append("DATE(al.timestamp) <= ?")
        params.append(date_to.strftime("%Y-%m-%d"))
    if sel_user != "すべて":
        where_clauses.append("u.display_name = ?")
        params.append(sel_user)
    if sel_action != "すべて":
        where_clauses.append("al.action = ?")
        params.append(sel_action)
    if sel_table != "すべて":
        where_clauses.append("al.table_name = ?")
        params.append(sel_table)
    if search_record_id.strip():
        where_clauses.append("CAST(al.record_id AS TEXT) = ?")
        params.append(search_record_id.strip())

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    with get_db() as conn:
        df = pd.read_sql_query(f"""
            SELECT al.id, al.timestamp as 日時,
                   u.display_name as ユーザー,
                   al.action as 操作,
                   al.table_name as テーブル,
                   al.record_id as 症例ID,
                   al.field_name as 項目,
                   al.old_value as 変更前,
                   al.new_value as 変更後
            FROM audit_log al
            LEFT JOIN users u ON al.user_id = u.id
            {where_sql}
            ORDER BY al.timestamp DESC
            LIMIT ?
        """, conn, params=params + [max_rows])

    # --- 結果表示 ---
    st.markdown(f"**検索結果: {len(df)} 件**")

    if df.empty:
        st.info("該当するログがありません")
    else:
        # 操作種別ごとの色分けと集計
        if len(df) > 0:
            action_counts = df["操作"].value_counts()
            ac_cols = st.columns(min(len(action_counts), 6))
            action_colors = {
                "INSERT": "🟢", "UPDATE": "🟡", "UPSERT": "🔵",
                "DELETE": "🔴", "BULK_DELETE": "🔴"
            }
            for i, (act, cnt) in enumerate(action_counts.items()):
                icon = action_colors.get(act, "⚪")
                ac_cols[i % len(ac_cols)].markdown(f"{icon} **{act}**: {cnt}件")

        # データフレーム表示（idカラムは非表示）
        display_df = df.drop(columns=["id"])
        st.dataframe(display_df, use_container_width=True, hide_index=True,
                      column_config={
                          "日時": st.column_config.DatetimeColumn("日時", format="YYYY/MM/DD HH:mm:ss"),
                          "症例ID": st.column_config.NumberColumn("症例ID", format="%d"),
                      })

    # --- ユーザー別アクティビティ ---
    with st.expander("📊 ユーザー別アクティビティ"):
        with get_db() as conn:
            user_activity = pd.read_sql_query("""
                SELECT u.display_name as ユーザー,
                       COUNT(*) as 総操作数,
                       SUM(CASE WHEN al.action='INSERT' THEN 1 ELSE 0 END) as 新規登録,
                       SUM(CASE WHEN al.action='UPDATE' THEN 1 ELSE 0 END) as 更新,
                       SUM(CASE WHEN al.action IN ('DELETE','BULK_DELETE') THEN 1 ELSE 0 END) as 削除,
                       MAX(al.timestamp) as 最終操作日時,
                       COUNT(DISTINCT DATE(al.timestamp)) as 操作日数
                FROM audit_log al
                JOIN users u ON al.user_id = u.id
                GROUP BY al.user_id
                ORDER BY 総操作数 DESC
            """, conn)
        if not user_activity.empty:
            st.dataframe(user_activity, use_container_width=True, hide_index=True)

    # --- 日別操作数推移 ---
    with st.expander("📈 日別操作数推移"):
        with get_db() as conn:
            daily = pd.read_sql_query("""
                SELECT DATE(timestamp) as 日付, COUNT(*) as 操作数
                FROM audit_log
                GROUP BY DATE(timestamp)
                ORDER BY 日付 DESC
                LIMIT 60
            """, conn)
        if not daily.empty:
            daily = daily.sort_values("日付")
            st.bar_chart(daily.set_index("日付"))


# ============================================================
# データ管理（一括削除）— 管理者専用
# ============================================================
def data_management_page():
    st.markdown("## 🗑️ データ管理（一括操作）")

    user_role = st.session_state.user.get("role", "entry")
    if user_role != "admin":
        st.warning("⚠️ この機能は管理者のみ使用できます。")
        return

    st.warning("⚠️ **注意**: この画面で削除したデータは復元できません。操作は監査ログに記録されます。")

    st.markdown("---")

    # --- 手動バックアップ ---
    section_card("💾 データベースバックアップ", "blue")

    bc1, bc2 = st.columns([1, 2])
    with bc1:
        if st.button("💾 手動バックアップを作成", type="primary"):
            with st.spinner("バックアップ作成中..."):
                success, result = backup_database(
                    user_id=st.session_state.user["id"], tag="manual"
                )
            if success:
                import os as _os
                size_mb = _os.path.getsize(result) / (1024 * 1024)
                st.success(f"✅ バックアップ完了: {_os.path.basename(result)} ({size_mb:.1f} MB)")
                # 古いバックアップ自動整理
                removed = delete_old_backups(keep_count=10)
                if removed:
                    st.info(f"🗂️ 古いバックアップ {removed} 件を自動削除しました（最新10件を保持）")
            else:
                st.error(f"❌ バックアップ失敗: {result}")

    with bc2:
        existing_backups = list_backups(limit=5)
        if existing_backups:
            st.markdown("**最近のバックアップ:**")
            for bk in existing_backups:
                size_mb = bk["size_bytes"] / (1024 * 1024)
                st.text(f"  {bk['created']}  |  {bk['filename']}  ({size_mb:.1f} MB)")
        else:
            st.info("バックアップはまだありません")

    st.markdown("---")

    # --- 削除条件 ---
    section_card("削除条件の設定", "red")

    fc1, fc2 = st.columns(2)
    with fc1:
        del_date_from = st.date_input("対象期間（開始）", value=None, key="del_from",
                                       format="YYYY/MM/DD")
        del_date_to = st.date_input("対象期間（終了）", value=None, key="del_to",
                                     format="YYYY/MM/DD")
    with fc2:
        with get_db() as conn:
            all_users = pd.read_sql_query(
                "SELECT id, display_name FROM users ORDER BY display_name", conn)
        user_opts = {"": "すべて"}
        for _, row in all_users.iterrows():
            user_opts[row["id"]] = row["display_name"]
        del_user = st.selectbox("登録ユーザー", options=list(user_opts.keys()),
                                 format_func=lambda x: user_opts[x], key="del_user")
        del_status = st.selectbox("ステータス", ["すべて", "draft", "submitted", "verified", "approved"],
                                   key="del_status")

    # study_id 指定
    del_study_ids = st.text_input("Study ID（カンマ区切りで複数指定可、空欄=全対象）",
                                   key="del_study_ids", placeholder="例: GC-001, GC-002")

    # --- プレビュー ---
    if st.button("🔍 対象症例をプレビュー", type="secondary"):
        where_parts = []
        prm = []

        if del_date_from:
            where_parts.append("DATE(p.created_at) >= ?")
            prm.append(del_date_from.strftime("%Y-%m-%d"))
        if del_date_to:
            where_parts.append("DATE(p.created_at) <= ?")
            prm.append(del_date_to.strftime("%Y-%m-%d"))
        if del_user:
            where_parts.append("p.created_by = ?")
            prm.append(del_user)
        if del_status != "すべて":
            where_parts.append("p.data_status = ?")
            prm.append(del_status)
        if del_study_ids.strip():
            ids = [s.strip() for s in del_study_ids.split(",") if s.strip()]
            placeholders = ",".join(["?"] * len(ids))
            where_parts.append(f"p.study_id IN ({placeholders})")
            prm.extend(ids)

        if not where_parts:
            st.error("⚠️ 条件を1つ以上指定してください。全件削除は許可されていません。")
        else:
            where_sql = " AND ".join(where_parts)
            with get_db() as conn:
                preview = pd.read_sql_query(f"""
                    SELECT p.id, p.study_id, p.patient_id, p.data_status,
                           u.display_name as 登録者,
                           p.created_at as 登録日時, p.updated_at as 最終更新
                    FROM patients p
                    LEFT JOIN users u ON p.created_by = u.id
                    WHERE p.is_deleted = 0 AND {where_sql}
                    ORDER BY p.created_at DESC
                """, conn, params=prm)

            st.session_state._del_preview = preview
            st.session_state._del_where = where_sql
            st.session_state._del_params = prm

    # プレビュー結果表示
    preview = st.session_state.get("_del_preview")
    if preview is not None:
        if preview.empty:
            st.info("該当する症例はありません")
        else:
            st.error(f"**⚠️ 削除対象: {len(preview)} 件**")
            st.dataframe(preview, use_container_width=True, hide_index=True)

            # 二段階確認
            st.markdown("---")
            st.markdown("### ⚠️ 削除の確認")
            confirm_text = st.text_input(
                f"削除を実行するには「削除する」と入力してください（対象: {len(preview)}件）",
                key="del_confirm"
            )
            if st.button("🗑️ 一括削除を実行", type="primary"):
                if confirm_text != "削除する":
                    st.error("確認テキストが一致しません。「削除する」と入力してください。")
                else:
                    # 削除実行
                    where_sql = st.session_state._del_where
                    prm = st.session_state._del_params
                    try:
                        with get_db() as conn:
                            # 対象 patient_id 一覧取得（JOINを含むフィルタに対応）
                            target_ids = conn.execute(
                                f"SELECT p.id, p.study_id FROM patients p "
                                f"LEFT JOIN users u ON p.created_by = u.id "
                                f"WHERE p.is_deleted = 0 AND {where_sql}",
                                prm
                            ).fetchall()

                            deleted_count = 0
                            for t_row in target_ids:
                                pid = t_row["id"]
                                sid = t_row["study_id"]
                                # 論理削除（is_deleted フラグ）
                                conn.execute(
                                    "UPDATE patients SET is_deleted = 1, "
                                    "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                                    (pid,)
                                )
                                # 監査ログ
                                log_audit(conn, st.session_state.user["id"],
                                          "SOFT_DELETE", "patients", pid,
                                          field_name="study_id", old_value=sid)
                                deleted_count += 1

                        st.success(f"✅ {deleted_count} 件の症例を削除しました（論理削除）")
                        # プレビュー状態クリア
                        for k in ["_del_preview", "_del_where", "_del_params"]:
                            st.session_state.pop(k, None)
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ 削除エラー: {e}")
                        import traceback
                        st.code(traceback.format_exc())

    st.markdown("---")

    # --- NCD 年度バージョン管理 ---
    section_card("📋 NCD 年度バージョン管理", "blue")

    ncd_versions = get_ncd_versions()
    if ncd_versions:
        ncd_df = pd.DataFrame([dict(v) for v in ncd_versions])
        ncd_display = ncd_df[["year", "version", "is_active", "notes"]].rename(
            columns={"year": "年度", "version": "バージョン",
                     "is_active": "有効", "notes": "備考"})
        st.dataframe(ncd_display, use_container_width=True, hide_index=True)

        # フィールド定義の表示
        with st.expander("📝 フィールド定義を表示"):
            active = [v for v in ncd_versions if v["is_active"]]
            if active:
                fields = get_ncd_field_defs(active[0]["id"])
                if fields:
                    fd_df = pd.DataFrame([dict(f) for f in fields])
                    fd_display = fd_df[["ncd_field_name", "display_name", "data_type",
                                        "is_required", "ugi_db_column"]].rename(
                        columns={"ncd_field_name": "NCDフィールド", "display_name": "表示名",
                                 "data_type": "型", "is_required": "必須",
                                 "ugi_db_column": "UGI_DB列"})
                    st.dataframe(fd_display, use_container_width=True, hide_index=True)
    else:
        st.info("NCD バージョンが未登録です")

    # 新年度追加
    with st.expander("➕ 新年度バージョンを追加"):
        nc1, nc2 = st.columns(2)
        with nc1:
            new_year = st.number_input("年度", min_value=2020, max_value=2040,
                                        value=date.today().year, key="ncd_new_year")
        with nc2:
            copy_from = st.selectbox(
                "コピー元",
                ["なし（空）"] + [f"{v['year']}年度" for v in (ncd_versions or [])],
                key="ncd_copy_from")
        if st.button("追加", key="ncd_add_btn"):
            copy_year = None
            if copy_from != "なし（空）":
                copy_year = int(copy_from.replace("年度", ""))
            try:
                add_ncd_version(new_year, f"v{new_year}", copy_from_year=copy_year)
                st.success(f"✅ {new_year} 年度バージョンを追加しました")
                st.rerun()
            except Exception as e:
                st.error(f"❌ 追加エラー: {e}")

    st.markdown("---")

    # --- 術式別必須フィールドマトリクス ---
    section_card("📊 術式別必須フィールド マトリクス", "blue")
    with st.expander("マトリクスを表示"):
        try:
            result = get_requirement_matrix()
            if result:
                procedures = result["procedures"]   # [(code, name), ...]
                all_fields = sorted(result["fields"])  # set → sorted list
                mat = result["matrix"]               # {proc_code: set_of_fields}
                matrix_data = []
                for f in all_fields:
                    row = {"フィールド": f}
                    for code, name in procedures:
                        row[name] = "●" if f in mat.get(code, set()) else ""
                    matrix_data.append(row)
                matrix_df = pd.DataFrame(matrix_data)
                st.dataframe(matrix_df, use_container_width=True, hide_index=True, height=600)
                st.caption(f"術式数: {len(procedures)}  /  フィールド数: {len(all_fields)}")
        except Exception as e:
            st.error(f"マトリクス表示エラー: {e}")


# ============================================================
# ユーザー管理
# ============================================================
def user_management_page():
    st.markdown("## 👥 ユーザー管理")
    if st.session_state.user["role"] != "admin":
        st.warning("管理者権限が必要です")
        return

    with get_db() as conn:
        users = pd.read_sql_query(
            "SELECT id, username, display_name, role, is_active, created_at FROM users", conn)
    st.dataframe(users, use_container_width=True, hide_index=True)

    st.markdown("### 新規ユーザー追加")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        new_user = st.text_input("ユーザー名", key="new_u")
    with col2:
        new_pw = st.text_input("パスワード", type="password", key="new_pw")
    with col3:
        new_name = st.text_input("表示名", key="new_name")
    with col4:
        new_role = st.selectbox("権限", ["entry", "reviewer", "admin"], key="new_role")

    if st.button("追加", type="primary"):
        if new_user and new_pw and new_name:
            with get_db() as conn:
                pw_hash = hash_password(new_pw)
                conn.execute(
                    "INSERT INTO users (username, password_hash, display_name, role) VALUES (?,?,?,?)",
                    (new_user, pw_hash, new_name, new_role))
                log_audit(conn, st.session_state.user["id"], "INSERT", "users")
            st.success(f"✅ ユーザー「{new_name}」を追加しました")
            st.rerun()


# ============================================================
# 通知ページ
# ============================================================
def notification_page():
    st.markdown("## 🔔 通知")
    user_id = st.session_state.user["id"]

    # --- Phase3/4 リマインド表示 ---
    with get_db() as conn:
        reminders = get_phase_reminders(conn)
    if reminders:
        with st.expander(f"⏰ Phase提出・承認リマインド（{len(reminders)}件）", expanded=True):
            for rem in reminders:
                phase_label = PHASE_LABELS.get(rem["phase"], rem["phase"])
                action_label = "提出" if rem["reminder_type"] == "submit" else "承認"
                st.warning(
                    f"**{rem['study_id']}** — {phase_label} の{action_label}が必要です "
                    f"（手術日: {rem['surgery_date']}）"
                )

    # 既読にするボタン
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("すべて既読にする"):
            with get_db() as conn:
                conn.execute("UPDATE notifications SET is_read=1 WHERE user_id=?", (user_id,))
            st.rerun()

    with get_db() as conn:
        notifs = pd.read_sql_query("""
            SELECT id, title, message, link_page, link_study_id, is_read, created_at
            FROM notifications WHERE user_id=?
            ORDER BY created_at DESC LIMIT 100
        """, conn, params=[user_id])

    if notifs.empty:
        st.info("通知はありません")
        return

    for _, row in notifs.iterrows():
        icon = "🔵" if row["is_read"] == 0 else "⚪"
        with st.container():
            c1, c2 = st.columns([5, 1])
            with c1:
                st.markdown(f"{icon} **{row['title']}**")
                if row["message"]:
                    st.caption(row["message"])
                st.caption(f"{row['created_at']}")
            with c2:
                if row["link_study_id"]:
                    if st.button("開く", key=f"notif_{row['id']}"):
                        st.session_state.edit_study_id = row["link_study_id"]
                        st.session_state._goto_page = "➕ 新規登録"
                        with get_db() as conn:
                            conn.execute("UPDATE notifications SET is_read=1 WHERE id=?", (row["id"],))
                        st.rerun()
        st.markdown("---")


# ============================================================
# マイページ（通知設定）
# ============================================================
def my_page():
    st.markdown("## ⚙️ マイページ")
    user = st.session_state.user
    user_id = user["id"]

    # --- パスワード変更 ---
    section_card("パスワード変更", "blue")
    pc1, pc2, pc3 = st.columns(3)
    with pc1:
        old_pw = st.text_input("現在のパスワード", type="password", key="my_old_pw")
    with pc2:
        new_pw = st.text_input("新しいパスワード", type="password", key="my_new_pw")
    with pc3:
        new_pw2 = st.text_input("新しいパスワード（確認）", type="password", key="my_new_pw2")
    if st.button("パスワード変更"):
        if not old_pw or not new_pw:
            st.error("すべてのフィールドを入力してください")
        elif new_pw != new_pw2:
            st.error("新しいパスワードが一致しません")
        else:
            from database import _verify_password, validate_password_strength
            # パスワード強度チェック
            is_strong, pw_msgs = validate_password_strength(new_pw)
            if not is_strong:
                for msg in pw_msgs:
                    st.error(f"⚠️ {msg}")
            else:
                with get_db() as conn:
                    row = conn.execute("SELECT password_hash FROM users WHERE id=?", (user_id,)).fetchone()
                    if row and _verify_password(old_pw, row["password_hash"]):
                        new_hash = hash_password(new_pw)
                        conn.execute("UPDATE users SET password_hash=? WHERE id=?", (new_hash, user_id))
                        log_audit(conn, user_id, "UPDATE", "users", user_id, field_name="password_hash")
                        st.success("✅ パスワードを変更しました")
                    else:
                        st.error("現在のパスワードが正しくありません")

    st.markdown("---")

    # --- 通知設定 ---
    section_card("通知設定", "green")
    with get_db() as conn:
        ns = conn.execute("SELECT * FROM notification_settings WHERE user_id=?", (user_id,)).fetchone()
        ns = dict(ns) if ns else {}

    nc1, nc2 = st.columns(2)
    with nc1:
        st.markdown("**通知手段**")
        enable_app = st.checkbox("アプリ内通知", value=ns.get("enable_app_notification", 1) == 1, key="ns_app")

        # --- LINE ---
        import os as _os
        _line_configured = bool(_os.environ.get("UGI_LINE_CHANNEL_TOKEN", ""))
        if _line_configured:
            line_user_id = st.text_input("LINE ユーザーID",
                                          value=ns.get("line_user_id", "") or "",
                                          key="ns_line_uid",
                                          help="U で始まる33文字のユーザーID")
            if line_user_id:
                if st.button("🔔 LINE テスト送信", key="ns_line_test"):
                    if _send_line_message(line_user_id,
                                          "✅ UGI_DB テスト通知\nこのメッセージが届いていれば設定は正常です。"):
                        st.success("✅ LINE 送信成功！LINEアプリを確認してください")
                    else:
                        st.error("❌ 送信失敗。ユーザーIDを確認してください")
        else:
            line_user_id = ""
            st.info("LINE 通知を利用するには、管理者が環境変数 `UGI_LINE_CHANNEL_TOKEN` を設定する必要があります")

        st.markdown("")  # spacer

        # --- Email ---
        _smtp_configured = bool(_os.environ.get("UGI_SMTP_HOST", ""))
        email_addr = st.text_input("メールアドレス", value=ns.get("email_address", "") or "", key="ns_email")
        if email_addr and _smtp_configured:
            if st.button("📧 Email テスト送信", key="ns_email_test"):
                _test_email_sent = False
                try:
                    import smtplib
                    from email.mime.text import MIMEText
                    smtp_host = _os.environ.get("UGI_SMTP_HOST", "")
                    smtp_port = int(_os.environ.get("UGI_SMTP_PORT", "587"))
                    smtp_user = _os.environ.get("UGI_SMTP_USER", "")
                    smtp_pass = _os.environ.get("UGI_SMTP_PASS", "")
                    from_addr = _os.environ.get("UGI_SMTP_FROM", smtp_user)
                    msg = MIMEText(
                        "これは UGI_DB からのテスト通知です。\n"
                        "このメールが届いていれば設定は正常です。",
                        "plain", "utf-8")
                    msg["Subject"] = "[UGI-DB] テスト通知"
                    msg["From"] = from_addr
                    msg["To"] = email_addr
                    with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as sv:
                        sv.starttls()
                        sv.login(smtp_user, smtp_pass)
                        sv.send_message(msg)
                    _test_email_sent = True
                except Exception as e:
                    st.error(f"❌ Email 送信失敗: {e}")
                if _test_email_sent:
                    st.success("✅ Email 送信成功！受信トレイを確認してください")
        elif email_addr and not _smtp_configured:
            st.info("Email 通知を利用するには、管理者が SMTP 環境変数を設定する必要があります")

    with nc2:
        st.markdown("**通知を受け取る条件**")
        nf_p1_deadline = st.checkbox("Phase 1 期限警告", value=ns.get("notify_phase1_deadline", 1) == 1, key="ns_p1d")
        nf_p1_approval = st.checkbox("Phase 1 承認期限警告", value=ns.get("notify_phase1_approval_deadline", 1) == 1, key="ns_p1a")
        nf_p3_deadline = st.checkbox("Phase 3 期限警告", value=ns.get("notify_phase3_deadline", 0) == 1, key="ns_p3d")
        nf_p4_deadline = st.checkbox("Phase 4 期限警告", value=ns.get("notify_phase4_deadline", 0) == 1, key="ns_p4d")
        nf_returned = st.checkbox("症例が差し戻された", value=ns.get("notify_case_returned", 1) == 1, key="ns_ret")
        nf_approved = st.checkbox("症例が承認された", value=ns.get("notify_case_approved", 0) == 1, key="ns_appr")
        nf_submitted = st.checkbox("症例が提出された（確認者向け）", value=ns.get("notify_case_submitted", 0) == 1, key="ns_sub")

    if st.button("通知設定を保存", type="primary"):
        with get_db() as conn:
            conn.execute("""
                INSERT INTO notification_settings
                    (user_id, line_user_id, email_address,
                     enable_app_notification,
                     notify_phase1_deadline, notify_phase1_approval_deadline,
                     notify_phase3_deadline, notify_phase4_deadline,
                     notify_case_returned, notify_case_approved, notify_case_submitted, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id) DO UPDATE SET
                    line_user_id=excluded.line_user_id,
                    email_address=excluded.email_address,
                    enable_app_notification=excluded.enable_app_notification,
                    notify_phase1_deadline=excluded.notify_phase1_deadline,
                    notify_phase1_approval_deadline=excluded.notify_phase1_approval_deadline,
                    notify_phase3_deadline=excluded.notify_phase3_deadline,
                    notify_phase4_deadline=excluded.notify_phase4_deadline,
                    notify_case_returned=excluded.notify_case_returned,
                    notify_case_approved=excluded.notify_case_approved,
                    notify_case_submitted=excluded.notify_case_submitted,
                    updated_at=CURRENT_TIMESTAMP
            """, (user_id, line_user_id or None,
                  email_addr or None, int(enable_app),
                  int(nf_p1_deadline), int(nf_p1_approval),
                  int(nf_p3_deadline), int(nf_p4_deadline),
                  int(nf_returned), int(nf_approved), int(nf_submitted)))
        st.success("✅ 通知設定を保存しました")

    # --- LINE ユーザーID 確認手順 ---
    st.markdown("---")
    with st.expander("📖 LINE ユーザーID の確認方法"):
        st.markdown("""
**LINE ユーザーID とは？**

LINE Messaging API で個人にメッセージを送るために必要な識別子です。
`U` で始まる33文字の英数字（例: `U1234567890abcdef1234567890abcde`）で、
LINE の表示名やアカウント名とは異なります。

---

**確認方法：**

1. **[LINE Developers](https://developers.line.biz/console/)** にログイン
2. 対象チャネル（マーゲングループ）をクリック
3. 「**チャネル基本設定**」タブの一番下にある「**あなたのユーザーID**」をコピー
4. 上のフィールドに貼り付けて保存してください

---

**他のメンバーのユーザーIDを確認するには：**

LINE Developers の「あなたのユーザーID」はチャネル作成者のIDのみ表示されます。
他メンバーのIDは、Bot に何かメッセージを送ってもらい、
Webhook ログから取得する必要があります。
管理者が各メンバーのIDを確認し、配布してください。

---

**注意事項:**
- 無料プラン（コミュニケーションプラン）で **月200通まで** 送信可能です
- チームで利用する場合、人数 × 通知頻度が200通/月を超えないか確認してください
""")


# ============================================================
# 自然言語クエリ
# ============================================================
def smart_query_page():
    st.markdown("## 🤖 自然言語クエリ")
    st.caption("日本語で質問すると、ローカルLLM がデータベースを検索して回答します。")

    # LLM接続状態
    ok, msg = check_llm_connection()
    if not ok:
        st.error(f"⚠️ {msg}")
        st.markdown("""
**セットアップ方法:**

1. [Ollama](https://ollama.com/) をインストール
2. ターミナルで `ollama pull qwen2.5:7b` を実行（モデルは環境変数 `UGI_LLM_MODEL` で変更可）
3. Ollama が起動していれば自動的に接続されます

**環境変数（任意）:**
- `UGI_LLM_BACKEND`: `ollama`（デフォルト）または `openai_compat`（vLLM等）
- `UGI_LLM_URL`: LLMサーバーURL（デフォルト: `http://localhost:11434`）
- `UGI_LLM_MODEL`: モデル名（デフォルト: `qwen2.5:7b`）
""")
        return

    st.success(f"✅ {msg}")

    # --- 質問例ボタン → session_state に保存して rerun ---
    st.markdown("#### 質問例（クリックで実行）")
    cols = st.columns(2)
    for i, q in enumerate(EXAMPLE_QUESTIONS):
        with cols[i % 2]:
            if st.button(q, key=f"eq_{i}", use_container_width=True):
                st.session_state["_smart_auto_run"] = q
                st.rerun()

    st.markdown("---")

    # 自動実行フラグを取得（rerun 後に実行される）
    auto_run_q = st.session_state.pop("_smart_auto_run", None)

    # 質問入力欄（auto_run時はその質問を表示）
    question = st.text_area(
        "質問を入力してください",
        value=auto_run_q or "",
        height=80,
        placeholder="例: 昨年1年間のロボット手術の症例数と合併症率は？",
        key="smart_q_input",
    )

    col1, col2 = st.columns([1, 5])
    with col1:
        run = st.button("🔍 検索", type="primary", use_container_width=True)
    with col2:
        show_sql = st.checkbox("生成されたSQLを表示", value=False)

    # auto_run（質問例クリック後） or 検索ボタン で実行
    run_question = auto_run_q or (question.strip() if run else None)

    if run_question:
        with st.spinner("LLM でクエリを生成・実行中..."):
            result = smart_ask(run_question.strip())

        if result["success"]:
            if show_sql and result["sql"]:
                st.code(result["sql"], language="sql")

            st.markdown(f"**{result['row_count']} 件の結果**")
            st.dataframe(result["dataframe"], use_container_width=True, hide_index=True)

            # CSV ダウンロード
            csv = result["dataframe"].to_csv(index=False).encode("utf-8-sig")
            st.download_button("📥 CSV ダウンロード", csv,
                               file_name="query_result.csv", mime="text/csv")
        else:
            st.error(f"❌ {result['error']}")
            if result["sql"]:
                st.code(result["sql"], language="sql")
                st.caption("↑ 生成されたSQL（エラーの原因特定用）")

    elif run:
        st.warning("質問を入力してください。")


# ============================================================
# メインルーティング
# ============================================================
def main():
    # 暗号化キー未設定警告（管理者向け）
    if not os.environ.get("UGI_DB_ENCRYPTION_KEY"):
        st.warning(
            "⚠️ 暗号化キー（UGI_DB_ENCRYPTION_KEY）が未設定です。"
            "患者IDや生年月日が平文で保存されます。"
            "本番運用前に `python database.py` で鍵を生成し、環境変数に設定してください。",
            icon="🔑"
        )

    if "user" not in st.session_state:
        login_page()
        return

    page = sidebar()

    if page == "📋 症例一覧":
        case_list_page()
    elif page == "➕ 新規登録":
        case_entry_page()
    elif page == "📊 進捗確認":
        progress_page()
    elif page == "📈 サマリー分析":
        summary_analysis_page()
    elif page == "📊 統計解析":
        statistical_analysis_standalone_page()
    elif page == "🔍 データ探索":
        data_explore_standalone_page()
    elif page == "🤖 自然言語クエリ":
        smart_query_page()
    elif page == "📤 データエクスポート":
        export_page()
    elif page == "🩸 検査値読取":
        lab_reader_page()
    elif page == "📜 監査ログ":
        audit_page()
    elif page == "🔔 通知":
        notification_page()
    elif page == "⚙️ マイページ":
        my_page()
    elif page == "🗑️ データ管理":
        data_management_page()
    elif page == "👥 ユーザー管理":
        user_management_page()


if __name__ == "__main__":
    main()
