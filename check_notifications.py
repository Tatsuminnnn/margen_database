#!/usr/bin/env python3
"""
check_notifications.py — バックグラウンド通知チェッカー
======================================================
cron で定期実行し、期限ベースの通知（条件 1〜4）を送信するスクリプト。

cron 設定例（毎朝 8:00 に実行）:
  0 8 * * * cd /path/to/ugi_db && /path/to/conda/envs/ugi_db/bin/python check_notifications.py

条件一覧:
  1. 初診日から6ヶ月経過 → Phase 1 が未提出
  2. 退院後1ヶ月経過     → Phase 1 が未提出
  3. 退院後3ヶ月経過     → Phase 1 が未承認
  4. 術後5年6ヶ月経過    → Phase 2 が未承認
"""

import sqlite3
import json
import logging
import os
import sys
from datetime import datetime, date, timedelta

# ---------------------------------------------------------------------------
# ログ設定
# ---------------------------------------------------------------------------
LOG_DIR = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "notification_cron.log")),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DB パス
# ---------------------------------------------------------------------------
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ugi_registry.db")

# ---------------------------------------------------------------------------
# 通知送信ヘルパー
# ---------------------------------------------------------------------------

_LINE_CHANNEL_TOKEN = os.environ.get("UGI_LINE_CHANNEL_TOKEN", "")


def send_line_message(user_id_line: str, message: str) -> bool:
    """LINE Messaging API でプッシュメッセージを送信。成功なら True。
    チャネルアクセストークンは環境変数 UGI_LINE_CHANNEL_TOKEN から取得。"""
    if not _LINE_CHANNEL_TOKEN:
        logger.debug("LINE チャネルトークン未設定のためスキップ")
        return False
    try:
        import requests
        resp = requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {_LINE_CHANNEL_TOKEN}",
            },
            json={
                "to": user_id_line,
                "messages": [{"type": "text", "text": message}],
            },
            timeout=10,
        )
        return resp.status_code == 200
    except Exception as e:
        logger.warning(f"LINE Messaging API 送信失敗: {e}")
        return False


def send_email_notify(to_addr: str, subject: str, body: str) -> bool:
    """Email でメッセージを送信（SMTP 設定は環境変数から）。"""
    smtp_host = os.environ.get("UGI_SMTP_HOST", "")
    smtp_port = int(os.environ.get("UGI_SMTP_PORT", "587"))
    smtp_user = os.environ.get("UGI_SMTP_USER", "")
    smtp_pass = os.environ.get("UGI_SMTP_PASS", "")
    from_addr = os.environ.get("UGI_SMTP_FROM", smtp_user)

    if not smtp_host or not smtp_user:
        logger.debug("SMTP 未設定のためメール送信スキップ")
        return False

    try:
        import smtplib
        from email.mime.text import MIMEText

        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = to_addr

        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as sv:
            sv.starttls()
            sv.login(smtp_user, smtp_pass)
            sv.send_message(msg)
        return True
    except Exception as e:
        logger.warning(f"Email 送信失敗 ({to_addr}): {e}")
        return False


def create_app_notification(conn, user_id: int, title: str,
                            message: str = "", link_study_id: str = ""):
    """アプリ内通知を作成。"""
    conn.execute(
        "INSERT INTO notifications (user_id, title, message, link_study_id) "
        "VALUES (?, ?, ?, ?)",
        (user_id, title, message, link_study_id),
    )


# ---------------------------------------------------------------------------
# 重複送信防止
# ---------------------------------------------------------------------------

def _notification_sent_today(conn, user_id: int, condition_no: int,
                             patient_id: int) -> bool:
    """今日すでに同じ条件で通知済みかチェック。audit_log を利用。"""
    today = date.today().isoformat()
    row = conn.execute(
        """SELECT COUNT(*) FROM audit_log
           WHERE user_id = ? AND action = 'CRON_NOTIFY'
             AND comment = ? AND record_id = ?
             AND DATE(timestamp) = ?""",
        (user_id, f"condition_{condition_no}", patient_id, today),
    ).fetchone()
    return (row[0] or 0) > 0


def _log_notification(conn, user_id: int, condition_no: int,
                      patient_id: int, method: str):
    """送信ログを audit_log に記録。"""
    conn.execute(
        """INSERT INTO audit_log (user_id, action, table_name, record_id,
                                  comment, field_name, timestamp)
           VALUES (?, 'CRON_NOTIFY', 'patients', ?, ?, ?, CURRENT_TIMESTAMP)""",
        (user_id, patient_id, f"condition_{condition_no}", method),
    )


# ---------------------------------------------------------------------------
# 通知条件チェック
# ---------------------------------------------------------------------------

def check_condition_1(conn, today: date) -> list:
    """条件1: 初診日から6ヶ月経過 + Phase 1 未提出 (draft)"""
    cutoff = (today - timedelta(days=183)).isoformat()  # 約6ヶ月
    rows = conn.execute(
        """SELECT id, study_id, first_visit_date, created_by
           FROM patients
           WHERE first_visit_date IS NOT NULL
             AND first_visit_date <= ?
             AND phase1_status = 'draft'""",
        (cutoff,),
    ).fetchall()
    return [(r[0], r[1], r[3], 1,
             f"⏰ Phase 1 未提出: {r[1]} — 初診日({r[2]})から6ヶ月経過")
            for r in rows]


def check_condition_2(conn, today: date) -> list:
    """条件2: 退院後1ヶ月経過 + Phase 1 未提出 (draft)"""
    cutoff = (today - timedelta(days=30)).isoformat()
    rows = conn.execute(
        """SELECT id, study_id, discharge_date, created_by
           FROM patients
           WHERE discharge_date IS NOT NULL
             AND discharge_date <= ?
             AND phase1_status = 'draft'""",
        (cutoff,),
    ).fetchall()
    return [(r[0], r[1], r[3], 2,
             f"⏰ Phase 1 未提出: {r[1]} — 退院日({r[2]})から1ヶ月経過")
            for r in rows]


def check_condition_3(conn, today: date) -> list:
    """条件3: 退院後3ヶ月経過 + Phase 1 未承認 (draft or submitted)"""
    cutoff = (today - timedelta(days=90)).isoformat()
    rows = conn.execute(
        """SELECT id, study_id, discharge_date, created_by
           FROM patients
           WHERE discharge_date IS NOT NULL
             AND discharge_date <= ?
             AND phase1_status != 'approved'""",
        (cutoff,),
    ).fetchall()
    return [(r[0], r[1], r[3], 3,
             f"⚠️ Phase 1 未承認: {r[1]} — 退院日({r[2]})から3ヶ月経過")
            for r in rows]


def check_condition_4(conn, today: date) -> list:
    """条件4: 術後5年6ヶ月経過 + Phase 2 未承認"""
    cutoff = (today - timedelta(days=365 * 5 + 183)).isoformat()  # 約5年6ヶ月
    rows = conn.execute(
        """SELECT id, study_id, surgery_date, created_by
           FROM patients
           WHERE surgery_date IS NOT NULL
             AND surgery_date <= ?
             AND phase2_status != 'approved'""",
        (cutoff,),
    ).fetchall()
    return [(r[0], r[1], r[3], 4,
             f"⚠️ Phase 2 未承認: {r[1]} — 術後({r[2]})5年6ヶ月経過")
            for r in rows]


# ---------------------------------------------------------------------------
# 通知送信メイン
# ---------------------------------------------------------------------------

def dispatch_notification(conn, user_id: int, patient_id: int,
                          condition_no: int, message: str, study_id: str):
    """ユーザーの通知設定に基づいて各チャネルで送信。"""
    # 重複チェック
    if _notification_sent_today(conn, user_id, condition_no, patient_id):
        return

    # 通知設定を取得
    ns = conn.execute(
        "SELECT * FROM notification_settings WHERE user_id = ?",
        (user_id,),
    ).fetchone()

    # 設定なし → アプリ内通知のみ（デフォルト）
    if not ns:
        create_app_notification(conn, user_id, message, study_id=study_id)
        _log_notification(conn, user_id, condition_no, patient_id, "app")
        return

    # カラム名取得
    cols = [desc[0] for desc in conn.execute(
        "SELECT * FROM notification_settings LIMIT 0"
    ).description]
    settings = dict(zip(cols, ns))

    # 条件別の通知フラグ確認
    flag_map = {
        1: "notify_phase1_deadline",
        2: "notify_phase1_deadline",
        3: "notify_phase1_approval_deadline",
        4: "notify_phase2_deadline",
    }
    flag_col = flag_map.get(condition_no)
    if flag_col and not settings.get(flag_col, 0):
        return  # ユーザーがこの条件の通知を無効化している

    methods = []

    # アプリ内通知
    if settings.get("enable_app_notification", 1):
        create_app_notification(conn, user_id, message,
                                link_study_id=study_id)
        methods.append("app")

    # LINE Messaging API
    line_uid = settings.get("line_user_id", "")
    if line_uid:
        if send_line_message(line_uid, message):
            methods.append("line")

    # Email
    email = settings.get("email_address", "")
    if email:
        subject = f"[UGI-DB] 通知: {study_id}"
        if send_email_notify(email, subject, message):
            methods.append("email")

    if methods:
        _log_notification(conn, user_id, condition_no, patient_id,
                          ",".join(methods))
        logger.info(f"  → user={user_id} cond={condition_no} "
                    f"patient={patient_id} methods={methods}")


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main():
    if not os.path.exists(DB_PATH):
        logger.error(f"DB ファイルが見つかりません: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    today = date.today()
    logger.info(f"=== 通知チェック開始 ({today.isoformat()}) ===")

    total_sent = 0

    for check_fn in [check_condition_1, check_condition_2,
                     check_condition_3, check_condition_4]:
        hits = check_fn(conn, today)
        logger.info(f"{check_fn.__name__}: {len(hits)} 件該当")
        for patient_id, study_id, created_by, cond_no, msg in hits:
            if created_by:
                dispatch_notification(conn, created_by, patient_id,
                                      cond_no, msg, study_id)
                total_sent += 1

    conn.commit()
    conn.close()
    logger.info(f"=== 通知チェック完了: {total_sent} 件処理 ===")


if __name__ == "__main__":
    main()
