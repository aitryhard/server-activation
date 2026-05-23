import os
import uuid
import threading
import time

import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel

load_dotenv()

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_ADMIN_ID = os.getenv("TELEGRAM_ADMIN_ID")


def get_db():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE,
            email TEXT UNIQUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            device_id TEXT UNIQUE NOT NULL,
            request_id TEXT NOT NULL,
            platform TEXT,
            app_version TEXT,
            username TEXT,
            approved BOOLEAN DEFAULT FALSE,
            denied BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            approved_at TIMESTAMPTZ,
            denied_at TIMESTAMPTZ
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) UNIQUE,
            tier TEXT NOT NULL DEFAULT 'free',
            started_at TIMESTAMPTZ DEFAULT NOW(),
            expires_at TIMESTAMPTZ,
            is_active BOOLEAN DEFAULT TRUE
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


class ActivationRequest(BaseModel):
    deviceId: str
    appVersion: str | None = None
    platform: str | None = None
    username: str | None = None


def send_telegram_activation_request(device: dict):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_ADMIN_ID:
        return

    text = (
        "🔐 Новый запрос активации Aivex\n\n"
        f"Device ID: {device['device_id']}\n"
        f"User: {device.get('username', '—')}\n"
        f"Version: {device.get('app_version', '—')}\n"
        f"Platform: {device.get('platform', '—')}"
    )

    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={
            "chat_id": TELEGRAM_ADMIN_ID,
            "text": text,
            "reply_markup": {
                "inline_keyboard": [
                    [
                        {
                            "text": "✅ Approve",
                            "callback_data": f"approve:{device['request_id']}",
                        },
                        {
                            "text": "❌ Deny",
                            "callback_data": f"deny:{device['request_id']}",
                        },
                    ]
                ]
            },
        },
    )


def telegram_polling():
    if not TELEGRAM_BOT_TOKEN:
        return

    offset = None

    while True:
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"timeout": 30, "offset": offset},
                timeout=35,
            )
            data = resp.json()

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                callback = update.get("callback_query")
                if not callback:
                    continue

                action, request_id = callback["data"].split(":", 1)
                conn = get_db()
                cur = conn.cursor(cursor_factory=RealDictCursor)

                cur.execute(
                    "SELECT * FROM devices WHERE request_id = %s", (request_id,)
                )
                device = cur.fetchone()

                if not device:
                    cur.close()
                    conn.close()
                    continue

                if action == "approve":
                    cur.execute(
                        "UPDATE devices SET approved = TRUE, denied = FALSE, approved_at = NOW() WHERE request_id = %s",
                        (request_id,),
                    )
                    cur.execute(
                        """INSERT INTO users (telegram_id) VALUES (%s)
                           ON CONFLICT (telegram_id) DO UPDATE SET updated_at = NOW()""",
                        (TELEGRAM_ADMIN_ID,),
                    )
                    cur.execute(
                        "UPDATE devices SET user_id = (SELECT id FROM users WHERE telegram_id = %s) WHERE request_id = %s",
                        (TELEGRAM_ADMIN_ID, request_id),
                    )
                    answer_text = "✅ Устройство одобрено"
                elif action == "deny":
                    cur.execute(
                        "UPDATE devices SET denied = TRUE, approved = FALSE, denied_at = NOW() WHERE request_id = %s",
                        (request_id,),
                    )
                    answer_text = "❌ Устройство отклонено"
                else:
                    cur.close()
                    conn.close()
                    continue

                conn.commit()
                cur.close()
                conn.close()

                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                    json={"callback_query_id": callback["id"], "text": answer_text},
                )
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id": TELEGRAM_ADMIN_ID, "text": answer_text},
                )

        except Exception as e:
            print("Telegram polling error:", e)
            time.sleep(3)


# ─── API ───────────────────────────────────────────────────────────────


@app.on_event("startup")
def startup():
    init_db()
    thread = threading.Thread(target=telegram_polling, daemon=True)
    thread.start()


@app.get("/")
def home():
    return {"status": "activation server works"}


@app.post("/request-access")
def request_access(data: ActivationRequest):
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT * FROM devices WHERE device_id = %s", (data.deviceId,))
    existing = cur.fetchone()

    if existing:
        cur.close()
        conn.close()
        return {"ok": True, "status": "already_requested"}

    request_id = uuid.uuid4().hex[:12]
    cur.execute(
        """INSERT INTO devices (device_id, request_id, platform, app_version, username)
           VALUES (%s, %s, %s, %s, %s)""",
        (data.deviceId, request_id, data.platform, data.appVersion, data.username),
    )
    conn.commit()

    cur.execute("SELECT * FROM devices WHERE device_id = %s", (data.deviceId,))
    device = cur.fetchone()
    cur.close()
    conn.close()

    send_telegram_activation_request(device)
    return {"ok": True, "status": "pending"}


@app.post("/check-access")
def check_access(data: ActivationRequest):
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM devices WHERE device_id = %s", (data.deviceId,))
    device = cur.fetchone()
    cur.close()
    conn.close()

    if not device:
        return {"allowed": False, "status": "not_requested"}

    if device["denied"]:
        return {"allowed": False, "status": "denied"}

    if device["approved"]:
        return {"allowed": True, "status": "approved"}

    return {"allowed": False, "status": "pending"}


@app.post("/admin/approve/{device_id}")
def approve_device(device_id: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE devices SET approved = TRUE, denied = FALSE, approved_at = NOW() WHERE device_id = %s",
        (device_id,),
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"ok": True, "status": "approved"}


@app.post("/admin/deny/{device_id}")
def deny_device(device_id: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE devices SET denied = TRUE, approved = FALSE, denied_at = NOW() WHERE device_id = %s",
        (device_id,),
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"ok": True, "status": "denied"}


# ─── SUBSCRIPTION API ──────────────────────────────────────────────────


class SubscriptionCreate(BaseModel):
    user_id: int
    tier: str
    expires_at: str | None = None


@app.get("/subscription/by-device/{device_id}")
def get_subscription_by_device(device_id: str):
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT user_id FROM devices WHERE device_id = %s", (device_id,))
    device = cur.fetchone()

    if not device or not device["user_id"]:
        cur.close()
        conn.close()
        return {"tier": "free", "is_active": False}

    cur.execute("SELECT * FROM subscriptions WHERE user_id = %s", (device["user_id"],))
    sub = cur.fetchone()
    cur.close()
    conn.close()

    if not sub:
        return {"tier": "free", "is_active": False}

    return {
        "tier": sub["tier"],
        "started_at": sub["started_at"].isoformat() if sub["started_at"] else None,
        "expires_at": sub["expires_at"].isoformat() if sub["expires_at"] else None,
        "is_active": sub["is_active"],
    }


@app.get("/subscription/{user_id}")
def get_subscription(user_id: int):
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM subscriptions WHERE user_id = %s", (user_id,))
    sub = cur.fetchone()
    cur.close()
    conn.close()

    if not sub:
        return {"tier": "free", "is_active": False}

    return {
        "tier": sub["tier"],
        "started_at": sub["started_at"].isoformat() if sub["started_at"] else None,
        "expires_at": sub["expires_at"].isoformat() if sub["expires_at"] else None,
        "is_active": sub["is_active"],
    }


@app.post("/subscription/create")
def create_subscription(data: SubscriptionCreate):
    conn = get_db()
    cur = conn.cursor()
    expires = data.expires_at if data.expires_at else None
    cur.execute(
        """INSERT INTO subscriptions (user_id, tier, expires_at)
           VALUES (%s, %s, %s)
           ON CONFLICT (user_id) DO UPDATE SET tier = EXCLUDED.tier, expires_at = EXCLUDED.expires_at, is_active = TRUE""",
        (data.user_id, data.tier, expires),
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"ok": True, "tier": data.tier}
