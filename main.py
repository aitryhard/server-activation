import os
import json
import uuid
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, Depends, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from openai import OpenAI

load_dotenv()

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_ADMIN_ID = os.getenv("TELEGRAM_ADMIN_ID")
ADMIN_API_TOKEN = os.getenv("ADMIN_API_TOKEN")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")

openai_client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
) if OPENROUTER_API_KEY else None

class RateLimiter:
    def __init__(self, default_limit=30, window=60):
        self.limit = default_limit
        self.window = window
        self._buckets: dict[str, list[float]] = defaultdict(list)
        self._lock = threading.Lock()

    def check(self, key: str) -> bool:
        now = time.time()
        with self._lock:
            bucket = self._buckets[key]
            cutoff = now - self.window
            bucket[:] = [t for t in bucket if t > cutoff]
            if len(bucket) >= self.limit:
                return False
            bucket.append(now)
            return True

chat_limiter = RateLimiter(default_limit=60)

# ожидание ввода длительности подписки: {chat_id: {"uid": int, "tier": str}}
_pending_duration = {}

def verify_admin_token(authorization: str | None = Header(None)):
    if not ADMIN_API_TOKEN:
        raise HTTPException(status_code=503, detail="Admin API not configured")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid token")
    token = authorization.removeprefix("Bearer ")
    if token != ADMIN_API_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")

def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def require_db():
    try:
        conn = get_db()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database not available: {e}")


def init_db():
    if not DATABASE_URL:
        print("WARNING: DATABASE_URL not set, DB not available")
        return
    try:
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
        print("Database initialized successfully")

        # Migration: split shared users — each device gets its own user
        conn = get_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """SELECT d.user_id, COUNT(*) as cnt
               FROM devices d
               WHERE d.user_id IS NOT NULL
               GROUP BY d.user_id
               HAVING COUNT(*) > 1"""
        )
        shared = cur.fetchall()
        for row in shared:
            old_user_id = row["user_id"]
            cur.execute("SELECT * FROM devices WHERE user_id = %s ORDER BY id", (old_user_id,))
            devices = cur.fetchall()
            for i, dev in enumerate(devices):
                if i == 0:
                    continue  # keep first device on the original user
                new_email = dev["device_id"]
                cur.execute(
                    """INSERT INTO users (email) VALUES (%s) ON CONFLICT DO NOTHING""",
                    (new_email,),
                )
                cur.execute("SELECT id FROM users WHERE email = %s", (new_email,))
                new_user = cur.fetchone()
                if new_user:
                    cur.execute("UPDATE devices SET user_id = %s WHERE id = %s", (new_user["id"], dev["id"]))
                    # copy subscription if exists
                    cur.execute("SELECT * FROM subscriptions WHERE user_id = %s", (old_user_id,))
                    old_sub = cur.fetchone()
                    if old_sub:
                        cur.execute(
                            """INSERT INTO subscriptions (user_id, tier, expires_at, is_active)
                               VALUES (%s, %s, %s, %s)
                               ON CONFLICT (user_id) DO NOTHING""",
                            (new_user["id"], old_sub["tier"], old_sub["expires_at"], old_sub["is_active"]),
                        )
        conn.commit()
        cur.close()
        conn.close()
        if shared:
            print(f"Migration: split {len(shared)} shared user(s) into individual device users")
    except Exception as e:
        print(f"ERROR: init_db failed: {e}")


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

                # ── handle text commands ──
                msg = update.get("message")
                if msg and msg.get("text"):
                    chat_id = msg["chat"]["id"]
                    text = msg["text"].strip()

                    if not TELEGRAM_ADMIN_ID or str(chat_id) == str(TELEGRAM_ADMIN_ID):
                        admin = True
                    else:
                        admin = False
                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={"chat_id": chat_id, "text": "⛔ У вас нет прав."},
                        )
                        continue

                    def send(text, **kw):
                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown", **kw},
                        )

                    def menu_keyboard():
                        return {
                            "inline_keyboard": [
                                [{"text": "👥 Пользователи", "callback_data": "menu_users"},
                                 {"text": "💻 Устройства", "callback_data": "menu_devices"}],
                                [{"text": "❓ Помощь", "callback_data": "menu_help"}],
                            ]
                        }

                    # ── handle pending duration input ──
                    if chat_id in _pending_duration:
                        if text.startswith("/"):
                            _pending_duration.pop(chat_id)
                            send("⏸ Ввод срока отменён.")
                            continue
                        pending = _pending_duration.pop(chat_id)
                        uid = pending["uid"]
                        tier = pending["tier"]
                        interval_str = text.strip("\"'")
                        try:
                            conn = get_db()
                            cur = conn.cursor()
                            cur.execute(
                                """INSERT INTO subscriptions (user_id, tier, expires_at)
                                   VALUES (%s, %s, NOW() + %s::interval)
                                   ON CONFLICT (user_id) DO UPDATE SET tier = EXCLUDED.tier, expires_at = EXCLUDED.expires_at, is_active = TRUE""",
                                (uid, tier, interval_str),
                            )
                            conn.commit()
                            cur.close()
                            conn.close()
                            send(f"✅ Пользователь #{uid} → *{tier}* на `{interval_str}`")
                        except Exception as e:
                            send(f"Ошибка: {e}")
                        continue

                    if text in ("/start", "/help", "/menu"):
                        send(
                            "*Aivex Bot*\n\n"
                            "Управляй подписками и устройствами 👇",
                            reply_markup=menu_keyboard(),
                        )
                        continue

                    try:
                        conn = get_db()
                    except Exception as e:
                        send(f"Ошибка БД: {e}")
                        continue

                    if text == "/resetall":
                        cur = conn.cursor()
                        cur.execute("DELETE FROM subscriptions")
                        cur.execute("DELETE FROM devices")
                        cur.execute("DELETE FROM users")
                        conn.commit()
                        cur.close()
                        send("✅ Все данные удалены.")
                        continue
                    elif text == "/users":
                        cur = conn.cursor(cursor_factory=RealDictCursor)
                        cur.execute(
                            """SELECT u.*,
                                      (SELECT COUNT(*) FROM devices d WHERE d.user_id = u.id AND d.approved) AS devices_count,
                                      (SELECT d.username FROM devices d WHERE d.user_id = u.id ORDER BY d.created_at DESC LIMIT 1) AS device_username,
                                      (SELECT d.device_id FROM devices d WHERE d.user_id = u.id ORDER BY d.created_at DESC LIMIT 1) AS device_id,
                                      s.tier
                               FROM users u
                               LEFT JOIN subscriptions s ON s.user_id = u.id
                               ORDER BY u.created_at DESC"""
                        )
                        rows = cur.fetchall()
                        cur.close()
                        conn.close()

                        if not rows:
                            send("Нет пользователей.")
                        else:
                            lines = []
                            for i, r in enumerate(rows, 1):
                                email = r["email"] or "-"
                                username = r["device_username"] or "-"
                                did = (r["device_id"] or "")[:8]
                                tier = r["tier"] or "free"
                                devices = r["devices_count"] or 0
                                lines.append(
                                    f"{i}. #{r['id']} | {username} | `{did}...`\n"
                                    f"   📱 {devices} | 💳 {tier}"
                                )
                            send(
                                f"*👥 Пользователи*\n\n" + "\n\n".join(lines),
                                reply_markup={
                                    "inline_keyboard": [
                                        [{"text": "🔙 Назад", "callback_data": "menu_back"}],
                                    ]
                                },
                            )

                    elif text == "/devices":
                        cur = conn.cursor(cursor_factory=RealDictCursor)
                        cur.execute(
                            """SELECT d.id, d.device_id, d.platform, d.app_version,
                                      d.username, d.approved, d.denied, d.created_at,
                                      u.email
                               FROM devices d
                               LEFT JOIN users u ON u.id = d.user_id
                               ORDER BY d.created_at DESC"""
                        )
                        rows = cur.fetchall()
                        cur.close()
                        conn.close()

                        if not rows:
                            send("Нет устройств.")
                        else:
                            lines = []
                            for r in rows:
                                status = "✅" if r["approved"] else ("❌" if r["denied"] else "⏳")
                                user = r["email"] or "-"
                                pid = r["device_id"][:8] if r["device_id"] else "???"
                                lines.append(f"{status} `{pid}...` | {user} | {r['platform'] or '-'}")
                            send(
                                f"*💻 Устройства*\n\n" + "\n".join(lines),
                                reply_markup={
                                    "inline_keyboard": [
                                        [{"text": "🔙 Назад", "callback_data": "menu_back"}],
                                    ]
                                },
                            )

                    elif text.startswith("/sub") and len(text.split()) == 2:
                        try:
                            uid = int(text.split()[1])
                            cur = conn.cursor(cursor_factory=RealDictCursor)
                            cur.execute("SELECT * FROM subscriptions WHERE user_id = %s", (uid,))
                            sub = cur.fetchone()
                            cur.close()
                            conn.close()
                            if sub:
                                tier = sub["tier"]
                                active = "✅" if sub["is_active"] else "❌"
                                started = sub["started_at"].isoformat()[:10] if sub["started_at"] else "-"
                                expires = sub["expires_at"].isoformat()[:10] if sub["expires_at"] else "бессрочно"
                                send(
                                    f"*👤 Пользователь #{uid}*\n\n"
                                    f"💳 Тариф: `{tier}`\n"
                                    f"✅ Активна: {active}\n"
                                    f"📅 С: {started}\n"
                                    f"⏳ До: {expires}",
                                    reply_markup={
                                        "inline_keyboard": [
                                            [{"text": "✏️ Изменить тариф", "callback_data": f"settier_{uid}"}],
                                            [{"text": "🔙 Назад", "callback_data": "menu_back"}],
                                        ]
                                    },
                                )
                            else:
                                send(
                                    f"*👤 Пользователь #{uid}*\n\n💳 Тариф: `free`\nУ пользователя нет подписки.",
                                    reply_markup={
                                        "inline_keyboard": [
                                            [{"text": "✏️ Назначить тариф", "callback_data": f"settier_{uid}"}],
                                            [{"text": "🔙 Назад", "callback_data": "menu_back"}],
                                        ]
                                    },
                                )
                        except Exception as e:
                            send(f"Ошибка: {e}")

                    continue

                # ── handle callback queries ──
                callback = update.get("callback_query")
                if not callback:
                    continue

                chat_id = callback["message"]["chat"]["id"]
                cid = callback["id"]
                data = callback["data"]

                def answer(text):
                    requests.post(
                        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                        json={"callback_query_id": cid, "text": text},
                    )

                def edit(text, **kw):
                    resp = requests.post(
                        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText",
                        json={
                            "chat_id": chat_id,
                            "message_id": callback["message"]["message_id"],
                            "text": text,
                            "parse_mode": "Markdown",
                            **kw,
                        },
                    )
                    if not resp.ok:
                        print(f"editMessageText failed: {resp.status_code} {resp.text[:200]}")

                def menu_keyboard():
                    return {
                        "inline_keyboard": [
                            [{"text": "👥 Пользователи", "callback_data": "menu_users"},
                             {"text": "💻 Устройства", "callback_data": "menu_devices"}],
                            [{"text": "❓ Помощь", "callback_data": "menu_help"}],
                        ]
                    }

                # ── menu navigation ──
                if data == "menu_back":
                    edit("*Aivex Bot*\n\nУправляй подписками и устройствами 👇", reply_markup=menu_keyboard())
                    answer("Меню")

                elif data == "menu_help":
                    edit(
                        "*Aivex Bot*\n\n"
                        "`/users` — список пользователей\n"
                        "`/devices` — список устройств\n"
                        "`/sub <id>` — информация о подписке\n"
                        "`/settier <id> <tier> [срок]` — назначить тариф\n"
                        "`/resetall` — удалить все данные\n\n"
                        "Пример: `/settier 5 premium \"30 days\"`\n"
                        "Срок: `\"1 hour\"`, `\"7 days\"`, `\"90 days\"`",
                        reply_markup={"inline_keyboard": [[{"text": "🔙 Назад", "callback_data": "menu_back"}]]},
                    )
                    answer("Помощь")

                elif data == "menu_users":
                    try:
                        conn = get_db()
                        cur = conn.cursor(cursor_factory=RealDictCursor)
                        cur.execute(
                            """SELECT u.*,
                                      (SELECT COUNT(*) FROM devices d WHERE d.user_id = u.id AND d.approved) AS devices_count,
                                      (SELECT d.username FROM devices d WHERE d.user_id = u.id ORDER BY d.created_at DESC LIMIT 1) AS device_username,
                                      (SELECT d.device_id FROM devices d WHERE d.user_id = u.id ORDER BY d.created_at DESC LIMIT 1) AS device_id,
                                      s.tier
                               FROM users u
                               LEFT JOIN subscriptions s ON s.user_id = u.id
                               ORDER BY u.created_at DESC"""
                        )
                        rows = cur.fetchall()
                        cur.close()
                        conn.close()
                    except Exception as e:
                        edit(f"Ошибка БД: {e}")
                        answer("Ошибка")
                        continue

                    if not rows:
                        edit("Нет пользователей.", reply_markup={"inline_keyboard": [[{"text": "🔙 Назад", "callback_data": "menu_back"}]]})
                    else:
                        kb = []
                        for i, r in enumerate(rows[:10], 1):
                            tier = r["tier"] or "free"
                            username = r["device_username"] or "-"
                            did = (r["device_id"] or "")[:8]
                            kb.append([{"text": f"{i}. #{r['id']} | {username} | {did}... | {tier}", "callback_data": f"sub_{r['id']}"}])
                        kb.append([{"text": "🔙 Назад", "callback_data": "menu_back"}])
                        edit("*👥 Пользователи*\n\nНажми на пользователя чтобы управлять 👇", reply_markup={"inline_keyboard": kb})
                    answer(f"{len(rows)} пользователей")

                elif data == "menu_devices":
                    try:
                        conn = get_db()
                        cur = conn.cursor(cursor_factory=RealDictCursor)
                        cur.execute(
                            """SELECT d.id, d.device_id, d.platform, d.app_version,
                                      d.username, d.approved, d.denied, d.created_at,
                                      u.email
                               FROM devices d
                               LEFT JOIN users u ON u.id = d.user_id
                               ORDER BY d.created_at DESC"""
                        )
                        rows = cur.fetchall()
                        cur.close()
                        conn.close()
                    except Exception as e:
                        edit(f"Ошибка БД: {e}")
                        answer("Ошибка")
                        continue

                    if not rows:
                        edit("Нет устройств.", reply_markup={"inline_keyboard": [[{"text": "🔙 Назад", "callback_data": "menu_back"}]]})
                    else:
                        kb = []
                        for r in rows[:15]:
                            status = "✅" if r["approved"] else ("❌" if r["denied"] else "⏳")
                            pid = r["device_id"][:8] if r["device_id"] else "???"
                            uname = r["username"] or "-"
                            label = f"{status} {pid}... | {uname}"
                            kb.append([{"text": label, "callback_data": f"dvc_{r['id']}"}])
                        kb.append([{"text": "🔙 Назад", "callback_data": "menu_back"}])
                        edit("*💻 Устройства*\n\nНажми на устройство чтобы управлять 👇", reply_markup={"inline_keyboard": kb})
                    answer(f"{len(rows)} устройств")

                elif data.startswith("dvc_"):
                    device_db_id = int(data[len("dvc_"):])
                    try:
                        conn = get_db()
                        cur = conn.cursor(cursor_factory=RealDictCursor)
                        cur.execute("SELECT * FROM devices WHERE id = %s", (device_db_id,))
                        dev = cur.fetchone()
                        cur.close()
                        conn.close()
                    except Exception as e:
                        edit(f"Ошибка: {e}")
                        answer("Ошибка")
                        continue

                    if not dev:
                        edit("Устройство не найдено", reply_markup={"inline_keyboard": [[{"text": "🔙 Назад", "callback_data": "menu_devices"}]]})
                        answer("Нет")
                        continue

                    status = "✅ Одобрено" if dev["approved"] else ("❌ Заблокировано" if dev["denied"] else "⏳ Ожидает")
                    pid = dev["device_id"][:12] if dev["device_id"] else "???"
                    uname = dev["username"] or "-"
                    plat = dev["platform"] or "-"
                    ver = dev["app_version"] or "-"
                    dev_pid = dev["id"]

                    toggle_btn = []
                    if dev["approved"] or dev["denied"]:
                        toggle_btn.append({"text": "🔓 Вернуть доступ", "callback_data": f"tog_{dev_pid}"})
                    if dev["approved"]:
                        toggle_btn.append({"text": "🔒 Заблокировать", "callback_data": f"blk_{dev_pid}"})
                    if dev["denied"]:
                        toggle_btn.append({"text": "✅ Одобрить", "callback_data": f"apr_{dev_pid}"})

                    kb = [toggle_btn] if toggle_btn else []
                    kb.append([{"text": "🔙 К списку", "callback_data": "menu_devices"}])

                    edit(
                        f"*💻 Устройство*\n\n"
                        f"ID: `{pid}...`\n"
                        f"Пользователь: {uname}\n"
                        f"Статус: {status}\n"
                        f"Платформа: {plat}\n"
                        f"Версия: {ver}\n"
                        f"Создан: {dev['created_at'].isoformat()[:10] if dev['created_at'] else '-'}",
                        reply_markup={"inline_keyboard": kb},
                    )
                    answer(f"Устройство {pid}...")

                elif data.startswith("tog_") or data.startswith("blk_") or data.startswith("apr_"):
                    parts = data.split("_", 1)
                    action = parts[0]
                    dev_pid = int(parts[1])
                    try:
                        conn = get_db()
                        cur = conn.cursor()
                        if action == "tog":
                            cur.execute("UPDATE devices SET approved = TRUE, denied = FALSE WHERE id = %s", (dev_pid,))
                            answer_text = "✅ Доступ возвращён"
                        elif action == "blk":
                            cur.execute("UPDATE devices SET denied = TRUE, approved = FALSE WHERE id = %s", (dev_pid,))
                            answer_text = "🔒 Устройство заблокировано"
                        else:
                            cur.execute("UPDATE devices SET approved = TRUE, denied = FALSE WHERE id = %s", (dev_pid,))
                            answer_text = "✅ Устройство одобрено"
                        conn.commit()
                        cur.close()
                        conn.close()
                    except Exception as e:
                        answer(f"Ошибка: {e}")
                        continue

                    answer(answer_text)
                    # refresh device view
                    edit(
                        f"{answer_text}\n\nОбнови список устройств чтобы увидеть изменения.",
                        reply_markup={"inline_keyboard": [[{"text": "🔄 Обновить", "callback_data": "menu_devices"}], [{"text": "🔙 Назад", "callback_data": "menu_back"}]]},
                    )

                elif data.startswith("sub_"):
                    uid = int(data.split("_")[1])
                    try:
                        conn = get_db()
                        cur = conn.cursor(cursor_factory=RealDictCursor)
                        cur.execute("SELECT * FROM subscriptions WHERE user_id = %s", (uid,))
                        sub = cur.fetchone()
                        cur.execute("SELECT * FROM devices WHERE user_id = %s ORDER BY created_at DESC", (uid,))
                        devices = cur.fetchall()
                        cur.close()
                        conn.close()
                    except Exception as e:
                        edit(f"Ошибка: {e}")
                        answer("Ошибка")
                        continue

                    tier = sub["tier"] if sub else "free"
                    active = "✅" if sub and sub["is_active"] else "❌"
                    started = sub["started_at"].isoformat()[:10] if sub and sub["started_at"] else "-"
                    expires = sub["expires_at"].isoformat()[:10] if sub and sub["expires_at"] else "бессрочно"

                    device_lines = []
                    device_btns = []
                    for d in devices:
                        s = "✅" if d["approved"] else ("❌" if d["denied"] else "⏳")
                        pid = d["device_id"][:8] if d["device_id"] else "???"
                        uname = d["username"] or "-"
                        device_lines.append(f"{s} `{pid}...` | {uname}")
                        device_btns.append([{"text": f"{'✅' if d['approved'] else '❌'} {uname}", "callback_data": f"dvc_{d['id']}"}])

                    text = (
                        f"*👤 Пользователь #{uid}*\n\n"
                        f"💳 Тариф: `{tier}`\n"
                        f"Активна: {active}\n"
                        f"📅 С: {started}\n"
                        f"⏳ До: {expires}"
                    )
                    if device_lines:
                        text += f"\n\n*💻 Устройства:*\n" + "\n".join(device_lines)

                    kb = device_btns + [
                        [{"text": "💎 Pro", "callback_data": f"tier_{uid}_pro"},
                         {"text": "👑 Premium", "callback_data": f"tier_{uid}_premium"}],
                        [{"text": "🆓 Сделать Free", "callback_data": f"dosettier_{uid}_free"}],
                        [{"text": "🔙 Назад", "callback_data": "menu_users"}],
                    ]

                    edit(text, reply_markup={"inline_keyboard": kb})
                    answer(f"Пользователь #{uid}")

                elif data.startswith("tier_"):
                    parts = data.split("_", 2)
                    uid = int(parts[1])
                    tier = parts[2]
                    tier_name = "Pro" if tier == "pro" else "Premium"
                    _pending_duration.pop(chat_id, None)
                    edit(
                        f"*👤 Пользователь #{uid}*\n\nВыбери срок для *{tier_name}*:",
                        reply_markup={
                            "inline_keyboard": [
                                [{"text": "1 день", "callback_data": f"dosettier_{uid}_{tier}_1 day"},
                                 {"text": "7 дней", "callback_data": f"dosettier_{uid}_{tier}_7 days"}],
                                [{"text": "30 дней", "callback_data": f"dosettier_{uid}_{tier}_30 days"},
                                 {"text": "90 дней", "callback_data": f"dosettier_{uid}_{tier}_90 days"}],
                                [{"text": "✏️ Свой срок", "callback_data": f"customdur_{uid}_{tier}"}],
                                [{"text": "🔙 Назад", "callback_data": f"sub_{uid}"}],
                            ]
                        },
                    )
                    answer("")

                elif data.startswith("customdur_"):
                    parts = data.split("_", 2)
                    uid = int(parts[1])
                    tier = parts[2]
                    _pending_duration[chat_id] = {"uid": uid, "tier": tier}
                    edit(
                        f"*👤 Пользователь #{uid}*\n\nНапиши срок в чат.\n\n"
                        f"Примеры:\n`30 days`\n`7 days`\n`1 hour`\n`90 days`\n`6 months`",
                        reply_markup={"inline_keyboard": [[{"text": "🔙 Отмена", "callback_data": f"sub_{uid}"}]]},
                    )
                    answer("✏️ Введи срок")

                elif data.startswith("dosettier_"):
                    parts = data.split("_", 2)
                    if len(parts) < 3:
                        continue
                    uid = int(parts[1])
                    rest = parts[2]
                    if rest == "free":
                        tier = "free"
                        interval_str = None
                    else:
                        sub = rest.split("_", 1)
                        tier = sub[0]
                        interval_str = sub[1] if len(sub) > 1 else "30 days"

                    try:
                        conn = get_db()
                        cur = conn.cursor()
                        if tier == "free":
                            cur.execute(
                                """INSERT INTO subscriptions (user_id, tier, expires_at, is_active)
                                   VALUES (%s, 'free', NULL, TRUE)
                                   ON CONFLICT (user_id) DO UPDATE SET tier = 'free', expires_at = NULL, is_active = TRUE""",
                                (uid,),
                            )
                            conn.commit()
                            cur.close()
                            conn.close()
                            answer(f"✅ Free")
                            edit(
                                f"*👤 Пользователь #{uid}*\n\n✅ Тариф изменён на Free.",
                                reply_markup={"inline_keyboard": [[{"text": "🔙 Назад", "callback_data": "menu_users"}]]},
                            )
                        else:
                            cur.execute(
                                """INSERT INTO subscriptions (user_id, tier, expires_at)
                                   VALUES (%s, %s, NOW() + %s::interval)
                                   ON CONFLICT (user_id) DO UPDATE SET tier = EXCLUDED.tier, expires_at = EXCLUDED.expires_at, is_active = TRUE""",
                                (uid, tier, interval_str),
                            )
                            conn.commit()
                            cur.close()
                            conn.close()
                            answer(f"✅ {tier} на {interval_str}")
                            edit(
                                f"*👤 Пользователь #{uid}*\n\n✅ Тариф изменён на `{tier}` на `{interval_str}`.",
                                reply_markup={"inline_keyboard": [[{"text": "🔙 Назад", "callback_data": "menu_users"}]]},
                            )
                    except Exception as e:
                        answer(f"Ошибка: {e}")

                # ── approve / deny from activation requests ──
                elif data.startswith("approve:") or data.startswith("deny:"):
                    action, request_id = data.split(":", 1)
                    try:
                        conn = get_db()
                        cur = conn.cursor(cursor_factory=RealDictCursor)
                        cur.execute("SELECT * FROM devices WHERE request_id = %s", (request_id,))
                        device = cur.fetchone()
                        if not device:
                            cur.close(); conn.close()
                            answer("Устройство не найдено")
                            continue

                        if action == "approve":
                            cur.execute("UPDATE devices SET approved = TRUE, denied = FALSE, approved_at = NOW() WHERE request_id = %s", (request_id,))
                            device_id_val = device["device_id"]
                            cur.execute(
                                """INSERT INTO users (email) VALUES (%s)
                                   ON CONFLICT (email) DO UPDATE SET updated_at = NOW()""",
                                (device_id_val,),
                            )
                            cur.execute(
                                "UPDATE devices SET user_id = (SELECT id FROM users WHERE email = %s) WHERE request_id = %s",
                                (device_id_val, request_id),
                            )
                            answer_text = "✅ Устройство одобрено"
                        else:
                            cur.execute("UPDATE devices SET denied = TRUE, approved = FALSE, denied_at = NOW() WHERE request_id = %s", (request_id,))
                            answer_text = "❌ Устройство отклонено"

                        conn.commit()
                        cur.close()
                        conn.close()
                        answer(answer_text)
                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={"chat_id": TELEGRAM_ADMIN_ID, "text": answer_text},
                        )
                    except Exception as e:
                        print(f"Callback error: {e}")
                        answer(f"Ошибка")
                else:
                    answer("Неизвестная команда")

        except Exception as e:
            print("Telegram polling error:", e)
            time.sleep(3)


# ─── API ───────────────────────────────────────────────────────────────


@app.on_event("startup")
def startup():
    for attempt in range(3):
        try:
            init_db()
            break
        except Exception as e:
            print(f"init_db attempt {attempt + 1} failed: {e}")
            time.sleep(2)
    thread = threading.Thread(target=telegram_polling, daemon=True)
    thread.start()


@app.get("/")
def home():
    return HTMLResponse("""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Aivex Activation Server</title>
    <style>
        body { font-family: -apple-system, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; background: #0c0c0e; color: #fff; }
        .card { text-align: center; padding: 2rem; }
        h1 { font-size: 2rem; font-weight: 600; }
        p { color: rgba(255,255,255,0.6); }
    </style>
</head>
<body>
    <div class="card">
        <h1>Aivex</h1>
        <p>Activation server is running</p>
    </div>
</body>
</html>
""")


@app.post("/request-access")
def request_access(data: ActivationRequest):
    require_db()
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
        """INSERT INTO devices (device_id, request_id, platform, app_version, username, approved)
           VALUES (%s, %s, %s, %s, %s, TRUE)""",
        (data.deviceId, request_id, data.platform, data.appVersion, data.username),
    )
    cur.execute(
        """INSERT INTO users (email) VALUES (%s) ON CONFLICT DO NOTHING""",
        (data.deviceId,),
    )
    cur.execute("SELECT id FROM users WHERE email = %s", (data.deviceId,))
    user = cur.fetchone()
    if user:
        cur.execute("UPDATE devices SET user_id = %s WHERE device_id = %s", (user["id"], data.deviceId))
    conn.commit()
    cur.close()
    conn.close()
    return {"ok": True, "status": "approved"}


@app.post("/check-access")
def check_access(data: ActivationRequest):
    require_db()
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM devices WHERE device_id = %s", (data.deviceId,))
    device = cur.fetchone()
    cur.close()
    conn.close()

    if not device:
        return {"allowed": False, "status": "not_requested"}

    return {"allowed": True, "status": "approved"}

    if not device:
        return {"allowed": False, "status": "not_requested"}

    if device["denied"]:
        return {"allowed": False, "status": "denied"}

    if device["approved"]:
        return {"allowed": True, "status": "approved"}

    return {"allowed": False, "status": "pending"}


@app.post("/admin/approve/{device_id}")
def approve_device(device_id: str, _auth=Depends(verify_admin_token)):
    require_db()
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
def deny_device(device_id: str, _auth=Depends(verify_admin_token)):
    require_db()
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


# ─── CHAT API ──────────────────────────────────────────────────────────


class HistoryMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    text: str = ""
    profile: str = "Tutor"
    images: list[str] = []
    history: list[HistoryMessage] = []
    custom_prompt: str | None = None
    model: str = "openai/gpt-4o-mini"
    device_id: str


SYSTEM_PROMPTS = {
    "Tutor": "Ты — опытный репетитор. Отвечай кратко, понятно и профессионально.",
    "Programmer": "Ты — senior разработчик. Отвечай с примерами кода и лучшими практиками.",
    "Writer": "Ты — профессиональный редактор. Помогай с текстом, стилем и грамматикой.",
    "Analyst": "Ты — аналитик. Отвечай структурированно, с цифрами и логикой.",
    "Creative": "Ты — креативный ассистент. Генерируй нестандартные идеи.",
}


@app.post("/chat")
def chat(payload: ChatRequest):
    if not openai_client:
        raise HTTPException(status_code=503, detail="OpenRouter not configured on server")
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="Database not configured")

    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT approved, denied FROM devices WHERE device_id = %s",
        (payload.device_id,),
    )
    device = cur.fetchone()
    cur.close()
    conn.close()

    if not device:
        raise HTTPException(status_code=403, detail="Device not registered")
    if device["denied"]:
        raise HTTPException(status_code=403, detail="Device access denied")
    if not device["approved"]:
        raise HTTPException(status_code=403, detail="Device not yet approved")

    if not chat_limiter.check(payload.device_id):
        raise HTTPException(status_code=429, detail="Too many requests. Try again later.")

    system_prompt = payload.custom_prompt or SYSTEM_PROMPTS.get(
        payload.profile, SYSTEM_PROMPTS["Tutor"],
    )
    system_prompt += """
    Общие правила:
    - не начинай ответ словами "Ответ:" или "Пояснение:";
    - не используй шаблонные заголовки без необходимости;
    - не здоровайся без причины;
    - не используй фразы вроде "Как posso помочь?";
    - отвечай кратко, спокойно и профессионально.
    """

    conversation_messages = [{"role": "system", "content": system_prompt}]

    for msg in payload.history[-12:]:
        conversation_messages.append({"role": msg.role, "content": msg.content})

    user_content = []

    if payload.text:
        user_content.append({"type": "text", "text": payload.text})

    for image in payload.images:
        user_content.append({"type": "image_url", "image_url": {"url": image}})

    if not user_content:
        user_content.append({"type": "text", "text": "Пользователь отправил пустой запрос."})

    conversation_messages.append({"role": "user", "content": user_content})

    try:
        completion = openai_client.chat.completions.create(
            model=payload.model,
            messages=conversation_messages,
            extra_headers={
                "HTTP-Referer": "https://server-activation-06sn.onrender.com",
                "X-OpenRouter-Title": "Aivex",
            },
            max_tokens=4096,
        )
        return {"response": completion.choices[0].message.content}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenRouter error: {str(e)}")


# ─── YOOKASSA PAYMENT ──────────────────────────────────────────────────


TIER_PRICES = {
    "pro": {"name": "Aivex Pro", "price": "499.00"},
    "premium": {"name": "Aivex Premium", "price": "999.00"},
}
payment_limiter = RateLimiter(default_limit=10)

YOOKASSA_API_URL = "https://api.yookassa.ru/v3/payments"
SUCCESS_URL = "https://github.com/aitryhard/AIVEX?payment=success"
CANCEL_URL = "https://github.com/aitryhard/AIVEX?payment=cancelled"


class PaymentRequest(BaseModel):
    device_id: str
    tier: str


@app.post("/payment/create")
def create_payment(payload: PaymentRequest):
    if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
        raise HTTPException(status_code=503, detail="YooKassa not configured")

    if payload.tier not in TIER_PRICES:
        raise HTTPException(status_code=400, detail="Invalid tier")

    require_db()
    conn = get_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT id, user_id, approved, denied FROM devices WHERE device_id = %s",
        (payload.device_id,),
    )
    device = cur.fetchone()

    if not device:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Device not found")
    if device["denied"]:
        cur.close(); conn.close()
        raise HTTPException(status_code=403, detail="Device denied")
    if not device["approved"]:
        cur.close(); conn.close()
        raise HTTPException(status_code=403, detail="Device not approved")

    user_id = device["user_id"]
    if not user_id:
        cur.close(); conn.close()
        raise HTTPException(status_code=403, detail="Device not linked to user")

    cur.execute(
        "SELECT tier, is_active FROM subscriptions WHERE user_id = %s",
        (user_id,),
    )
    sub = cur.fetchone()
    cur.close(); conn.close()

    if sub and sub["is_active"]:
        rank = {"free": 0, "pro": 1, "premium": 2}
        if rank.get(payload.tier, 0) <= rank.get(sub["tier"], 0):
            raise HTTPException(status_code=400, detail="Already on this or higher tier")

    if not payment_limiter.check(payload.device_id):
        raise HTTPException(status_code=429, detail="Too many requests")

    tier_info = TIER_PRICES[payload.tier]
    auth = (YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY)
    idempotence_key = str(uuid.uuid4())

    try:
        resp = requests.post(
            YOOKASSA_API_URL,
            auth=auth,
            headers={
                "Content-Type": "application/json",
                "Idempotence-Key": idempotence_key,
            },
            json={
                "amount": {
                    "value": tier_info["price"],
                    "currency": "RUB",
                },
                "confirmation": {
                    "type": "redirect",
                    "return_url": SUCCESS_URL,
                },
                "description": tier_info["name"],
                "metadata": {
                    "device_id": payload.device_id,
                    "tier": payload.tier,
                },
                "capture": True,
            },
            timeout=15,
        )
        data = resp.json()
        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"YooKassa error: {data}")

        return {
            "url": data["confirmation"]["confirmation_url"],
            "payment_id": data["id"],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"YooKassa error: {str(e)}")


@app.post("/payment/webhook")
async def payment_webhook(request: Request):
    body = await request.body()
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event = data.get("event")
    if event not in ("payment.succeeded", "payment.waiting_for_capture"):
        return {"ok": True}

    payment = data.get("object", {})
    metadata = payment.get("metadata", {})
    device_id = metadata.get("device_id")
    tier = metadata.get("tier")

    if device_id and tier and payment.get("paid"):
        require_db()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT id FROM devices WHERE device_id = %s", (device_id,))
        device = cur.fetchone()
        if device:
            user_id = device[0]
            cur.execute(
                """INSERT INTO subscriptions (user_id, tier, expires_at, is_active)
                   VALUES (%s, %s, NOW() + INTERVAL '30 days', TRUE)
                   ON CONFLICT (user_id) DO UPDATE SET tier = EXCLUDED.tier, expires_at = EXCLUDED.expires_at, is_active = TRUE""",
                (user_id, tier),
            )
            conn.commit()
        cur.close(); conn.close()

    return {"ok": True}


# ─── SUBSCRIPTION API ──────────────────────────────────────────────────


class SubscriptionCreate(BaseModel):
    user_id: int
    tier: str
    expires_at: str | None = None


@app.get("/subscription/by-device/{device_id}")
def get_subscription_by_device(device_id: str):
    require_db()
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
def get_subscription(user_id: int, _auth=Depends(verify_admin_token)):
    require_db()
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
def create_subscription(data: SubscriptionCreate, _auth=Depends(verify_admin_token)):
    require_db()
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
