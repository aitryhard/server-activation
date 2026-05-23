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

def verify_admin_token(authorization: str | None = Header(None)):
    if not ADMIN_API_TOKEN:
        return
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

                    if text in ("/start", "/help"):
                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={
                                "chat_id": chat_id,
                                "text": (
                                    "*Aivex Bot*\n\n"
                                    "Доступные команды:\n"
                                    "/users - список всех пользователей\n"
                                    "/devices - список всех устройств\n"
                                    "/sub <user_id> - подписка пользователя\n"
                                    "/settier <user_id> <tier> - изменить тариф\n"
                                    "/help - эта справка"
                                ),
                                "parse_mode": "Markdown",
                            },
                        )
                        continue

                    if TELEGRAM_ADMIN_ID and str(chat_id) != str(TELEGRAM_ADMIN_ID):
                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={"chat_id": chat_id, "text": "⛔ У вас нет прав на эту команду."},
                        )
                        continue

                    try:
                        conn = get_db()
                    except Exception as e:
                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={"chat_id": chat_id, "text": f"Ошибка БД: {e}"},
                        )
                        continue

                    if text == "/users":
                        cur = conn.cursor(cursor_factory=RealDictCursor)
                        cur.execute(
                            """SELECT u.*,
                                      (SELECT COUNT(*) FROM devices d WHERE d.user_id = u.id AND d.approved) AS devices_count,
                                      s.tier
                               FROM users u
                               LEFT JOIN subscriptions s ON s.user_id = u.id
                               ORDER BY u.created_at DESC"""
                        )
                        rows = cur.fetchall()
                        cur.close()
                        conn.close()

                        if not rows:
                            reply = "Нет пользователей."
                        else:
                            lines = ["*Все пользователи:*\n"]
                            for r in rows:
                                email = r["email"] or "-"
                                tier = r["tier"] or "free"
                                devices = r["devices_count"] or 0
                                tag = f"({r['telegram_id']})" if r["telegram_id"] else ""
                                lines.append(
                                    f"#{r['id']} {tag} {email}\n"
                                    f"  Устройств: {devices} | Тариф: {tier}"
                                )
                            reply = "\n\n".join(lines)

                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={"chat_id": chat_id, "text": reply, "parse_mode": "Markdown"},
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
                            reply = "Нет устройств."
                        else:
                            lines = ["*Все устройства:*\n"]
                            for r in rows:
                                status = "OK" if r["approved"] else ("NO" if r["denied"] else "..")
                                user = r["email"] or "-"
                                pid = r["device_id"][:8] if r["device_id"] else "???"
                                lines.append(
                                    f"{status} {pid}..\n"
                                    f"  Пользователь: {user} | {r['platform'] or '-'} | {r['app_version'] or '-'}"
                                )
                            reply = "\n\n".join(lines)

                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={"chat_id": chat_id, "text": reply, "parse_mode": "Markdown"},
                        )

                    elif text.startswith("/settier"):
                        parts = text.split()
                        if len(parts) != 3:
                            reply = "Использование: `/settier <user_id> <tier>`\nТарифы: free, pro, premium"
                        else:
                            _, uid, tier = parts
                            if tier not in ("free", "pro", "premium"):
                                reply = "Неверный тариф. Допустимо: free, pro, premium"
                            else:
                                try:
                                    cur = conn.cursor()
                                    cur.execute(
                                        """INSERT INTO subscriptions (user_id, tier)
                                           VALUES (%s, %s)
                                           ON CONFLICT (user_id) DO UPDATE SET tier = EXCLUDED.tier, is_active = TRUE""",
                                        (int(uid), tier),
                                    )
                                    conn.commit()
                                    cur.close()
                                    conn.close()
                                    reply = f"Тариф пользователя #{uid} изменён на *{tier}*"
                                except Exception as e:
                                    cur.close()
                                    conn.close()
                                    reply = f"Ошибка: {e}"

                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={"chat_id": chat_id, "text": reply, "parse_mode": "Markdown"},
                        )

                    elif text.startswith("/subscription") or text.startswith("/sub"):
                        parts = text.split()
                        if len(parts) < 2:
                            reply = "Использование: `/subscription <user_id>`"
                        else:
                            try:
                                uid = int(parts[1])
                                cur = conn.cursor(cursor_factory=RealDictCursor)
                                cur.execute("SELECT * FROM subscriptions WHERE user_id = %s", (uid,))
                                sub = cur.fetchone()
                                cur.close()
                                conn.close()
                                if sub:
                                    tier = sub["tier"]
                                    active = "Да" if sub["is_active"] else "Нет"
                                    started = sub["started_at"].isoformat() if sub["started_at"] else "-"
                                    reply = f"Пользователь #{uid}\nТариф: {tier}\nАктивна: {active}\nС: {started}"
                                else:
                                    reply = f"У пользователя #{uid} нет подписки (тариф free)"
                            except Exception as e:
                                reply = f"Ошибка: {e}"

                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                            json={"chat_id": chat_id, "text": reply, "parse_mode": "Markdown"},
                        )
                    continue

                # ── handle callback queries (approve / deny) ──
                callback = update.get("callback_query")
                if not callback:
                    continue

                try:
                    conn = get_db()
                except Exception as e:
                    print(f"Telegram polling: get_db failed: {e}")
                    continue

                action, request_id = callback["data"].split(":", 1)
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
    return {"status": "activation server works"}


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
    require_db()
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

    if not chat_limiter.check(payload.device_id):
        raise HTTPException(status_code=429, detail="Too many requests. Try again later.")

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
        "SELECT id, approved, denied FROM devices WHERE device_id = %s",
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

    cur.execute(
        "SELECT tier, is_active FROM subscriptions WHERE user_id = %s",
        (device["id"],),
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
