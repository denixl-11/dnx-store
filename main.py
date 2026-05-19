import asyncio
import logging
import os
import json
import random
import hashlib
import hmac
from urllib.parse import parse_qs
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PAYMENT_REQUISITES = os.getenv("PAYMENT_REQUISITES", "Реквизиты скрыты")
WEBAPP_URL = "https://denixl-11.github.io/dnx-store/"
CORS_ORIGIN = "https://denixl-11.github.io"

if not BOT_TOKEN:
    print("❌ BOT_TOKEN не найден!")
    exit(1)

DB_CONFIG = {
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": DB_PASSWORD,
    "host": "ep-shy-sun-an8be4el.c-6.us-east-1.aws.neon.tech",
    "port": "5432",
    "sslmode": "require"
}

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id VARCHAR(255) PRIMARY KEY,
                        username VARCHAR(255),
                        balance NUMERIC DEFAULT 0.0
                    )
                """)
                cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS number VARCHAR(20)")
                cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS last_event VARCHAR(50)")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS game_history (
                        id SERIAL PRIMARY KEY,
                        game_number INT,
                        winner_name TEXT,
                        win_amount NUMERIC,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                # Таблица лидеров (победы)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS leaderboard (
                        user_id VARCHAR(255) PRIMARY KEY,
                        username VARCHAR(255),
                        wins INT DEFAULT 0
                    )
                """)
                # Таблица сезона (одна строка)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS season (
                        id INT PRIMARY KEY DEFAULT 1,
                        end_time TIMESTAMPTZ
                    )
                """)
                # Инициализация сезона, если нет
                cur.execute("SELECT end_time FROM season WHERE id = 1")
                season = cur.fetchone()
                if not season:
                    moscow_now = datetime.now(timezone.utc) + timedelta(hours=3)
                    end = moscow_now + timedelta(days=14)
                    cur.execute("INSERT INTO season (id, end_time) VALUES (1, %s)", (end,))
                else:
                    end_time = season[0]
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    if end_time < now:
                        moscow_now = now + timedelta(hours=3)
                        new_end = moscow_now + timedelta(days=14)
                        cur.execute("UPDATE season SET end_time = %s WHERE id = 1", (new_end,))
                        cur.execute("DELETE FROM leaderboard")
                # Устанавливаем номер последней игры
                cur.execute("SELECT MAX(game_number) FROM game_history")
                max_num = cur.fetchone()[0]
                global game_state
                game_state["game_number"] = max_num if max_num else 0
                conn.commit()
    except Exception as e:
        logging.error(f"DB Init Error: {e}")

init_db()

def extract_user_from_initdata(init_data_str: str) -> dict | None:
    if not init_data_str:
        return None
    try:
        parsed = parse_qs(init_data_str)
        user_json = parsed.get('user')
        if not user_json:
            return None
        user = json.loads(user_json[0])
        return {"id": str(user.get('id')), "username": user.get('username', 'Unknown')}
    except:
        return None

def require_auth(handler):
    async def wrapper(request):
        if request.method == "OPTIONS":
            return await handler(request)
        init_data = request.headers.get('X-Telegram-Init-Data') or request.query.get('initData')
        if not init_data and request.method == "POST":
            try:
                body = await request.json()
                init_data = body.get('initData')
            except:
                pass
        if not init_data:
            return web.json_response({"error": "missing_init_data"}, status=401,
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        user = extract_user_from_initdata(init_data)
        if not user:
            return web.json_response({"error": "invalid_signature"}, status=401,
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        request['telegram_user'] = user
        return await handler(request)
    return wrapper

# Предрассчёт траектории
def generate_trajectory(initial_speed: float, direction: int, duration_ms=10000, dt=16):
    x = 500
    velocity = direction * initial_speed
    elapsed = 0
    frames = []
    while elapsed < duration_ms:
        progress = elapsed / duration_ms
        speed_factor = 1 - 0.8 * (progress / 0.5) if progress <= 0.5 else 0.2 * (1 - (progress - 0.5) / 0.5)
        step = (1 if velocity > 0 else -1) * initial_speed * speed_factor * (dt / 1000)
        x += step
        if x <= 0:
            x = 0
            velocity = abs(velocity) * 0.9
        elif x >= 1000:
            x = 1000
            velocity = -abs(velocity) * 0.9
        frames.append(x / 1000)
        elapsed += dt
    frames.append(x / 1000)
    return frames

# Палитра из 20 хорошо различимых нежных цветов
PLAYER_COLORS = [
    "#FFADAD", "#FFD6A5", "#FDFFB6", "#CAFFBF", "#9BF6FF",
    "#A0C4FF", "#BDB2FF", "#FFC6FF", "#FFC09F", "#F3FFB6",
    "#B5EAD7", "#C7CEEA", "#FFDAC1", "#E2F0CB", "#B5D8FF",
    "#D0BFFF", "#FFB3C6", "#AFCBFF", "#FFC8A2", "#C1E1C1"
]

# Игра
game_lock = asyncio.Lock()
game_state = {
    "status": "waiting",
    "players": {},
    "pool": 0.0,
    "timer": 15,
    "target_position": None,
    "spin_params": None,
    "winner": None,
    "last_winner_id": None,
    "round_id": None,
    "game_number": 0   # будет переопределено в init_db
}

async def get_user_photo(user_id: int) -> str | None:
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count == 0:
            return None
        file_id = photos.photos[0][-1].file_id
        file = await bot.get_file(file_id)
        return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
    except Exception as e:
        logging.error(f"Failed to get photo for user {user_id}: {e}")
        return None

async def finish_round(winner_x: float, pool: float, players: dict) -> dict | None:
    if pool <= 0 or not players:
        return None
    winner_x = max(0.0, min(1.0 - 1e-12, winner_x))
    cumulative = 0.0
    winner_id = None
    winner_username = None
    sorted_uids = sorted(players.keys())
    for i, uid in enumerate(sorted_uids):
        p = players[uid]
        sector_start = cumulative
        sector_end = cumulative + (p["amount"] / pool)
        if i == len(sorted_uids) - 1:
            if sector_start <= winner_x <= sector_end + 1e-12:
                winner_id = str(uid)
                winner_username = p.get("username", "Игрок")
                break
        else:
            if sector_start <= winner_x < sector_end:
                winner_id = str(uid)
                winner_username = p.get("username", "Игрок")
                break
        cumulative = sector_end
    if not winner_id:
        return None
    winner_bet = players[winner_id]["amount"]
    others_bets = pool - winner_bet
    profit = winner_bet + (others_bets * 0.7)

    photo_url = await get_user_photo(int(winner_id))

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE id = %s", (winner_id,))
                if not cur.fetchone():
                    return None
                cur.execute("UPDATE users SET balance = balance + %s WHERE id = %s", (profit, winner_id))
                # Обновление статистики побед
                cur.execute("""
                    INSERT INTO leaderboard (user_id, username, wins) VALUES (%s, %s, 1)
                    ON CONFLICT (user_id) DO UPDATE SET wins = leaderboard.wins + 1, username = EXCLUDED.username
                """, (winner_id, winner_username))
                conn.commit()
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO game_history (game_number, winner_name, win_amount) VALUES (%s, %s, %s)",
                    (game_state["game_number"], winner_username, profit)
                )
                cur.execute("DELETE FROM game_history WHERE id NOT IN (SELECT id FROM game_history ORDER BY game_number DESC LIMIT 100)")
                conn.commit()
        return {
            "user_id": winner_id,
            "username": winner_username,
            "win_amount": profit,
            "photo_url": photo_url,
            "round_id": game_state["round_id"]
        }
    except Exception as e:
        logging.error(f"DB error finish_round: {e}")
        return None

async def game_worker():
    global game_state
    while True:
        await asyncio.sleep(1)
        async with game_lock:
            if game_state["status"] == "counting":
                game_state["timer"] -= 1
                if game_state["timer"] <= 0:
                    initial_speed = random.uniform(4000, 4500)
                    direction = 1 if random.random() > 0.5 else -1
                    trajectory = generate_trajectory(initial_speed, direction)
                    target = trajectory[-1]
                    game_state["spin_params"] = {
                        "trajectory": trajectory,
                        "target_position": target
                    }
                    game_state["target_position"] = target
                    game_state["round_id"] = random.randint(1, 10**9)
                    game_state["game_number"] += 1
                    game_state["status"] = "spinning"
                    game_state["winner"] = None
                    game_state["last_winner_id"] = None
                    logging.info(f"Spinning: target={target:.4f}, round_id={game_state['round_id']}, game_number={game_state['game_number']}")

        if game_state["status"] == "spinning":
            await asyncio.sleep(10.2)
            async with game_lock:
                if game_state["status"] == "spinning":
                    winner_data = await finish_round(
                        game_state["target_position"],
                        game_state["pool"],
                        game_state["players"]
                    )
                    game_state["winner"] = winner_data
                    game_state["last_winner_id"] = winner_data["user_id"] if winner_data else None
                    game_state["status"] = "waiting"
                    game_state["players"] = {}
                    game_state["pool"] = 0.0
                    game_state["timer"] = 15
                    logging.info(f"Round finished, winner: {winner_data}")

# API
async def handle_options(request):
    return web.Response(headers={
        "Access-Control-Allow-Origin": CORS_ORIGIN,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Telegram-Init-Data",
    })

@require_auth
async def handle_get_user(request):
    user = request['telegram_user']
    user_id = user['id']
    username = user['username']
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "INSERT INTO users (id, username) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username",
                    (user_id, username))
                cur.execute("SELECT balance FROM users WHERE id = %s", (user_id,))
                db_user = cur.fetchone()
                conn.commit()
                balance = float(db_user['balance']) if db_user else 0.0
    except Exception as e:
        logging.error(f"Get user error: {e}")
        balance = 0.0
    return web.json_response({"balance": balance}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_topup_request(request):
    try:
        data = await request.json()
        user = request['telegram_user']
        user_id = user['id']
        username = user['username']
        amount = float(data.get('amount', 0))
        admin_msg = f"💸 **Заявка на пополнение**\n👤 @{username} (ID: {user_id})\n💰 Сумма: {amount} ₽"
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"topup_no_{user_id}_{amount}"),
            InlineKeyboardButton(text="✅ Зачислить", callback_data=f"topup_yes_{user_id}_{amount}")
        ]])
        await bot.send_message(ADMIN_ID, admin_msg, reply_markup=admin_kb)
        return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Topup error: {e}")
        return web.json_response({"success": False}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

async def handle_get_items(request):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, name, price, status, image_url, nft_link, traits, number FROM items WHERE status = 'Доступен'")
                items = cur.fetchall()
                for item in items:
                    if isinstance(item.get('traits'), str):
                        try: item['traits'] = json.loads(item['traits'])
                        except: item['traits'] = []
        return web.json_response(items, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        return web.json_response([], headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_get_inventory(request):
    user = request['telegram_user']
    user_id = user['id']
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, name, image_url, nft_link, status, traits, number FROM items WHERE buyer_id = %s AND status IN ('Продан','withdrawn','Выведен','pending_withdraw')",
                    (user_id,))
                items = cur.fetchall()
                for item in items:
                    if item['status'] in ('Выведен','withdrawn'): item['status'] = 'withdrawn'
                    if isinstance(item.get('traits'), str):
                        try: item['traits'] = json.loads(item['traits'])
                        except: item['traits'] = []
        return web.json_response(items, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        return web.json_response([], headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_buy(request):
    try:
        data = await request.json()
        user = request['telegram_user']
        user_id = user['id']
        item_ids = data.get('items', [])
        if not item_ids:
            return web.json_response({"success": False, "error": "no_items"}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, price FROM items WHERE id = ANY(%s) AND status = 'Доступен'", (item_ids,))
                items = cur.fetchall()
                if len(items) != len(item_ids):
                    return web.json_response({"success": False, "error": "items_unavailable"}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                total_price = sum(i['price'] for i in items)
                cur.execute("SELECT balance FROM users WHERE id = %s", (user_id,))
                db_user = cur.fetchone()
                if not db_user or float(db_user['balance']) < float(total_price):
                    return web.json_response({"success": False, "error": "insufficient_funds"}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                cur.execute("UPDATE users SET balance = balance - %s WHERE id = %s", (total_price, user_id))
                cur.execute("UPDATE items SET status = 'Продан', buyer_id = %s, last_event = 'approved' WHERE id = ANY(%s)", (user_id, item_ids))
                conn.commit()
        return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_request_withdraw(request):
    try:
        data = await request.json()
        user = request['telegram_user']
        user_id = user['id']
        username = user['username']
        item_id = data.get('itemId')
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, name, nft_link FROM items WHERE id = %s AND buyer_id = %s AND status = 'Продан'",
                            (item_id, user_id))
                item = cur.fetchone()
                if not item:
                    return web.json_response({"success": False, "error": "not_found"}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                cur.execute("UPDATE items SET status = 'pending_withdraw', last_event = 'withdraw_requested' WHERE id = %s", (item_id,))
                conn.commit()
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"with_no_{user_id}_{item_id}"),
            InlineKeyboardButton(text="✅ Вывести", callback_data=f"with_yes_{user_id}_{item_id}")
        ]])
        await bot.send_message(ADMIN_ID,
            f"📤 **Запрос на вывод**\n👤 @{username} (ID: {user_id})\n📦 {item['name']} (ID: {item['id']})\n🔗 {item['nft_link']}",
            reply_markup=admin_kb, disable_web_page_preview=True)
        return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

async def handle_game_state(request):
    async with game_lock:
        sorted_players = [game_state["players"][uid] for uid in sorted(game_state["players"].keys())]
        resp = {
            "status": game_state["status"],
            "players": sorted_players,
            "pool": game_state["pool"],
            "timer": game_state["timer"],
            "spin_params": game_state.get("spin_params"),
            "winner": game_state.get("winner"),
            "last_winner_id": game_state.get("last_winner_id"),
            "round_id": game_state.get("round_id"),
            "game_number": game_state.get("game_number", 0)
        }
    return web.json_response(resp, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_game_bet(request):
    global game_state
    try:
        data = await request.json()
        user = request['telegram_user']
        user_id = user['id']
        username = user['username']
        amount = float(data.get('amount', 0))
        if amount < 10:
            return web.json_response({"success": False, "error": "min_bet"}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        if amount != int(amount):
            return web.json_response({"success": False, "error": "invalid_amount"}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        async with game_lock:
            if game_state["status"] not in ("waiting", "counting"):
                return web.json_response({"success": False, "error": "game_started"}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
            if len(game_state["players"]) >= 20 and user_id not in game_state["players"]:
                return web.json_response({"success": False, "error": "room_full"}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
            with get_db_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT balance FROM users WHERE id = %s", (user_id,))
                    db_user = cur.fetchone()
                    if not db_user or float(db_user['balance']) < amount:
                        return web.json_response({"success": False, "error": "insufficient_funds"}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                    cur.execute("UPDATE users SET balance = balance - %s WHERE id = %s", (amount, user_id))
                    conn.commit()
            if user_id in game_state["players"]:
                game_state["players"][user_id]["amount"] += amount
            else:
                occupied_colors = {p["color"] for p in game_state["players"].values()}
                available = [c for c in PLAYER_COLORS if c not in occupied_colors]
                if not available:
                    available = PLAYER_COLORS
                color = random.choice(available)
                game_state["players"][user_id] = {
                    "id": user_id, "username": username,
                    "amount": amount, "color": color
                }
            game_state["pool"] += amount
            if len(game_state["players"]) >= 2 and game_state["status"] == "waiting":
                game_state["status"] = "counting"
                game_state["timer"] = 15
        return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_game_cancel(request):
    global game_state
    try:
        user = request['telegram_user']
        user_id = user['id']
        async with game_lock:
            if len(game_state["players"]) == 1 and user_id in game_state["players"]:
                refund = game_state["players"][user_id]["amount"]
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE users SET balance = balance + %s WHERE id = %s", (refund, user_id))
                        conn.commit()
                game_state["players"] = {}
                game_state["pool"] = 0.0
                return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        return web.json_response({"success": False, "error": "cannot_cancel"}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

async def handle_game_finish(request):
    return web.json_response({"success": True, "message": "Server handles finish"}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_game_history(request):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT game_number, winner_name, win_amount FROM game_history ORDER BY game_number DESC LIMIT 100")
                rows = cur.fetchall()
        result = []
        for row in rows:
            result.append({
                "game_number": row["game_number"],
                "winner_name": row["winner_name"],
                "win_amount": float(row["win_amount"])
            })
        return web.json_response(result, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Game history error: {e}")
        return web.json_response([], status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_leaderboard(request):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT username, wins FROM leaderboard ORDER BY wins DESC LIMIT 10")
                rows = cur.fetchall()
        result = []
        for idx, row in enumerate(rows, 1):
            result.append({
                "rank": idx,
                "username": row["username"],
                "wins": row["wins"]
            })
        return web.json_response(result, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Leaderboard error: {e}")
        return web.json_response([], status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_season_state(request):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT end_time FROM season WHERE id = 1")
                row = cur.fetchone()
                if row:
                    end_time = row["end_time"]
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=timezone.utc)
                    return web.json_response({"end_time": end_time.timestamp() * 1000}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        return web.json_response({"end_time": None}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        return web.json_response({"end_time": None}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_get_requisites(request):
    return web.json_response({"req": PAYMENT_REQUISITES}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

# Callbacks админа (без изменений)
@dp.callback_query(F.data.startswith("topup_yes_"))
async def admin_topup_approve(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 4: return
    _, _, uid, amount = parts
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users (id, balance) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET balance = users.balance + EXCLUDED.balance", (uid, float(amount)))
                conn.commit()
        await callback.message.edit_text(f"✅ Баланс пополнен на {amount} ₽!")
        try: await bot.send_message(uid, f"💰 Ваш баланс пополнен на {amount} ₽!")
        except: pass
    except Exception as e: logging.error(f"Topup approve error: {e}")

@dp.callback_query(F.data.startswith("topup_no_"))
async def admin_topup_reject(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 4: return
    _, _, uid, amount = parts
    await callback.message.edit_text(f"❌ Заявка на {amount} ₽ отклонена.")
    try: await bot.send_message(uid, f"❌ Заявка на пополнение {amount} ₽ отклонена.")
    except: pass

@dp.callback_query(F.data.startswith("with_yes_"))
async def admin_withdraw_approve(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 4: return
    _, _, uid, item_id = parts
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE items SET status = 'withdrawn', last_event = 'withdraw_approved' WHERE id = %s AND buyer_id = %s", (int(item_id), uid))
                conn.commit()
        await callback.message.edit_text(f"{callback.message.text}\n\n✅ **ВЫВОД ПОДТВЕРЖДЕН**")
        try: await bot.send_message(uid, f"🎉 NFT (ID: {item_id}) выведен!")
        except: pass
    except Exception as e: logging.error(f"Withdraw approve error: {e}")

@dp.callback_query(F.data.startswith("with_no_"))
async def admin_withdraw_reject(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 4: return
    _, _, uid, item_id = parts
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE items SET status = 'Продан', last_event = 'withdraw_rejected' WHERE id = %s AND buyer_id = %s", (int(item_id), uid))
                conn.commit()
        await callback.message.edit_text(f"{callback.message.text}\n\n❌ **ВЫВОД ОТКЛОНЕН**")
        try: await bot.send_message(uid, f"❌ Вывод NFT (ID: {item_id}) отклонён.")
        except: pass
    except Exception as e: logging.error(f"Withdraw reject error: {e}")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✨ Магазин", web_app=WebAppInfo(url=WEBAPP_URL))]])
    await message.answer("Добро пожаловать в DNX Store!", reply_markup=kb)

app = web.Application()
app.router.add_get('/user', handle_get_user)
app.router.add_get('/items', handle_get_items)
app.router.add_get('/inventory', handle_get_inventory)
app.router.add_get('/req', handle_get_requisites)
app.router.add_post('/topup', handle_topup_request)
app.router.add_post('/buy', handle_buy)
app.router.add_post('/request-withdraw', handle_request_withdraw)
app.router.add_get('/game/state', handle_game_state)
app.router.add_post('/game/bet', handle_game_bet)
app.router.add_post('/game/cancel', handle_game_cancel)
app.router.add_post('/game/finish', handle_game_finish)
app.router.add_get('/game/history', handle_game_history)
app.router.add_get('/leaderboard', handle_leaderboard)
app.router.add_get('/season/state', handle_season_state)
app.router.add_options('/{tail:.*}', handle_options)

async def main():
    asyncio.create_task(game_worker())
    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', port).start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())