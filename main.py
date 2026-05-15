import asyncio
import logging
import os
import json
import random
import hashlib
import hmac
from urllib.parse import parse_qs
import psycopg2
from psycopg2.extras import RealDictCursor
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

# --------------------- Настройки ---------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PAYMENT_REQUISITES = os.getenv("PAYMENT_REQUISITES", "Реквизиты скрыты")
WEBAPP_URL = "https://denixl-11.github.io/dnx-store/"
CORS_ORIGIN = "https://denixl-11.github.io"

if not BOT_TOKEN:
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: BOT_TOKEN не найден!")
    exit(1)

# --------------------- База данных ---------------------
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
                try:
                    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS number VARCHAR(20)")
                except Exception as e:
                    logging.warning(f"Column number may already exist: {e}")
                try:
                    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS last_event VARCHAR(50)")
                except Exception as e:
                    logging.warning(f"Column last_event may already exist: {e}")
                conn.commit()
    except Exception as e:
        logging.error(f"DB Init Error: {e}")


init_db()


# --------------------- Извлечение пользователя (без проверки подписи) ---------------------
def extract_user_from_initdata(init_data_str: str) -> dict | None:
    """Извлекает ID и username из initData без проверки подписи (временно)"""
    if not init_data_str:
        logging.warning("extract_user: empty init_data")
        return None
    try:
        parsed = parse_qs(init_data_str)
        logging.info(f"initData keys: {list(parsed.keys())}")

        # Логируем хеши для отладки (вдруг понадобится)
        received_hash = parsed.get('hash', [None])[0]
        if received_hash:
            data_check_string = "\n".join(
                f"{k}={v[0]}" for k, v in sorted(parsed.items()) if k not in ('hash', 'signature')
            )
            secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()
            mac = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
            logging.info(f"data_check_string (first 100): {data_check_string[:100]}")
            logging.info(f"Computed hash: {mac}")
            logging.info(f"Received hash: {received_hash}")

        user_json = parsed.get('user')
        if not user_json:
            logging.warning("No user field in initData")
            return None
        user = json.loads(user_json[0])
        logging.info(f"User extracted: id={user.get('id')}, username={user.get('username')}")
        return {"id": str(user.get('id')), "username": user.get('username', 'Unknown')}
    except Exception as e:
        logging.error(f"extract_user error: {e}")
        return None


def require_auth(handler):
    """Декоратор, требующий initData (но без проверки подписи пока)"""
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
            logging.warning("require_auth: no initData provided")
            return web.json_response({"error": "missing_init_data"}, status=401,
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

        user = extract_user_from_initdata(init_data)
        if not user:
            logging.warning("require_auth: invalid initData")
            return web.json_response({"error": "invalid_signature"}, status=401,
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

        request['telegram_user'] = user
        return await handler(request)
    return wrapper


# --------------------- Игра ---------------------
game_lock = asyncio.Lock()
game_state = {
    "status": "waiting",
    "players": {},
    "pool": 0.0,
    "timer": 15,
    "target_position": None,
    "winner": None
}


def generate_color():
    return f"#{random.randint(50, 200):02x}{random.randint(50, 200):02x}{random.randint(50, 200):02x}"


async def finish_round(winner_x: float, pool: float, players: dict) -> dict | None:
    if pool <= 0 or not players:
        return None
    winner_x = max(0.0, min(1.0 - 1e-12, winner_x))
    cumulative = 0.0
    winner_id = None
    winner_username = None
    sorted_uids = list(players.keys())
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

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users WHERE id = %s", (winner_id,))
                if not cur.fetchone():
                    return None
                cur.execute("UPDATE users SET balance = balance + %s WHERE id = %s", (profit, winner_id))
                conn.commit()
        return {"user_id": winner_id, "username": winner_username, "win_amount": profit}
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
                    game_state["status"] = "spinning"
                    game_state["target_position"] = random.random()
                    game_state["winner"] = None
                    logging.info(f"Spinning target: {game_state['target_position']:.4f}")

        if game_state["status"] == "spinning":
            await asyncio.sleep(12)
            async with game_lock:
                if game_state["status"] == "spinning":
                    winner_data = await finish_round(
                        game_state["target_position"],
                        game_state["pool"],
                        game_state["players"]
                    )
                    game_state["winner"] = winner_data
                    game_state["status"] = "waiting"
                    game_state["players"] = {}
                    game_state["pool"] = 0.0
                    game_state["timer"] = 15
                    logging.info(f"Round finished, winner: {winner_data}")


# --------------------- API ---------------------
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
                        try:
                            item['traits'] = json.loads(item['traits'])
                        except:
                            item['traits'] = []
        return web.json_response(items, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Get items error: {e}")
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
                    if item['status'] in ('Выведен','withdrawn'):
                        item['status'] = 'withdrawn'
                    if isinstance(item.get('traits'), str):
                        try:
                            item['traits'] = json.loads(item['traits'])
                        except:
                            item['traits'] = []
        return web.json_response(items, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Inventory error: {e}")
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
        logging.error(f"Buy error: {e}")
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
        logging.error(f"Withdraw error: {e}")
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


async def handle_game_state(request):
    async with game_lock:
        resp = {
            "status": game_state["status"],
            "players": list(game_state["players"].values()),
            "pool": game_state["pool"],
            "timer": game_state["timer"],
            "target_position": game_state.get("target_position"),
            "winner": game_state.get("winner")
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
        if amount <= 0:
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
                game_state["players"][user_id] = {
                    "id": user_id, "username": username,
                    "amount": amount, "color": generate_color()
                }
            game_state["pool"] += amount

            if len(game_state["players"]) >= 2 and game_state["status"] == "waiting":
                game_state["status"] = "counting"
                game_state["timer"] = 15

        return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Bet error: {e}")
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
        logging.error(f"Cancel error: {e}")
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


async def handle_game_finish(request):
    return web.json_response({"success": True, "message": "Server handles finish"}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


@require_auth
async def handle_get_requisites(request):
    return web.json_response({"req": PAYMENT_REQUISITES}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


# --------------------- Callbacks админа ---------------------
@dp.callback_query(F.data.startswith("topup_yes_"))
async def admin_topup_approve(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 4:
        await callback.answer("Неверный формат")
        return
    _, _, uid, amount = parts
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (id, balance) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET balance = users.balance + EXCLUDED.balance",
                    (uid, float(amount)))
                conn.commit()
        await callback.message.edit_text(f"✅ Баланс пополнен на {amount} ₽!")
        try:
            await bot.send_message(uid, f"💰 Ваш баланс пополнен на {amount} ₽!")
        except:
            pass
    except Exception as e:
        logging.error(f"Topup approve error: {e}")
        await callback.answer("Ошибка при пополнении")


@dp.callback_query(F.data.startswith("topup_no_"))
async def admin_topup_reject(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 4:
        await callback.answer("Неверный формат")
        return
    _, _, uid, amount = parts
    await callback.message.edit_text(f"❌ Заявка на {amount} ₽ отклонена.")
    try:
        await bot.send_message(uid, f"❌ Заявка на пополнение {amount} ₽ отклонена.")
    except:
        pass


@dp.callback_query(F.data.startswith("with_yes_"))
async def admin_withdraw_approve(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 4:
        await callback.answer("Неверный формат")
        return
    _, _, uid, item_id = parts
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE items SET status = 'withdrawn', last_event = 'withdraw_approved' WHERE id = %s AND buyer_id = %s",
                    (int(item_id), uid))
                conn.commit()
        await callback.message.edit_text(f"{callback.message.text}\n\n✅ **ВЫВОД ПОДТВЕРЖДЕН**")
        try:
            await bot.send_message(uid, f"🎉 NFT (ID: {item_id}) выведен!")
        except:
            pass
    except Exception as e:
        logging.error(f"Withdraw approve error: {e}")
        await callback.answer("Ошибка при выводе")


@dp.callback_query(F.data.startswith("with_no_"))
async def admin_withdraw_reject(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 4:
        await callback.answer("Неверный формат")
        return
    _, _, uid, item_id = parts
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE items SET status = 'Продан', last_event = 'withdraw_rejected' WHERE id = %s AND buyer_id = %s",
                    (int(item_id), uid))
                conn.commit()
        await callback.message.edit_text(f"{callback.message.text}\n\n❌ **ВЫВОД ОТКЛОНЕН**")
        try:
            await bot.send_message(uid, f"❌ Вывод NFT (ID: {item_id}) отклонён.")
        except:
            pass
    except Exception as e:
        logging.error(f"Withdraw reject error: {e}")
        await callback.answer("Ошибка при отклонении")


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✨ Магазин", web_app=WebAppInfo(url=WEBAPP_URL))]])
    await message.answer("Добро пожаловать в DNX Store!", reply_markup=kb)


# --------------------- Запуск ---------------------
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