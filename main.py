import asyncio
import logging
import os
import psycopg2
import json
import random
from psycopg2.extras import RealDictCursor
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PASSWORD = os.getenv("DB_PASSWORD")
# Скрытые реквизиты и админ
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PAYMENT_REQUISITES = os.getenv("PAYMENT_REQUISITES", "Реквизиты скрыты/не настроены")

if not BOT_TOKEN:
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: BOT_TOKEN не найден!")
    exit(1)

WEBAPP_URL = "https://denixl-11.github.io/dnx-store/"

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


# Автосоздание нужных таблиц
def init_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Таблица пользователей для баланса
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id VARCHAR(255) PRIMARY KEY,
                        username VARCHAR(255),
                        balance NUMERIC DEFAULT 0.0
                    )
                """)
                # Добавляем колонку number, если ее еще нет
                cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS number VARCHAR(20)")
                conn.commit()
    except Exception as e:
        logging.error(f"DB Init Error: {e}")


init_db()

# --- ЛОГИКА МИНИ-ИГРЫ ---
game_lock = asyncio.Lock()
game_state = {
    "status": "waiting",  # waiting, counting, spinning
    "players": {},  # user_id -> {"username": str, "amount": float, "color": str}
    "pool": 0.0,
    "timer": 15,
    "winner_x": 0.0
}


def generate_color():
    return f"#{random.randint(50, 200):02x}{random.randint(50, 200):02x}{random.randint(50, 200):02x}"


async def game_worker():
    global game_state
    while True:
        await asyncio.sleep(1)
        async with game_lock:
            if game_state["status"] == "counting":
                game_state["timer"] -= 1
                if game_state["timer"] <= 0:
                    game_state["status"] = "spinning"

                    # Определение победителя
                    winner_val = random.uniform(0, game_state["pool"])
                    current_sum = 0
                    winner_id = None

                    # Для визуализации: маппинг на шкалу от 0.0 до 1.0
                    for uid, p in game_state["players"].items():
                        start_pct = current_sum / game_state["pool"]
                        current_sum += p["amount"]
                        end_pct = current_sum / game_state["pool"]

                        if winner_val <= current_sum and winner_id is None:
                            winner_id = uid
                            # Шарик остановится где-то внутри сегмента этого игрока
                            game_state["winner_x"] = random.uniform(start_pct + 0.02, end_pct - 0.02)

                    if winner_id:
                        profit = game_state["pool"] * 0.9
                        try:
                            with get_db_connection() as conn:
                                with conn.cursor() as cur:
                                    cur.execute("UPDATE users SET balance = balance + %s WHERE id = %s",
                                                (profit, winner_id))
                                    conn.commit()
                        except Exception as e:
                            logging.error(f"Game DB error: {e}")

        if game_state["status"] == "spinning":
            await asyncio.sleep(7)  # Ждем 7 секунд пока идет анимация на фронте
            async with game_lock:
                game_state = {
                    "status": "waiting",
                    "players": {},
                    "pool": 0.0,
                    "timer": 15,
                    "winner_x": 0.0
                }


# --- API МЕТОДЫ ---

async def handle_get_user(request):
    user_id = request.query.get('user_id')
    username = request.query.get('username', 'Unknown')
    if not user_id: return web.json_response({"error": "no user"})
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "INSERT INTO users (id, username) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username",
                    (str(user_id), username))
                cur.execute("SELECT balance FROM users WHERE id = %s", (str(user_id),))
                user = cur.fetchone()
                conn.commit()
                return web.json_response({"balance": float(user['balance']) if user else 0.0},
                                         headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return web.json_response({"balance": 0.0}, status=500, headers={"Access-Control-Allow-Origin": "*"})


async def handle_topup_request(request):
    try:
        data = await request.json()
        user_id = str(data.get('userId'))
        username = data.get('username', 'Unknown')
        amount = float(data.get('amount', 0))

        admin_msg = f"💸 **Заявка на пополнение USDT!**\n👤 @{username} (ID: {user_id})\n💰 Сумма: {amount} USDT\n\nПроверьте поступление по реквизитам."
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"topup_no_{user_id}_{amount}"),
            InlineKeyboardButton(text="✅ Зачислить", callback_data=f"topup_yes_{user_id}_{amount}")
        ]])
        await bot.send_message(ADMIN_ID, admin_msg, reply_markup=admin_kb)
        return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": "*"})
    except:
        return web.json_response({"success": False}, headers={"Access-Control-Allow-Origin": "*"})


async def handle_get_items(request):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, name, price, status, image_url, nft_link, traits, number FROM items WHERE status = 'Доступен'")
                items = cur.fetchall()
                for item in items:
                    traits_raw = item.get('traits')
                    if isinstance(traits_raw, str):
                        try:
                            item['traits'] = json.loads(traits_raw)
                        except:
                            item['traits'] = []
                return web.json_response(items, headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return web.json_response([], status=500, headers={"Access-Control-Allow-Origin": "*"})


async def handle_get_inventory(request):
    user_id = request.query.get('user_id')
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, name, image_url, nft_link, status, traits, number FROM items WHERE buyer_id = %s AND status IN ('Продан', 'withdrawn', 'Выведен')",
                    (str(user_id),))
                items = cur.fetchall()
                for item in items:
                    if item['status'] == 'Выведен': item['status'] = 'withdrawn'
                    traits_raw = item.get('traits')
                    if isinstance(traits_raw, str):
                        try:
                            item['traits'] = json.loads(traits_raw)
                        except:
                            item['traits'] = []
                return web.json_response(items, headers={"Access-Control-Allow-Origin": "*"})
    except Exception:
        return web.json_response([], headers={"Access-Control-Allow-Origin": "*"})


# НОВАЯ ЛОГИКА МГНОВЕННОЙ ПОКУПКИ (Списание с баланса)
async def handle_buy(request):
    try:
        data = await request.json()
        item_ids = data.get('items', [])
        user_id = str(data.get('userId'))

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, price FROM items WHERE id = ANY(%s) AND status = 'Доступен'", (item_ids,))
                items = cur.fetchall()

                if len(items) == len(item_ids) and len(item_ids) > 0:
                    total_price = sum(i['price'] for i in items)

                    cur.execute("SELECT balance FROM users WHERE id = %s", (user_id,))
                    user = cur.fetchone()

                    if user and float(user['balance']) >= float(total_price):
                        # Списываем баланс
                        cur.execute("UPDATE users SET balance = balance - %s WHERE id = %s", (total_price, user_id))
                        # Выдаем предмет
                        cur.execute(
                            "UPDATE items SET status = 'Продан', buyer_id = %s, last_event = 'approved' WHERE id = ANY(%s)",
                            (user_id, item_ids))
                        conn.commit()
                        return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": "*"})
                    else:
                        return web.json_response({"success": False, "error": "insufficient_funds"},
                                                 headers={"Access-Control-Allow-Origin": "*"})
                return web.json_response({"success": False, "error": "items_unavailable"},
                                         headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": "*"})


async def handle_request_withdraw(request):
    try:
        data = await request.json()
        item_id = data.get('itemId')
        user_id = str(data.get('userId'))
        username = data.get('username', 'Unknown')

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, name, nft_link FROM items WHERE id = %s AND buyer_id = %s AND status = 'Продан'",
                    (item_id, user_id))
                item = cur.fetchone()
                if item:
                    admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"with_no_{user_id}_{item['id']}"),
                        InlineKeyboardButton(text="✅ Вывести", callback_data=f"with_yes_{user_id}_{item['id']}")
                    ]])
                    msg = f"📤 **Новый запрос на вывод NFT!**\n👤 @{username} (ID: {user_id})\n📦 Товар: {item['name']} (ID: {item['id']})\n🔗 Ссылка: {item['nft_link']}"
                    await bot.send_message(ADMIN_ID, msg, reply_markup=admin_kb, disable_web_page_preview=True)
                    return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": "*"})
        return web.json_response({"success": False}, headers={"Access-Control-Allow-Origin": "*"})
    except:
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": "*"})


# --- ИГРОВЫЕ API ---
async def handle_game_state(request):
    async with game_lock:
        return web.json_response({
            "status": game_state["status"],
            "players": list(game_state["players"].values()),
            "pool": game_state["pool"],
            "timer": game_state["timer"],
            "winner_x": game_state["winner_x"]
        }, headers={"Access-Control-Allow-Origin": "*"})


async def handle_game_bet(request):
    try:
        data = await request.json()
        user_id = str(data.get('userId'))
        username = data.get('username', 'Player')
        amount = float(data.get('amount', 0))

        if amount <= 0: return web.json_response({"success": False})

        async with game_lock:
            if game_state["status"] != "waiting" and game_state["status"] != "counting":
                return web.json_response({"success": False, "error": "game_started"})
            if len(game_state["players"]) >= 20 and user_id not in game_state["players"]:
                return web.json_response({"success": False, "error": "room_full"})

            # Проверка баланса
            with get_db_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT balance FROM users WHERE id = %s", (user_id,))
                    user = cur.fetchone()
                    if not user or float(user['balance']) < amount:
                        return web.json_response({"success": False, "error": "insufficient_funds"})

                    cur.execute("UPDATE users SET balance = balance - %s WHERE id = %s", (amount, user_id))
                    conn.commit()

            # Добавляем ставку в пул
            if user_id in game_state["players"]:
                game_state["players"][user_id]["amount"] += amount
            else:
                game_state["players"][user_id] = {
                    "id": user_id, "username": username,
                    "amount": amount, "color": generate_color()
                }

            game_state["pool"] += amount

            # Запуск таймера если 2+ игроков
            if len(game_state["players"]) >= 2 and game_state["status"] == "waiting":
                game_state["status"] = "counting"
                game_state["timer"] = 15

        return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        logging.error(f"Bet error: {e}")
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": "*"})


async def handle_game_cancel(request):
    try:
        data = await request.json()
        user_id = str(data.get('userId'))

        async with game_lock:
            if len(game_state["players"]) == 1 and user_id in game_state["players"]:
                refund = game_state["players"][user_id]["amount"]
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE users SET balance = balance + %s WHERE id = %s", (refund, user_id))
                        conn.commit()
                game_state["players"] = {}
                game_state["pool"] = 0.0
                return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": "*"})
        return web.json_response({"success": False, "error": "cannot_cancel"},
                                 headers={"Access-Control-Allow-Origin": "*"})
    except:
        return web.json_response({"success": False}, headers={"Access-Control-Allow-Origin": "*"})


async def handle_get_requisites(request):
    return web.json_response({"req": PAYMENT_REQUISITES}, headers={"Access-Control-Allow-Origin": "*"})


async def handle_options(request):
    return web.Response(
        headers={"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                 "Access-Control-Allow-Headers": "Content-Type"})


# --- CALLBACKS ДЛЯ АДМИНА ---
@dp.callback_query(F.data.startswith("topup_yes_"))
async def admin_topup_approve(callback: types.CallbackQuery):
    _, _, uid, amount = callback.data.split("_")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (id, balance) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET balance = users.balance + EXCLUDED.balance",
                (uid, float(amount)))
            conn.commit()
    await callback.message.edit_text(f"✅ Баланс пополнен на {amount} USDT!")
    try:
        await bot.send_message(uid, f"💰 Ваш баланс успешно пополнен на {amount} USDT!")
    except:
        pass


@dp.callback_query(F.data.startswith("topup_no_"))
async def admin_topup_reject(callback: types.CallbackQuery):
    _, _, uid, amount = callback.data.split("_")
    await callback.message.edit_text(f"❌ Заявка на {amount} USDT отклонена.")
    try:
        await bot.send_message(uid, f"❌ Отказ: заявка на пополнение {amount} USDT отклонена.")
    except:
        pass


@dp.callback_query(F.data.startswith("with_yes_"))
async def admin_withdraw_approve(callback: types.CallbackQuery):
    _, _, uid, item_id = callback.data.split("_")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE items SET status = 'withdrawn', last_event = 'withdraw_approved' WHERE id = %s AND buyer_id = %s",
                (int(item_id), uid))
            conn.commit()
    await callback.message.edit_text(f"{callback.message.text}\n\n✅ **ВЫВОД ПОДТВЕРЖДЕН**")
    try:
        await bot.send_message(uid, f"🎉 Ваш запрос на вывод NFT (ID: {item_id}) успешно выполнен! Проверьте кошелек.")
    except:
        pass


@dp.callback_query(F.data.startswith("with_no_"))
async def admin_withdraw_reject(callback: types.CallbackQuery):
    _, _, uid, item_id = callback.data.split("_")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE items SET last_event = 'withdraw_rejected' WHERE id = %s AND buyer_id = %s",
                        (int(item_id), uid))
            conn.commit()
    await callback.message.edit_text(f"{callback.message.text}\n\n❌ **ВЫВОД ОТКЛОНЕН**")
    try:
        await bot.send_message(uid, f"❌ Отказ: вывод NFT (ID: {item_id}) отклонен администратором.")
    except:
        pass


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✨ Магазин", web_app=WebAppInfo(url=WEBAPP_URL))]])
    await message.answer("Добро пожаловать в DNX Store!", reply_markup=kb)


# --- ЗАПУСК ---
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