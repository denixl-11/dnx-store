import asyncio
import logging
import os
import psycopg2
import json
from psycopg2.extras import RealDictCursor
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiohttp import web

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = "8701376578:AAGqehQYFf3ePE61lDhWhCnR7Q2lthnR3Oc"
ADMIN_ID = 7106612591
WEBAPP_URL = "https://denixl-11.github.io/dnx-store/"

PAYMENT_REQUISITES = "💳 Карта: **** **** **** 0000 (Т-Банк)\n👤 Получатель: Твое Имя"

DB_CONFIG = {
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_mY3snQVxT7OF",
    "host": "ep-shy-sun-an8be4el.c-6.us-east-1.aws.neon.tech",
    "port": "5432",
    "sslmode": "require"
}
# ===================================================

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# --- ЛОГИКА ДЛЯ WEB APP (API) ---

async def handle_get_items(request):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, name, price, status, image_url, nft_link FROM items")
                items = cur.fetchall()
                # ВАЖНО: заголовки CORS разрешают браузеру брать данные
                return web.json_response(items, headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, OPTIONS"
                })
    except Exception as e:
        logging.error(f"Ошибка API: {e}")
        return web.json_response([], status=500, headers={"Access-Control-Allow-Origin": "*"})

async def handle_reserve(request):
    try:
        data = await request.json()
        item_id = data.get('item_id')
        user_id = data.get('user_id')
        username = data.get('username', 'Unknown')

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM items WHERE id = %s AND status = 'Доступен'", (item_id,))
                item = cur.fetchone()

                if item:
                    cur.execute("""
                        UPDATE items SET status = 'Забронирован', buyer_id = %s, reserved_at = %s
                        WHERE id = %s
                    """, (user_id, datetime.now(), item_id))
                    conn.commit()

                    admin_kb = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="✅ Подтвердить оплату", callback_data=f"adm_{item_id}_{user_id}")]
                    ])
                    await bot.send_message(ADMIN_ID, f"💰 Бронь из Web App: @{username}\nТовар: {item['name']}",
                                           reply_markup=admin_kb)

                    return web.json_response({
                        "success": True,
                        "requisites": PAYMENT_REQUISITES
                    }, headers={"Access-Control-Allow-Origin": "*"})
                else:
                    return web.json_response({
                        "success": False,
                        "error": "Товар уже занят"
                    }, headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        logging.error(f"Ошибка бронирования: {e}")
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": "*"})

async def handle_options(request):
    # Необходим для предварительной проверки браузером (Preflight request)
    return web.Response(headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    })

# --- ЛОГИКА БОТА ---

def get_catalog_items():
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                UPDATE items SET status = 'Доступен', buyer_id = NULL, reserved_at = NULL
                WHERE status = 'Забронирован' AND reserved_at < NOW() - INTERVAL '30 minutes'
            """)
            conn.commit()
            cur.execute("SELECT * FROM items WHERE status = 'Доступен'")
            return cur.fetchall()

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✨ Открыть магазин (Web App)", web_app=WebAppInfo(url=WEBAPP_URL))],
        [InlineKeyboardButton(text="🛒 Текстовый Каталог", callback_data="show_catalog")],
        [InlineKeyboardButton(text="📦 Мои покупки", callback_data="my_inventory")]
    ])
    await message.answer("Привет! 🎁 Добро пожаловать в DNX Store.\nЗдесь ты можешь купить NFT.", reply_markup=kb)

@dp.callback_query(F.data == "show_catalog")
async def callback_catalog(callback: types.CallbackQuery):
    items = get_catalog_items()
    if not items:
        await callback.message.answer("😔 В каталоге пока пусто.")
    else:
        for item in items:
            nft_url = item.get('nft_link', 'https://t.me/your_bot')
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔗 Посмотреть NFT", url=nft_url)],
                [InlineKeyboardButton(text=f"💳 Купить за {item['price']}₽", callback_data=f"buy_{item['id']}")]
            ])
            await callback.message.answer(f"🎁 **{item['name']}**\n💰 Цена: {item['price']} руб.",
                                          reply_markup=kb, parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data.startswith("buy_"))
async def callback_buy(callback: types.CallbackQuery):
    item_id = int(callback.data.replace("buy_", ""))
    user = callback.from_user
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM items WHERE id = %s AND status = 'Доступен'", (item_id,))
            item = cur.fetchone()
            if item:
                cur.execute("UPDATE items SET status = 'Забронирован', buyer_id = %s, reserved_at = %s WHERE id = %s",
                            (user.id, datetime.now(), item_id))
                conn.commit()
                await callback.message.answer(f"✅ Забронировано: {item['name']}\n\n{PAYMENT_REQUISITES}")
                admin_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"adm_{item_id}_{user.id}")]
                ])
                await bot.send_message(ADMIN_ID, f"💰 Бронь: @{user.username}", reply_markup=admin_kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("adm_"))
async def admin_confirm(callback: types.CallbackQuery):
    _, item_id, uid = callback.data.split("_")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE items SET status = 'Продан' WHERE id = %s", (item_id,))
            conn.commit()
            await callback.message.edit_text("✅ Статус: Продан")
            await bot.send_message(uid, "🎉 Оплата подтверждена!")
    await callback.answer()

@dp.callback_query(F.data == "my_inventory")
async def callback_inventory(callback: types.CallbackQuery):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT name, status FROM items WHERE buyer_id = %s", (callback.from_user.id,))
            items = cur.fetchall()
            if not items:
                await callback.message.answer("📦 У вас пока нет покупок.")
            else:
                res = "📦 Ваши покупки:\n" + "\n".join([f"- {i['name']} ({i['status']})" for i in items])
                await callback.message.answer(res)
    await callback.answer()

# --- ЗАПУСК ---

app = web.Application()
app.router.add_get('/items', handle_get_items)
app.router.add_post('/reserve', handle_reserve)
app.router.add_options('/{tail:.*}', handle_options)

async def main():
    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())