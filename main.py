import asyncio
import logging
import os
import psycopg2
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


# --- ЛОГИКА ДЛЯ WEB APP (HTTP СЕРВЕР) ---

async def handle_get_items(request):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, name, price, status, image_url FROM items")
                items = cur.fetchall()
                return web.json_response(items, headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type"
                })
    except Exception as e:
        logging.error(f"Ошибка API: {e}")
        return web.json_response([], status=500)


async def handle_options(request):
    return web.Response(headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
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
            # Используем .get() для безопасности
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
    user = callback.from_user  # ИСПРАВЛЕНО: добавлено нижнее подчеркивание
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM items WHERE id = %s AND status = 'Доступен'", (item_id,))
            item = cur.fetchone()
            if item:
                cur.execute("""
                    UPDATE items SET status = 'Забронирован', buyer_id = %s, reserved_at = %s
                    WHERE id = %s
                """, (user.id, datetime.now(), item_id))
                conn.commit()
                await callback.message.answer(
                    f"✅ **Товар забронирован: {item['name']}**\n\n💰 К оплате: {item['price']} руб.\n{PAYMENT_REQUISITES}",
                    parse_mode="Markdown"
                )
                admin_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Подтвердить оплату", callback_data=f"adm_{item_id}_{user.id}")]
                ])
                await bot.send_message(ADMIN_ID, f"💰 Бронь: @{user.username}\nТовар: {item['name']}",
                                       reply_markup=admin_kb)
            else:
                await callback.message.answer("❌ Товар уже успели забронировать.")
    await callback.answer()


@dp.callback_query(F.data.startswith("adm_"))
async def admin_confirm(callback: types.CallbackQuery):
    # Исправлен разбор параметров
    data_parts = callback.data.split("_")
    item_id = data_parts[1]
    uid = data_parts[2]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE items SET status = 'Продан' WHERE id = %s", (item_id,))
            conn.commit()
            await callback.message.edit_text("✅ Статус изменен на 'Продан'.")
            inv_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📦 Посмотреть в инвентаре", callback_data="my_inventory")]
            ])
            await bot.send_message(uid, "🎉 Оплата подтверждена! NFT теперь твой.", reply_markup=inv_kb)
    await callback.answer()


@dp.callback_query(F.data == "my_inventory")
async def inventory_handler(callback: types.CallbackQuery):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM items WHERE buyer_id = %s", (callback.from_user.id,))
            my_stuff = cur.fetchall()
            if not my_stuff:
                await callback.message.answer("У тебя еще нет купленных NFT. 😉")
            else:
                text = "📦 **Твоя коллекция:**\n\n" + "\n".join([f"• {i['name']} — {i['status']}" for i in my_stuff])
                await callback.message.answer(text, parse_mode="Markdown")
    await callback.answer()


@dp.message(F.web_app_data)
async def web_app_receive(message: types.Message):
    data = message.web_app_data.data
    await message.answer(
        f"🛒 **Получен сигнал из Web App!**\n\n{data}\n\nЧтобы завершить покупку, используй текстовый каталог или дождись сообщения от админа.")


# --- ЗАПУСК ---

app = web.Application()
app.router.add_get('/items', handle_get_items)
app.router.add_options('/items', handle_options)


async def main():
    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

    print(f"🚀 Веб-сервер запущен на порту {port}")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Бот выключен")