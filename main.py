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


# --- API МЕТОДЫ ---

async def handle_get_items(request):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, name, price, status, image_url, nft_link FROM items WHERE status = 'Доступен'")
                items = cur.fetchall()
                return web.json_response(items, headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return web.json_response([], status=500, headers={"Access-Control-Allow-Origin": "*"})


async def handle_get_inventory(request):
    user_id = request.query.get('user_id')
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT name, image_url, nft_link FROM items WHERE buyer_id = %s AND status = 'Продан'",
                            (user_id,))
                items = cur.fetchall()
                return web.json_response(items, headers={"Access-Control-Allow-Origin": "*"})
    except:
        return web.json_response([], headers={"Access-Control-Allow-Origin": "*"})


async def handle_reserve(request):
    try:
        data = await request.json()
        item_ids = data.get('item_ids', [])
        user_id = data.get('user_id')
        username = data.get('username', 'Unknown')

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT name, price FROM items WHERE id = ANY(%s) AND status = 'Доступен'", (item_ids,))
                items = cur.fetchall()

                if len(items) == len(item_ids):
                    total_price = sum(i['price'] for i in items)
                    cur.execute(
                        "UPDATE items SET status = 'Забронирован', buyer_id = %s, reserved_at = %s, last_event = NULL WHERE id = ANY(%s)",
                        (user_id, datetime.now(), item_ids))
                    conn.commit()

                    admin_msg = f"⏳ **Бронь корзины!**\n👤 @{username}\n📦 Товаров: {len(items)}\n💰 Итого: {total_price} ₽"
                    await bot.send_message(ADMIN_ID, admin_msg)
                    return web.json_response({"success": True, "requisites": PAYMENT_REQUISITES},
                                             headers={"Access-Control-Allow-Origin": "*"})
                return web.json_response({"success": False, "error": "Часть товаров уже занята"},
                                         headers={"Access-Control-Allow-Origin": "*"})
    except:
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": "*"})


async def handle_check_payment(request):
    try:
        data = await request.json()
        user_id = data.get('user_id')
        username = data.get('username', 'Unknown')

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Находим все забронированные товары этого юзера
                cur.execute("SELECT id, name, price FROM items WHERE buyer_id = %s AND status = 'Забронирован'",
                            (user_id,))
                items = cur.fetchall()

                if items:
                    ids_str = ",".join([str(i['id']) for i in items])
                    total = sum(i['price'] for i in items)

                    admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="❌ Нет", callback_data=f"bulk_no_{user_id}_{ids_str}"),
                        InlineKeyboardButton(text="✅ Да", callback_data=f"bulk_yes_{user_id}_{ids_str}")
                    ]])

                    msg = f"💰 **Запрос оплаты!**\n👤 @{username}\nСумма: {total} ₽\nТовары: {', '.join([i['name'] for i in items])}"
                    await bot.send_message(ADMIN_ID, msg, reply_markup=admin_kb)
                    return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": "*"})
    except:
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": "*"})


# --- CALLBACKS ДЛЯ АДМИНА (МАССОВЫЕ) ---

@dp.callback_query(F.data.startswith("bulk_yes_"))
async def admin_bulk_approve(callback: types.CallbackQuery):
    _, _, uid, ids_str = callback.data.split("_")
    ids = [int(i) for i in ids_str.split(",")]
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE items SET status = 'Продан', last_event = 'approved' WHERE id = ANY(%s)", (ids,))
            conn.commit()
    await callback.message.edit_text("✅ Заказ подтвержден!")
    try:
        await bot.send_message(uid, "✅ Ваши покупки подтверждены! Проверьте вкладку 'Инвентарь'.")
    except:
        pass


@dp.callback_query(F.data.startswith("bulk_no_"))
async def admin_bulk_reject(callback: types.CallbackQuery):
    _, _, uid, ids_str = callback.data.split("_")
    ids = [int(i) for i in ids_str.split(",")]
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE items SET status = 'Доступен', buyer_id = NULL, last_event = 'rejected' WHERE id = ANY(%s)",
                (ids,))
            conn.commit()
    await callback.message.edit_text("❌ Заказ отклонен!")
    try:
        await bot.send_message(uid, "❌ Оплата не подтверждена. Товары возвращены в магазин.")
    except:
        pass


# --- ОСТАЛЬНОЕ (Status, Clear, Options, Start) ---

async def handle_get_status(request):
    user_id = request.query.get('user_id')
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT last_event FROM items WHERE buyer_id = %s AND last_event IS NOT NULL LIMIT 1",
                        (user_id,))
            res = cur.fetchone()
            return web.json_response({"last_event": res['last_event'] if res else None},
                                     headers={"Access-Control-Allow-Origin": "*"})


async def handle_clear_event(request):
    data = await request.json()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE items SET last_event = NULL WHERE buyer_id = %s", (data.get('user_id'),))
            conn.commit()
    return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": "*"})


async def handle_options(request):
    return web.Response(
        headers={"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                 "Access-Control-Allow-Headers": "Content-Type"})


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✨ Магазин", web_app=WebAppInfo(url=WEBAPP_URL))]])
    await message.answer("Добро пожаловать в DNX Store!", reply_markup=kb)


# --- ЗАПУСК ---
app = web.Application()
app.router.add_get('/items', handle_get_items)
app.router.add_get('/inventory', handle_get_inventory)
app.router.add_post('/reserve', handle_reserve)
app.router.add_post('/check_payment', handle_check_payment)
app.router.add_get('/get_status', handle_get_status)
app.router.add_post('/clear_event', handle_clear_event)
app.router.add_options('/{tail:.*}', handle_options)


async def main():
    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', port).start()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())