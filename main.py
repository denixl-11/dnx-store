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


# --- ФОНОВАЯ ЗАДАЧА: АВТОСНЯТИЕ БРОНИ (20 МИНУТ) ---
async def auto_cancel_reservations():
    """Проверяет базу каждую минуту и освобождает просроченные брони"""
    while True:
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE items 
                        SET status = 'Доступен', buyer_id = NULL, reserved_at = NULL 
                        WHERE status = 'Забронирован' 
                        AND reserved_at < NOW() - INTERVAL '20 minutes'
                    """)
                    if cur.rowcount > 0:
                        conn.commit()
                        logging.info(f"Снята просроченная бронь с {cur.rowcount} товаров.")
        except Exception as e:
            logging.error(f"Ошибка таймера бронирования: {e}")

        await asyncio.sleep(60)  # Пауза 1 минута


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
                            (str(user_id),))
                items = cur.fetchall()
                return web.json_response(items, headers={"Access-Control-Allow-Origin": "*"})
    except:
        return web.json_response([], headers={"Access-Control-Allow-Origin": "*"})


# Заменяет старый /reserve для совместимости с фронтендом
async def handle_book(request):
    try:
        data = await request.json()
        item_ids = data.get('items', [])
        user_id = str(data.get('userId'))
        username = data.get('username', 'Unknown')

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Проверяем, доступны ли ВСЕ запрошенные товары
                cur.execute("SELECT id, name, price FROM items WHERE id = ANY(%s) AND status = 'Доступен'", (item_ids,))
                items = cur.fetchall()

                if len(items) == len(item_ids) and len(item_ids) > 0:
                    total_price = sum(i['price'] for i in items)
                    # Ставим статус 'Забронирован' и записываем текущее время
                    cur.execute(
                        "UPDATE items SET status = 'Забронирован', buyer_id = %s, reserved_at = NOW(), last_event = NULL WHERE id = ANY(%s)",
                        (user_id, item_ids)
                    )
                    conn.commit()

                    admin_msg = f"⏳ **Бронь!**\n👤 @{username}\n📦 Товаров: {len(items)}\n💰 Итого: {total_price} ₽\nОжидаем оплату (20 мин)..."
                    await bot.send_message(ADMIN_ID, admin_msg)
                    return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": "*"})

                return web.json_response({"success": False, "error": "Товары уже заняты"},
                                         headers={"Access-Control-Allow-Origin": "*"})
    except Exception as e:
        logging.error(e)
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": "*"})


# Заменяет старый /check_payment
async def handle_notify_admin(request):
    try:
        data = await request.json()
        item_ids = data.get('items', [])
        user_id = str(data.get('userId'))
        username = data.get('username', 'Unknown')

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, name, price FROM items WHERE id = ANY(%s) AND buyer_id = %s AND status = 'Забронирован'",
                    (item_ids, user_id))
                items = cur.fetchall()

                if items:
                    ids_str = ",".join([str(i['id']) for i in items])
                    total = sum(i['price'] for i in items)

                    admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="❌ Нет", callback_data=f"bulk_no_{user_id}_{ids_str}"),
                        InlineKeyboardButton(text="✅ Да", callback_data=f"bulk_yes_{user_id}_{ids_str}")
                    ]])

                    msg = f"💰 **Запрос проверки оплаты!**\n👤 @{username} нажал кнопку проверки.\nСумма к проверке: {total} ₽\nТовары: {', '.join([i['name'] for i in items])}"
                    await bot.send_message(ADMIN_ID, msg, reply_markup=admin_kb)
                    return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": "*"})
                return web.json_response({"success": False}, headers={"Access-Control-Allow-Origin": "*"})
    except:
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": "*"})


# Отмена брони пользователем вручную (кнопка "Отмена" на фронтенде)
async def handle_cancel_booking(request):
    try:
        data = await request.json()
        item_ids = data.get('items', [])

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE items SET status = 'Доступен', buyer_id = NULL, reserved_at = NULL WHERE id = ANY(%s) AND status = 'Забронирован'",
                    (item_ids,)
                )
                conn.commit()
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
            cur.execute(
                "UPDATE items SET status = 'Продан', last_event = 'approved', reserved_at = NULL WHERE id = ANY(%s)",
                (ids,))
            conn.commit()
    await callback.message.edit_text("✅ Заказ подтвержден!")
    try:
        await bot.send_message(uid, "✅ Оплата получена! NFT добавлены в ваш инвентарь.")
    except:
        pass


@dp.callback_query(F.data.startswith("bulk_no_"))
async def admin_bulk_reject(callback: types.CallbackQuery):
    _, _, uid, ids_str = callback.data.split("_")
    ids = [int(i) for i in ids_str.split(",")]
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE items SET status = 'Доступен', buyer_id = NULL, last_event = 'rejected', reserved_at = NULL WHERE id = ANY(%s)",
                (ids,)
            )
            conn.commit()
    await callback.message.edit_text("❌ Заказ отклонен! Товары возвращены на витрину.")
    try:
        await bot.send_message(uid, "❌ Оплата не найдена. Бронь снята, товары возвращены в магазин.")
    except:
        pass


# --- ОСТАЛЬНОЕ ---

async def handle_get_status(request):
    user_id = request.query.get('user_id')
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT last_event FROM items WHERE buyer_id = %s AND last_event IS NOT NULL LIMIT 1",
                        (str(user_id),))
            res = cur.fetchone()
            return web.json_response({"last_event": res['last_event'] if res else None},
                                     headers={"Access-Control-Allow-Origin": "*"})


async def handle_clear_event(request):
    data = await request.json()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE items SET last_event = NULL WHERE buyer_id = %s", (str(data.get('user_id')),))
            conn.commit()
    return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": "*"})


async def handle_options(request):
    return web.Response(headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type"
    })


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✨ Магазин", web_app=WebAppInfo(url=WEBAPP_URL))]]
    )
    await message.answer("Добро пожаловать в DNX Store!", reply_markup=kb)


# --- ЗАПУСК ---
app = web.Application()
app.router.add_get('/items', handle_get_items)
app.router.add_get('/inventory', handle_get_inventory)
app.router.add_post('/book', handle_book)  # НОВОЕ: Бронь
app.router.add_post('/notify-admin', handle_notify_admin)  # НОВОЕ: Проверка оплаты
app.router.add_post('/cancel-booking', handle_cancel_booking)  # НОВОЕ: Отмена юзером
app.router.add_get('/get_status', handle_get_status)
app.router.add_post('/clear_event', handle_clear_event)
app.router.add_options('/{tail:.*}', handle_options)


async def main():
    # Запускаем фоновую задачу таймера брони
    asyncio.create_task(auto_cancel_reservations())

    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', port).start()

    # Запуск бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())