import asyncio
import logging
import os
import json
import random
import math
from urllib.parse import parse_qs
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiohttp import web
from dotenv import load_dotenv
import numpy as np

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
    "game_number": 0,
    "polygons": None,
    "last_polygons": None          # храним полигоны после конца раунда для подсветки
}


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
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS game_counter (
                        id INT PRIMARY KEY DEFAULT 1,
                        last_game_number INT NOT NULL DEFAULT 0
                    )
                """)
                cur.execute("INSERT INTO game_counter (id, last_game_number) VALUES (1, 0) ON CONFLICT (id) DO NOTHING")
                cur.execute("SELECT last_game_number FROM game_counter WHERE id = 1")
                last_num = cur.fetchone()[0]
                game_state["game_number"] = last_num

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS leaderboard (
                        user_id VARCHAR(255) PRIMARY KEY,
                        username VARCHAR(255),
                        wins INT DEFAULT 0
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS season (
                        id INT PRIMARY KEY DEFAULT 1,
                        end_time TIMESTAMPTZ
                    )
                """)
                cur.execute(
                    "INSERT INTO season (id, end_time) VALUES (1, '2026-06-30 15:00:00+00') ON CONFLICT (id) DO NOTHING")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS prize_items (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        image_url TEXT NOT NULL,
                        nft_link TEXT NOT NULL DEFAULT '',
                        traits JSONB DEFAULT '[]'::jsonb
                    )
                """)
                conn.commit()
                logging.info(f"DB initialized. Game number: {last_num}")
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


# ------------------------------------------------------------
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ДИАГРАММЫ ВОРОНОГО
# ------------------------------------------------------------
def clip_polygon_by_halfplane(poly, point_on_line, normal):
    clipped = []
    n = len(poly)
    for i in range(n):
        p1 = poly[i]
        p2 = poly[(i + 1) % n]
        d1 = (p1[0] - point_on_line[0]) * normal[0] + (p1[1] - point_on_line[1]) * normal[1]
        d2 = (p2[0] - point_on_line[0]) * normal[0] + (p2[1] - point_on_line[1]) * normal[1]
        if d1 >= -1e-9:
            clipped.append(p1)
        if (d1 >= -1e-9 and d2 < -1e-9) or (d1 < -1e-9 and d2 >= -1e-9):
            t = d1 / (d1 - d2)
            inter_x = p1[0] + t * (p2[0] - p1[0])
            inter_y = p1[1] + t * (p2[1] - p1[1])
            clipped.append((inter_x, inter_y))
    return clipped


def build_voronoi_cell_in_rect(points, i, bounds):
    xmin, ymin, xmax, ymax = bounds
    margin = 0.2
    cell = [
        (xmin - margin, ymin - margin),
        (xmax + margin, ymin - margin),
        (xmax + margin, ymax + margin),
        (xmin - margin, ymax + margin)
    ]
    pi = points[i]
    for j in range(len(points)):
        if i == j:
            continue
        pj = points[j]
        mid = ((pi[0] + pj[0]) / 2, (pi[1] + pj[1]) / 2)
        dx = pj[0] - pi[0]
        dy = pj[1] - pi[1]
        length = math.hypot(dx, dy)
        if length < 1e-12:
            continue
        normal = (-dx / length, -dy / length)
        cell = clip_polygon_by_halfplane(cell, mid, normal)
        if len(cell) < 3:
            break
    cell = clip_polygon_by_halfplane(cell, (xmin, 0), (1, 0))
    cell = clip_polygon_by_halfplane(cell, (xmax, 0), (-1, 0))
    cell = clip_polygon_by_halfplane(cell, (0, ymin), (0, 1))
    cell = clip_polygon_by_halfplane(cell, (0, ymax), (0, -1))
    return cell


def polygon_area_and_centroid(poly):
    n = len(poly)
    if n < 3:
        return 0.0, (0.0, 0.0)
    area = 0.0
    cx = 0.0
    cy = 0.0
    for i in range(n):
        x1, y1 = poly[i]
        x2, y2 = poly[(i + 1) % n]
        cross = x1 * y2 - x2 * y1
        area += cross
        cx += (x1 + x2) * cross
        cy += (y1 + y2) * cross
    area *= 0.5
    if abs(area) < 1e-12:
        return 0.0, (0.0, 0.0)
    cx /= (6.0 * area)
    cy /= (6.0 * area)
    return abs(area), (cx, cy)


def build_weighted_voronoi(players, bounds, target_areas=None, iterations=5000):
    """
    Универсальная функция построения взвешенного Вороного для любого числа игроков.
    Если target_areas не заданы, вычисляются пропорционально ставкам с гарантией минимума 4%.
    """
    if not players:
        return []

    n = len(players)
    if target_areas is None:
        total = sum(p["amount"] for p in players)
        if total == 0:
            return []
        # Вычисляем доли и применяем гарантию 4%
        raw = np.array([p["amount"] / total for p in players])
        adjusted = np.maximum(raw, 0.04)
        adjusted /= adjusted.sum()
        target_areas = adjusted.tolist()
    else:
        # Убедимся, что сумма target_areas равна 1
        total = sum(target_areas)
        if total == 0:
            return []
        target_areas = [a / total for a in target_areas]

    xmin, ymin, xmax, ymax = bounds
    width = xmax - xmin
    height = ymax - ymin

    # Абсолютно случайные начальные позиции
    points = np.random.rand(n, 2)
    points[:, 0] = xmin + width * (0.02 + 0.96 * points[:, 0])
    points[:, 1] = ymin + height * (0.02 + 0.96 * points[:, 1])

    prev_error = float('inf')
    for it in range(iterations):
        areas = np.zeros(n)
        centroids = [None] * n
        for i in range(n):
            cell = build_voronoi_cell_in_rect(points, i, bounds)
            if len(cell) >= 3:
                area, cent = polygon_area_and_centroid(cell)
                areas[i] = area
                centroids[i] = cent
            else:
                areas[i] = 0.0
                centroids[i] = None

        error = np.mean(np.abs(areas - target_areas))
        if error < 0.0005:
            break

        step_scale = 0.5 if error > prev_error else 1.0
        prev_error = error

        for i in range(n):
            if areas[i] <= 0 or centroids[i] is None:
                continue
            target = target_areas[i]
            if areas[i] < target:
                # увеличить площадь: точку от центроида
                direction = points[i] - np.array(centroids[i])
                norm = np.linalg.norm(direction)
                if norm > 0.001:
                    points[i] += step_scale * 0.5 * direction / norm * (1 - areas[i] / target)
            else:
                # уменьшить площадь: точку к центроиду
                direction = np.array(centroids[i]) - points[i]
                norm = np.linalg.norm(direction)
                if norm > 0.001:
                    points[i] += step_scale * 0.5 * direction / norm * (areas[i] / target - 1)

        points[:, 0] = np.clip(points[:, 0], xmin + 0.02 * width, xmax - 0.02 * width)
        points[:, 1] = np.clip(points[:, 1], ymin + 0.02 * height, ymax - 0.02 * height)

    final_polygons = []
    for i, player in enumerate(players):
        cell = build_voronoi_cell_in_rect(points, i, bounds)
        if len(cell) < 3:
            continue
        coords = [{"x": float(p[0]), "y": float(p[1])} for p in cell]
        final_polygons.append({
            "player_id": player["id"],
            "username": player["username"],
            "color": player["color"],
            "polygon": coords
        })
    return final_polygons


def weighted_voronoi_polygons(players_dict: dict) -> list:
    """
    Точка входа. Всегда использует единый алгоритм взвешенного Вороного с гарантией 4%.
    Никакой рекурсии – она была источником ошибок.
    """
    if not players_dict:
        return []
    players = list(players_dict.values())
    if not players:
        return []
    return build_weighted_voronoi(players, (0.0, 0.0, 1.0, 1.0))


# ------------------------------------------------------------
# Генерация траектории
# ------------------------------------------------------------
def generate_motion_trajectory(start_x, start_y, angle, speed, duration_ms, dt=16):
    x = start_x
    y = start_y
    vx = math.cos(angle) * speed
    vy = math.sin(angle) * speed
    frames = []
    elapsed = 0
    while elapsed < duration_ms:
        progress = elapsed / duration_ms
        if progress <= 0.5:
            speed_factor = 1 - 0.8 * (progress / 0.5)
        else:
            speed_factor = 0.2 * (1 - (progress - 0.5) / 0.5)
        step_x = vx * speed_factor * (dt / 1000)
        step_y = vy * speed_factor * (dt / 1000)
        x += step_x
        y += step_y
        if x <= 0:
            x = 0
            vx = abs(vx) * 0.9
        elif x >= 1000:
            x = 1000
            vx = -abs(vx) * 0.9
        if y <= 0:
            y = 0
            vy = abs(vy) * 0.9
        elif y >= 1000:
            y = 1000
            vy = -abs(vy) * 0.9
        frames.append({"x": x / 1000, "y": y / 1000})
        elapsed += dt
    frames.append({"x": x / 1000, "y": y / 1000})
    return frames


def generate_spin_params(polygons: list) -> dict:
    start_x = random.uniform(0.1, 0.9) * 1000
    start_y = random.uniform(0.1, 0.9) * 1000

    spin_duration = 3000
    spin_angle_speed = random.uniform(4.5 * math.pi, 13.5 * math.pi)
    spin_angle_start = random.uniform(0, 2 * math.pi)

    angle_total = 0.5 * spin_angle_speed * (spin_duration / 1000)
    final_angle = spin_angle_start + angle_total

    base_speed = random.uniform(4000, 4500)
    motion_speed = base_speed * (2.2 / 1.5)   # уменьшена в 1.5 раза

    motion_trajectory = generate_motion_trajectory(
        start_x, start_y, final_angle, motion_speed, 10000, dt=16
    )
    final_point = motion_trajectory[-1]
    target_x = final_point["x"]

    return {
        "startPos": {"x": start_x / 1000, "y": start_y / 1000},
        "spinDuration": spin_duration,
        "spinAngleStart": spin_angle_start,
        "spinAngleSpeed": spin_angle_speed,
        "trajectory": motion_trajectory,
        "target_position": target_x,
        "polygons": polygons
    }


# ------------------------------------------------------------
# Игровая механика
# ------------------------------------------------------------
PLAYER_COLORS = [
    "#FFADAD", "#FFD6A5", "#FDFFB6", "#CAFFBF", "#9BF6FF",
    "#A0C4FF", "#BDB2FF", "#FFC6FF", "#FFC09F", "#F3FFB6",
    "#B5EAD7", "#C7CEEA", "#FFDAC1", "#E2F0CB", "#B5D8FF",
    "#D0BFFF", "#FFB3C6", "#AFCBFF", "#FFC8A2", "#C1E1C1"
]


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


def point_in_polygon(point, polygon):
    x, y = point
    inside = False
    n = len(polygon)
    p1x, p1y = polygon[0]["x"], polygon[0]["y"]
    for i in range(1, n + 1):
        p2x, p2y = polygon[i % n]["x"], polygon[i % n]["y"]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    return inside


async def finish_round(final_point: dict, pool: float, players: dict, polygons: list) -> dict | None:
    if not final_point or not polygons:
        return None
    x = final_point["x"]
    y = final_point["y"]
    winner_id = None
    winner_username = None
    winner_polygon = None
    for poly in polygons:
        if point_in_polygon((x, y), poly["polygon"]):
            winner_id = poly["player_id"]
            winner_username = poly["username"]
            winner_polygon = poly["polygon"]
            break
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
                cur.execute("""
                    INSERT INTO leaderboard (user_id, username, wins) VALUES (%s, %s, 1)
                    ON CONFLICT (user_id) DO UPDATE SET wins = leaderboard.wins + 1, username = EXCLUDED.username
                """, (winner_id, winner_username))
                cur.execute(
                    "INSERT INTO game_history (game_number, winner_name, win_amount) VALUES (%s, %s, %s)",
                    (game_state["game_number"], winner_username, profit)
                )
                cur.execute(
                    "DELETE FROM game_history WHERE id NOT IN (SELECT id FROM game_history ORDER BY game_number DESC LIMIT 100)")
                cur.execute(
                    "UPDATE game_counter SET last_game_number = last_game_number + 1 WHERE id = 1 RETURNING last_game_number")
                new_num = cur.fetchone()[0]
                game_state["game_number"] = new_num
                conn.commit()
        return {
            "user_id": winner_id,
            "username": winner_username,
            "win_amount": profit,
            "photo_url": photo_url,
            "round_id": game_state["round_id"],
            "polygon": winner_polygon
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
                    # Финальная точная генерация полигонов (5000 итераций)
                    if game_state["polygons"] is None or len(game_state["players"]) != len(game_state["polygons"]):
                        game_state["polygons"] = weighted_voronoi_polygons(game_state["players"])
                    spin_params = generate_spin_params(game_state["polygons"])
                    game_state["spin_params"] = spin_params
                    game_state["polygons"] = spin_params["polygons"]
                    game_state["target_position"] = spin_params["target_position"]
                    game_state["round_id"] = random.randint(1, 10 ** 9)
                    game_state["status"] = "spinning"
                    game_state["winner"] = None
                    game_state["last_winner_id"] = None
                    logging.info("Spinning with weighted Voronoi")

        if game_state["status"] == "spinning":
            await asyncio.sleep(3 + 1 + 10 + 2 + 0.5)
            async with game_lock:
                if game_state["status"] == "spinning":
                    final_point = game_state["spin_params"]["trajectory"][-1]
                    winner_data = await finish_round(
                        final_point,
                        game_state["pool"],
                        game_state["players"],
                        game_state["polygons"]
                    )
                    game_state["winner"] = winner_data
                    game_state["last_winner_id"] = winner_data["user_id"] if winner_data else None
                    game_state["last_polygons"] = game_state["polygons"]
                    game_state["status"] = "waiting"
                    game_state["players"] = {}
                    game_state["pool"] = 0.0
                    game_state["timer"] = 15
                    game_state["polygons"] = None
                    logging.info(f"Round finished, winner: {winner_data}")


# ------------------- API (без изменений) -------------------
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
                cur.execute(
                    "SELECT id, name, price, status, image_url, nft_link, traits, number FROM items WHERE status = 'Доступен'")
                items = cur.fetchall()
                for item in items:
                    if isinstance(item.get('traits'), str):
                        try:
                            item['traits'] = json.loads(item['traits'])
                        except:
                            item['traits'] = []
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
                    if item['status'] in ('Выведен', 'withdrawn'): item['status'] = 'withdrawn'
                    if isinstance(item.get('traits'), str):
                        try:
                            item['traits'] = json.loads(item['traits'])
                        except:
                            item['traits'] = []
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
            return web.json_response({"success": False, "error": "no_items"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, price FROM items WHERE id = ANY(%s) AND status = 'Доступен'", (item_ids,))
                items = cur.fetchall()
                if len(items) != len(item_ids):
                    return web.json_response({"success": False, "error": "items_unavailable"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                total_price = sum(i['price'] for i in items)
                cur.execute("SELECT balance FROM users WHERE id = %s", (user_id,))
                db_user = cur.fetchone()
                if not db_user or float(db_user['balance']) < float(total_price):
                    return web.json_response({"success": False, "error": "insufficient_funds"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                cur.execute("UPDATE users SET balance = balance - %s WHERE id = %s", (total_price, user_id))
                cur.execute(
                    "UPDATE items SET status = 'Продан', buyer_id = %s, last_event = 'approved' WHERE id = ANY(%s)",
                    (user_id, item_ids))
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
                cur.execute(
                    "SELECT id, name, nft_link FROM items WHERE id = %s AND buyer_id = %s AND status = 'Продан'",
                    (item_id, user_id))
                item = cur.fetchone()
                if not item:
                    return web.json_response({"success": False, "error": "not_found"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                cur.execute(
                    "UPDATE items SET status = 'pending_withdraw', last_event = 'withdraw_requested' WHERE id = %s",
                    (item_id,))
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
        # Отдаём last_polygons, если активных полигонов нет
        polys = game_state.get("polygons") or game_state.get("last_polygons")
        resp = {
            "status": game_state["status"],
            "players": sorted_players,
            "pool": game_state["pool"],
            "timer": game_state["timer"],
            "spin_params": game_state.get("spin_params"),
            "winner": game_state.get("winner"),
            "last_winner_id": game_state.get("last_winner_id"),
            "round_id": game_state.get("round_id"),
            "game_number": game_state.get("game_number", 0),
            "polygons": polys
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
            return web.json_response({"success": False, "error": "min_bet"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        if amount != int(amount):
            return web.json_response({"success": False, "error": "invalid_amount"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        async with game_lock:
            if game_state["status"] not in ("waiting", "counting"):
                return web.json_response({"success": False, "error": "game_started"},
                                         headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
            if len(game_state["players"]) >= 20 and user_id not in game_state["players"]:
                return web.json_response({"success": False, "error": "room_full"},
                                         headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
            with get_db_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT balance FROM users WHERE id = %s", (user_id,))
                    db_user = cur.fetchone()
                    if not db_user or float(db_user['balance']) < amount:
                        return web.json_response({"success": False, "error": "insufficient_funds"},
                                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                    cur.execute("UPDATE users SET balance = balance - %s WHERE id = %s", (amount, user_id))
                    conn.commit()
            if user_id in game_state["players"]:
                game_state["players"][user_id]["amount"] += amount
            else:
                occupied_colors = {p["color"] for p in game_state["players"].values()}
                available = [c for c in PLAYER_COLORS if c not in occupied_colors]
                if not available:
                    available = ["#" + ''.join(random.choices('0123456789ABCDEF', k=6))]
                color = random.choice(available)
                game_state["players"][user_id] = {
                    "id": user_id, "username": username,
                    "amount": amount, "color": color
                }
            game_state["pool"] += amount

            # При первой ставке сбрасываем прошлые полигоны
            if len(game_state["players"]) == 1:
                game_state["last_polygons"] = None

            # Пересчитываем полигоны с небольшим числом итераций для быстроты
            game_state["polygons"] = build_weighted_voronoi(
                list(game_state["players"].values()),
                (0.0, 0.0, 1.0, 1.0),
                iterations=300   # быстрая предварительная генерация
            )

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
                game_state["polygons"] = None
                game_state["last_polygons"] = None
                game_state["status"] = "waiting"
                return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        return web.json_response({"success": False, "error": "cannot_cancel"},
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Cancel error: {e}")
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


async def handle_game_finish(request):
    return web.json_response({"success": True, "message": "Server handles finish"},
                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


@require_auth
async def handle_game_history(request):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT game_number, winner_name, win_amount FROM game_history ORDER BY game_number DESC LIMIT 100")
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
    user_id = request['telegram_user']['id']
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT username, wins FROM leaderboard ORDER BY wins DESC LIMIT 5")
                top_rows = cur.fetchall()
                cur.execute("SELECT username, wins FROM leaderboard WHERE user_id = %s", (user_id,))
                user_row = cur.fetchone()
        result = {"top": [], "user": None}
        for r in top_rows:
            result["top"].append({"username": r["username"], "wins": r["wins"]})
        if user_row:
            result["user"] = {"username": user_row["username"], "wins": user_row["wins"]}
        return web.json_response(result, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Leaderboard error: {e}")
        return web.json_response({"top": [], "user": None}, status=500,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


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
                    return web.json_response({"end_time": end_time.timestamp() * 1000},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        return web.json_response({"end_time": None}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        return web.json_response({"end_time": None}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


async def handle_get_prize_items(request):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, name, image_url, nft_link, traits FROM prize_items ORDER BY id")
                items = cur.fetchall()
                for item in items:
                    traits = item.get('traits')
                    if isinstance(traits, str):
                        try:
                            item['traits'] = json.loads(traits)
                        except:
                            item['traits'] = []
        return web.json_response(items, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Prize items error: {e}")
        return web.json_response([], status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


@require_auth
async def handle_get_requisites(request):
    return web.json_response({"req": PAYMENT_REQUISITES}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


# ---------- Админ-коллбэки ----------
@dp.callback_query(F.data.startswith("topup_yes_"))
async def admin_topup_approve(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 4: return
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


@dp.callback_query(F.data.startswith("topup_no_"))
async def admin_topup_reject(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 4: return
    _, _, uid, amount = parts
    await callback.message.edit_text(f"❌ Заявка на {amount} ₽ отклонена.")
    try:
        await bot.send_message(uid, f"❌ Заявка на пополнение {amount} ₽ отклонена.")
    except:
        pass


@dp.callback_query(F.data.startswith("with_yes_"))
async def admin_withdraw_approve(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 4: return
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


@dp.callback_query(F.data.startswith("with_no_"))
async def admin_withdraw_reject(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) != 4: return
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


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✨ Магазин", web_app=WebAppInfo(url=WEBAPP_URL))]])
    await message.answer("Добро пожаловать в DNX Store!", reply_markup=kb)


# ---------- Настройка сервера ----------
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
app.router.add_get('/prize/items', handle_get_prize_items)
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