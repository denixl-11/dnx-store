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
    "last_polygons": None
}


def init_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Пользователи
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id VARCHAR(255) PRIMARY KEY,
                        username VARCHAR(255),
                        balance NUMERIC DEFAULT 0.0
                    )
                """)
                # Предметы
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS items (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255),
                        price NUMERIC DEFAULT 0.0,
                        status VARCHAR(50) DEFAULT 'Доступен',
                        image_url TEXT,
                        nft_link TEXT DEFAULT '',
                        traits JSONB DEFAULT '[]'::jsonb,
                        buyer_id VARCHAR(255),
                        number VARCHAR(20),
                        last_event VARCHAR(50)
                    )
                """)
                # Игровая история
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS game_history (
                        id SERIAL PRIMARY KEY,
                        game_number INT,
                        winner_name TEXT,
                        win_amount NUMERIC,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("ALTER TABLE game_history ADD COLUMN IF NOT EXISTS win_percent NUMERIC DEFAULT 0")
                # Счётчик игр
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

                # Лидерборд
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS leaderboard (
                        user_id VARCHAR(255) PRIMARY KEY,
                        username VARCHAR(255),
                        wins INT DEFAULT 0
                    )
                """)
                # Сезон
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS season (
                        id INT PRIMARY KEY DEFAULT 1,
                        end_time TIMESTAMPTZ
                    )
                """)
                cur.execute(
                    "INSERT INTO season (id, end_time) VALUES (1, '2026-06-30 15:00:00+00') ON CONFLICT (id) DO NOTHING")
                # Призовые предметы
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS prize_items (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        image_url TEXT NOT NULL,
                        nft_link TEXT NOT NULL DEFAULT '',
                        traits JSONB DEFAULT '[]'::jsonb
                    )
                """)
                # Кейсы
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cases (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255),
                        price NUMERIC DEFAULT 0.0,
                        image_url TEXT
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS case_drops (
                        id SERIAL PRIMARY KEY,
                        case_id INTEGER REFERENCES cases(id),
                        name VARCHAR(255),
                        image_url TEXT,
                        nft_link TEXT DEFAULT '',
                        chance NUMERIC,
                        value NUMERIC DEFAULT 0.0
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
#  ГЕОМЕТРИЯ: BSP RECURSIVE POLYGON CLIPPING (TRIANGLES)
# ------------------------------------------------------------
def adjust_weights_to_minimum(weights, min_ratio=0.02):
    N = len(weights)
    if N == 0: return []
    if N == 1: return [1.0]

    if N * min_ratio > 1.0:
        min_ratio = 1.0 / N

    total = sum(weights)
    if total == 0: return [1.0 / N] * N

    ratios = [w / total for w in weights]

    while True:
        deficient = [i for i, r in enumerate(ratios) if r < min_ratio]
        if not deficient:
            break

        for i in deficient:
            ratios[i] = min_ratio

        surplus_indices = [i for i, r in enumerate(ratios) if r > min_ratio]
        if not surplus_indices:
            break

        total_surplus_weight = sum(weights[i] for i in surplus_indices)
        remaining_ratio = 1.0 - len(deficient) * min_ratio

        if total_surplus_weight > 0:
            for i in surplus_indices:
                ratios[i] = (weights[i] / total_surplus_weight) * remaining_ratio
        else:
            for i in surplus_indices:
                ratios[i] = remaining_ratio / len(surplus_indices)

    return ratios


def get_polygon_area(poly):
    area = 0.0
    n = len(poly)
    if n < 3: return 0.0
    for i in range(n):
        area += (poly[i][0] * poly[(i + 1) % n][1] - poly[(i + 1) % n][0] * poly[i][1])
    return abs(area) * 0.5


def split_polygon_by_line(poly, pt, normal):
    poly1, poly2 = [], []
    n = len(poly)
    dists = [(p[0] - pt[0]) * normal[0] + (p[1] - pt[1]) * normal[1] for p in poly]

    for i in range(n):
        p1, p2 = poly[i], poly[(i + 1) % n]
        d1, d2 = dists[i], dists[(i + 1) % n]

        if d1 >= -1e-9: poly1.append(p1)
        if d1 <= 1e-9: poly2.append(p1)

        if (d1 > 1e-9 and d2 < -1e-9) or (d1 < -1e-9 and d2 > 1e-9):
            t = d1 / (d1 - d2)
            inter = (p1[0] + t * (p2[0] - p1[0]), p1[1] + t * (p2[1] - p1[1]))
            poly1.append(inter)
            poly2.append(inter)

    def clean_poly(p_list):
        if not p_list: return []
        res = [p_list[0]]
        for p in p_list[1:]:
            if math.hypot(p[0] - res[-1][0], p[1] - res[-1][1]) > 1e-9:
                res.append(p)
        if len(res) > 1 and math.hypot(res[0][0] - res[-1][0], res[0][1] - res[-1][1]) <= 1e-9:
            res.pop()
        return res

    return clean_poly(poly1), clean_poly(poly2)


def clip_polygon_exact_area(poly, target_ratio, angle):
    total_area = get_polygon_area(poly)
    if total_area < 1e-12:
        return poly, []

    target = total_area * target_ratio
    nx, ny = math.cos(angle), math.sin(angle)
    projs = [p[0] * nx + p[1] * ny for p in poly]
    low, high = min(projs), max(projs)

    for _ in range(50):
        mid = (low + high) / 2.0
        p1, p2 = split_polygon_by_line(poly, (mid * nx, mid * ny), (nx, ny))
        if get_polygon_area(p1) < target:
            high = mid
        else:
            low = mid

    mid = (low + high) / 2.0
    return split_polygon_by_line(poly, (mid * nx, mid * ny), (nx, ny))


def recursive_bsp_split(poly, players, weights, depth=0):
    if len(players) == 1:
        return [{"player": players[0], "polygon": poly}]

    half = len(players) // 2
    w_left = sum(weights[:half])
    w_total = sum(weights)
    ratio = w_left / w_total if w_total > 0 else 0.5

    if not poly or len(poly) < 3:
        res = []
        res.extend(recursive_bsp_split([], players[:half], weights[:half], depth + 1))
        res.extend(recursive_bsp_split([], players[half:], weights[half:], depth + 1))
        return res

    if depth == 0:
        base_angle = random.choice([math.pi / 4, 3 * math.pi / 4])
        angle = base_angle + random.uniform(-0.2, 0.2)
    else:
        angle = random.uniform(0, math.pi)

    poly_left, poly_right = clip_polygon_exact_area(poly, ratio, angle)

    res = []
    res.extend(recursive_bsp_split(poly_left, players[:half], weights[:half], depth + 1))
    res.extend(recursive_bsp_split(poly_right, players[half:], weights[half:], depth + 1))

    return res


def build_weighted_voronoi(players, bounds, target_areas=None, iterations=0):
    if not players: return []

    sorted_players = sorted(players, key=lambda p: float(p["amount"]), reverse=True)
    total_real = sum(float(p["amount"]) for p in players)
    visual_amounts = [
        float(p["amount"]) + p.get("bets_count", 0) * 0.0001 * total_real
        for p in sorted_players
    ]
    weights = adjust_weights_to_minimum(visual_amounts, min_ratio=0.02)

    xmin, ymin, xmax, ymax = bounds
    root_poly = [(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)]

    polygons_data = recursive_bsp_split(root_poly, sorted_players, weights, depth=0)

    final_polygons = []
    for item in polygons_data:
        player = item["player"]
        poly = item["polygon"]
        coords = [{"x": float(p[0]), "y": float(p[1])} for p in poly]
        final_polygons.append({
            "player_id": player["id"],
            "username": player["username"],
            "color": player["color"],
            "photo_url": player.get("photo_url"),
            "polygon": coords
        })
    return final_polygons


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

    BALL_RADIUS = 25

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

        if x <= BALL_RADIUS:
            x = BALL_RADIUS
            vx = abs(vx) * 0.9
        elif x >= 1000 - BALL_RADIUS:
            x = 1000 - BALL_RADIUS
            vx = -abs(vx) * 0.9

        if y <= BALL_RADIUS:
            y = BALL_RADIUS
            vy = abs(vy) * 0.9
        elif y >= 1000 - BALL_RADIUS:
            y = 1000 - BALL_RADIUS
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
    motion_speed = base_speed * (2.2 / 1.5)

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
    ("#7F00FF", "#E100FF"),
    ("#FF416C", "#FF4B2B"),
    ("#00C6FF", "#0072FF"),
    ("#11998E", "#38EF7D"),
    ("#F2994A", "#F2C94C"),
    ("#4FACFE", "#00F2FE"),
    ("#FF8C00", "#E0115F"),
    ("#757F9A", "#D7DDE8"),
    ("#00FF87", "#60EFFF"),
    ("#A8BFFF", "#884AF6")
]


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
    photo_url = None

    for poly in polygons:
        if point_in_polygon((x, y), poly["polygon"]):
            winner_id = poly["player_id"]
            winner_username = poly["username"]
            winner_polygon = poly["polygon"]
            photo_url = poly.get("photo_url")
            break

    if not winner_id:
        return None

    winner_bet = players[winner_id]["amount"]
    others_bets = pool - winner_bet
    profit = winner_bet + (others_bets * 0.7)

    if pool > 0:
        win_percent = round((winner_bet / pool) * 100, 1)
    else:
        win_percent = 100.0

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
                    "INSERT INTO game_history (game_number, winner_name, win_amount, win_percent) VALUES (%s, %s, %s, %s)",
                    (game_state["game_number"], winner_username, profit, win_percent)
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


async def clear_last_polygons_after_delay(delay=0.3):
    await asyncio.sleep(delay)
    async with game_lock:
        if game_state["status"] == "waiting" and not game_state["players"]:
            game_state["last_polygons"] = None


async def game_worker():
    global game_state
    while True:
        await asyncio.sleep(1)
        async with game_lock:
            if game_state["status"] == "counting":
                game_state["timer"] -= 1
                if game_state["timer"] <= 0:

                    if not game_state.get("polygons"):
                        game_state["polygons"] = build_weighted_voronoi(
                            list(game_state["players"].values()),
                            (0.0, 0.0, 1.0, 1.0)
                        )

                    spin_params = generate_spin_params(game_state["polygons"])
                    game_state["spin_params"] = spin_params
                    game_state["target_position"] = spin_params["target_position"]
                    game_state["round_id"] = random.randint(1, 10 ** 9)
                    game_state["status"] = "spinning"
                    game_state["winner"] = None
                    game_state["last_winner_id"] = None

        if game_state["status"] == "spinning":
            await asyncio.sleep(3 + 1 + 10 + 1 + 0.5)
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
                    asyncio.create_task(clear_last_polygons_after_delay(0.3))


# ------------------- API -------------------
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
                game_state["players"][user_id]["bets_count"] += 1
            else:
                occupied_colors = {p["color"] for p in game_state["players"].values()}
                available = [c for c in PLAYER_COLORS if c not in occupied_colors]
                if not available:
                    available = [(
                        "#" + ''.join(random.choices('0123456789ABCDEF', k=6)),
                        "#" + ''.join(random.choices('0123456789ABCDEF', k=6))
                    )]
                color = random.choice(available)
                photo_url = data.get('photo_url', '')
                game_state["players"][user_id] = {
                    "id": user_id, "username": username,
                    "amount": amount, "color": color,
                    "bets_count": 1,
                    "photo_url": photo_url
                }
            game_state["pool"] += amount

            if len(game_state["players"]) == 1:
                game_state["last_polygons"] = None

            game_state["polygons"] = build_weighted_voronoi(
                list(game_state["players"].values()),
                (0.0, 0.0, 1.0, 1.0)
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
                    "SELECT game_number, winner_name, win_amount, win_percent FROM game_history ORDER BY game_number DESC LIMIT 100")
                rows = cur.fetchall()
        result = []
        for row in rows:
            result.append({
                "game_number": row["game_number"],
                "winner_name": row["winner_name"],
                "win_amount": float(row["win_amount"]),
                "win_percent": float(row.get("win_percent", 0))
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


# ------------------------------------------------------------
#  КЕЙСЫ – новые эндпоинты
# ------------------------------------------------------------
async def handle_get_cases(request):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, name, price, image_url FROM cases")
            cases = cur.fetchall()
    return web.json_response(cases, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


async def handle_get_case_details(request):
    case_id = request.query.get('id')
    if not case_id:
        return web.json_response({"error": "missing_id"}, status=400,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, name, price, image_url FROM cases WHERE id = %s", (case_id,))
            case = cur.fetchone()
            if not case:
                return web.json_response({"error": "case_not_found"}, status=404,
                                         headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
            cur.execute("SELECT id, name, image_url, chance, value FROM case_drops WHERE case_id = %s", (case_id,))
            drops = cur.fetchall()
            case['drops'] = drops
    return web.json_response(case, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


@require_auth
async def handle_open_case(request):
    try:
        data = await request.json()
        user = request['telegram_user']
        user_id = user['id']
        case_id = data.get('caseId')

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT price FROM cases WHERE id = %s", (case_id,))
                case = cur.fetchone()
                if not case:
                    return web.json_response({"success": False, "error": "case_not_found"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

                cur.execute("SELECT balance FROM users WHERE id = %s", (user_id,))
                db_user = cur.fetchone()
                if not db_user or float(db_user['balance']) < float(case['price']):
                    return web.json_response({"success": False, "error": "insufficient_funds"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

                cur.execute("SELECT * FROM case_drops WHERE case_id = %s", (case_id,))
                drops = cur.fetchall()
                if not drops:
                    return web.json_response({"success": False, "error": "empty_case"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

                total_chance = sum(float(drop['chance']) for drop in drops)
                rand_val = random.uniform(0, total_chance)
                current_sum = 0
                won_drop = drops[-1]
                for drop in drops:
                    current_sum += float(drop['chance'])
                    if rand_val <= current_sum:
                        won_drop = drop
                        break

                cur.execute("UPDATE users SET balance = balance - %s WHERE id = %s", (case['price'], user_id))
                cur.execute(
                    "INSERT INTO items (name, price, status, image_url, nft_link, buyer_id, last_event) VALUES (%s, %s, 'Продан', %s, %s, %s, 'case_drop') RETURNING id",
                    (won_drop['name'], won_drop['value'], won_drop['image_url'], won_drop['nft_link'], user_id))
                new_item_id = cur.fetchone()['id']
                won_drop['generated_item_id'] = new_item_id
                conn.commit()

        return web.json_response({"success": True, "won_item": won_drop},
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Case open error: {e}")
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


# ---------- Админ-коллбэки ----------
@dp.callback_query(F.data.startswith("topup_yes_"))
async def admin_topup_approve(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    _, _, uid, amount = parts
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (id, balance) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET balance = users.balance + EXCLUDED.balance",
                (uid, float(amount)))
            conn.commit()
    await callback.message.edit_text(f"✅ Баланс пополнен на {amount} ₽!")
    try:
        await bot.send_message(int(uid), f"💰 Ваш баланс пополнен на {amount} ₽!")
    except:
        pass


@dp.callback_query(F.data.startswith("topup_no_"))
async def admin_topup_reject(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    _, _, uid, amount = parts
    await callback.message.edit_text(f"❌ Заявка на {amount} ₽ отклонена.")
    try:
        await bot.send_message(int(uid), f"❌ Заявка на пополнение {amount} ₽ отклонена.")
    except:
        pass


@dp.callback_query(F.data.startswith("with_yes_"))
async def admin_withdraw_approve(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    _, _, uid, item_id = parts
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE items SET status = 'withdrawn', last_event = 'withdraw_approved' WHERE id = %s AND buyer_id = %s",
                (int(item_id), uid))
            conn.commit()
    await callback.message.edit_text(f"{callback.message.text}\n\n✅ **ВЫВОД ПОДТВЕРЖДЕН**")
    try:
        await bot.send_message(int(uid), f"🎉 NFT (ID: {item_id}) выведен!")
    except:
        pass


@dp.callback_query(F.data.startswith("with_no_"))
async def admin_withdraw_reject(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    _, _, uid, item_id = parts
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE items SET status = 'Продан', last_event = 'withdraw_rejected' WHERE id = %s AND buyer_id = %s",
                (int(item_id), uid))
            conn.commit()
    await callback.message.edit_text(f"{callback.message.text}\n\n❌ **ВЫВОД ОТКЛОНЕН**")
    try:
        await bot.send_message(int(uid), f"❌ Вывод NFT (ID: {item_id}) отклонён.")
    except:
        pass


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
app.router.add_get('/cases', handle_get_cases)
app.router.add_get('/case-details', handle_get_case_details)
app.router.add_post('/open-case', handle_open_case)
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