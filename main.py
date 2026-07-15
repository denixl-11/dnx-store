import asyncio
import hashlib
import hmac
import logging
import os
import json
import random
import math
import time
import secrets
import uuid
from collections import defaultdict, deque
from decimal import Decimal, InvalidOperation
from functools import wraps
from urllib.parse import parse_qsl, urlencode, urlparse, urlsplit, urlunsplit
from datetime import timezone

import asyncpg
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://denixl-11.github.io/dnx-store/")
CORS_ORIGIN = os.getenv("CORS_ORIGIN", "https://denixl-11.github.io")
DATABASE_URL = os.getenv("DATABASE_URL")
INIT_DATA_MAX_AGE = int(os.getenv("INIT_DATA_MAX_AGE", "86400"))
DB_POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN_SIZE", "1"))
DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", "10"))
TON_RECEIVER_ADDRESS = os.getenv("TON_RECEIVER_ADDRESS", "").strip()
TONCENTER_API_URL = os.getenv("TONCENTER_API_URL", "https://toncenter.com/api/v2").rstrip("/")
TONCENTER_API_KEY = os.getenv("TONCENTER_API_KEY", "").strip()
TON_MIN_DEPOSIT = Decimal(os.getenv("TON_MIN_DEPOSIT", "0.05"))
TON_DEPOSIT_TIMEOUT = int(os.getenv("TON_DEPOSIT_TIMEOUT", "900"))
TON_MIN_BET = Decimal(os.getenv("TON_MIN_BET", "0.1"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not DATABASE_URL and not DB_PASSWORD:
    raise RuntimeError("DATABASE_URL or DB_PASSWORD is required")

DB_CONFIG = {
    "database": "neondb",
    "user": "neondb_owner",
    "password": DB_PASSWORD,
    "host": "ep-shy-sun-an8be4el.c-6.us-east-1.aws.neon.tech",
    "port": 5432,
    "ssl": "require"
}

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
db_pool: asyncpg.Pool | None = None
ton_http_session: aiohttp.ClientSession | None = None

# ======================== ГЛОБАЛЬНЫЙ КЕШ КЕЙСОВ ========================
# Загружается из переменной окружения CASES_JSON (хранится в Render).
# Если переменная не задана, используется демо-кейс (для тестов).
def load_cases_from_env():
    global CASES_CACHE
    raw = os.getenv("CASES_JSON")
    if raw:
        try:
            CASES_CACHE = json.loads(raw)
            # Преобразуем ключи-строки в int (JSON всегда строковые ключи)
            CASES_CACHE = {int(k): v for k, v in CASES_CACHE.items()}
            logging.info(f"✅ Загружено {len(CASES_CACHE)} кейсов из CASES_JSON")
        except Exception as e:
            logging.error(f"❌ Ошибка парсинга CASES_JSON: {e}")
            CASES_CACHE = {}
    else:
        logging.warning("⚠️ CASES_JSON не задан. Используется демо-кейс.")
        # Демо-кейс (замените на свои данные после заполнения переменной в Render)
        CASES_CACHE = {
            1: {
                "id": 1,
                "name": "Демо-кейс",
                "price": 0.5,
                "image_url": "https://via.placeholder.com/150",
                "drops": [
                    {
                        "id": 1,
                        "case_id": 1,
                        "name": "Демо-скин",
                        "image_url": "https://via.placeholder.com/100",
                        "model": "Demo",
                        "chance": 100.0,
                        "value": 0.25,
                        "real_chance": 100.0
                    }
                ]
            }
        }

load_cases_from_env()   # заполнили CASES_CACHE

async def create_db_pool() -> asyncpg.Pool:
    if DATABASE_URL:
        dsn = normalize_database_url(DATABASE_URL)
        return await asyncpg.create_pool(
            dsn=dsn,
            min_size=DB_POOL_MIN_SIZE,
            max_size=DB_POOL_MAX_SIZE,
            command_timeout=15,
            max_inactive_connection_lifetime=300,
        )
    return await asyncpg.create_pool(
        **DB_CONFIG,
        min_size=DB_POOL_MIN_SIZE,
        max_size=DB_POOL_MAX_SIZE,
        command_timeout=15,
        max_inactive_connection_lifetime=300,
    )


def normalize_database_url(value: str) -> str:
    """Remove libpq-only options that asyncpg would send as PostgreSQL settings."""
    parts = urlsplit(value)
    query = [(key, item) for key, item in parse_qsl(parts.query, keep_blank_values=True)
             if key.lower() != "channel_binding"]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def get_pool() -> asyncpg.Pool:
    if db_pool is None:
        raise RuntimeError("Database pool is not initialized")
    return db_pool


def get_ton_session() -> aiohttp.ClientSession:
    if ton_http_session is None:
        raise RuntimeError("TON HTTP session is not initialized")
    return ton_http_session


def ton_api_headers() -> dict[str, str]:
    return {"X-API-Key": TONCENTER_API_KEY} if TONCENTER_API_KEY else {}


async def normalize_ton_address(address: str) -> str:
    if not isinstance(address, str) or not 20 <= len(address) <= 100:
        raise ValueError("invalid TON address")
    async with get_ton_session().get(
        f"{TONCENTER_API_URL}/detectAddress",
        params={"address": address},
        headers=ton_api_headers(),
    ) as response:
        payload = await response.json(content_type=None)
        if response.status != 200 or not payload.get("ok"):
            raise ValueError("TON address was not recognized")
        raw = payload.get("result", {}).get("raw_form")
        if not raw:
            raise ValueError("TON API did not return a raw address")
        return str(raw).lower()

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

async def init_db():
    try:
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id VARCHAR(255) PRIMARY KEY,
                        username VARCHAR(255),
                        balance NUMERIC DEFAULT 0.0
                    )
                """)
                await conn.execute("""
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
                await conn.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS model VARCHAR(255) DEFAULT ''")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS game_history (
                        id SERIAL PRIMARY KEY,
                        game_number INT,
                        winner_name TEXT,
                        win_amount NUMERIC,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                await conn.execute("ALTER TABLE game_history ADD COLUMN IF NOT EXISTS win_percent NUMERIC DEFAULT 0")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS game_counter (
                        id INT PRIMARY KEY DEFAULT 1,
                        last_game_number INT NOT NULL DEFAULT 0
                    )
                """)
                await conn.execute("INSERT INTO game_counter (id, last_game_number) VALUES (1, 0) ON CONFLICT (id) DO NOTHING")
                last_num = await conn.fetchval("SELECT last_game_number FROM game_counter WHERE id = 1")
                game_state["game_number"] = last_num

                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS leaderboard (
                        user_id VARCHAR(255) PRIMARY KEY,
                        username VARCHAR(255),
                        wins INT DEFAULT 0
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS season (
                        id INT PRIMARY KEY DEFAULT 1,
                        end_time TIMESTAMPTZ
                    )
                """)
                await conn.execute(
                    "INSERT INTO season (id, end_time) VALUES (1, '2026-06-30 15:00:00+00') ON CONFLICT (id) DO NOTHING")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS prize_items (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        image_url TEXT NOT NULL,
                        nft_link TEXT NOT NULL DEFAULT '',
                        traits JSONB DEFAULT '[]'::jsonb
                    )
                """)
                # Таблицы cases и case_drops больше не нужны для чтения, но оставим для совместимости и администрирования
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS cases (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255),
                        price NUMERIC DEFAULT 0.0,
                        image_url TEXT
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS case_drops (
                        id SERIAL PRIMARY KEY,
                        case_id INTEGER REFERENCES cases(id),
                        name VARCHAR(255),
                        image_url TEXT,
                        model VARCHAR(255) DEFAULT '',
                        chance NUMERIC,
                        value NUMERIC DEFAULT 0.0
                    )
                """)
                await conn.execute("ALTER TABLE case_drops ADD COLUMN IF NOT EXISTS real_chance NUMERIC DEFAULT 0")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS ton_deposits (
                        id UUID PRIMARY KEY,
                        user_id VARCHAR(255) NOT NULL,
                        wallet_address TEXT NOT NULL,
                        wallet_raw TEXT NOT NULL,
                        amount_nano BIGINT NOT NULL UNIQUE,
                        amount_ton NUMERIC(20, 9) NOT NULL,
                        status VARCHAR(32) NOT NULL DEFAULT 'pending',
                        tx_hash TEXT UNIQUE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        expires_at TIMESTAMPTZ NOT NULL,
                        credited_at TIMESTAMPTZ
                    )
                """)
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_items_status ON items(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_items_buyer_status ON items(buyer_id, status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_game_history_number ON game_history(game_number DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_ton_deposits_pending ON ton_deposits(status, expires_at)")
                logging.info(f"DB initialized. Game number: {last_num}")
    except Exception as e:
        logging.error(f"DB Init Error: {e}")
        raise

def extract_user_from_initdata(init_data_str: str) -> dict | None:
    if not init_data_str:
        return None
    try:
        pairs = dict(parse_qsl(init_data_str, keep_blank_values=True))
        received_hash = pairs.pop("hash", None)
        if not received_hash:
            return None
        data_check_string = "\n".join(f"{key}={pairs[key]}" for key in sorted(pairs))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        expected_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected_hash, received_hash):
            return None
        auth_date = int(pairs.get("auth_date", "0"))
        now = int(time.time())
        if auth_date <= 0 or auth_date > now + 60 or now - auth_date > INIT_DATA_MAX_AGE:
            return None
        user = json.loads(pairs.get("user", "{}"))
        user_id = user.get("id")
        if not isinstance(user_id, int) or user_id <= 0:
            return None
        username = user.get("username") or user.get("first_name") or "Unknown"
        return {
            "id": str(user_id),
            "username": str(username)[:255],
            "photo_url": safe_https_url(user.get("photo_url")),
        }
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def safe_https_url(value) -> str:
    if not isinstance(value, str) or len(value) > 2048:
        return ""
    parsed = urlparse(value)
    return value if parsed.scheme == "https" and parsed.netloc else ""


def parse_positive_amount(value, *, minimum=Decimal("0.01"), maximum=Decimal("1000000")) -> Decimal | None:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not amount.is_finite() or amount < minimum or amount > maximum:
        return None
    return amount


def normalize_records(rows) -> list[dict]:
    result = []
    for row in rows:
        item = dict(row)
        for key, value in list(item.items()):
            if isinstance(value, Decimal):
                item[key] = float(value)
        if isinstance(item.get("traits"), str):
            try:
                item["traits"] = json.loads(item["traits"])
            except json.JSONDecodeError:
                item["traits"] = []
        result.append(item)
    return result

def require_auth(handler):
    @wraps(handler)
    async def wrapper(request):
        if request.method == "OPTIONS":
            return await handler(request)
        init_data = request.headers.get('X-Telegram-Init-Data')
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


_rate_limit_events: dict[tuple[str, str], deque] = defaultdict(deque)


def rate_limit(limit: int, window_seconds: int):
    def decorator(handler):
        @wraps(handler)
        async def wrapper(request):
            user = request.get('telegram_user')
            identity = user['id'] if user else (request.remote or 'unknown')
            key = (handler.__name__, identity)
            now = time.monotonic()
            events = _rate_limit_events[key]
            while events and now - events[0] >= window_seconds:
                events.popleft()
            if len(events) >= limit:
                return web.json_response(
                    {"success": False, "error": "rate_limited"},
                    status=429,
                    headers={"Access-Control-Allow-Origin": CORS_ORIGIN},
                )
            events.append(now)
            return await handler(request)
        return wrapper
    return decorator

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

def get_centroid_and_safe_radius(poly, target_ratio=0.22):
    n = len(poly)
    if n < 3:
        return 0.0, 0.0, 0.0

    area = 0.0
    cx = 0.0
    cy = 0.0

    for i in range(n):
        p1 = poly[i]
        p2 = poly[(i + 1) % n]
        cross = p1[0] * p2[1] - p2[0] * p1[1]
        area += cross
        cx += (p1[0] + p2[0]) * cross
        cy += (p1[1] + p2[1]) * cross

    signed_area = area * 0.5
    abs_area = abs(signed_area)

    if abs_area < 1e-9:
        return poly[0][0], poly[0][1], 0.0

    cx /= (6.0 * signed_area)
    cy /= (6.0 * signed_area)

    target_radius = math.sqrt((abs_area * target_ratio) / math.pi)

    def point_line_dist(pt, v, w):
        l2 = (w[0] - v[0])**2 + (w[1] - v[1])**2
        if l2 == 0:
            return math.hypot(pt[0] - v[0], pt[1] - v[1])
        t = max(0.0, min(1.0, ((pt[0] - v[0]) * (w[0] - v[0]) + (pt[1] - v[1]) * (w[1] - v[1])) / l2))
        proj_x = v[0] + t * (w[0] - v[0])
        proj_y = v[1] + t * (w[1] - v[1])
        return math.hypot(pt[0] - proj_x, pt[1] - proj_y)

    min_dist = min(point_line_dist((cx, cy), poly[i], poly[(i + 1) % n]) for i in range(n))
    safe_radius = min(target_radius, min_dist * 0.90)

    return cx, cy, safe_radius

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

        cx, cy, avatar_radius = get_centroid_and_safe_radius(poly, target_ratio=0.22)

        final_polygons.append({
            "player_id": player["id"],
            "username": player["username"],
            "color": player["color"],
            "photo_url": player.get("photo_url"),
            "polygon": coords,
            "center": {"x": cx, "y": cy},
            "avatar_radius": avatar_radius
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
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                if not await conn.fetchval("SELECT id FROM users WHERE id = $1", winner_id):
                    return None
                await conn.execute(
                    "UPDATE users SET balance = balance + $1 WHERE id = $2",
                    Decimal(str(profit)), winner_id)
                await conn.execute("""
                    INSERT INTO leaderboard (user_id, username, wins) VALUES ($1, $2, 1)
                    ON CONFLICT (user_id) DO UPDATE SET wins = leaderboard.wins + 1, username = EXCLUDED.username
                """, winner_id, winner_username)
                await conn.execute(
                    "INSERT INTO game_history (game_number, winner_name, win_amount, win_percent) VALUES ($1, $2, $3, $4)",
                    game_state["game_number"], winner_username,
                    Decimal(str(profit)), Decimal(str(win_percent))
                )
                await conn.execute(
                    "DELETE FROM game_history WHERE id NOT IN (SELECT id FROM game_history ORDER BY game_number DESC LIMIT 100)")
                new_num = await conn.fetchval(
                    "UPDATE game_counter SET last_game_number = last_game_number + 1 WHERE id = 1 RETURNING last_game_number")
                game_state["game_number"] = new_num
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


async def scan_ton_deposits() -> int:
    if not TON_RECEIVER_ADDRESS:
        return 0
    await get_pool().execute(
        "UPDATE ton_deposits SET status = 'expired' WHERE status = 'pending' AND expires_at < NOW()")
    pending = await get_pool().fetch("""
        SELECT id, user_id, wallet_raw, amount_nano, amount_ton, created_at, expires_at
        FROM ton_deposits
        WHERE status = 'pending' AND expires_at >= NOW()
        ORDER BY created_at
        LIMIT 200
    """)
    if not pending:
        return 0

    async with get_ton_session().get(
        f"{TONCENTER_API_URL}/getTransactions",
        params={"address": TON_RECEIVER_ADDRESS, "limit": 100, "archival": "false"},
        headers=ton_api_headers(),
    ) as response:
        payload = await response.json(content_type=None)
        if response.status != 200 or not payload.get("ok"):
            raise RuntimeError(f"TON Center error: {payload.get('error', response.status)}")

    by_amount = {int(row["amount_nano"]): row for row in pending}
    credited = 0
    for transaction in payload.get("result", []):
        if transaction.get("aborted") is True:
            continue
        incoming = transaction.get("in_msg") or {}
        try:
            value = int(incoming.get("value", "0"))
            created_at = int(transaction.get("utime", 0))
        except (TypeError, ValueError):
            continue
        deposit = by_amount.get(value)
        if not deposit:
            continue
        source = str(incoming.get("source") or "").lower()
        if not source or source != deposit["wallet_raw"]:
            continue
        earliest = int(deposit["created_at"].timestamp()) - 30
        latest = int(deposit["expires_at"].timestamp()) + 300
        if not earliest <= created_at <= latest:
            continue
        tx_hash = str((transaction.get("transaction_id") or {}).get("hash") or incoming.get("hash") or "")
        if not tx_hash:
            continue

        async with get_pool().acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("""
                    UPDATE ton_deposits
                    SET status = 'credited', tx_hash = $1, credited_at = NOW()
                    WHERE id = $2 AND status = 'pending'
                    RETURNING user_id, amount_ton
                """, tx_hash, deposit["id"])
                if not row:
                    continue
                result = await conn.execute(
                    "UPDATE users SET balance = balance + $1 WHERE id = $2",
                    row["amount_ton"], row["user_id"])
                if result != "UPDATE 1":
                    raise RuntimeError("TON deposit user does not exist")
        credited += 1
        by_amount.pop(value, None)
    return credited


async def ton_payment_worker():
    if not TON_RECEIVER_ADDRESS:
        logging.warning("TON_RECEIVER_ADDRESS is not set; TON deposits are disabled")
        return
    while True:
        try:
            credited = await scan_ton_deposits()
            if credited:
                logging.info("Credited %s TON deposit(s)", credited)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.error("TON deposit scan failed: %s", exc)
        await asyncio.sleep(5)

# ------------------- API -------------------
async def handle_options(request):
    return web.Response(headers={
        "Access-Control-Allow-Origin": CORS_ORIGIN,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Telegram-Init-Data",
    })


@web.middleware
async def security_headers_middleware(request, handler):
    response = await handler(request)
    response.headers["Access-Control-Allow-Origin"] = CORS_ORIGIN
    response.headers["Vary"] = "Origin"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Cache-Control"] = "no-store"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return response


async def handle_health(request):
    try:
        await asyncio.wait_for(get_pool().fetchval("SELECT 1"), timeout=3)
        return web.json_response({"status": "ok"})
    except Exception:
        return web.json_response({"status": "unavailable"}, status=503)

@require_auth
async def handle_get_user(request):
    user = request['telegram_user']
    user_id = user['id']
    username = user['username']
    try:
        balance_value = await get_pool().fetchval(
            """INSERT INTO users (id, username) VALUES ($1, $2)
               ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username
               RETURNING balance""",
            user_id, username,
        )
        balance = float(balance_value or 0)
    except Exception as e:
        logging.error(f"Get user error: {e}")
        return web.json_response({"error": "database_unavailable"}, status=503,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    return web.json_response({"balance": balance}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
@rate_limit(10, 60)
async def handle_create_ton_deposit(request):
    if not TON_RECEIVER_ADDRESS:
        return web.json_response({"error": "ton_deposits_disabled"}, status=503,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    try:
        data = await request.json()
        user = request['telegram_user']
        user_id = user['id']
        username = user['username']
        amount = parse_positive_amount(data.get('amount'), minimum=TON_MIN_DEPOSIT,
                                       maximum=Decimal("10000"))
        if amount is None:
            return web.json_response({"success": False, "error": "invalid_amount"}, status=400)
        scaled = amount * Decimal("1000000000")
        if scaled != scaled.to_integral_value():
            return web.json_response({"success": False, "error": "too_many_decimals"}, status=400)
        wallet_address = str(data.get("walletAddress") or "").strip()
        try:
            wallet_raw = await normalize_ton_address(wallet_address)
        except ValueError:
            return web.json_response({"success": False, "error": "invalid_wallet"}, status=400)

        async with get_pool().acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO users (id, username) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username",
                    user_id, username)
                existing = await conn.fetchrow("""
                    SELECT id, amount_nano, amount_ton, expires_at
                    FROM ton_deposits
                    WHERE user_id = $1 AND wallet_raw = $2 AND status = 'pending'
                      AND expires_at > NOW() AND amount_ton >= $3 AND amount_ton < $3 + 0.001
                    ORDER BY created_at DESC LIMIT 1
                """, user_id, wallet_raw, amount)
                if existing:
                    return web.json_response({
                        "success": True,
                        "depositId": str(existing["id"]),
                        "receiverAddress": TON_RECEIVER_ADDRESS,
                        "amountNano": str(existing["amount_nano"]),
                        "amountTon": format(existing["amount_ton"], "f"),
                        "expiresAt": existing["expires_at"].isoformat(),
                    }, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                pending_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM ton_deposits WHERE user_id = $1 AND status = 'pending' AND expires_at > NOW()",
                    user_id)
                if pending_count >= 3:
                    return web.json_response({"success": False, "error": "too_many_pending"}, status=429)

                deposit_id = uuid.uuid4()
                for _ in range(10):
                    amount_nano = int(scaled) + secrets.randbelow(999999) + 1
                    amount_ton = Decimal(amount_nano) / Decimal("1000000000")
                    try:
                        async with conn.transaction():
                            expires_at = await conn.fetchval("""
                                INSERT INTO ton_deposits
                                    (id, user_id, wallet_address, wallet_raw, amount_nano, amount_ton, expires_at)
                                VALUES ($1, $2, $3, $4, $5, $6, NOW() + $7 * INTERVAL '1 second')
                                RETURNING expires_at
                            """, deposit_id, user_id, wallet_address, wallet_raw, amount_nano,
                                 amount_ton, TON_DEPOSIT_TIMEOUT)
                        break
                    except asyncpg.UniqueViolationError:
                        continue
                else:
                    raise RuntimeError("could not allocate a unique TON amount")

        return web.json_response({
            "success": True,
            "depositId": str(deposit_id),
            "receiverAddress": TON_RECEIVER_ADDRESS,
            "amountNano": str(amount_nano),
            "amountTon": format(amount_ton, "f"),
            "expiresAt": expires_at.isoformat(),
        }, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"TON deposit creation error: {e}")
        return web.json_response({"success": False, "error": "ton_payment_error"}, status=500,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


@require_auth
async def handle_ton_deposit_status(request):
    try:
        deposit_id = uuid.UUID(request.query.get("id", ""))
    except (ValueError, TypeError):
        return web.json_response({"error": "invalid_deposit_id"}, status=400)
    row = await get_pool().fetchrow("""
        SELECT status, amount_ton, tx_hash, expires_at
        FROM ton_deposits WHERE id = $1 AND user_id = $2
    """, deposit_id, request['telegram_user']['id'])
    if not row:
        return web.json_response({"error": "deposit_not_found"}, status=404)
    return web.json_response({
        "status": row["status"],
        "amountTon": format(row["amount_ton"], "f"),
        "txHash": row["tx_hash"],
        "expiresAt": row["expires_at"].isoformat(),
    }, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_get_items(request):
    try:
        rows = await get_pool().fetch(
            "SELECT id, name, price, status, image_url, nft_link, traits, number FROM items WHERE status = 'Доступен'")
        items = normalize_records(rows)
        return web.json_response(items, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Get items error: {e}")
        return web.json_response({"error": "database_unavailable"}, status=503,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_get_inventory(request):
    user = request['telegram_user']
    user_id = user['id']
    try:
        rows = await get_pool().fetch(
            "SELECT id, name, image_url, nft_link, model, status, traits, number FROM items WHERE buyer_id = $1 AND status IN ('Продан','withdrawn','Выведен','pending_withdraw')",
            user_id)
        items = normalize_records(rows)
        for item in items:
            if item['status'] in ('Выведен', 'withdrawn'):
                item['status'] = 'withdrawn'
        return web.json_response(items, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Get inventory error: {e}")
        return web.json_response({"error": "database_unavailable"}, status=503,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
@rate_limit(30, 60)
async def handle_buy(request):
    try:
        data = await request.json()
        user = request['telegram_user']
        user_id = user['id']
        raw_item_ids = data.get('items', [])
        if not isinstance(raw_item_ids, list) or not raw_item_ids or len(raw_item_ids) > 50:
            return web.json_response({"success": False, "error": "no_items"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        try:
            item_ids = [int(item_id) for item_id in raw_item_ids]
        except (TypeError, ValueError):
            return web.json_response({"success": False, "error": "invalid_items"}, status=400)
        if any(item_id <= 0 for item_id in item_ids) or len(set(item_ids)) != len(item_ids):
            return web.json_response({"success": False, "error": "invalid_items"}, status=400)
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                items = await conn.fetch(
                    "SELECT id, price FROM items WHERE id = ANY($1::int[]) AND status = 'Доступен' FOR UPDATE",
                    item_ids)
                if len(items) != len(item_ids):
                    return web.json_response({"success": False, "error": "items_unavailable"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                total_price = sum(i['price'] for i in items)
                new_balance = await conn.fetchval(
                    "UPDATE users SET balance = balance - $1 WHERE id = $2 AND balance >= $1 RETURNING balance",
                    total_price, user_id)
                if new_balance is None:
                    return web.json_response({"success": False, "error": "insufficient_funds"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                await conn.execute(
                    "UPDATE items SET status = 'Продан', buyer_id = $1, last_event = 'approved' WHERE id = ANY($2::int[])",
                    user_id, item_ids)
        return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Buy error: {e}")
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
@rate_limit(10, 60)
async def handle_request_withdraw(request):
    try:
        data = await request.json()
        user = request['telegram_user']
        user_id = user['id']
        username = user['username']
        try:
            item_id = int(data.get('itemId'))
        except (TypeError, ValueError):
            return web.json_response({"success": False, "error": "invalid_item"}, status=400)
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                item = await conn.fetchrow(
                    """UPDATE items SET status = 'pending_withdraw', last_event = 'withdraw_requested'
                       WHERE id = $1 AND buyer_id = $2 AND status = 'Продан'
                       RETURNING id, name, nft_link""",
                    item_id, user_id)
                if not item:
                    return web.json_response({"success": False, "error": "not_found"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"with_no_{user_id}_{item_id}"),
            InlineKeyboardButton(text="✅ Вывести", callback_data=f"with_yes_{user_id}_{item_id}")
        ]])
        try:
            await bot.send_message(
                ADMIN_ID,
                f"📤 **Запрос на вывод**\n👤 @{username} (ID: {user_id})\n📦 {item['name']} (ID: {item['id']})\n🔗 {item['nft_link']}",
                reply_markup=admin_kb,
                disable_web_page_preview=True,
            )
        except Exception:
            await get_pool().execute(
                """UPDATE items SET status = 'Продан', last_event = 'withdraw_notification_failed'
                   WHERE id = $1 AND buyer_id = $2 AND status = 'pending_withdraw'""",
                item_id, user_id)
            raise
        return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        return web.json_response({"success": False}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
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
@rate_limit(60, 60)
async def handle_game_bet(request):
    global game_state
    try:
        data = await request.json()
        user = request['telegram_user']
        user_id = user['id']
        username = user['username']
        parsed_amount = parse_positive_amount(data.get('amount'), minimum=TON_MIN_BET)
        if parsed_amount is None:
            return web.json_response({"success": False, "error": "min_bet"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        if parsed_amount.as_tuple().exponent < -9:
            return web.json_response({"success": False, "error": "invalid_amount"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        amount = float(parsed_amount)
        async with game_lock:
            if game_state["status"] not in ("waiting", "counting"):
                return web.json_response({"success": False, "error": "game_started"},
                                         headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
            if len(game_state["players"]) >= 20 and user_id not in game_state["players"]:
                return web.json_response({"success": False, "error": "room_full"},
                                         headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
            new_balance = await get_pool().fetchval(
                "UPDATE users SET balance = balance - $1 WHERE id = $2 AND balance >= $1 RETURNING balance",
                parsed_amount, user_id)
            if new_balance is None:
                return web.json_response({"success": False, "error": "insufficient_funds"},
                                         headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
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
                photo_url = user.get('photo_url', '')
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
@rate_limit(10, 60)
async def handle_game_cancel(request):
    global game_state
    try:
        user = request['telegram_user']
        user_id = user['id']
        async with game_lock:
            if len(game_state["players"]) == 1 and user_id in game_state["players"]:
                refund = game_state["players"][user_id]["amount"]
                await get_pool().execute(
                    "UPDATE users SET balance = balance + $1 WHERE id = $2",
                    Decimal(str(refund)), user_id)
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

@require_auth
async def handle_game_finish(request):
    return web.json_response({"success": True, "message": "Server handles finish"},
                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_game_history(request):
    try:
        rows = await get_pool().fetch(
            "SELECT game_number, winner_name, win_amount, win_percent FROM game_history ORDER BY game_number DESC LIMIT 100")
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
        async with get_pool().acquire() as conn:
            top_rows = await conn.fetch("SELECT username, wins FROM leaderboard ORDER BY wins DESC LIMIT 5")
            user_row = await conn.fetchrow("SELECT username, wins FROM leaderboard WHERE user_id = $1", user_id)
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
        end_time = await get_pool().fetchval("SELECT end_time FROM season WHERE id = 1")
        if end_time:
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            return web.json_response({"end_time": end_time.timestamp() * 1000},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        return web.json_response({"end_time": None}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        return web.json_response({"end_time": None}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_get_prize_items(request):
    try:
        rows = await get_pool().fetch("SELECT id, name, image_url, nft_link, traits FROM prize_items ORDER BY id")
        items = normalize_records(rows)
        return web.json_response(items, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Prize items error: {e}")
        return web.json_response([], status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

# ------------------------------------------------------------
#  НОВЫЕ ЭНДПОИНТЫ КЕЙСОВ (используют CASES_CACHE)
# ------------------------------------------------------------
@require_auth
async def handle_get_cases(request):
    """Возвращает список кейсов (без дропов) из кеша"""
    cases_list = []
    for case_id, case_data in CASES_CACHE.items():
        cases_list.append({
            "id": case_data["id"],
            "name": case_data["name"],
            "price": case_data["price"],
            "image_url": case_data["image_url"]
        })
    return web.json_response(cases_list, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_get_case_details(request):
    """Детали кейса + дропы из кеша"""
    case_id = request.query.get('id')
    if not case_id:
        return web.json_response({"error": "missing_id"}, status=400,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    try:
        case_id = int(case_id)
    except ValueError:
        return web.json_response({"error": "invalid_id"}, status=400,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

    case = CASES_CACHE.get(case_id)
    if not case:
        return web.json_response({"error": "case_not_found"}, status=404,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

    result = {
        "id": case["id"],
        "name": case["name"],
        "price": case["price"],
        "image_url": case["image_url"],
        "drops": [
            {key: value for key, value in drop.items() if key != "real_chance"}
            for drop in case["drops"]
        ],
    }
    return web.json_response(result, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
@rate_limit(30, 60)
async def handle_open_case(request):
    """Открытие кейса (цена и дропы из кеша, запись в БД)"""
    try:
        data = await request.json()
        user_id = request['telegram_user']['id']
        try:
            case_id = int(data.get('caseId'))
        except (TypeError, ValueError):
            return web.json_response({"success": False, "error": "invalid_case_id"}, status=400)
        case = CASES_CACHE.get(case_id)
        if not case:
            return web.json_response({"success": False, "error": "case_not_found"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

        drops = case.get('drops')
        if not isinstance(drops, list) or not drops:
            return web.json_response({"success": False, "error": "empty_case"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        case_price = parse_positive_amount(case.get('price'))
        if case_price is None:
            logging.error("Invalid price in CASES_JSON for case %s", case_id)
            return web.json_response({"success": False, "error": "invalid_case_config"}, status=500)
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                new_balance = await conn.fetchval("""
                    UPDATE users
                    SET balance = balance - $1
                    WHERE id = $2 AND balance >= $1
                    RETURNING balance
                """, case_price, user_id)
                if new_balance is None:
                    return web.json_response({"success": False, "error": "insufficient_funds"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

                real_chances = [float(drop['real_chance']) for drop in drops]
                if any(not math.isfinite(chance) or chance < 0 for chance in real_chances):
                    raise ValueError("invalid real_chance in CASES_JSON")
                total_chance = sum(real_chances)
                if total_chance <= 0:
                    raise ValueError("total real_chance must be positive")
                rand_val = random.uniform(0, total_chance)
                current_sum = 0
                won_drop = drops[-1]
                for drop, real_chance in zip(drops, real_chances):
                    current_sum += real_chance
                    if rand_val <= current_sum:
                        won_drop = drop
                        break

                drop_value = Decimal(str(won_drop['value']))
                if not drop_value.is_finite() or drop_value < 0 or drop_value > Decimal("1000000"):
                    raise ValueError("invalid drop value in CASES_JSON")

                new_item_id = await conn.fetchval("""
                    INSERT INTO items (name, price, status, image_url, model, buyer_id, last_event)
                    VALUES ($1, $2, 'Продан', $3, $4, $5, 'case_drop') RETURNING id
                """, str(won_drop['name'])[:255], drop_value,
                      safe_https_url(won_drop.get('image_url')), str(won_drop.get('model', ''))[:255], user_id)

                won_item_dict = dict(won_drop)
                won_item_dict.pop('real_chance', None)
                won_item_dict['generated_item_id'] = new_item_id

        return web.json_response({"success": True, "won_item": won_item_dict},
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

    except Exception as e:
        logging.error(f"Case open error: {e}")
        return web.json_response({"success": False}, status=500,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
@rate_limit(30, 60)
async def handle_sell_drop(request):
    """Продажа предмета (работает с таблицей items)"""
    try:
        data = await request.json()
        user_id = request['telegram_user']['id']
        try:
            item_id = int(data.get('itemId'))
        except (TypeError, ValueError):
            return web.json_response({"success": False, "error": "missing_item_id"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                item = await conn.fetchrow(
                    """DELETE FROM items
                       WHERE id = $1 AND buyer_id = $2 AND status = 'Продан' AND last_event = 'case_drop'
                       RETURNING id, price""",
                    item_id, user_id)
                if not item:
                    return web.json_response({"success": False, "error": "item_not_found"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                await conn.execute("UPDATE users SET balance = balance + $1 WHERE id = $2", item['price'], user_id)
        return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Sell drop error: {e}")
        return web.json_response({"success": False, "error": "server_error"}, status=500,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

# ---------- Админ-коллбэки ----------
async def is_admin_callback(callback: types.CallbackQuery) -> bool:
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа", show_alert=True)
        logging.warning("Unauthorized admin callback from Telegram user %s", callback.from_user.id)
        return False
    return True


@dp.callback_query(F.data.startswith("with_yes_"))
async def admin_withdraw_approve(callback: types.CallbackQuery):
    if not await is_admin_callback(callback):
        return
    parts = callback.data.split("_")
    _, _, uid, item_id = parts
    result = await get_pool().execute(
        """UPDATE items SET status = 'withdrawn', last_event = 'withdraw_approved'
           WHERE id = $1 AND buyer_id = $2 AND status = 'pending_withdraw'""",
        int(item_id), uid)
    if result != "UPDATE 1":
        await callback.answer("Запрос уже обработан", show_alert=True)
        return
    await callback.message.edit_text(f"{callback.message.text}\n\n✅ **ВЫВОД ПОДТВЕРЖДЕН**")
    try:
        await bot.send_message(int(uid), f"🎉 NFT (ID: {item_id}) выведен!")
    except Exception:
        pass

@dp.callback_query(F.data.startswith("with_no_"))
async def admin_withdraw_reject(callback: types.CallbackQuery):
    if not await is_admin_callback(callback):
        return
    parts = callback.data.split("_")
    _, _, uid, item_id = parts
    result = await get_pool().execute(
        """UPDATE items SET status = 'Продан', last_event = 'withdraw_rejected'
           WHERE id = $1 AND buyer_id = $2 AND status = 'pending_withdraw'""",
        int(item_id), uid)
    if result != "UPDATE 1":
        await callback.answer("Запрос уже обработан", show_alert=True)
        return
    await callback.message.edit_text(f"{callback.message.text}\n\n❌ **ВЫВОД ОТКЛОНЕН**")
    try:
        await bot.send_message(int(uid), f"❌ Вывод NFT (ID: {item_id}) отклонён.")
    except Exception:
        pass

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✨ Магазин", web_app=WebAppInfo(url=WEBAPP_URL))]])
    await message.answer("Добро пожаловать в DNX Store!", reply_markup=kb)

# ---------- Настройка сервера ----------
app = web.Application(middlewares=[security_headers_middleware], client_max_size=64 * 1024)
app.router.add_get('/health', handle_health)
app.router.add_get('/user', handle_get_user)
app.router.add_get('/items', handle_get_items)
app.router.add_get('/inventory', handle_get_inventory)
app.router.add_post('/ton/deposit/create', handle_create_ton_deposit)
app.router.add_get('/ton/deposit/status', handle_ton_deposit_status)
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
app.router.add_post('/sell-drop', handle_sell_drop)
app.router.add_options('/{tail:.*}', handle_options)

async def main():
    global db_pool, ton_http_session
    runner = None
    game_task = None
    ton_task = None
    try:
        db_pool = await create_db_pool()
        ton_http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        await init_db()
        game_task = asyncio.create_task(game_worker())
        ton_task = asyncio.create_task(ton_payment_worker())
        port = int(os.environ.get("PORT", 8080))
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, '0.0.0.0', port).start()
        await dp.start_polling(bot)
    finally:
        tasks = [task for task in (game_task, ton_task) if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if runner is not None:
            await runner.cleanup()
        await bot.session.close()
        if ton_http_session is not None:
            await ton_http_session.close()
        if db_pool is not None:
            await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())
