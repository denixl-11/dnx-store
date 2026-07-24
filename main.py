import asyncio
import hashlib
import hmac
import logging
import os
import json
import random
import math
import colorsys
import re
import time
import secrets
import uuid
from collections import OrderedDict, defaultdict, deque
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from functools import wraps
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urlencode, urlparse, urlsplit, urlunsplit
from datetime import date, datetime, timezone

import asyncpg
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, LabeledPrice
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PASSWORD = os.getenv("DB_PASSWORD")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://denixl-11.github.io/dnx-store/")
CORS_ORIGIN = os.getenv("CORS_ORIGIN", "https://denixl-11.github.io").rstrip("/")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_HOST = os.getenv("DB_HOST", "").strip()
DB_NAME = os.getenv("DB_NAME", "").strip()
DB_USER = os.getenv("DB_USER", "").strip()
DB_PORT = int(os.getenv("DB_PORT", "5432"))
INIT_DATA_MAX_AGE = int(os.getenv("INIT_DATA_MAX_AGE", "86400"))
DB_POOL_MIN_SIZE = int(os.getenv("DB_POOL_MIN_SIZE", "1"))
DB_POOL_MAX_SIZE = int(os.getenv("DB_POOL_MAX_SIZE", "6"))
TON_RECEIVER_ADDRESS = os.getenv("TON_RECEIVER_ADDRESS", "").strip()
TONCENTER_API_URL = os.getenv("TONCENTER_API_URL", "https://toncenter.com/api/v2").rstrip("/")
TONCENTER_API_KEY = os.getenv("TONCENTER_API_KEY", "").strip()
TON_DEPOSIT_TIMEOUT = int(os.getenv("TON_DEPOSIT_TIMEOUT", "900"))
TON_STAR_RATE = Decimal(os.getenv("TON_STAR_RATE", "85"))
STAR_MIN_TOPUP = int(os.getenv("STAR_MIN_TOPUP", "10"))
STAR_MAX_TOPUP = int(os.getenv("STAR_MAX_TOPUP", "10000"))
STAR_MIN_BET = Decimal(os.getenv("STAR_MIN_BET", "10"))
STAR_BET_STEP = Decimal(os.getenv("STAR_BET_STEP", "10"))
REFERRAL_RATE = Decimal("0.10")
REFERRAL_CODE_LENGTH = 25
REFERRAL_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip().lstrip("@")
WITHDRAW_WINDOW_SECONDS = 2 * 60 * 60

if TON_STAR_RATE <= 0:
    raise RuntimeError("TON_STAR_RATE must be positive")
if STAR_MIN_TOPUP <= 0 or STAR_MAX_TOPUP < STAR_MIN_TOPUP:
    raise RuntimeError("Invalid Stars top-up limits")
if STAR_MIN_BET <= 0 or STAR_BET_STEP <= 0:
    raise RuntimeError("Invalid Stars bet limits")


def _require_https_url(name: str, value: str, *, origin_only: bool = False) -> None:
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise RuntimeError(f"{name} must be a public HTTPS URL")
    if origin_only and (parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment):
        raise RuntimeError(f"{name} must contain only scheme and host")


_require_https_url("WEBAPP_URL", WEBAPP_URL)
_require_https_url("CORS_ORIGIN", CORS_ORIGIN, origin_only=True)
_require_https_url("TONCENTER_API_URL", TONCENTER_API_URL)
if DATABASE_URL and urlparse(DATABASE_URL).scheme not in {"postgres", "postgresql"}:
    raise RuntimeError("DATABASE_URL must use the postgres or postgresql scheme")
if ADMIN_ID <= 0:
    raise RuntimeError("ADMIN_ID must be a positive Telegram user id")
if not (1 <= DB_POOL_MIN_SIZE <= DB_POOL_MAX_SIZE <= 30):
    raise RuntimeError("DB pool sizes must satisfy 1 <= MIN <= MAX <= 30")
if not (60 <= INIT_DATA_MAX_AGE <= 7 * 24 * 60 * 60):
    raise RuntimeError("INIT_DATA_MAX_AGE must be between 60 and 604800 seconds")
if not (60 <= TON_DEPOSIT_TIMEOUT <= 24 * 60 * 60):
    raise RuntimeError("TON_DEPOSIT_TIMEOUT must be between 60 and 86400 seconds")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not DATABASE_URL and not all((DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)):
    raise RuntimeError(
        "DATABASE_URL is required (recommended), or set DB_HOST, DB_NAME, "
        "DB_USER and DB_PASSWORD together. The removed hard-coded Neon host "
        "could silently connect Render to an obsolete database."
    )

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
db_pool: asyncpg.Pool | None = None
ton_http_session: aiohttp.ClientSession | None = None
runtime_state = {
    "ready": False,
    "phase": "booting",
    "attempt": 0,
    "database": "pending",
}
nft_media_cache: dict[str, tuple[float, dict]] = {}
nft_media_fetch_semaphore = asyncio.Semaphore(4)
star_reconcile_attempts: dict[str, float] = {}
auth_cache: OrderedDict[str, tuple[float, dict]] = OrderedDict()
restriction_cache: OrderedDict[str, tuple[float, dict | None]] = OrderedDict()
item_nft_source_cache: OrderedDict[int, tuple[float, str]] = OrderedDict()
AUTH_CACHE_TTL = 300
AUTH_CACHE_MAX = 2048
RESTRICTION_CACHE_TTL = 3
RESTRICTION_CACHE_MAX = 4096
ITEM_SOURCE_CACHE_TTL = 60
ITEM_SOURCE_CACHE_MAX = 2048
API_RELEASE = "8.9-opt.18"
PROCESS_INSTANCE_ID = uuid.uuid4()
secure_random = random.SystemRandom()


def floor_stars(value) -> Decimal:
    """Return a finite, non-negative Stars amount rounded down to an integer."""
    amount = Decimal(str(value))
    if not amount.is_finite() or amount < 0:
        raise ValueError("Stars amount must be finite and non-negative")
    return amount.to_integral_value(rounding=ROUND_FLOOR)


async def record_user_event(
    conn,
    user_id: str,
    event_type: str,
    *,
    amount=None,
    balance_type: str | None = None,
    title: str | None = None,
    metadata: dict | None = None,
):
    """Append an immutable account-history event inside the caller transaction."""
    event_amount = Decimal(str(amount)) if amount is not None else None
    await conn.execute(
        """
        INSERT INTO user_events (user_id, event_type, amount, balance_type, title, metadata)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        """,
        str(user_id),
        event_type[:50],
        event_amount,
        balance_type if balance_type in {"main", "bonus"} else None,
        (title or "")[:255],
        json.dumps(metadata or {}, ensure_ascii=False),
    )


async def get_account_restriction(user_id: str) -> dict | None:
    """Return a current restriction and lazily clear an expired mute."""
    user_id = str(user_id)
    now_mono = time.monotonic()
    cached = restriction_cache.get(user_id)
    if cached and cached[0] > now_mono:
        restriction_cache.move_to_end(user_id)
        return dict(cached[1]) if cached[1] else None
    if cached:
        restriction_cache.pop(user_id, None)

    row = await get_pool().fetchrow(
        "SELECT ban, mut, mut_until FROM users WHERE id = $1",
        str(user_id),
    )
    if not row:
        result = None
        _cache_restriction(user_id, result)
        return result
    if row["ban"] is True:
        result = {"type": "ban"}
        _cache_restriction(user_id, result)
        return result
    if row["mut"] is None:
        result = None
        _cache_restriction(user_id, result)
        return result

    mut_until = row["mut_until"]
    if mut_until is None:
        mut_until = await get_pool().fetchval(
            """
            UPDATE users
            SET mut_until = NOW() + make_interval(hours => GREATEST(mut, 0))
            WHERE id = $1 AND mut IS NOT NULL AND mut_until IS NULL
            RETURNING mut_until
            """,
            str(user_id),
        )
    if not mut_until:
        result = None
        _cache_restriction(user_id, result)
        return result
    if mut_until.tzinfo is None:
        mut_until = mut_until.replace(tzinfo=timezone.utc)
    remaining = math.ceil((mut_until - datetime.now(timezone.utc)).total_seconds())
    if remaining <= 0:
        await get_pool().execute(
            "UPDATE users SET mut = NULL, mut_until = NULL WHERE id = $1 AND mut_until <= NOW()",
            str(user_id),
        )
        result = None
        _cache_restriction(user_id, result)
        return result
    result = {
        "type": "mut",
        "remainingSeconds": remaining,
        "expiresAt": mut_until.isoformat(),
    }
    _cache_restriction(user_id, result)
    return result


def _cache_restriction(user_id: str, value: dict | None) -> None:
    restriction_cache[str(user_id)] = (
        time.monotonic() + RESTRICTION_CACHE_TTL,
        dict(value) if value else None,
    )
    restriction_cache.move_to_end(str(user_id))
    while len(restriction_cache) > RESTRICTION_CACHE_MAX:
        restriction_cache.popitem(last=False)


def new_referral_code() -> str:
    return "".join(secrets.choice(REFERRAL_ALPHABET) for _ in range(REFERRAL_CODE_LENGTH))


async def ensure_user(conn, user_id: str, username: str, referral_code: str | None = None):
    """Create a user once and bind a referrer only during that first insert."""
    existing = await conn.fetchrow(
        "SELECT id, balance, bonus_balance, referral_code, referred_by FROM users WHERE id = $1",
        user_id,
    )
    if existing:
        await conn.execute(
            "UPDATE users SET username = $1 WHERE id = $2 AND username IS DISTINCT FROM $1",
            username[:255],
            user_id,
        )
        return existing

    referrer_id = None
    if referral_code and re.fullmatch(r"[A-Za-z0-9]{25}", referral_code):
        referrer_id = await conn.fetchval(
            "SELECT id FROM users WHERE referral_code = $1 AND id <> $2",
            referral_code,
            user_id,
        )

    for _ in range(12):
        code = new_referral_code()
        try:
            async with conn.transaction():
                return await conn.fetchrow(
                    """
                    INSERT INTO users (id, username, balance, bonus_balance, referral_code, referred_by)
                    VALUES ($1, $2, 0, 0, $3, $4)
                    ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username
                    RETURNING id, balance, bonus_balance, referral_code, referred_by
                    """,
                    user_id,
                    username[:255],
                    code,
                    referrer_id,
                )
        except asyncpg.UniqueViolationError:
            continue
    raise RuntimeError("Could not allocate a unique referral code")


async def credit_main_deposit(
    conn,
    user_id: str,
    amount,
    source_type: str,
    source_id: str,
) -> Decimal:
    """Credit a real deposit and award its one-time 10% referral bonus."""
    deposit_amount = floor_stars(amount)
    result = await conn.execute(
        "UPDATE users SET balance = balance + $1 WHERE id = $2",
        deposit_amount,
        user_id,
    )
    if result != "UPDATE 1":
        raise RuntimeError("Deposit user does not exist")
    await record_user_event(
        conn,
        user_id,
        "deposit",
        amount=deposit_amount,
        balance_type="main",
        title="Пополнение баланса",
        metadata={"source": source_type, "source_id": source_id},
    )

    referrer_id = await conn.fetchval("SELECT referred_by FROM users WHERE id = $1", user_id)
    if not referrer_id:
        return Decimal("0")
    reward = floor_stars(deposit_amount * REFERRAL_RATE)
    if reward <= 0:
        return Decimal("0")
    inserted = await conn.fetchval(
        """
        INSERT INTO referral_rewards
            (referrer_id, referral_id, source_type, source_id, deposit_amount, reward_amount)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (source_type, source_id) DO NOTHING
        RETURNING reward_amount
        """,
        referrer_id,
        user_id,
        source_type,
        source_id,
        deposit_amount,
        reward,
    )
    if inserted is not None:
        await conn.execute(
            "UPDATE users SET bonus_balance = bonus_balance + $1 WHERE id = $2",
            inserted,
            referrer_id,
        )
        await record_user_event(
            conn,
            referrer_id,
            "referral_bonus",
            amount=inserted,
            balance_type="bonus",
            title="Реферальное начисление",
            metadata={"referral_id": user_id, "source": source_type},
        )
        return Decimal(str(inserted))
    return Decimal("0")

# ======================== ГЛОБАЛЬНЫЙ КЕШ КЕЙСОВ ========================
# Загружается из переменной окружения CASES_JSON (хранится в Render).
# Если переменная не задана, используется демо-кейс (для тестов).
def load_cases_from_env():
    global CASES_CACHE
    raw = os.getenv("CASES_JSON")
    if raw:
        try:
            parsed_cases = json.loads(raw)
            if not isinstance(parsed_cases, dict) or not (1 <= len(parsed_cases) <= 100):
                raise ValueError("CASES_JSON must contain 1 to 100 case objects")
            # Преобразуем ключи-строки в int (JSON всегда строковые ключи)
            CASES_CACHE = {int(k): v for k, v in parsed_cases.items()}
            for case_id, case in CASES_CACHE.items():
                if case_id <= 0 or not isinstance(case, dict):
                    raise ValueError("Every case must have a positive integer id and an object value")
                if str(case.get("name") or "").strip() == "":
                    raise ValueError(f"Case {case_id} has no name")
                price = Decimal(str(case.get("price")))
                if not price.is_finite() or price <= 0 or price > Decimal("1000000"):
                    raise ValueError(f"Case {case_id} has an invalid price")
                drops = case.get("drops")
                if not isinstance(drops, list) or not (1 <= len(drops) <= 500):
                    raise ValueError(f"Case {case_id} must contain 1 to 500 drops")
                total_chance = Decimal("0")
                for drop_index, drop in enumerate(drops):
                    if not isinstance(drop, dict) or str(drop.get("name") or "").strip() == "":
                        raise ValueError(f"Case {case_id}, drop {drop_index} is invalid")
                    chance = Decimal(str(drop.get("real_chance", drop.get("chance", 0))))
                    value = Decimal(str(drop.get("value", 0)))
                    if not chance.is_finite() or chance < 0:
                        raise ValueError(f"Case {case_id}, drop {drop_index} has an invalid chance")
                    if not value.is_finite() or value < 0 or value > Decimal("1000000"):
                        raise ValueError(f"Case {case_id}, drop {drop_index} has an invalid value")
                    total_chance += chance
                if total_chance <= 0:
                    raise ValueError(f"Case {case_id} has no positive drop chances")
            logging.info(f"✅ Загружено {len(CASES_CACHE)} кейсов из CASES_JSON")
        except Exception as e:
            logging.error(f"❌ Ошибка парсинга CASES_JSON: {e}")
            raise RuntimeError("CASES_JSON is invalid; refusing to start with unsafe case data") from e
    else:
        logging.warning("⚠️ CASES_JSON не задан. Используется демо-кейс.")
        # Демо-кейс (замените на свои данные после заполнения переменной в Render)
        CASES_CACHE = {
            1: {
                "id": 1,
                "name": "Демо-кейс",
                "price": 42,
                "image_url": "https://via.placeholder.com/150",
                "drops": [
                    {
                        "id": 1,
                        "case_id": 1,
                        "name": "Демо-скин",
                        "image_url": "https://via.placeholder.com/100",
                        "model": "Demo",
                        "chance": 100.0,
                        "value": 21,
                        "real_chance": 100.0
                    }
                ]
            }
        }

load_cases_from_env()   # заполнили CASES_CACHE


def case_drop_attribute(drop: dict, attribute: str) -> str | None:
    """Read a case-drop attribute from new fields or legacy CASES_JSON traits."""
    direct = drop.get(attribute)
    if direct in (None, ""):
        direct = drop.get(attribute.capitalize())
    if direct not in (None, ""):
        return str(direct)[:255]
    aliases = {
        "model": {"model", "модель"},
        "pattern": {"pattern", "узор"},
        "background": {"background", "фон"},
    }
    legacy_traits = drop.get("traits") if isinstance(drop.get("traits"), list) else []
    for trait in legacy_traits:
        label = str(trait.get("label") or trait.get("name") or trait.get("trait_type") or "").strip().lower()
        if label in aliases.get(attribute, set()) and trait.get("value") not in (None, ""):
            return str(trait["value"])[:255]
    return None


def case_allows_bonus(case: dict) -> bool:
    value = case.get("bonus_enabled", False)
    return value is True or value == 1 or (isinstance(value, str) and value.strip().lower() in {"true", "yes", "1"})

async def create_db_pool() -> asyncpg.Pool:
    if DATABASE_URL:
        dsn = normalize_database_url(DATABASE_URL)
        return await asyncpg.create_pool(
            dsn=dsn,
            min_size=DB_POOL_MIN_SIZE,
            max_size=DB_POOL_MAX_SIZE,
            timeout=10,
            command_timeout=15,
            max_inactive_connection_lifetime=300,
        )
    return await asyncpg.create_pool(
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        ssl="require",
        min_size=DB_POOL_MIN_SIZE,
        max_size=DB_POOL_MAX_SIZE,
        timeout=10,
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


async def database_identity(conn) -> dict[str, str]:
    """Return a non-secret fingerprint of the database Render is using."""
    row = await conn.fetchrow(
        """SELECT current_database() AS database_name,
                  current_schema() AS schema_name,
                  COALESCE(inet_server_addr()::TEXT, 'local') AS server_address"""
    )
    raw = f"{row['server_address']}|{row['database_name']}|{row['schema_name']}"
    return {
        "database": row["database_name"],
        "schema": row["schema_name"],
        "fingerprint": hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12],
    }


async def log_database_audit(pool: asyncpg.Pool) -> None:
    """Make wrong Neon branch/schema connections obvious in Render logs."""
    async with pool.acquire() as conn:
        identity = await database_identity(conn)
        columns = await conn.fetch(
            """SELECT column_name
               FROM information_schema.columns
               WHERE table_schema = current_schema() AND table_name = 'items'
               ORDER BY ordinal_position"""
        )
    names = [row["column_name"] for row in columns]
    logging.info(
        "DNX database audit: release=%s database=%s schema=%s fingerprint=%s items_columns=%s",
        API_RELEASE,
        identity["database"],
        identity["schema"],
        identity["fingerprint"],
        ",".join(names),
    )
    required = {"id", "name", "model", "pattern", "background"}
    missing = sorted(required.difference(names))
    if missing:
        raise RuntimeError(f"items table is missing required columns: {', '.join(missing)}")
    if "traits" in names:
        logging.warning(
            "Legacy items.traits still exists in the connected database, but release %s never reads it",
            API_RELEASE,
        )


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


async def ensure_game_ledger_schema(conn) -> None:
    """Create the durable wager tables even when an old migration marker exists.

    Several deployed V8.9 builds used the same migration marker while their
    schema differed.  Keeping this tiny, idempotent guard ahead of the marker
    check prevents a valid bet from turning into a generic HTTP 500 merely
    because Render restarted on one of those databases.
    """
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS game_counter (
            id INT PRIMARY KEY DEFAULT 1,
            last_game_number INT NOT NULL DEFAULT 0
        )
    """)
    await conn.execute(
        "INSERT INTO game_counter (id, last_game_number) VALUES (1, 0) ON CONFLICT (id) DO NOTHING"
    )
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS active_game_bets (
            game_number INTEGER NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            username VARCHAR(255) NOT NULL,
            amount NUMERIC(20, 0) NOT NULL CHECK (amount > 0),
            balance_type VARCHAR(16) NOT NULL CHECK (balance_type IN ('main', 'bonus')),
            history_event_id BIGINT,
            owner_token UUID,
            status VARCHAR(16) NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'settled', 'refunded')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (game_number, user_id)
        )
    """)
    await conn.execute("ALTER TABLE active_game_bets ADD COLUMN IF NOT EXISTS owner_token UUID")
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_active_game_bets_recovery
        ON active_game_bets(status, updated_at)
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS game_round_settlements (
            game_number INTEGER PRIMARY KEY,
            round_id BIGINT,
            winner_id VARCHAR(255) NOT NULL,
            winner_username VARCHAR(255) NOT NULL,
            payout NUMERIC(20, 0) NOT NULL,
            balance_type VARCHAR(16) NOT NULL,
            win_percent NUMERIC(6, 2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)


async def init_db():
    try:
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS dnx_schema_migrations (
                        version VARCHAR(64) PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                # This guard intentionally runs before the migration shortcut.
                # Old V8.9 releases reused markers and could otherwise leave
                # the wager table absent forever.
                await ensure_game_ledger_schema(conn)
                # Keep the migration marker in sync with durable schema
                # additions. Reusing the old opt.2 marker made upgraded
                # databases return above before `active_game_bets` existed.
                migration_version = "v8.9-opt.5-game-ledger"
                if await conn.fetchval(
                    "SELECT 1 FROM dnx_schema_migrations WHERE version = $1",
                    migration_version,
                ):
                    game_state["game_number"] = int(
                        await conn.fetchval(
                            "SELECT last_game_number FROM game_counter WHERE id = 1"
                        ) or 0
                    )
                    logging.info("DB schema already initialized. Game number: %s", game_state["game_number"])
                    return
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id VARCHAR(255) PRIMARY KEY,
                        username VARCHAR(255),
                        balance NUMERIC(20, 0) DEFAULT 0,
                        bonus_balance NUMERIC(20, 0) NOT NULL DEFAULT 0,
                        referral_code VARCHAR(25),
                        referred_by VARCHAR(255),
                        mut INTEGER,
                        ban BOOLEAN,
                        mut_until TIMESTAMPTZ
                    )
                """)
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS bonus_balance NUMERIC(20, 0) DEFAULT 0")
                await conn.execute("UPDATE users SET bonus_balance = 0 WHERE bonus_balance IS NULL")
                await conn.execute("""
                    UPDATE users
                    SET balance = FLOOR(GREATEST(COALESCE(balance, 0), 0)),
                        bonus_balance = FLOOR(GREATEST(COALESCE(bonus_balance, 0), 0))
                    WHERE balance IS NULL OR balance < 0 OR balance <> FLOOR(balance)
                       OR bonus_balance IS NULL OR bonus_balance < 0
                       OR bonus_balance <> FLOOR(bonus_balance)
                """)
                await conn.execute("ALTER TABLE users ALTER COLUMN balance TYPE NUMERIC(20, 0) USING FLOOR(GREATEST(COALESCE(balance, 0), 0))")
                await conn.execute("ALTER TABLE users ALTER COLUMN bonus_balance TYPE NUMERIC(20, 0) USING FLOOR(GREATEST(COALESCE(bonus_balance, 0), 0))")
                await conn.execute("ALTER TABLE users ALTER COLUMN bonus_balance SET NOT NULL")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_code VARCHAR(25)")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by VARCHAR(255)")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS mut INTEGER")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS ban BOOLEAN")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS mut_until TIMESTAMPTZ")
                await conn.execute("""
                    CREATE OR REPLACE FUNCTION dnx_set_mut_deadline()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        IF NEW.mut IS NULL OR NEW.mut <= 0 THEN
                            NEW.mut := NULL;
                            NEW.mut_until := NULL;
                        ELSIF TG_OP = 'INSERT' THEN
                            NEW.mut_until := NOW() + make_interval(hours => NEW.mut);
                        ELSIF OLD.mut IS DISTINCT FROM NEW.mut THEN
                            NEW.mut_until := NOW() + make_interval(hours => NEW.mut);
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                """)
                await conn.execute("DROP TRIGGER IF EXISTS trg_users_mut_deadline ON users")
                await conn.execute("""
                    CREATE TRIGGER trg_users_mut_deadline
                    BEFORE INSERT OR UPDATE OF mut ON users
                    FOR EACH ROW EXECUTE FUNCTION dnx_set_mut_deadline()
                """)
                await conn.execute("""
                    UPDATE users
                    SET mut_until = NOW() + make_interval(hours => GREATEST(mut, 0))
                    WHERE mut IS NOT NULL AND mut_until IS NULL
                """)
                users_without_codes = await conn.fetch("SELECT id FROM users WHERE referral_code IS NULL")
                for missing_user in users_without_codes:
                    for _ in range(12):
                        code = new_referral_code()
                        if not await conn.fetchval("SELECT 1 FROM users WHERE referral_code = $1", code):
                            await conn.execute(
                                "UPDATE users SET referral_code = $1 WHERE id = $2 AND referral_code IS NULL",
                                code,
                                missing_user["id"],
                            )
                            break
                await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_referred_by ON users(referred_by)")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS items (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255),
                        price NUMERIC DEFAULT 0.0,
                        status VARCHAR(50) DEFAULT 'Доступен',
                        image_url TEXT,
                        nft_link TEXT DEFAULT '',
                        model VARCHAR(255),
                        pattern VARCHAR(255),
                        background VARCHAR(255),
                        buyer_id VARCHAR(255),
                        number VARCHAR(20),
                        last_event VARCHAR(50)
                    )
                """)
                await conn.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS model VARCHAR(255)")
                await conn.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS pattern VARCHAR(255)")
                await conn.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS background VARCHAR(255)")
                await conn.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS buyer_id VARCHAR(255)")
                await conn.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS nft_link TEXT DEFAULT ''")
                # Catalog traits are authoritative only in these three columns.
                # Do not silently repopulate them from the removed legacy JSON
                # field: that made edited values appear to "come back".
                await conn.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS number VARCHAR(20)")
                await conn.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS last_event VARCHAR(50)")
                await conn.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS acquisition_source VARCHAR(20) DEFAULT 'catalog'")
                await conn.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS withdraw_requested_at TIMESTAMPTZ")
                await conn.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS withdraw_expires_at TIMESTAMPTZ")
                await conn.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS disposed_at TIMESTAMPTZ")
                await conn.execute("UPDATE items SET acquisition_source = 'catalog' WHERE acquisition_source IS NULL")
                await conn.execute("""
                    UPDATE items SET acquisition_source = 'case'
                    WHERE last_event IN ('case_drop', 'case_drop_sold')
                """)
                await conn.execute("ALTER TABLE items ALTER COLUMN acquisition_source SET NOT NULL")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS purchase_records (
                        id UUID PRIMARY KEY,
                        user_id VARCHAR(255) NOT NULL,
                        item_ids INTEGER[] NOT NULL,
                        total_price NUMERIC(20, 2) NOT NULL,
                        status VARCHAR(32) NOT NULL DEFAULT 'completed',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS case_open_records (
                        id UUID PRIMARY KEY,
                        user_id VARCHAR(255) NOT NULL,
                        case_id INTEGER NOT NULL,
                        balance_type VARCHAR(16) NOT NULL,
                        price NUMERIC(20, 0) NOT NULL,
                        generated_item_id INTEGER NOT NULL,
                        winner_payload JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS item_events (
                        id BIGSERIAL PRIMARY KEY,
                        item_id INTEGER NOT NULL,
                        user_id VARCHAR(255),
                        event_type VARCHAR(50) NOT NULL,
                        amount NUMERIC(20, 2),
                        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS user_events (
                        id BIGSERIAL PRIMARY KEY,
                        user_id VARCHAR(255) NOT NULL,
                        event_type VARCHAR(50) NOT NULL,
                        amount NUMERIC(20, 2),
                        balance_type VARCHAR(20),
                        title VARCHAR(255),
                        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                await conn.execute("DELETE FROM user_events WHERE event_type = 'game_loss'")
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
                        credit_stars BIGINT NOT NULL,
                        status VARCHAR(32) NOT NULL DEFAULT 'pending',
                        tx_hash TEXT UNIQUE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        expires_at TIMESTAMPTZ NOT NULL,
                        credited_at TIMESTAMPTZ
                    )
                """)
                await conn.execute("ALTER TABLE ton_deposits ADD COLUMN IF NOT EXISTS credit_stars BIGINT")
                await conn.execute(
                    "UPDATE ton_deposits SET credit_stars = ROUND(amount_ton * $1)::BIGINT WHERE credit_stars IS NULL",
                    TON_STAR_RATE)
                await conn.execute("ALTER TABLE ton_deposits ALTER COLUMN credit_stars SET NOT NULL")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS star_payments (
                        id UUID PRIMARY KEY,
                        user_id VARCHAR(255) NOT NULL,
                        stars BIGINT NOT NULL CHECK (stars > 0),
                        invoice_payload VARCHAR(128) NOT NULL UNIQUE,
                        telegram_payment_charge_id TEXT UNIQUE,
                        status VARCHAR(32) NOT NULL DEFAULT 'pending',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        expires_at TIMESTAMPTZ NOT NULL,
                        paid_at TIMESTAMPTZ
                    )
                """)
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_items_status ON items(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_items_buyer_status ON items(buyer_id, status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_items_withdraw_expiry ON items(status, withdraw_expires_at)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_item_events_item ON item_events(item_id, created_at DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_item_events_user ON item_events(user_id, created_at DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_purchase_records_user ON purchase_records(user_id, created_at DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_case_open_records_user ON case_open_records(user_id, created_at DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_events_owner ON user_events(user_id, id DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_game_history_number ON game_history(game_number DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_ton_deposits_pending ON ton_deposits(status, expires_at)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_star_payments_user_status ON star_payments(user_id, status, expires_at)")
                await conn.execute("ALTER TABLE star_payments ADD COLUMN IF NOT EXISTS telegram_star_transaction_id TEXT UNIQUE")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS active_game_bets (
                        game_number INTEGER NOT NULL,
                        user_id VARCHAR(255) NOT NULL,
                        username VARCHAR(255) NOT NULL,
                        amount NUMERIC(20, 0) NOT NULL CHECK (amount > 0),
                        balance_type VARCHAR(16) NOT NULL CHECK (balance_type IN ('main', 'bonus')),
                        history_event_id BIGINT,
                        owner_token UUID,
                        status VARCHAR(16) NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'settled', 'refunded')),
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (game_number, user_id)
                    )
                """)
                await conn.execute("ALTER TABLE active_game_bets ADD COLUMN IF NOT EXISTS owner_token UUID")
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_active_game_bets_recovery
                    ON active_game_bets(status, updated_at)
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS game_round_settlements (
                        game_number INTEGER PRIMARY KEY,
                        round_id BIGINT,
                        winner_id VARCHAR(255) NOT NULL,
                        winner_username VARCHAR(255) NOT NULL,
                        payout NUMERIC(20, 0) NOT NULL,
                        balance_type VARCHAR(16) NOT NULL,
                        win_percent NUMERIC(6, 2) NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS referral_rewards (
                        id BIGSERIAL PRIMARY KEY,
                        referrer_id VARCHAR(255) NOT NULL,
                        referral_id VARCHAR(255) NOT NULL,
                        source_type VARCHAR(20) NOT NULL,
                        source_id VARCHAR(128) NOT NULL,
                        deposit_amount NUMERIC(20, 2) NOT NULL,
                        reward_amount NUMERIC(20, 2) NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE (source_type, source_id)
                    )
                """)
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_referral_rewards_owner ON referral_rewards(referrer_id, created_at DESC)")
                await conn.execute(
                    "INSERT INTO dnx_schema_migrations(version) VALUES($1) ON CONFLICT DO NOTHING",
                    migration_version,
                )
                logging.info(f"DB initialized. Game number: {last_num}")
    except Exception as e:
        logging.error(f"DB Init Error: {e}")
        raise


async def recover_unfinished_game_bets() -> int:
    """Refund bets left pending by a previous Render process.

    The visual round lives in memory and cannot be resumed safely after a
    process restart. The durable wager ledger makes the only safe recovery
    deterministic: return the exact stake to the balance it came from.
    """
    refunded = 0
    next_game_number = None
    # Keep wager recovery and choosing the next in-memory round atomic with
    # respect to new bets.  A refunded/old-process ledger row uses
    # (game_number, user_id) as its primary key; reusing that game number made
    # every later wager conflict forever even though the balance was valid.
    async with game_lock:
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch("""
                    UPDATE active_game_bets
                    SET status = 'refunded', updated_at = NOW()
                    WHERE status = 'pending'
                      AND owner_token IS DISTINCT FROM $1
                      AND updated_at <= NOW() - INTERVAL '2 minutes'
                    RETURNING game_number, user_id, amount, balance_type
                """, PROCESS_INSTANCE_ID)
                for row in rows:
                    balance_column = "bonus_balance" if row["balance_type"] == "bonus" else "balance"
                    updated = await conn.execute(
                        f"UPDATE users SET {balance_column} = {balance_column} + $1 WHERE id = $2",
                        row["amount"],
                        row["user_id"],
                    )
                    if updated != "UPDATE 1":
                        raise RuntimeError(f"Cannot refund missing game user {row['user_id']}")
                    await record_user_event(
                        conn,
                        row["user_id"],
                        "game_refund",
                        amount=row["amount"],
                        balance_type=row["balance_type"],
                        title="Возврат ставки после перезапуска",
                        metadata={"game_number": row["game_number"], "reason": "server_restart"},
                    )
                    refunded += 1

                # On a Render restart the database can still contain either a
                # live wager owned by the previous process or an already
                # refunded terminal row for the current counter value.  Start
                # this process on a fresh number immediately instead of making
                # every user wait for/deadlock on that primary-key collision.
                if not game_state["players"] and game_state["status"] == "waiting":
                    current_number = int(game_state.get("game_number", 0))
                    collision = await conn.fetchval(
                        "SELECT 1 FROM active_game_bets WHERE game_number = $1 LIMIT 1",
                        current_number,
                    )
                    if collision:
                        next_game_number = int(await conn.fetchval(
                            """
                            UPDATE game_counter
                            SET last_game_number = GREATEST(last_game_number, $1)
                            WHERE id = 1
                            RETURNING last_game_number
                            """,
                            current_number + 1,
                        ))
            if next_game_number is not None:
                game_state["game_number"] = next_game_number
                game_state["last_polygons"] = None
                game_state["polygons"] = None
                logging.warning(
                    "Advanced game number to %s after detecting a stale wager ledger row",
                    next_game_number,
                )
    if refunded:
        logging.warning("Safely refunded %s unfinished game bet(s)", refunded)
    return refunded

def extract_user_from_initdata(init_data_str: str) -> dict | None:
    if not init_data_str:
        return None
    cache_key = hashlib.sha256(init_data_str.encode("utf-8")).hexdigest()
    now_mono = time.monotonic()
    cached = auth_cache.get(cache_key)
    if cached and cached[0] > now_mono:
        auth_cache.move_to_end(cache_key)
        return dict(cached[1])
    if cached:
        auth_cache.pop(cache_key, None)
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
        result = {
            "id": str(user_id),
            "username": str(username)[:255],
            "photo_url": safe_https_url(user.get("photo_url")),
        }
        remaining_validity = max(1, INIT_DATA_MAX_AGE - max(0, now - auth_date))
        auth_cache[cache_key] = (
            now_mono + min(AUTH_CACHE_TTL, remaining_validity),
            result,
        )
        auth_cache.move_to_end(cache_key)
        while len(auth_cache) > AUTH_CACHE_MAX:
            auth_cache.popitem(last=False)
        return dict(result)
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def safe_https_url(value) -> str:
    if not isinstance(value, str) or len(value) > 2048:
        return ""
    parsed = urlparse(value)
    return value if parsed.scheme == "https" and parsed.netloc else ""


def canonical_telegram_nft_url(value) -> str:
    if not isinstance(value, str) or len(value) > 512:
        return ""
    parsed = urlparse(value.strip())
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or hostname not in {"t.me", "telegram.me"}:
        return ""
    match = re.fullmatch(r"/nft/([A-Za-z0-9_-]{3,96})/?", parsed.path)
    if not match:
        return ""
    return f"https://t.me/nft/{match.group(1)}"


def safe_telegram_media_url(value) -> str:
    if not isinstance(value, str) or len(value) > 4096:
        return ""
    parsed = urlparse(value)
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not (
        hostname == "telesco.pe" or hostname.endswith(".telesco.pe")
    ):
        return ""
    return value


def normalize_hex_color(value, fallback="") -> str:
    if isinstance(value, str) and re.fullmatch(r"#[0-9a-fA-F]{6}", value.strip()):
        return value.strip().upper()
    return fallback


class TelegramNftMediaParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tgs_url = ""
        self.pattern_url = ""
        self.preview_url = ""
        self.gradient_colors: list[str] = []
        self.pattern_color = "#000000"
        self._inside_gift_gradient = False

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "source" and attributes.get("type") == "application/x-tgsticker":
            self.tgs_url = self.tgs_url or safe_telegram_media_url(attributes.get("srcset", ""))
        elif tag == "image" and attributes.get("id") == "giftPattern":
            raw_url = attributes.get("xlink:href") or attributes.get("href") or ""
            self.pattern_url = self.pattern_url or safe_telegram_media_url(raw_url)
        elif tag == "meta" and attributes.get("property") == "og:image":
            self.preview_url = self.preview_url or safe_telegram_media_url(attributes.get("content", ""))
        elif tag == "radialgradient" and attributes.get("id") == "giftGradient":
            self._inside_gift_gradient = True
        elif tag == "stop" and self._inside_gift_gradient:
            color = normalize_hex_color(attributes.get("stop-color"))
            if color and len(self.gradient_colors) < 4:
                self.gradient_colors.append(color)
        elif tag == "feflood" and attributes.get("id") == "giftGradienPatternColor":
            self.pattern_color = normalize_hex_color(attributes.get("flood-color"), "#000000")

    def handle_endtag(self, tag):
        if tag == "radialgradient":
            self._inside_gift_gradient = False


def parse_positive_amount(value, *, minimum=Decimal("0.01"), maximum=Decimal("1000000")) -> Decimal | None:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not amount.is_finite() or amount < minimum or amount > maximum:
        return None
    return amount


def parse_star_amount(value, *, minimum=STAR_MIN_TOPUP, maximum=STAR_MAX_TOPUP) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not amount.is_finite() or amount != amount.to_integral_value():
        return None
    integer = int(amount)
    return integer if minimum <= integer <= maximum else None


def normalize_records(rows) -> list[dict]:
    result = []
    for row in rows:
        item = dict(row)
        for key, value in list(item.items()):
            if isinstance(value, Decimal):
                item[key] = int(value) if value == value.to_integral_value() else float(value)
            elif isinstance(value, (datetime, date)):
                item[key] = value.isoformat()
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
        if handler.__name__ != "handle_get_user":
            try:
                restriction = await get_account_restriction(user['id'])
            except Exception as exc:
                logging.error("Restriction check failed for %s: %s", user['id'], exc)
                return web.json_response(
                    {"error": "database_unavailable"},
                    status=503,
                    headers={"Access-Control-Allow-Origin": CORS_ORIGIN},
                )
            if restriction:
                return web.json_response(
                    {"success": False, "error": "account_restricted", "restriction": restriction},
                    status=423,
                    headers={"Access-Control-Allow-Origin": CORS_ORIGIN},
                )
        return await handler(request)

    return wrapper


_rate_limit_events: dict[tuple[str, str], deque] = defaultdict(deque)
_rate_limit_calls = 0


def _prune_rate_limits(now: float) -> None:
    """Keep the process-local limiter bounded when many users visit once."""
    if len(_rate_limit_events) <= 2048:
        return
    for key, events in list(_rate_limit_events.items()):
        if not events or now - events[-1] > 3600:
            _rate_limit_events.pop(key, None)
    while len(_rate_limit_events) > 4096:
        _rate_limit_events.pop(next(iter(_rate_limit_events)), None)


def rate_limit(limit: int, window_seconds: int):
    def decorator(handler):
        @wraps(handler)
        async def wrapper(request):
            global _rate_limit_calls
            user = request.get('telegram_user')
            identity = user['id'] if user else (request.remote or 'unknown')
            key = (handler.__name__, identity)
            now = time.monotonic()
            _rate_limit_calls += 1
            if _rate_limit_calls % 256 == 0:
                _prune_rate_limits(now)
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
        base_angle = secure_random.choice([math.pi / 4, 3 * math.pi / 4])
        angle = base_angle + secure_random.uniform(-0.2, 0.2)
    else:
        angle = secure_random.uniform(0, math.pi)

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
    start_x = secure_random.uniform(0.1, 0.9) * 1000
    start_y = secure_random.uniform(0.1, 0.9) * 1000

    spin_duration = 3000
    spin_angle_speed = secure_random.uniform(4.5 * math.pi, 13.5 * math.pi)
    spin_angle_start = secure_random.uniform(0, 2 * math.pi)

    angle_total = 0.5 * spin_angle_speed * (spin_duration / 1000)
    final_angle = spin_angle_start + angle_total

    base_speed = secure_random.uniform(4000, 4500)
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
# Три основных цвета в каждой палитре. Зелёный диапазон намеренно исключён:
# остаются фиолетовый, розовый, красный, оранжевый, голубой и синий — тона,
# которые поддерживают космический стиль и не теряются на фоне игрового поля.
PLAYER_COLORS: list[tuple[str, str, str]] = [
    ("#5024D6", "#8B5CFF", "#F149D8"),
    ("#233BD4", "#4D8DFF", "#A855F7"),
    ("#8F1CB8", "#E13FFF", "#FF5B9D"),
    ("#A60D4E", "#FF356F", "#FF8A66"),
    ("#0569C7", "#23C4FF", "#7C5CFF"),
    ("#B83B0D", "#FF7A24", "#FF3D81"),
    ("#3420A4", "#665DFF", "#FF4EB8"),
    ("#8D123A", "#E22B69", "#9D4DFF"),
    ("#0867B5", "#45B7FF", "#5B46E8"),
    ("#A94C05", "#FFB224", "#DD3FE4"),
    ("#B30F74", "#FF4D9D", "#5E64FF"),
    ("#4B148C", "#A53DF2", "#FF6F61"),
    ("#1438A6", "#4777FF", "#C16BFF"),
    ("#70133F", "#E23578", "#FF9D42"),
    ("#164FD4", "#22B5F4", "#F143C1"),
    ("#5A146F", "#D02A8A", "#FFB43B"),
    ("#2B2CB9", "#784BFF", "#FF45A5"),
    ("#A5122E", "#FF4B57", "#B848F1"),
    ("#0758D8", "#3C9CFF", "#D14EFF"),
    ("#8A2B0A", "#F26A2E", "#B83DD8"),
]
def _hex_hue(color: str) -> float:
    value = color.lstrip("#")
    red, green, blue = (int(value[pos:pos + 2], 16) / 255.0 for pos in (0, 2, 4))
    hue, _, _ = colorsys.rgb_to_hsv(red, green, blue)
    return hue * 360.0


PLAYER_PALETTE_HUES = {palette: _hex_hue(palette[0]) for palette in PLAYER_COLORS}


def _hue_distance(first: float, second: float) -> float:
    direct = abs(first - second) % 360.0
    return min(direct, 360.0 - direct)


def choose_player_palette(occupied_colors: set[tuple[str, ...]]) -> tuple[str, ...]:
    available = [palette for palette in PLAYER_COLORS if palette not in occupied_colors]
    if not available:
        return secure_random.choice(PLAYER_COLORS)
    if not occupied_colors:
        return secure_random.choice(available)

    occupied_hues = [
        PLAYER_PALETTE_HUES.get(palette, _hex_hue(palette[0]))
        for palette in occupied_colors
    ]
    scored = []
    for palette in available:
        hue = PLAYER_PALETTE_HUES[palette]
        score = min(_hue_distance(hue, occupied) for occupied in occupied_hues)
        scored.append((score, palette))
    best_score = max(score for score, _ in scored)
    return secure_random.choice([palette for score, palette in scored if abs(score - best_score) < 1e-9])

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
        raise RuntimeError("Round has no final point or polygons")
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
        raise RuntimeError("Final point is outside every player polygon")

    winner_bet = players[winner_id]["amount"]
    others_bets = pool - winner_bet
    winner_balance_type = players[winner_id].get("balance_type", "main")
    profit_share = 0.4 if winner_balance_type == "bonus" else 0.7
    profit = floor_stars(winner_bet + (others_bets * profit_share))

    if pool > 0:
        win_percent = round((winner_bet / pool) * 100, 1)
    else:
        win_percent = 100.0

    game_number = int(game_state["game_number"])
    round_id = game_state["round_id"]
    async with get_pool().acquire() as conn:
        async with conn.transaction():
            existing_settlement = await conn.fetchrow(
                """
                SELECT winner_id, winner_username, payout, balance_type, win_percent
                FROM game_round_settlements WHERE game_number = $1
                """,
                game_number,
            )
            if existing_settlement:
                # A connection may fail after PostgreSQL has committed. The
                # unique settlement row proves the payout already happened.
                winner_id = existing_settlement["winner_id"]
                winner_username = existing_settlement["winner_username"]
                profit = existing_settlement["payout"]
                winner_balance_type = existing_settlement["balance_type"]
                game_state["game_number"] = int(
                    await conn.fetchval(
                        "SELECT last_game_number FROM game_counter WHERE id = 1"
                    ) or game_number
                )
            else:
                ledger_rows = await conn.fetch(
                    """
                    SELECT user_id, amount, balance_type
                    FROM active_game_bets
                    WHERE game_number = $1 AND status = 'pending' AND owner_token = $2
                    FOR UPDATE
                    """,
                    game_number,
                    PROCESS_INSTANCE_ID,
                )
                ledger = {row["user_id"]: row for row in ledger_rows}
                if set(ledger) != set(players):
                    raise RuntimeError("Game wager ledger does not match active players")
                ledger_total = sum((row["amount"] for row in ledger_rows), Decimal("0"))
                if ledger_total != floor_stars(pool):
                    raise RuntimeError("Game wager ledger total does not match pool")
                for player_id, player in players.items():
                    if ledger[player_id]["amount"] != floor_stars(player["amount"]):
                        raise RuntimeError(f"Game wager mismatch for user {player_id}")
                    if ledger[player_id]["balance_type"] != player.get("balance_type", "main"):
                        raise RuntimeError(f"Game balance type mismatch for user {player_id}")
                if not await conn.fetchval("SELECT id FROM users WHERE id = $1", winner_id):
                    raise RuntimeError("Winner account does not exist")

                inserted = await conn.fetchval(
                    """
                    INSERT INTO game_round_settlements
                        (game_number, round_id, winner_id, winner_username, payout,
                         balance_type, win_percent)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (game_number) DO NOTHING
                    RETURNING game_number
                    """,
                    game_number,
                    round_id,
                    winner_id,
                    winner_username,
                    profit,
                    winner_balance_type,
                    Decimal(str(win_percent)),
                )
                if inserted is None:
                    raise RuntimeError("Concurrent game settlement detected")

                updated = await conn.execute(
                    "UPDATE users SET balance = balance + $1 WHERE id = $2",
                    profit,
                    winner_id,
                )
                if updated != "UPDATE 1":
                    raise RuntimeError("Winner credit failed")
                await conn.execute("""
                    INSERT INTO leaderboard (user_id, username, wins) VALUES ($1, $2, 1)
                    ON CONFLICT (user_id) DO UPDATE SET wins = leaderboard.wins + 1, username = EXCLUDED.username
                """, winner_id, winner_username)
                await conn.execute(
                    "INSERT INTO game_history (game_number, winner_name, win_amount, win_percent) VALUES ($1, $2, $3, $4)",
                    game_number, winner_username, profit, Decimal(str(win_percent))
                )
                await record_user_event(
                    conn,
                    winner_id,
                    "game_win",
                    amount=profit,
                    balance_type="main",
                    title="Выигрыш в игре",
                    metadata={
                        "game_number": game_number,
                        "bet": winner_bet,
                        "stake_balance_type": winner_balance_type,
                        "win_percent": win_percent,
                    },
                )
                ledger_update = await conn.execute(
                    """
                    UPDATE active_game_bets
                    SET status = 'settled', updated_at = NOW()
                    WHERE game_number = $1 AND status = 'pending' AND owner_token = $2
                    """,
                    game_number,
                    PROCESS_INSTANCE_ID,
                )
                if ledger_update != f"UPDATE {len(players)}":
                    raise RuntimeError("Not every wager was marked settled")
                await conn.execute(
                    "DELETE FROM game_history WHERE id NOT IN (SELECT id FROM game_history ORDER BY game_number DESC LIMIT 100)")
                new_num = await conn.fetchval(
                    "UPDATE game_counter SET last_game_number = last_game_number + 1 WHERE id = 1 RETURNING last_game_number")
                game_state["game_number"] = int(new_num)
    return {
        "user_id": winner_id,
        "username": winner_username,
        "win_amount": int(profit),
        "photo_url": photo_url,
        "round_id": round_id,
        "polygon": winner_polygon,
        "balance_type": winner_balance_type,
    }

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
                    game_state["round_id"] = secure_random.randint(1, 10 ** 9)
                    game_state["status"] = "spinning"
                    game_state["winner"] = None
                    game_state["last_winner_id"] = None

        if game_state["status"] == "spinning":
            await asyncio.sleep(3 + 1 + 10 + 1 + 0.5)
            while True:
                settlement_failed = False
                async with game_lock:
                    if game_state["status"] != "spinning":
                        break
                    try:
                        final_point = game_state["spin_params"]["trajectory"][-1]
                        winner_data = await finish_round(
                            final_point,
                            game_state["pool"],
                            game_state["players"],
                            game_state["polygons"]
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        settlement_failed = True
                        logging.exception(
                            "Round %s settlement failed; keeping wagers locked and retrying",
                            game_state.get("game_number"),
                        )
                    else:
                        game_state["winner"] = winner_data
                        game_state["last_winner_id"] = winner_data["user_id"]
                        game_state["last_polygons"] = game_state["polygons"]
                        game_state["status"] = "waiting"
                        game_state["players"] = {}
                        game_state["pool"] = 0.0
                        game_state["timer"] = 15
                        game_state["polygons"] = None
                        asyncio.create_task(clear_last_polygons_after_delay(0.3))
                        break
                if settlement_failed:
                    await asyncio.sleep(2)


async def scan_ton_deposits() -> int:
    if not TON_RECEIVER_ADDRESS:
        return 0
    await get_pool().execute(
        "UPDATE ton_deposits SET status = 'expired' WHERE status = 'pending' AND expires_at < NOW()")
    pending = await get_pool().fetch("""
        SELECT id, user_id, wallet_raw, amount_nano, amount_ton, credit_stars, created_at, expires_at
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
                    RETURNING user_id, credit_stars
                """, tx_hash, deposit["id"])
                if not row:
                    continue
                await credit_main_deposit(
                    conn,
                    row["user_id"],
                    row["credit_stars"],
                    "ton",
                    str(deposit["id"]),
                )
        try:
            await bot.send_message(
                int(row["user_id"]),
                "✨ ПОПОЛНЕНИЕ УСПЕШНО\n\n"
                f"⭐ Ваш баланс успешно пополнен на {row['credit_stars']} Stars.\n"
                "Способ оплаты: Toncoin (TON)\n\n"
                "Спасибо, что выбираете DNX Store!"
            )
        except Exception as exc:
            logging.warning("Could not send TON credit notification to %s: %s", row["user_id"], exc)
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
async def read_json_object(request: web.Request) -> dict:
    """Parse only JSON objects; malformed/list bodies become harmless input."""
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


async def handle_options(request):
    return web.Response(headers={
        "Access-Control-Allow-Origin": CORS_ORIGIN,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Telegram-Init-Data",
    })


@web.middleware
async def security_headers_middleware(request, handler):
    request_origin = request.headers.get("Origin")
    if request_origin and request_origin != CORS_ORIGIN:
        response = web.json_response({"error": "origin_not_allowed"}, status=403)
    else:
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            response = exc
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception("Unhandled API error on %s %s", request.method, request.path)
            response = web.json_response({"error": "internal_error"}, status=500)
    response.headers["Access-Control-Allow-Origin"] = CORS_ORIGIN
    response.headers["Vary"] = "Origin"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Cache-Control"] = "no-store"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    if (
        response.content_type == "application/json"
        and response.body is not None
        and len(response.body) >= 2048
    ):
        response.enable_compression()
    return response


async def handle_health(request):
    """Render liveness endpoint.

    This endpoint deliberately stays HTTP 200 while external dependencies are
    reconnecting.  Render must be able to discover the listening port before a
    cold Neon connection or a schema migration has completed.
    """
    database_status = runtime_state["database"]
    try:
        if db_pool is not None:
            await asyncio.wait_for(db_pool.fetchval("SELECT 1"), timeout=2)
            database_status = "ok"
    except Exception:
        database_status = "unavailable"
    return web.json_response({
        "status": "ok" if runtime_state["ready"] else "starting",
        "ready": runtime_state["ready"],
        "phase": runtime_state["phase"],
        "database": database_status,
        "attempt": runtime_state["attempt"],
        "release": API_RELEASE,
    })

@require_auth
async def handle_get_user(request):
    user = request['telegram_user']
    user_id = user['id']
    username = user['username']
    try:
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                await ensure_user(conn, user_id, username)
                row = await conn.fetchrow(
                    """
                    SELECT balance, bonus_balance, referral_code, mut, ban, mut_until,
                           (SELECT COUNT(*) FROM users referred WHERE referred.referred_by = users.id) AS referrals_count,
                           (SELECT COALESCE(SUM(reward_amount), 0) FROM referral_rewards WHERE referrer_id = users.id) AS referral_earned
                    FROM users WHERE id = $1
                    """,
                    user_id,
                )
        balance = int(row["balance"] or 0)
        bonus_balance = int(row["bonus_balance"] or 0)
        referral_link = (
            f"https://t.me/{BOT_USERNAME}?start=ref_{row['referral_code']}"
            if BOT_USERNAME and row["referral_code"]
            else ""
        )
        restriction = await get_account_restriction(user_id)
    except Exception as e:
        logging.error(f"Get user error: {e}")
        return web.json_response({"error": "database_unavailable"}, status=503,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    return web.json_response({
        "balance": balance,
        "bonusBalance": bonus_balance,
        "referralLink": referral_link,
        "referralsCount": int(row["referrals_count"] or 0),
        "referralEarned": int(Decimal(str(row["referral_earned"] or 0)).to_integral_value(rounding=ROUND_FLOOR)),
        "restriction": restriction,
    }, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
@rate_limit(10, 60)
async def handle_create_ton_deposit(request):
    if not TON_RECEIVER_ADDRESS:
        return web.json_response({"error": "ton_deposits_disabled"}, status=503,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    try:
        data = await read_json_object(request)
        user = request['telegram_user']
        user_id = user['id']
        username = user['username']
        stars = parse_star_amount(data.get('stars'))
        if stars is None:
            return web.json_response({"success": False, "error": "invalid_stars"}, status=400)
        base_nano = int(
            (Decimal(stars) / TON_STAR_RATE * Decimal("1000000000")).to_integral_value(
                rounding=ROUND_CEILING))
        wallet_address = str(data.get("walletAddress") or "").strip()
        try:
            wallet_raw = await normalize_ton_address(wallet_address)
        except ValueError:
            return web.json_response({"success": False, "error": "invalid_wallet"}, status=400)

        async with get_pool().acquire() as conn:
            async with conn.transaction():
                await ensure_user(conn, user_id, username)
                existing = await conn.fetchrow("""
                    SELECT id, amount_nano, amount_ton, credit_stars, expires_at
                    FROM ton_deposits
                    WHERE user_id = $1 AND wallet_raw = $2 AND status = 'pending'
                      AND expires_at > NOW() AND credit_stars = $3
                    ORDER BY created_at DESC LIMIT 1
                """, user_id, wallet_raw, stars)
                if existing:
                    return web.json_response({
                        "success": True,
                        "depositId": str(existing["id"]),
                        "receiverAddress": TON_RECEIVER_ADDRESS,
                        "amountNano": str(existing["amount_nano"]),
                        "amountTon": format(existing["amount_ton"], "f"),
                        "creditStars": existing["credit_stars"],
                        "expiresAt": existing["expires_at"].isoformat(),
                    }, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                pending_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM ton_deposits WHERE user_id = $1 AND status = 'pending' AND expires_at > NOW()",
                    user_id)
                if pending_count >= 3:
                    return web.json_response({"success": False, "error": "too_many_pending"}, status=429)

                deposit_id = uuid.uuid4()
                for _ in range(10):
                    amount_nano = base_nano + secrets.randbelow(999999) + 1
                    amount_ton = Decimal(amount_nano) / Decimal("1000000000")
                    try:
                        async with conn.transaction():
                            expires_at = await conn.fetchval("""
                                INSERT INTO ton_deposits
                                    (id, user_id, wallet_address, wallet_raw, amount_nano, amount_ton, credit_stars, expires_at)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW() + $8 * INTERVAL '1 second')
                                RETURNING expires_at
                            """, deposit_id, user_id, wallet_address, wallet_raw, amount_nano,
                                 amount_ton, stars, TON_DEPOSIT_TIMEOUT)
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
            "creditStars": stars,
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
        SELECT status, amount_ton, credit_stars, tx_hash, expires_at
        FROM ton_deposits WHERE id = $1 AND user_id = $2
    """, deposit_id, request['telegram_user']['id'])
    if not row:
        return web.json_response({"error": "deposit_not_found"}, status=404)
    return web.json_response({
        "status": row["status"],
        "amountTon": format(row["amount_ton"], "f"),
        "creditStars": row["credit_stars"],
        "txHash": row["tx_hash"],
        "expiresAt": row["expires_at"].isoformat(),
    }, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


@require_auth
@rate_limit(10, 60)
async def handle_create_star_invoice(request):
    try:
        data = await read_json_object(request)
        stars = parse_star_amount(data.get("stars"))
        if stars is None:
            return web.json_response({"success": False, "error": "invalid_stars"}, status=400)
        user = request["telegram_user"]
        payment_id = uuid.uuid4()
        payload = f"dnx-stars:{payment_id}"
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                await ensure_user(conn, user["id"], user["username"])
                await conn.execute("""
                    UPDATE star_payments SET status = 'expired'
                    WHERE user_id = $1 AND status = 'pending' AND expires_at < NOW()
                """, user["id"])
                pending_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM star_payments
                    WHERE user_id = $1 AND status = 'pending' AND expires_at > NOW()
                """, user["id"])
                if pending_count >= 3:
                    return web.json_response({"success": False, "error": "too_many_pending"}, status=429)
                await conn.execute("""
                    INSERT INTO star_payments (id, user_id, stars, invoice_payload, expires_at)
                    VALUES ($1, $2, $3, $4, NOW() + INTERVAL '30 minutes')
                """, payment_id, user["id"], stars, payload)
        try:
            invoice_link = await bot.create_invoice_link(
                title="Пополнение DNX Store",
                description=f"Зачисление {stars} Telegram Stars на баланс DNX Store",
                payload=payload,
                currency="XTR",
                prices=[LabeledPrice(label=f"{stars} Stars", amount=stars)],
            )
        except Exception:
            await get_pool().execute(
                "UPDATE star_payments SET status = 'failed' WHERE id = $1 AND status = 'pending'",
                payment_id)
            raise
        return web.json_response({
            "success": True,
            "paymentId": str(payment_id),
            "stars": stars,
            "invoiceLink": invoice_link,
        })
    except Exception as exc:
        logging.error("Stars invoice creation error: %s", exc)
        return web.json_response({"success": False, "error": "stars_invoice_error"}, status=500)


async def reconcile_star_payment(payment_id: uuid.UUID, user_id: str) -> bool:
    """Recover a paid invoice if Telegram's successful_payment update was missed."""
    payment_key = str(payment_id)
    now = time.monotonic()
    # Normal payments arrive through successful_payment. This is a fallback,
    # so querying Telegram on every 1.8 s UI poll only creates latency and
    # rate-limit pressure without improving the normal path.
    if now - star_reconcile_attempts.get(payment_key, 0) < 15:
        return False
    star_reconcile_attempts[payment_key] = now
    if len(star_reconcile_attempts) > 1024:
        cutoff = now - 900
        for key, attempted_at in list(star_reconcile_attempts.items()):
            if attempted_at < cutoff:
                star_reconcile_attempts.pop(key, None)

    payment_row = await get_pool().fetchrow("""
        SELECT id, user_id, stars, invoice_payload, status, created_at
        FROM star_payments WHERE id = $1 AND user_id = $2
    """, payment_id, user_id)
    if not payment_row or payment_row["status"] != "pending":
        return False
    if payment_row["created_at"].timestamp() > time.time() - 2:
        return False

    transactions = await bot.get_star_transactions(offset=0, limit=100)
    matched_transaction = None
    for transaction in transactions.transactions:
        source = transaction.source
        source_user = getattr(source, "user", None)
        if (
            getattr(source, "invoice_payload", None) == payment_row["invoice_payload"]
            and int(transaction.amount) == int(payment_row["stars"])
            and source_user is not None
            and str(source_user.id) == str(payment_row["user_id"])
        ):
            matched_transaction = transaction
            break
    if matched_transaction is None:
        return False

    credited = False
    async with get_pool().acquire() as conn:
        async with conn.transaction():
            updated = await conn.fetchrow("""
                UPDATE star_payments
                SET status = 'paid', telegram_star_transaction_id = $1, paid_at = NOW()
                WHERE id = $2 AND user_id = $3 AND status = 'pending'
                RETURNING user_id, stars
            """, matched_transaction.id, payment_id, user_id)
            if updated:
                await credit_main_deposit(
                    conn,
                    updated["user_id"],
                    updated["stars"],
                    "stars",
                    str(payment_id),
                )
                credited = True
    if credited:
        try:
            await bot.send_message(
                int(user_id),
                "✨ ПОПОЛНЕНИЕ ВОССТАНОВЛЕНО\n\n"
                f"⭐ На внутренний баланс начислено {payment_row['stars']} Stars.\n"
                "Платёж найден в официальной истории транзакций Telegram."
            )
        except Exception:
            pass
        logging.info("Reconciled Telegram Stars payment %s for user %s", payment_id, user_id)
    return credited


async def reconcile_recent_star_transactions(limit: int = 100) -> int:
    """Credit authoritative incoming Telegram Stars transactions missed by polling."""
    history = await bot.get_star_transactions(offset=0, limit=max(1, min(limit, 100)))
    credited_count = 0
    for transaction in history.transactions:
        source = transaction.source
        invoice_payload = getattr(source, "invoice_payload", None)
        source_user = getattr(source, "user", None)
        if not invoice_payload or source_user is None or int(transaction.amount) <= 0:
            continue
        just_credited = False
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("""
                    SELECT id, user_id, stars, status
                    FROM star_payments WHERE invoice_payload = $1 FOR UPDATE
                """, invoice_payload)
                if (
                    not row
                    or row["status"] == "paid"
                    or row["user_id"] != str(source_user.id)
                    or int(row["stars"]) != int(transaction.amount)
                ):
                    continue
                updated = await conn.fetchrow("""
                    UPDATE star_payments
                    SET status = 'paid', telegram_star_transaction_id = $1, paid_at = NOW()
                    WHERE id = $2 AND status <> 'paid'
                    RETURNING user_id, stars
                """, transaction.id, row["id"])
                if not updated:
                    continue
                await credit_main_deposit(
                    conn,
                    updated["user_id"],
                    updated["stars"],
                    "stars",
                    str(row["id"]),
                )
                credited_count += 1
                just_credited = True
        if just_credited:
            try:
                await bot.send_message(
                    int(source_user.id),
                    "✨ ПОПОЛНЕНИЕ ВОССТАНОВЛЕНО\n\n"
                    f"⭐ На внутренний баланс начислено {int(transaction.amount)} Stars.\n"
                    "Платёж подтверждён официальной историей Telegram."
                )
            except Exception:
                pass
    return credited_count


async def star_reconciliation_worker():
    while True:
        next_delay = 300
        try:
            needs_reconciliation = await get_pool().fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM star_payments
                    WHERE status IN ('pending', 'expired')
                      AND created_at > NOW() - INTERVAL '2 days'
                )
            """)
            recovered = (
                await reconcile_recent_star_transactions()
                if needs_reconciliation else 0
            )
            if recovered:
                logging.info("Recovered %s missed Telegram Stars payment(s)", recovered)
                next_delay = 60
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning("Telegram Stars reconciliation worker error: %s", exc)
            next_delay = 60
        await asyncio.sleep(next_delay)


async def moderation_expiry_worker():
    """Run small periodic integrity cleanups outside latency-sensitive requests."""
    while True:
        try:
            result = await get_pool().execute(
                "UPDATE users SET mut = NULL, mut_until = NULL WHERE mut IS NOT NULL AND mut_until <= NOW()"
            )
            if result != "UPDATE 0":
                logging.info("Expired account restrictions cleared: %s", result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning("Moderation expiry cleanup failed: %s", exc)
        try:
            await recover_unfinished_game_bets()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning("Orphan wager recovery failed: %s", exc)
        await asyncio.sleep(60)


@require_auth
async def handle_star_payment_status(request):
    try:
        payment_id = uuid.UUID(request.query.get("id", ""))
    except (ValueError, TypeError):
        return web.json_response({"error": "invalid_payment_id"}, status=400)
    row = await get_pool().fetchrow("""
        SELECT status, stars, expires_at FROM star_payments
        WHERE id = $1 AND user_id = $2
    """, payment_id, request["telegram_user"]["id"])
    if not row:
        return web.json_response({"error": "payment_not_found"}, status=404)
    status = row["status"]
    if status == "pending":
        try:
            if await reconcile_star_payment(payment_id, request["telegram_user"]["id"]):
                row = await get_pool().fetchrow(
                    "SELECT status, stars, expires_at FROM star_payments WHERE id = $1 AND user_id = $2",
                    payment_id, request["telegram_user"]["id"])
                status = row["status"]
        except Exception as exc:
            logging.warning("Stars payment reconciliation failed for %s: %s", payment_id, exc)
    if status == "pending" and row["expires_at"].timestamp() < time.time():
        await get_pool().execute(
            "UPDATE star_payments SET status = 'expired' WHERE id = $1 AND status = 'pending'",
            payment_id)
        status = "expired"
    return web.json_response({"status": status, "stars": row["stars"]})

@require_auth
async def handle_get_items(request):
    try:
        rows = await get_pool().fetch(
            "SELECT id, name, price, status, image_url, nft_link, number, model, pattern, background FROM items WHERE status = 'Доступен'")
        items = normalize_records(rows)
        now = time.monotonic()
        for item in items:
            source_url = canonical_telegram_nft_url(item.get("nft_link"))
            cached = nft_media_cache.get(source_url) if source_url else None
            if cached and cached[0] > now and cached[1].get("animated"):
                item["nft_media"] = cached[1]
        return web.json_response(items, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Get items error: {e}")
        return web.json_response({"error": "database_unavailable"}, status=503,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


@require_auth
@rate_limit(90, 60)
async def handle_get_item(request):
    """Return one authoritative item row for a catalogue/inventory modal.

    The modal intentionally does not trust the catalogue snapshot: model,
    pattern and background are always read from the current `items` row by id.
    """
    try:
        item_id = int(request.query.get("id", "0"))
    except (TypeError, ValueError):
        item_id = 0
    if item_id <= 0:
        return web.json_response({"error": "invalid_item_id"}, status=400)

    user_id = request["telegram_user"]["id"]
    try:
        row = await get_pool().fetchrow(
            """
            SELECT id, name, price, status, image_url, nft_link, number,
                   model, pattern, background, acquisition_source, last_event,
                   withdraw_requested_at, withdraw_expires_at, disposed_at,
                   GREATEST(0, EXTRACT(EPOCH FROM (withdraw_expires_at - NOW())))::BIGINT
                       AS withdraw_remaining_seconds
            FROM items
            WHERE id = $1
              AND (status = 'Доступен' OR buyer_id = $2)
            """,
            item_id,
            user_id,
        )
        if not row:
            return web.json_response({"error": "item_not_found"}, status=404)

        item = normalize_records([row])[0]
        raw_status = item.get("status")
        item["is_shop_item"] = raw_status == "Доступен"
        if raw_status in ("Выведен", "withdrawn"):
            item["status"] = "withdrawn"
        item["can_sell"] = (
            item.get("acquisition_source") == "case"
            and item.get("status") == "Продан"
        )
        item["sell_value"] = item.get("price", 0)
        item["withdraw_remaining_seconds"] = int(item.get("withdraw_remaining_seconds") or 0)
        item["traits_source"] = "items.model/items.pattern/items.background"
        item["api_release"] = API_RELEASE
        item["row_digest"] = hashlib.sha256(
            "|".join(
                str(item.get(key) if item.get(key) is not None else "NULL")
                for key in ("id", "model", "pattern", "background")
            ).encode("utf-8")
        ).hexdigest()[:12]
        return web.json_response(
            item,
            headers={
                "Access-Control-Allow-Origin": CORS_ORIGIN,
                "Cache-Control": "no-store, no-cache, must-revalidate",
                "X-DNX-Release": API_RELEASE,
            },
        )
    except Exception as exc:
        logging.error("Get item %s error: %s", item_id, exc)
        return web.json_response({"error": "database_unavailable"}, status=503,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


@require_auth
@rate_limit(30, 60)
async def handle_admin_db_audit(request):
    """Admin-only proof of the exact Neon database and exact item row in use."""
    if str(request["telegram_user"]["id"]) != str(ADMIN_ID):
        return web.json_response({"error": "forbidden"}, status=403)
    try:
        item_id = int(request.query.get("item_id", "0"))
    except (TypeError, ValueError):
        item_id = 0
    async with get_pool().acquire() as conn:
        identity = await database_identity(conn)
        columns = [
            row["column_name"]
            for row in await conn.fetch(
                """SELECT column_name
                   FROM information_schema.columns
                   WHERE table_schema = current_schema() AND table_name = 'items'
                   ORDER BY ordinal_position"""
            )
        ]
        item = None
        if item_id > 0:
            row = await conn.fetchrow(
                """SELECT id, name, model, pattern, background, status, buyer_id,
                          acquisition_source, last_event
                   FROM items WHERE id = $1""",
                item_id,
            )
            item = dict(row) if row else None
    return web.json_response(
        {
            "api_release": API_RELEASE,
            "database": identity,
            "items_columns": columns,
            "legacy_traits_present": "traits" in columns,
            "item": item,
        },
        headers={"Cache-Control": "no-store"},
    )


async def fetch_telegram_nft_media(source_url: str, item_id: int) -> dict:
    now = time.monotonic()
    cached = nft_media_cache.get(source_url)
    if cached and cached[0] > now:
        return cached[1]

    async with nft_media_fetch_semaphore:
        cached = nft_media_cache.get(source_url)
        if cached and cached[0] > time.monotonic():
            return cached[1]
        try:
            async with get_ton_session().get(
                source_url,
                allow_redirects=False,
                headers={"User-Agent": "DNXStore/1.0 TelegramMiniApp"},
            ) as response:
                content_type = response.headers.get("Content-Type", "").lower()
                if response.status != 200 or "text/html" not in content_type:
                    raise ValueError(f"unexpected Telegram response: {response.status}")
                html_bytes = await response.content.read(512 * 1024 + 1)
                if len(html_bytes) > 512 * 1024:
                    raise ValueError("Telegram NFT page is too large")

            parser = TelegramNftMediaParser()
            parser.feed(html_bytes.decode("utf-8", errors="replace"))
            colors = parser.gradient_colors[:2]
            result = {
                "animated": bool(parser.tgs_url),
                "tgsUrl": parser.tgs_url,
                "patternUrl": parser.pattern_url,
                "previewUrl": parser.preview_url,
                "colors": colors if len(colors) == 2 else ["#3E245D", "#160D27"],
                "patternColor": parser.pattern_color,
            }
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            logging.warning("Telegram NFT media unavailable for item %s: %s", item_id, exc)
            result = {"animated": False}

        if len(nft_media_cache) >= 512:
            expired = [key for key, (expires, _) in nft_media_cache.items() if expires <= time.monotonic()]
            for key in expired[:256]:
                nft_media_cache.pop(key, None)
            if len(nft_media_cache) >= 512:
                nft_media_cache.pop(next(iter(nft_media_cache)), None)
        # telesco.pe media URLs are signed. Keeping the parsed descriptor for
        # ten minutes caused an otherwise healthy client to retry an expired
        # URL after switching tabs. The TGS bytes remain browser/CDN cached;
        # only the short-lived descriptor is refreshed here.
        cache_ttl = 75 if result.get("animated") else 30
        nft_media_cache[source_url] = (time.monotonic() + cache_ttl, result)
        return result


async def warm_nft_media_cache(max_items: int = 12):
    rows = await get_pool().fetch("""
        SELECT id, nft_link FROM items
        WHERE COALESCE(nft_link, '') <> ''
        ORDER BY CASE WHEN status = 'Доступен' THEN 0 ELSE 1 END, id
        LIMIT $1
    """, max_items)
    jobs = []
    seen_sources = set()
    for row in rows:
        source_url = canonical_telegram_nft_url(row["nft_link"])
        if not source_url or source_url in seen_sources:
            continue
        item_nft_source_cache[int(row["id"])] = (
            time.monotonic() + ITEM_SOURCE_CACHE_TTL,
            source_url,
        )
        seen_sources.add(source_url)
        jobs.append(fetch_telegram_nft_media(source_url, int(row["id"])))
    if jobs:
        await asyncio.gather(*jobs, return_exceptions=True)


async def warm_nft_media_cache_safely():
    try:
        await warm_nft_media_cache()
    except asyncio.CancelledError:
        raise
    except Exception:
        logging.exception("Background NFT cache warm-up failed")


async def get_item_nft_source(item_id: int) -> str:
    now = time.monotonic()
    cached = item_nft_source_cache.get(item_id)
    if cached and cached[0] > now:
        item_nft_source_cache.move_to_end(item_id)
        return cached[1]
    if cached:
        item_nft_source_cache.pop(item_id, None)
    nft_link = await get_pool().fetchval("SELECT nft_link FROM items WHERE id = $1", item_id)
    source_url = canonical_telegram_nft_url(nft_link)
    item_nft_source_cache[item_id] = (now + ITEM_SOURCE_CACHE_TTL, source_url)
    item_nft_source_cache.move_to_end(item_id)
    while len(item_nft_source_cache) > ITEM_SOURCE_CACHE_MAX:
        item_nft_source_cache.popitem(last=False)
    return source_url


@require_auth
@rate_limit(90, 60)
async def handle_nft_media(request):
    try:
        item_id = int(request.query.get("item_id", "0"))
    except (TypeError, ValueError):
        item_id = 0
    if item_id <= 0:
        return web.json_response({"animated": False, "error": "invalid_item_id"}, status=400)

    source_url = await get_item_nft_source(item_id)
    if not source_url:
        return web.json_response({"animated": False})
    return web.json_response(await fetch_telegram_nft_media(source_url, item_id))

@require_auth
async def handle_get_inventory(request):
    user = request['telegram_user']
    user_id = user['id']
    try:
        async with get_pool().acquire() as conn:
            await conn.execute(
                """UPDATE items
                   SET status = 'Продан', last_event = 'withdraw_expired'
                   WHERE buyer_id = $1 AND status = 'pending_withdraw'
                     AND withdraw_expires_at IS NOT NULL AND withdraw_expires_at <= NOW()""",
                user_id,
            )
            rows = await conn.fetch(
                """SELECT id, name, price, image_url, nft_link, model, pattern, background, status, number,
                          acquisition_source, last_event, withdraw_requested_at, withdraw_expires_at,
                          disposed_at,
                          GREATEST(0, EXTRACT(EPOCH FROM (withdraw_expires_at - NOW())))::BIGINT
                              AS withdraw_remaining_seconds
                   FROM items
                   WHERE buyer_id = $1
                     AND status IN ('Продан','pending_withdraw')
                   ORDER BY id DESC""",
                user_id,
            )
        items = normalize_records(rows)
        for item in items:
            item['can_sell'] = (
                item.get('acquisition_source') == 'case'
                and item.get('status') == 'Продан'
            )
            item['sell_value'] = item.get('price', 0)
            item['withdraw_remaining_seconds'] = int(item.get('withdraw_remaining_seconds') or 0)
        return web.json_response(items, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.error(f"Get inventory error: {e}")
        return web.json_response({"error": "database_unavailable"}, status=503,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


@require_auth
async def handle_activity_history(request):
    user_id = request['telegram_user']['id']
    try:
        limit = max(1, min(int(request.query.get("limit", "80")), 100))
        before = int(request.query.get("before", "0"))
    except (TypeError, ValueError):
        return web.json_response({"error": "invalid_pagination"}, status=400)
    try:
        rows = await get_pool().fetch(
            """
            SELECT id, event_type, amount, balance_type, title, metadata, created_at
            FROM user_events
            WHERE user_id = $1 AND ($2::BIGINT = 0 OR id < $2)
            ORDER BY id DESC
            LIMIT $3
            """,
            user_id,
            before,
            limit + 1,
        )
        has_more = len(rows) > limit
        visible = rows[:limit]
        events = normalize_records(visible)
        return web.json_response({
            "events": events,
            "hasMore": has_more,
            "nextBefore": int(visible[-1]["id"]) if has_more and visible else None,
        }, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as exc:
        logging.error("Activity history error for %s: %s", user_id, exc)
        return web.json_response({"events": [], "hasMore": False}, status=500,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
@rate_limit(30, 60)
async def handle_buy(request):
    purchase_id = uuid.uuid4()
    try:
        data = await read_json_object(request)
        try:
            if data.get("requestId"):
                purchase_id = uuid.UUID(str(data["requestId"]))
        except (ValueError, TypeError, AttributeError):
            return web.json_response({"success": False, "error": "invalid_request_id"}, status=400)
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
                await ensure_user(conn, user_id, user['username'])
                locked_user = await conn.fetchrow(
                    "SELECT COALESCE(balance, 0) AS balance FROM users WHERE id = $1 FOR UPDATE",
                    user_id,
                )
                if not locked_user:
                    raise RuntimeError("purchase user was not created")
                # Блокировка пользователя сериализует два одновременных запроса
                # с одним requestId. После ожидания второй запрос увидит запись
                # первого и безопасно вернёт уже завершённый результат.
                completed_purchase = await conn.fetchrow(
                    "SELECT item_ids, total_price FROM purchase_records WHERE id = $1 AND user_id = $2",
                    purchase_id,
                    user_id,
                )
                if completed_purchase:
                    if sorted(int(value) for value in completed_purchase["item_ids"]) != sorted(item_ids):
                        return web.json_response(
                            {"success": False, "error": "request_id_conflict"},
                            status=409,
                            headers={"Access-Control-Allow-Origin": CORS_ORIGIN},
                        )
                    return web.json_response({
                        "success": True,
                        "balance": float(locked_user['balance'] or 0),
                        "purchaseId": str(purchase_id),
                        "replayed": True,
                    }, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                items = await conn.fetch(
                    "SELECT id, price FROM items WHERE id = ANY($1::int[]) AND status = 'Доступен' FOR UPDATE",
                    item_ids)
                if len(items) != len(item_ids):
                    return web.json_response({"success": False, "error": "items_unavailable"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                raw_prices = [Decimal(str(item['price'])) for item in items]
                if any(not price.is_finite() or price <= 0 for price in raw_prices):
                    logging.error("Catalog contains an invalid Stars price")
                    return web.json_response({"success": False, "error": "invalid_item_price"}, status=500)
                prices = [floor_stars(price) for price in raw_prices]
                if any(price <= 0 for price in prices):
                    logging.error("Catalog price became zero after flooring")
                    return web.json_response({"success": False, "error": "invalid_item_price"}, status=500)
                total_price = sum(prices, Decimal("0"))
                current_balance = Decimal(str(locked_user['balance']))
                if current_balance < total_price:
                    return web.json_response({"success": False, "error": "insufficient_funds"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                updated_items = await conn.fetch(
                    """UPDATE items
                       SET status = 'Продан', buyer_id = $1,
                           acquisition_source = 'catalog', last_event = 'catalog_purchase'
                       WHERE id = ANY($2::int[]) AND status = 'Доступен'
                       RETURNING id""",
                    user_id, item_ids)
                if len(updated_items) != len(item_ids):
                    raise RuntimeError("catalog item state changed during purchase")
                new_balance = await conn.fetchval(
                    "UPDATE users SET balance = balance - $1 WHERE id = $2 RETURNING balance",
                    total_price,
                    user_id,
                )
                await conn.execute(
                    """INSERT INTO purchase_records (id, user_id, item_ids, total_price)
                       VALUES ($1, $2, $3::int[], $4)""",
                    purchase_id,
                    user_id,
                    item_ids,
                    total_price,
                )
                await conn.executemany(
                    """INSERT INTO item_events (item_id, user_id, event_type, amount, metadata)
                       VALUES ($1, $2, 'catalog_purchase', $3, $4::jsonb)""",
                    [
                        (
                            int(item['id']),
                            user_id,
                            floor_stars(item['price']),
                            json.dumps({"purchase_id": str(purchase_id)}),
                        )
                        for item in items
                    ],
                )
                await record_user_event(
                    conn,
                    user_id,
                    "catalog_purchase",
                    amount=-total_price,
                    balance_type="main",
                    title="Покупка NFT",
                    metadata={"purchase_id": str(purchase_id), "item_ids": item_ids},
                )
        return web.json_response(
            {"success": True, "balance": int(new_balance), "purchaseId": str(purchase_id)},
            headers={"Access-Control-Allow-Origin": CORS_ORIGIN},
        )
    except Exception as e:
        logging.exception("Buy transaction %s failed: %s", purchase_id, e)
        return web.json_response(
            {"success": False, "error": "purchase_failed", "requestId": str(purchase_id)},
            status=500,
            headers={"Access-Control-Allow-Origin": CORS_ORIGIN},
        )

@require_auth
@rate_limit(10, 60)
async def handle_request_withdraw(request):
    try:
        data = await read_json_object(request)
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
                    """UPDATE items
                       SET status = 'pending_withdraw', last_event = 'withdraw_requested',
                           withdraw_requested_at = NOW(),
                           withdraw_expires_at = NOW() + make_interval(secs => $3::int)
                       WHERE id = $1 AND buyer_id = $2 AND status = 'Продан'
                         AND (withdraw_expires_at IS NULL OR withdraw_expires_at <= NOW())
                       RETURNING id, name, nft_link, withdraw_expires_at""",
                    item_id, user_id, WITHDRAW_WINDOW_SECONDS)
                if not item:
                    remaining = await conn.fetchval(
                        """SELECT GREATEST(0, EXTRACT(EPOCH FROM (withdraw_expires_at - NOW())))::BIGINT
                           FROM items WHERE id = $1 AND buyer_id = $2""",
                        item_id,
                        user_id,
                    )
                    return web.json_response({
                        "success": False,
                        "error": "withdraw_locked" if remaining else "not_found",
                        "remainingSeconds": int(remaining or 0),
                    },
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                await conn.execute(
                    """INSERT INTO item_events (item_id, user_id, event_type, metadata)
                       VALUES ($1, $2, 'withdraw_requested', $3::jsonb)""",
                    item_id,
                    user_id,
                    json.dumps({"expires_at": item['withdraw_expires_at'].isoformat()}),
                )
                await record_user_event(
                    conn,
                    user_id,
                    "withdraw_request",
                    title=f"Запрос на вывод · {item['name']}",
                    metadata={"item_id": item_id, "expires_at": item['withdraw_expires_at'].isoformat()},
                )
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
        except Exception as notify_error:
            logging.exception("Withdraw notification failed for item %s: %s", item_id, notify_error)
            restored = await get_pool().fetchval(
                """UPDATE items
                   SET status = 'Продан', last_event = 'withdraw_notification_failed',
                       withdraw_requested_at = NULL, withdraw_expires_at = NULL
                   WHERE id = $1 AND buyer_id = $2 AND status = 'pending_withdraw'
                   RETURNING id""",
                item_id, user_id,
            )
            if restored:
                return web.json_response(
                    {"success": False, "error": "withdraw_notification_failed"},
                    status=503,
                    headers={"Access-Control-Allow-Origin": CORS_ORIGIN},
                )
        return web.json_response({
            "success": True,
            "remainingSeconds": WITHDRAW_WINDOW_SECONDS,
            "expiresAt": item['withdraw_expires_at'].isoformat(),
        }, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.exception("Withdraw request failed: %s", e)
        return web.json_response({"success": False, "error": "withdraw_failed"}, status=500, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
async def handle_game_state(request):
    compact = request.query.get("compact") == "1"
    async with game_lock:
        sorted_players = [game_state["players"][uid] for uid in sorted(game_state["players"].keys())]
        polys = game_state.get("polygons") or game_state.get("last_polygons")
        resp = {
            "status": game_state["status"],
            "players": sorted_players,
            "pool": game_state["pool"],
            "timer": game_state["timer"],
            # The trajectory is the largest object in the game payload.  A client
            # only needs it once, when it starts the local animation.
            "spin_params": (
                game_state.get("spin_params")
                if not compact and game_state["status"] == "spinning"
                else None
            ),
            "winner": game_state.get("winner"),
            "last_winner_id": game_state.get("last_winner_id"),
            "round_id": game_state.get("round_id"),
            "game_number": game_state.get("game_number", 0),
            "polygons": None if compact else polys
        }
    return web.json_response(resp, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
@rate_limit(60, 60)
async def handle_game_bet(request):
    global game_state
    try:
        data = await read_json_object(request)
        user = request['telegram_user']
        user_id = user['id']
        username = user['username']
        balance_type = str(data.get("balanceType") or "main")
        if balance_type not in {"main", "bonus"}:
            return web.json_response({"success": False, "error": "invalid_balance_type"}, status=400)
        parsed_amount = parse_positive_amount(data.get('amount'), minimum=STAR_MIN_BET)
        if parsed_amount is None:
            return web.json_response({"success": False, "error": "min_bet"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        parsed_amount = floor_stars(parsed_amount)
        if parsed_amount < STAR_MIN_BET or parsed_amount % STAR_BET_STEP != 0:
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
            if (
                user_id in game_state["players"]
                and game_state["players"][user_id].get("balance_type", "main") != balance_type
            ):
                return web.json_response({"success": False, "error": "balance_type_locked"}, status=409)

            # A Render restart loses the in-memory round but keeps its durable
            # wager ledger. Never reuse that round number: its primary key
            # would otherwise reject every later wager. Startup recovery does
            # this normally; this guard also self-heals older deployments.
            if not game_state["players"]:
                current_number = int(game_state.get("game_number", 0))
                async with get_pool().acquire() as round_conn:
                    collision = await round_conn.fetchval(
                        "SELECT 1 FROM active_game_bets WHERE game_number = $1 LIMIT 1",
                        current_number,
                    )
                    if collision:
                        game_state["game_number"] = int(await round_conn.fetchval(
                            """
                            UPDATE game_counter
                            SET last_game_number = GREATEST(last_game_number, $1)
                            WHERE id = 1
                            RETURNING last_game_number
                            """,
                            current_number + 1,
                        ))
                        game_state["last_polygons"] = None
                        game_state["polygons"] = None
            balance_column = "bonus_balance" if balance_type == "bonus" else "balance"
            existing_player = game_state["players"].get(user_id)

            # Prepare every fallible in-memory calculation before charging the
            # user.  Player palettes are JSON arrays on the wire, therefore
            # convert them back to tuples before using them as set members.
            candidate_players = {
                player_id: dict(player)
                for player_id, player in game_state["players"].items()
            }
            if existing_player:
                candidate_players[user_id]["amount"] += amount
                candidate_players[user_id]["bets_count"] += 1
            else:
                occupied_colors = {
                    tuple(player.get("color") or ())
                    for player in candidate_players.values()
                    if player.get("color")
                }
                color = choose_player_palette(occupied_colors)
                candidate_players[user_id] = {
                    "id": user_id,
                    "username": username,
                    "amount": amount,
                    "color": color,
                    "balance_type": balance_type,
                    "bets_count": 1,
                    "history_event_id": None,
                    "photo_url": user.get('photo_url', ''),
                }
            candidate_polygons = build_weighted_voronoi(
                list(candidate_players.values()),
                (0.0, 0.0, 1.0, 1.0),
            )

            async with get_pool().acquire() as conn:
                async with conn.transaction():
                    new_balance = await conn.fetchval(
                        f"UPDATE users SET {balance_column} = {balance_column} - $1 WHERE id = $2 AND {balance_column} >= $1 RETURNING {balance_column}",
                        parsed_amount, user_id)
                    if new_balance is None:
                        return web.json_response({"success": False, "error": "insufficient_funds"},
                                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                    history_event_id = existing_player.get("history_event_id") if existing_player else None
                    if history_event_id:
                        updated_event = await conn.fetchval(
                            """
                            UPDATE user_events
                            SET amount = amount - $1,
                                metadata = jsonb_set(metadata, '{total_bet}', to_jsonb((ABS(amount) + $1)::numeric), true)
                            WHERE id = $2 AND user_id = $3 AND event_type = 'game_bet'
                            RETURNING id
                            """,
                            parsed_amount,
                            history_event_id,
                            user_id,
                        )
                        if not updated_event:
                            history_event_id = None
                    if not history_event_id:
                        history_event_id = await conn.fetchval(
                            """
                            INSERT INTO user_events (user_id, event_type, amount, balance_type, title, metadata)
                            VALUES ($1, 'game_bet', $2, $3, 'Ставка в игре', $4::jsonb)
                            RETURNING id
                            """,
                            user_id,
                            -parsed_amount,
                            balance_type,
                            json.dumps({
                                "game_number": game_state.get("game_number", 0),
                                "total_bet": int(parsed_amount),
                            }),
                        )
                    ledger_amount = await conn.fetchval(
                        """
                        INSERT INTO active_game_bets
                            (game_number, user_id, username, amount, balance_type, history_event_id, owner_token)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (game_number, user_id) DO UPDATE
                        SET amount = active_game_bets.amount + EXCLUDED.amount,
                            username = EXCLUDED.username,
                            history_event_id = EXCLUDED.history_event_id,
                            owner_token = EXCLUDED.owner_token,
                            updated_at = NOW()
                        WHERE active_game_bets.status = 'pending'
                          AND active_game_bets.balance_type = EXCLUDED.balance_type
                          AND active_game_bets.owner_token IS NOT DISTINCT FROM EXCLUDED.owner_token
                        RETURNING amount
                        """,
                        int(game_state.get("game_number", 0)),
                        user_id,
                        username,
                        parsed_amount,
                        balance_type,
                        history_event_id,
                        PROCESS_INSTANCE_ID,
                    )
                    if ledger_amount is None:
                        raise RuntimeError("Game wager ledger conflict")
            candidate_players[user_id]["history_event_id"] = history_event_id
            game_state["players"] = candidate_players
            game_state["pool"] += amount

            if len(game_state["players"]) == 1:
                game_state["last_polygons"] = None

            game_state["polygons"] = candidate_polygons

            if len(game_state["players"]) >= 2 and game_state["status"] == "waiting":
                game_state["status"] = "counting"
                game_state["timer"] = 15

        return web.json_response({"success": True}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.exception("Bet error: %s", e)
        return web.json_response(
            {"success": False, "error": "bet_failed"},
            status=500,
            headers={"Access-Control-Allow-Origin": CORS_ORIGIN},
        )

@require_auth
@rate_limit(10, 60)
async def handle_game_cancel(request):
    global game_state
    try:
        user = request['telegram_user']
        user_id = user['id']
        async with game_lock:
            if len(game_state["players"]) == 1 and user_id in game_state["players"]:
                async with get_pool().acquire() as conn:
                    async with conn.transaction():
                        ledger = await conn.fetchrow(
                            """
                            UPDATE active_game_bets
                            SET status = 'refunded', updated_at = NOW()
                            WHERE game_number = $1 AND user_id = $2 AND status = 'pending'
                              AND owner_token = $3
                            RETURNING amount, balance_type
                            """,
                            int(game_state.get("game_number", 0)),
                            user_id,
                            PROCESS_INSTANCE_ID,
                        )
                        if not ledger:
                            return web.json_response(
                                {"success": False, "error": "wager_not_pending"},
                                status=409,
                                headers={"Access-Control-Allow-Origin": CORS_ORIGIN},
                            )
                        refund = ledger["amount"]
                        balance_type = ledger["balance_type"]
                        balance_column = "bonus_balance" if balance_type == "bonus" else "balance"
                        updated = await conn.execute(
                            f"UPDATE users SET {balance_column} = {balance_column} + $1 WHERE id = $2",
                            refund,
                            user_id,
                        )
                        if updated != "UPDATE 1":
                            raise RuntimeError("Refund user disappeared")
                        await record_user_event(
                            conn,
                            user_id,
                            "game_refund",
                            amount=Decimal(str(refund)),
                            balance_type=balance_type,
                            title="Возврат ставки",
                            metadata={"game_number": game_state.get("game_number", 0)},
                        )
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
            "SELECT game_number, winner_name, win_amount, win_percent, created_at FROM game_history ORDER BY game_number DESC LIMIT 100")
        result = []
        for row in rows:
            result.append({
                "game_number": row["game_number"],
                "winner_name": row["winner_name"],
                "win_amount": float(row["win_amount"]),
                "win_percent": float(row.get("win_percent", 0)),
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
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
            top_rows = await conn.fetch("SELECT user_id, username, wins FROM leaderboard ORDER BY wins DESC, username LIMIT 3")
            user_row = await conn.fetchrow("""
                SELECT username, wins, rank FROM (
                    SELECT user_id, username, wins,
                           ROW_NUMBER() OVER (ORDER BY wins DESC, username) AS rank
                    FROM leaderboard
                ) ranked WHERE user_id = $1
            """, user_id)
        result = {"top": [], "user": None}
        for r in top_rows:
            result["top"].append({
                "username": r["username"],
                "wins": r["wins"],
                "isYou": r["user_id"] == user_id,
            })
        if user_row:
            result["user"] = {
                "username": user_row["username"],
                "wins": user_row["wins"],
                "rank": int(user_row["rank"]),
            }
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
            "image_url": case_data["image_url"],
            "bonus_enabled": case_allows_bonus(case_data),
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
        "bonus_enabled": case_allows_bonus(case),
        "drops": [
            {
                **{key: value for key, value in drop.items() if key not in {"real_chance", "nft_link", "traits"}},
                "drop_index": index,
                "has_live_nft": bool(canonical_telegram_nft_url(drop.get("nft_link"))),
                "model": case_drop_attribute(drop, "model"),
                "pattern": case_drop_attribute(drop, "pattern"),
                "background": case_drop_attribute(drop, "background"),
            }
            for index, drop in enumerate(case["drops"])
        ],
    }
    return web.json_response(result, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})


@require_auth
@rate_limit(90, 60)
async def handle_case_nft_media(request):
    """Serve case animation metadata while keeping its Telegram NFT link private."""
    try:
        case_id = int(request.query.get("case_id", "0"))
        drop_index = int(request.query.get("drop_index", "-1"))
    except (TypeError, ValueError):
        return web.json_response({"animated": False, "error": "invalid_drop"}, status=400)
    case = CASES_CACHE.get(case_id)
    drops = case.get("drops") if isinstance(case, dict) else None
    if not isinstance(drops, list) or not 0 <= drop_index < len(drops):
        return web.json_response({"animated": False, "error": "drop_not_found"}, status=404)
    source_url = canonical_telegram_nft_url(drops[drop_index].get("nft_link"))
    if not source_url:
        return web.json_response({"animated": False})
    media = dict(await fetch_telegram_nft_media(source_url, -(case_id * 1000 + drop_index + 1)))
    media.update({
        "patternUrl": None,
        "patternColor": "#242424",
        "colors": ["#242424", "#242424"],
        "caseMode": True,
    })
    return web.json_response(media)

@require_auth
@rate_limit(30, 60)
async def handle_open_case(request):
    """Открытие кейса (цена и дропы из кеша, запись в БД)"""
    try:
        data = await read_json_object(request)
        user_id = request['telegram_user']['id']
        raw_request_id = data.get("requestId")
        if raw_request_id in (None, ""):
            # Backwards compatibility for an older client. Current clients always
            # send an idempotency key so a lost response can be retried safely.
            request_id = uuid.uuid4()
        else:
            try:
                request_id = uuid.UUID(str(raw_request_id))
            except (ValueError, TypeError, AttributeError):
                return web.json_response(
                    {"success": False, "error": "invalid_request_id"}, status=400
                )
        balance_type = str(data.get("balanceType") or "main")
        if balance_type not in {"main", "bonus"}:
            return web.json_response({"success": False, "error": "invalid_balance_type"}, status=400)
        try:
            case_id = int(data.get('caseId'))
        except (TypeError, ValueError):
            return web.json_response({"success": False, "error": "invalid_case_id"}, status=400)
        case = CASES_CACHE.get(case_id)
        if not case:
            return web.json_response({"success": False, "error": "case_not_found"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        if balance_type == "bonus" and not case_allows_bonus(case):
            return web.json_response({"success": False, "error": "bonus_not_allowed"}, status=409)

        drops = case.get('drops')
        if not isinstance(drops, list) or not drops:
            return web.json_response({"success": False, "error": "empty_case"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        case_price = parse_positive_amount(case.get('price'))
        if case_price is None:
            logging.error("Invalid price in CASES_JSON for case %s", case_id)
            return web.json_response({"success": False, "error": "invalid_case_config"}, status=500)
        case_price = floor_stars(case_price)
        if case_price <= 0:
            logging.error("Case price became zero after flooring for case %s", case_id)
            return web.json_response({"success": False, "error": "invalid_case_config"}, status=500)
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                await ensure_user(conn, user_id, request['telegram_user']['username'])
                await conn.fetchval("SELECT id FROM users WHERE id = $1 FOR UPDATE", user_id)
                replay = await conn.fetchrow(
                    """SELECT case_id, balance_type, winner_payload FROM case_open_records
                       WHERE id = $1 AND user_id = $2""",
                    request_id, user_id,
                )
                if replay:
                    if int(replay["case_id"]) != case_id or replay["balance_type"] != balance_type:
                        return web.json_response(
                            {"success": False, "error": "idempotency_conflict"}, status=409
                        )
                    payload = replay["winner_payload"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    return web.json_response(
                        {"success": True, "won_item": payload,
                         "requestId": str(request_id), "replayed": True},
                        headers={"Access-Control-Allow-Origin": CORS_ORIGIN},
                    )
                balance_column = "bonus_balance" if balance_type == "bonus" else "balance"
                new_balance = await conn.fetchval("""
                    UPDATE users
                    SET {column} = {column} - $1
                    WHERE id = $2 AND {column} >= $1
                    RETURNING {column}
                """.format(column=balance_column), case_price, user_id)
                if new_balance is None:
                    return web.json_response({"success": False, "error": "insufficient_funds"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                await record_user_event(
                    conn,
                    user_id,
                    "case_open",
                    amount=-case_price,
                    balance_type=balance_type,
                    title=f"Открытие кейса · {str(case.get('name') or 'Кейс')[:180]}",
                    metadata={"case_id": case_id},
                )

                real_chances = [float(drop['real_chance']) for drop in drops]
                if any(not math.isfinite(chance) or chance < 0 for chance in real_chances):
                    raise ValueError("invalid real_chance in CASES_JSON")
                total_chance = sum(real_chances)
                if total_chance <= 0:
                    raise ValueError("total real_chance must be positive")
                # Preserve the configured probabilities, but use an OS-backed
                # random source for a monetary outcome.
                rand_val = (secrets.randbelow(10**12) / 10**12) * total_chance
                current_sum = 0
                won_drop = drops[-1]
                won_drop_index = len(drops) - 1
                for drop_index, (drop, real_chance) in enumerate(zip(drops, real_chances)):
                    current_sum += real_chance
                    if rand_val <= current_sum:
                        won_drop = drop
                        won_drop_index = drop_index
                        break

                drop_value = Decimal(str(won_drop['value']))
                if not drop_value.is_finite() or drop_value < 0 or drop_value > Decimal("1000000"):
                    raise ValueError("invalid drop value in CASES_JSON")
                drop_value = floor_stars(drop_value)

                new_item_id = await conn.fetchval("""
                    INSERT INTO items (
                        name, price, status, image_url, model, pattern, background, buyer_id, nft_link, number,
                        acquisition_source, last_event
                    )
                    VALUES ($1, $2, 'Продан', $3, $4, $5, $6, $7, $8, $9, 'case', 'case_drop') RETURNING id
                """, str(won_drop['name'])[:255], drop_value,
                      safe_https_url(won_drop.get('image_url')), case_drop_attribute(won_drop, "model"),
                      case_drop_attribute(won_drop, "pattern"), case_drop_attribute(won_drop, "background"), user_id,
                      canonical_telegram_nft_url(won_drop.get('nft_link')),
                      str(won_drop.get('number') or '')[:20])

                await conn.execute(
                    """INSERT INTO item_events (item_id, user_id, event_type, amount, metadata)
                       VALUES ($1, $2, 'case_drop', $3, $4::jsonb)""",
                    new_item_id,
                    user_id,
                    drop_value,
                    json.dumps({"case_id": case_id, "balance_type": balance_type}),
                )
                await record_user_event(
                    conn,
                    user_id,
                    "case_prize",
                    title=f"Приз из кейса · {str(won_drop['name'])[:180]}",
                    metadata={"case_id": case_id, "item_id": new_item_id, "item_value": float(drop_value)},
                )

                won_item_dict = dict(won_drop)
                won_item_dict.pop('real_chance', None)
                won_item_dict.pop('nft_link', None)
                won_item_dict.pop('traits', None)
                won_item_dict['generated_item_id'] = new_item_id
                won_item_dict['drop_index'] = won_drop_index
                won_item_dict['has_live_nft'] = bool(canonical_telegram_nft_url(won_drop.get('nft_link')))

                await conn.execute(
                    """INSERT INTO case_open_records
                       (id, user_id, case_id, balance_type, price, generated_item_id, winner_payload)
                       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)""",
                    request_id, user_id, case_id, balance_type, case_price,
                    new_item_id, json.dumps(won_item_dict),
                )

        return web.json_response({"success": True, "won_item": won_item_dict,
                                  "requestId": str(request_id)},
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

    except Exception as e:
        logging.exception("Case open failed: %s", e)
        return web.json_response({"success": False, "error": "case_open_failed"}, status=500,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

@require_auth
@rate_limit(30, 60)
async def handle_sell_drop(request):
    """Продажа кейсового предмета с сохранением строки и аудита."""
    try:
        data = await read_json_object(request)
        user_id = request['telegram_user']['id']
        try:
            item_id = int(data.get('itemId'))
        except (TypeError, ValueError):
            return web.json_response({"success": False, "error": "missing_item_id"},
                                     headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                item = await conn.fetchrow(
                    """UPDATE items
                       SET status = 'disposed', last_event = 'case_drop_sold', disposed_at = NOW()
                       WHERE id = $1 AND buyer_id = $2 AND status = 'Продан'
                         AND acquisition_source = 'case'
                         AND (withdraw_expires_at IS NULL OR withdraw_expires_at <= NOW())
                       RETURNING id, price, name""",
                    item_id, user_id)
                if not item:
                    return web.json_response({"success": False, "error": "item_not_found"},
                                             headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
                sale_price = floor_stars(item['price'])
                credited = await conn.fetchval(
                    """UPDATE users SET balance = balance + $1
                       WHERE id = $2 RETURNING balance""",
                    sale_price,
                    user_id,
                )
                if credited is None:
                    raise RuntimeError("case drop owner disappeared during sale")
                await conn.execute(
                    """INSERT INTO item_events (item_id, user_id, event_type, amount)
                       VALUES ($1, $2, 'case_drop_sold', $3)""",
                    item_id,
                    user_id,
                    sale_price,
                )
                await record_user_event(
                    conn,
                    user_id,
                    "case_sale",
                    amount=sale_price,
                    balance_type="main",
                    title=f"Продажа приза · {item['name']}",
                    metadata={"item_id": item_id},
                )
        return web.json_response({"success": True, "credited": int(sale_price)}, headers={"Access-Control-Allow-Origin": CORS_ORIGIN})
    except Exception as e:
        logging.exception("Sell drop failed: %s", e)
        return web.json_response({"success": False, "error": "server_error"}, status=500,
                                 headers={"Access-Control-Allow-Origin": CORS_ORIGIN})

# ---------- Админ-коллбэки ----------
async def is_admin_callback(callback: types.CallbackQuery) -> bool:
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа", show_alert=True)
        logging.warning("Unauthorized admin callback from Telegram user %s", callback.from_user.id)
        return False
    return True


async def parse_withdraw_callback(callback: types.CallbackQuery):
    parts = str(callback.data or "").split("_")
    if len(parts) != 4 or parts[0] != "with" or parts[1] not in {"yes", "no"}:
        await callback.answer("Некорректная команда", show_alert=True)
        return None
    uid, item_id_text = parts[2], parts[3]
    if not uid.isdigit() or not item_id_text.isdigit():
        await callback.answer("Некорректные данные", show_alert=True)
        return None
    return uid, int(item_id_text)


@dp.callback_query(F.data.startswith("with_yes_"))
async def admin_withdraw_approve(callback: types.CallbackQuery):
    if not await is_admin_callback(callback):
        return
    parsed = await parse_withdraw_callback(callback)
    if not parsed:
        return
    uid, item_id = parsed
    async with get_pool().acquire() as conn:
        async with conn.transaction():
            item = await conn.fetchrow(
                """UPDATE items SET status = 'withdrawn', last_event = 'withdraw_approved'
                   WHERE id = $1 AND buyer_id = $2 AND status = 'pending_withdraw'
                     AND withdraw_expires_at > NOW()
                   RETURNING name""",
                item_id, uid)
            result = "UPDATE 1" if item else "UPDATE 0"
            if item:
                await conn.execute(
                    """INSERT INTO item_events (item_id, user_id, event_type)
                       VALUES ($1, $2, 'withdraw_approved')""",
                    item_id, uid,
                )
                await record_user_event(
                    conn, uid, "withdraw_approved",
                    title=f"NFT выведен · {item['name']}",
                    metadata={"item_id": item_id},
                )
    if result != "UPDATE 1":
        await get_pool().execute(
            """UPDATE items SET status = 'Продан', last_event = 'withdraw_expired'
               WHERE id = $1 AND buyer_id = $2 AND status = 'pending_withdraw'
                 AND withdraw_expires_at <= NOW()""",
            item_id,
            uid,
        )
        await callback.answer("Запрос уже обработан или двухчасовое окно истекло", show_alert=True)
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
    parsed = await parse_withdraw_callback(callback)
    if not parsed:
        return
    uid, item_id = parsed
    async with get_pool().acquire() as conn:
        async with conn.transaction():
            item = await conn.fetchrow(
                """UPDATE items SET status = 'Продан', last_event = 'withdraw_rejected'
                   WHERE id = $1 AND buyer_id = $2 AND status = 'pending_withdraw'
                   RETURNING name""",
                item_id, uid)
            result = "UPDATE 1" if item else "UPDATE 0"
            if item:
                await conn.execute(
                    """INSERT INTO item_events (item_id, user_id, event_type)
                       VALUES ($1, $2, 'withdraw_rejected')""",
                    item_id, uid,
                )
                await record_user_event(
                    conn, uid, "withdraw_rejected",
                    title=f"Вывод отклонён · {item['name']}",
                    metadata={"item_id": item_id},
                )
    if result != "UPDATE 1":
        await callback.answer("Запрос уже обработан", show_alert=True)
        return
    await callback.message.edit_text(f"{callback.message.text}\n\n❌ **ВЫВОД ОТКЛОНЕН**")
    try:
        await bot.send_message(int(uid), f"❌ Вывод NFT (ID: {item_id}) отклонён.")
    except Exception:
        pass


@dp.pre_checkout_query()
async def process_star_pre_checkout(query: types.PreCheckoutQuery):
    try:
        row = await get_pool().fetchrow("""
            SELECT user_id, stars, status, expires_at
            FROM star_payments WHERE invoice_payload = $1
        """, query.invoice_payload)
        valid = bool(
            row
            and row["status"] == "pending"
            and row["expires_at"].timestamp() >= time.time()
            and row["user_id"] == str(query.from_user.id)
            and query.currency == "XTR"
            and query.total_amount == row["stars"]
        )
        if valid:
            await query.answer(ok=True)
        else:
            await query.answer(ok=False, error_message="Счёт устарел или его данные не совпадают. Создайте новый счёт в приложении.")
    except Exception as exc:
        logging.error("Stars pre-checkout error: %s", exc)
        await query.answer(ok=False, error_message="Платёж временно недоступен. Попробуйте ещё раз.")


@dp.message(F.successful_payment)
async def process_successful_star_payment(message: types.Message):
    payment = message.successful_payment
    if not payment or payment.currency != "XTR":
        return
    try:
        credited = False
        async with get_pool().acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("""
                    SELECT id, user_id, stars, status, telegram_payment_charge_id
                    FROM star_payments WHERE invoice_payload = $1 FOR UPDATE
                """, payment.invoice_payload)
                if not row:
                    raise ValueError("unknown Stars invoice payload")
                if row["status"] == "paid":
                    if row["telegram_payment_charge_id"] is None:
                        await conn.execute(
                            "UPDATE star_payments SET telegram_payment_charge_id = $1 WHERE id = $2",
                            payment.telegram_payment_charge_id, row["id"])
                    elif row["telegram_payment_charge_id"] != payment.telegram_payment_charge_id:
                        raise ValueError("invoice already paid with another charge")
                    return
                if (
                    row["status"] not in {"pending", "expired"}
                    or row["user_id"] != str(message.from_user.id)
                    or row["stars"] != payment.total_amount
                ):
                    raise ValueError("Stars payment does not match invoice")
                updated = await conn.fetchrow("""
                    UPDATE star_payments
                    SET status = 'paid', telegram_payment_charge_id = $1, paid_at = NOW()
                    WHERE id = $2 AND status IN ('pending', 'expired')
                    RETURNING user_id, stars
                """, payment.telegram_payment_charge_id, row["id"])
                if not updated:
                    return
                await credit_main_deposit(
                    conn,
                    updated["user_id"],
                    updated["stars"],
                    "stars",
                    str(row["id"]),
                )
                credited = True
        if credited:
            await message.answer(
                "✨ ПОПОЛНЕНИЕ УСПЕШНО\n\n"
                f"⭐ Ваш баланс успешно пополнен на {payment.total_amount} Stars.\n"
                "💫 Способ оплаты: Telegram Stars\n\n"
                "Спасибо, что выбираете DNX Store!"
            )
    except Exception as exc:
        logging.error("Successful Stars payment processing error: %s", exc)


@dp.message(Command("paysupport"))
async def cmd_paysupport(message: types.Message):
    await message.answer(
        "Поддержка платежей DNX Store. Пришлите описание проблемы, сумму, время и идентификатор платежа из чека Telegram. Никому не отправляйте seed-фразу или приватный ключ."
    )


@dp.message(Command("stars_balance"))
async def cmd_stars_balance(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        logging.warning("Unauthorized /stars_balance request from Telegram user %s", message.from_user.id)
        return
    try:
        balance = await bot.get_my_star_balance()
        history = await bot.get_star_transactions(offset=0, limit=10)
        counts = await get_pool().fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
                COUNT(*) FILTER (WHERE status = 'paid') AS paid_count
            FROM star_payments
        """)
        lines = [
            "⭐ БАЛАНС TELEGRAM-БОТА",
            "",
            f"Доступно боту: {balance.amount} Stars",
            f"Ожидают обработки в DNX: {counts['pending_count']}",
            f"Успешно записано в DNX: {counts['paid_count']}",
            "",
            "Последние операции Telegram:"
        ]
        for transaction in history.transactions[:10]:
            source_user = getattr(transaction.source, "user", None)
            source_label = f"user {source_user.id}" if source_user else type(transaction.source).__name__.replace("TransactionPartner", "")
            invoice_payload = getattr(transaction.source, "invoice_payload", None)
            payload_label = f" · {invoice_payload}" if invoice_payload else ""
            lines.append(
                f"{transaction.date:%d.%m %H:%M} · {transaction.amount:+d} ⭐ · {source_label}{payload_label}"
            )
        if not history.transactions:
            lines.append("Операций пока нет")
        lines.extend([
            "",
            "Это доходный баланс бота, а не ваш личный пользовательский баланс Stars."
        ])
        await message.answer("\n".join(lines))
    except Exception as exc:
        logging.error("Admin Stars balance command error: %s", exc)
        await message.answer("Не удалось получить баланс Stars. Проверьте логи Render и актуальность BOT_TOKEN.")


@dp.message(Command("stars_reconcile"))
async def cmd_stars_reconcile(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        logging.warning("Unauthorized /stars_reconcile request from Telegram user %s", message.from_user.id)
        return
    try:
        recovered = await reconcile_recent_star_transactions()
        await message.answer(
            "✅ Проверка завершена.\n"
            f"Восстановлено пропущенных пополнений: {recovered}.\n\n"
            "Команда сверяет последние операции с базой и не может начислить один платёж дважды."
        )
    except Exception as exc:
        logging.error("Admin Stars reconciliation command error: %s", exc)
        await message.answer("Не удалось выполнить сверку. Проверьте логи Render.")


@dp.message(Command("db_audit"))
async def cmd_db_audit(message: types.Message):
    """Show the admin which database and item row the deployed bot reads."""
    if message.from_user.id != ADMIN_ID:
        logging.warning("Unauthorized /db_audit request from Telegram user %s", message.from_user.id)
        return
    item_id = 0
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) == 2 and parts[1].strip().isdigit():
            item_id = int(parts[1].strip())
    async with get_pool().acquire() as conn:
        identity = await database_identity(conn)
        columns = [
            row["column_name"]
            for row in await conn.fetch(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema = current_schema() AND table_name = 'items'
                   ORDER BY ordinal_position"""
            )
        ]
        row = None
        if item_id > 0:
            row = await conn.fetchrow(
                """SELECT id, name, model, pattern, background, status, buyer_id
                   FROM items WHERE id = $1""",
                item_id,
            )
    lines = [
        f"DNX DB AUDIT · V{API_RELEASE}",
        f"База: {identity['database']}",
        f"Схема: {identity['schema']}",
        f"Fingerprint: {identity['fingerprint']}",
        f"traits column: {'ЕСТЬ (не читается)' if 'traits' in columns else 'НЕТ'}",
        "Источник: items.model / items.pattern / items.background",
    ]
    if item_id > 0:
        if row:
            lines.extend([
                "",
                f"ID: {row['id']} · {row['name']}",
                f"model = {row['model']!r}",
                f"pattern = {row['pattern']!r}",
                f"background = {row['background']!r}",
                f"status = {row['status']!r}",
            ])
        else:
            lines.extend(["", f"Строка items.id={item_id} не найдена"])
    else:
        lines.extend(["", "Для проверки строки: /db_audit ID"])
    await message.answer("\n".join(lines))

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    start_arg = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        start_arg = parts[1].strip() if len(parts) == 2 else ""
    referral_code = start_arg[4:] if re.fullmatch(r"ref_[A-Za-z0-9]{25}", start_arg) else None
    if message.from_user:
        username = message.from_user.username or message.from_user.first_name or "Unknown"
        try:
            async with get_pool().acquire() as conn:
                async with conn.transaction():
                    await ensure_user(conn, str(message.from_user.id), username, referral_code)
        except Exception as exc:
            logging.error("Could not create user from /start: %s", exc)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✨ Магазин", web_app=WebAppInfo(url=WEBAPP_URL))]])
    await message.answer("Добро пожаловать в DNX Store!", reply_markup=kb)

# ---------- Настройка сервера ----------
app = web.Application(middlewares=[security_headers_middleware], client_max_size=64 * 1024)
app.router.add_get('/health', handle_health)
app.router.add_get('/user', handle_get_user)
app.router.add_get('/items', handle_get_items)
app.router.add_get('/item', handle_get_item)
app.router.add_get('/admin/db-audit', handle_admin_db_audit)
app.router.add_get('/nft/media', handle_nft_media)
app.router.add_get('/inventory', handle_get_inventory)
app.router.add_get('/activity/history', handle_activity_history)
app.router.add_post('/ton/deposit/create', handle_create_ton_deposit)
app.router.add_get('/ton/deposit/status', handle_ton_deposit_status)
app.router.add_post('/stars/invoice/create', handle_create_star_invoice)
app.router.add_get('/stars/payment/status', handle_star_payment_status)
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
app.router.add_get('/case/nft-media', handle_case_nft_media)
app.router.add_post('/open-case', handle_open_case)
app.router.add_post('/sell-drop', handle_sell_drop)
app.router.add_options('/{tail:.*}', handle_options)

async def main():
    global db_pool, ton_http_session, BOT_USERNAME
    runner = None
    game_task = None
    ton_task = None
    star_task = None
    moderation_task = None
    warmup_task = None
    try:
        # Open the HTTP port before touching Neon or Telegram.  Render kills a
        # service when it cannot discover a port during a cold dependency start.
        port = int(os.environ.get("PORT", 8080))
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", port).start()
        runtime_state["phase"] = "connecting_database"
        logging.info("HTTP server listening on 0.0.0.0:%s (release %s)", port, API_RELEASE)

        retry_delay = 2
        while db_pool is None:
            runtime_state["attempt"] += 1
            runtime_state["database"] = "connecting"
            try:
                candidate_pool = await asyncio.wait_for(create_db_pool(), timeout=15)
                db_pool = candidate_pool
                runtime_state["database"] = "migrating"
                runtime_state["phase"] = "initializing_database"
                await init_db()
                await recover_unfinished_game_bets()
                await log_database_audit(db_pool)
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception(
                    "Backend initialization attempt %s failed; retrying in %ss",
                    runtime_state["attempt"],
                    retry_delay,
                )
                runtime_state["phase"] = "waiting_for_database"
                runtime_state["database"] = "unavailable"
                if db_pool is not None:
                    await db_pool.close()
                    db_pool = None
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)

        ton_http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10, connect=4, sock_read=8),
            connector=aiohttp.TCPConnector(limit=16, limit_per_host=8, ttl_dns_cache=300),
        )
        if not BOT_USERNAME:
            try:
                me = await asyncio.wait_for(bot.get_me(), timeout=5)
                BOT_USERNAME = me.username or ""
            except Exception as exc:
                logging.warning("Could not prefetch bot username: %s", exc)
        game_task = asyncio.create_task(game_worker())
        ton_task = asyncio.create_task(ton_payment_worker())
        star_task = asyncio.create_task(star_reconciliation_worker())
        moderation_task = asyncio.create_task(moderation_expiry_worker())
        warmup_task = asyncio.create_task(warm_nft_media_cache_safely())
        runtime_state["ready"] = True
        runtime_state["phase"] = "ready"
        runtime_state["database"] = "ok"
        logging.info("Backend initialization complete")

        polling_retry_delay = 2
        while True:
            try:
                await dp.start_polling(bot)
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception(
                    "Telegram polling stopped unexpectedly; retrying in %ss",
                    polling_retry_delay,
                )
                runtime_state["phase"] = "telegram_reconnecting"
                await asyncio.sleep(polling_retry_delay)
                polling_retry_delay = min(polling_retry_delay * 2, 30)
                runtime_state["phase"] = "ready"
    finally:
        runtime_state["ready"] = False
        runtime_state["phase"] = "stopping"
        tasks = [
            task for task in
            (game_task, ton_task, star_task, moderation_task, warmup_task)
            if task is not None
        ]
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
