import io
import os
import html
import threading
import time
import json
import logging
import re
from datetime import datetime, timedelta
from urllib.parse import urlparse

import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import Json as PgJson
import telebot
from telebot import types

# ========================= НАСТРОЙКИ ЛОГИРОВАНИЯ =========================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ========================= НАСТРОЙКИ =========================

BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Переменная окружения BOT_TOKEN не найдена.")
_raw_len = len(BOT_TOKEN)
BOT_TOKEN = BOT_TOKEN.strip()
if len(BOT_TOKEN) != _raw_len:
    logger.warning(
        "[INIT] BOT_TOKEN содержал лишние пробелы/переносы строк (было %s символов, стало %s) — автоматически очищено",
        _raw_len, len(BOT_TOKEN)
    )
logger.info("[INIT] Длина BOT_TOKEN после очистки: %s символов", len(BOT_TOKEN))

ADMIN_IDS = {5749410592, 7452588268, 8895760043}
SUPER_ADMIN_IDS = {8885801755}
ALL_ADMIN_IDS = ADMIN_IDS | SUPER_ADMIN_IDS

SUPPORT_USERNAME = "Dw_Worlds.t.me"
CHANNEL_LINK = "https://t.me/vp_hub_news"
CHANNEL_USERNAME = "@vp_hub_news"

FIND_PAGE_SIZE = 5
SEARCH_PAGE_SIZE = 5
MY_VP_PAGE_SIZE = 5

CHANNEL_SUBSCRIBERS_UPDATE_INTERVAL = 300

SYNONYMS = {
    "кф": ["кф", "конфа", "конференция", "confession", "conf", "cf"],
    "сетка": ["сетка", "сеточка", "grid", "сет"],
    "дейли": ["дейли", "дейлик", "daily", "дэйли", "dayly"],
    "ролевая": ["ролевая", "ролка", "roleplay", "rp", "рп", "ролевка"],
    "флуд": ["флуд", "flood", "флудилка", "болталка"],
    "чат": ["чат", "chat", "общение", "общалка", "беседа"],
    "вп": ["вп", "взаимопиар", "взаимный пиар", "пиар", "vp", "mp", "mutual promo"],
}

CATEGORIES = {
    "сетка": "🌐 Сетка",
    "дейли": "📅 Дейли",
    "кф": "💬 КФ",
    "ролевая": "🎭 Ролевая",
    "флуд": "💭 Флуд",
    "чат": "📢 Чат",
    "игры": "🎮 Игры",
    "список": "🗂️ Список",
    "Дейли+кф": "📅💬 Дейли+кф"
}

FIELD_NAMES_RU = {
    "title": "название",
    "dni": "DNI",
    "threshold": "порог",
    "subscribers": "количество подписчиков",
    "rt_post": "ссылку на Ртпост",
    "vp_post": "ссылку на Вппост",
    "extra": "доп. информацию",
    "category": "категорию",
    "time_range": "промежуток",
    "schedule": "расписание",
    "photo": "фото",
}

VPSHER_FIELD_NAMES_RU = {
    "paid_type": "тип (платно/бесплатно)",
    "frequency": "частоту",
    "work_days": "дни работы",
    "channel_links": "списки/каналы",
    "details": "подробности",
    "price_info": "цену",
    "contact": "контакт",
}

BULK_TEMPLATE = "Название | DNI | Порог | Ссылка на Ртпост | Ссылка на Вппост | Подписчики | Доп. информация | Категория | Промежуток | Расписание"

EXPORT_COLUMNS = (
    "id", "title", "dni", "threshold", "rt_post", "vp_post", "subscribers",
    "extra", "photo_id", "status", "submitted_by", "created_at", "pinned",
    "category", "time_range", "schedule",
)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

logger.info("[INIT] Бот инициализирован. Админы: %s, Супер-админы: %s", ADMIN_IDS, SUPER_ADMIN_IDS)

# ========================= БАЗА ДАННЫХ (потокобезопасно) =========================

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Переменная окружения DATABASE_URL не найдена.")

_db_local = threading.local()

def _get_db():
    if not hasattr(_db_local, 'conn') or _db_local.conn is None or _db_local.conn.closed:
        logger.info("[DB] Создание нового подключения к БД")
        _db_local.conn = psycopg2.connect(DATABASE_URL)
        _db_local.conn.autocommit = False
        _db_local.cur = _db_local.conn.cursor()
    return _db_local.conn, _db_local.cur

def db_query(sql, params=()):
    conn, cur = _get_db()
    try:
        logger.debug("[DB QUERY] SQL: %s | Params: %s", sql[:100], str(params)[:100])
        cur.execute(sql, params)
        return cur.fetchall()
    except OperationalError as e:
        logger.warning("[DB] OperationalError, переподключение: %s", e)
        _db_local.conn = None
        conn, cur = _get_db()
        cur.execute(sql, params)
        return cur.fetchall()
    except Exception as e:
        logger.error("[DB QUERY ERROR] %s | SQL: %s | Params: %s", e, sql[:100], str(params)[:100])
        try:
            conn.rollback()
        except Exception:
            pass
        raise

def db_execute(sql, params=()):
    conn, cur = _get_db()
    try:
        logger.debug("[DB EXECUTE] SQL: %s | Params: %s", sql[:100], str(params)[:100])
        cur.execute(sql, params)
        conn.commit()
        logger.debug("[DB EXECUTE] Успешно выполнено")
    except OperationalError as e:
        logger.warning("[DB] OperationalError при execute, переподключение: %s", e)
        _db_local.conn = None
        conn, cur = _get_db()
        cur.execute(sql, params)
        conn.commit()
    except Exception as e:
        logger.error("[DB EXECUTE ERROR] %s | SQL: %s | Params: %s", e, sql[:100], str(params)[:100])
        try:
            conn.rollback()
        except Exception:
            pass
        raise

def db_insert_returning_id(sql, params=()):
    conn, cur = _get_db()
    try:
        logger.debug("[DB INSERT] SQL: %s | Params: %s", sql[:100], str(params)[:100])
        cur.execute(sql, params)
        new_id = cur.fetchone()[0]
        conn.commit()
        logger.info("[DB INSERT] Создана новая запись с ID=%s", new_id)
        return new_id
    except OperationalError as e:
        logger.warning("[DB] OperationalError при insert, переподключение: %s", e)
        _db_local.conn = None
        conn, cur = _get_db()
        cur.execute(sql, params)
        new_id = cur.fetchone()[0]
        conn.commit()
        return new_id
    except Exception as e:
        logger.error("[DB INSERT ERROR] %s | SQL: %s | Params: %s", e, sql[:100], str(params)[:100])
        try:
            conn.rollback()
        except Exception:
            pass
        raise

def init_db():
    logger.info("[DB INIT] Начало инициализации базы данных...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    tables = [
        ("channels", """
        CREATE TABLE IF NOT EXISTS channels (
            id SERIAL PRIMARY KEY,
            title TEXT,
            dni TEXT,
            threshold INTEGER,
            rt_post TEXT,
            vp_post TEXT,
            subscribers INTEGER,
            extra TEXT,
            photo_id TEXT,
            status TEXT DEFAULT 'pending',
            submitted_by BIGINT,
            created_at TEXT,
            pinned INTEGER DEFAULT 0,
            category TEXT DEFAULT 'чат',
            time_range TEXT,
            schedule TEXT,
            submitted_by_username TEXT
        )
        """),
        ("admin_actions", """
        CREATE TABLE IF NOT EXISTS admin_actions (
            id SERIAL PRIMARY KEY,
            admin_id BIGINT,
            action TEXT,
            details TEXT,
            created_at TEXT
        )
        """),
        ("deleted_channels", """
        CREATE TABLE IF NOT EXISTS deleted_channels (
            id INTEGER PRIMARY KEY,
            channel_data JSONB,
            deleted_by BIGINT,
            deleted_at TEXT
        )
        """),
        ("deleted_vpshers", """
        CREATE TABLE IF NOT EXISTS deleted_vpshers (
            id INTEGER PRIMARY KEY,
            vpsher_data JSONB,
            deleted_by BIGINT,
            deleted_at TEXT
        )
        """),
        ("vpshers", """
        CREATE TABLE IF NOT EXISTS vpshers (
            id SERIAL PRIMARY KEY,
            submitted_by BIGINT,
            submitted_by_username TEXT,
            paid_type TEXT,
            frequency TEXT,
            work_days TEXT,
            channel_links TEXT,
            details TEXT,
            price_info TEXT,
            contact TEXT,
            created_at TEXT
        )
        """),
        ("linked_channels", """
        CREATE TABLE IF NOT EXISTS linked_channels (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            channel_username TEXT,
            channel_chat_id BIGINT,
            linked_at TEXT,
            last_subscribers_count INTEGER DEFAULT 0,
            last_updated TEXT
        )
        """),
        ("active_users", """
        CREATE TABLE IF NOT EXISTS active_users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            first_seen TEXT,
            last_active TEXT,
            message_count INTEGER DEFAULT 0
        )
        """),
        ("broadcasts", """
        CREATE TABLE IF NOT EXISTS broadcasts (
            id SERIAL PRIMARY KEY,
            admin_id BIGINT,
            message_text TEXT,
            sent_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            total_users INTEGER DEFAULT 0,
            created_at TEXT,
            completed_at TEXT
        )
        """),
    ]

    for table_name, sql in tables:
        cur.execute(sql)
        conn.commit()
        logger.info("[DB INIT] Таблица '%s' проверена/создана", table_name)

    try:
        cur.execute("ALTER TABLE channels ADD COLUMN IF NOT EXISTS linked_chat_id BIGINT")
        conn.commit()
        logger.info("[DB INIT] Колонка linked_chat_id проверена")
    except Exception as e:
        conn.rollback()
        logger.warning("[DB INIT] Колонка linked_chat_id: %s", e)

    cur.close()
    conn.close()
    logger.info("[DB INIT] Инициализация завершена успешно")

init_db()

def log_admin_action(admin_id, action, details=""):
    logger.info("[ADMIN ACTION] admin_id=%s, action=%s, details=%s", admin_id, action, details)
    db_execute(
        "INSERT INTO admin_actions (admin_id, action, details, created_at) VALUES (%s,%s,%s,%s)",
        (admin_id, action, details, datetime.now().isoformat()),
    )

# ========================= ВАЛИДАЦИЯ URL (ИСПРАВЛЕНИЕ ОШИБКИ #1) =========================

def is_valid_tg_url(url):
    """
    Строгая валидация URL для Telegram inline кнопок.
    Предотвращает ошибку: "Bad Request: inline keyboard button URL is invalid"
    """
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    if not re.match(r'^https?://', url):
        return False
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            return False
        # Проверяем, что нет пробелов и эмодзи в хосте и пути
        if re.search(r'[\s\u2600-\u27BF\u2B50-\u2BFF\U0001F300-\U0001F9FF]', parsed.netloc + parsed.path):
            return False
        if '.' not in parsed.netloc:
            return False
        return True
    except Exception as e:
        logger.warning("[URL VALIDATION] Невалидный URL '%s': %s", url[:50], e)
        return False

# ========================= СЕРВИС ПРИВЯЗКИ КАНАЛОВ =========================

def get_linked_channels(user_id):
    rows = db_query("SELECT * FROM linked_channels WHERE user_id=%s ORDER BY linked_at DESC", (user_id,))
    return rows

def get_linked_channel(user_id, channel_chat_id):
    rows = db_query("SELECT * FROM linked_channels WHERE user_id=%s AND channel_chat_id=%s LIMIT 1", (user_id, channel_chat_id))
    return rows[0] if rows else None

def link_channel(user_id, channel_username, channel_chat_id):
    now = datetime.now().isoformat()
    existing = get_linked_channel(user_id, channel_chat_id)
    if existing:
        logger.info("[LINK] Обновление привязки канала user_id=%s, chat_id=%s", user_id, channel_chat_id)
        db_execute(
            "UPDATE linked_channels SET channel_username=%s, linked_at=%s WHERE user_id=%s AND channel_chat_id=%s",
            (channel_username, now, user_id, channel_chat_id)
        )
    else:
        logger.info("[LINK] Новая привязка канала user_id=%s, chat_id=%s, username=%s", user_id, channel_chat_id, channel_username)
        db_execute(
            "INSERT INTO linked_channels (user_id, channel_username, channel_chat_id, linked_at) VALUES (%s,%s,%s,%s)",
            (user_id, channel_username, channel_chat_id, now)
        )

def unlink_channel(user_id, channel_chat_id=None):
    if channel_chat_id:
        logger.info("[UNLINK] Отвязка канала user_id=%s, chat_id=%s", user_id, channel_chat_id)
        db_execute("DELETE FROM linked_channels WHERE user_id=%s AND channel_chat_id=%s", (user_id, channel_chat_id))
    else:
        logger.info("[UNLINK] Отвязка ВСЕХ каналов user_id=%s", user_id)
        db_execute("DELETE FROM linked_channels WHERE user_id=%s", (user_id,))

def update_subscribers_count(linked_id, count):
    db_execute(
        "UPDATE linked_channels SET last_subscribers_count=%s, last_updated=%s WHERE id=%s",
        (count, datetime.now().isoformat(), linked_id)
    )

def get_all_linked_channels():
    return db_query("SELECT id, user_id, channel_chat_id FROM linked_channels WHERE channel_chat_id IS NOT NULL")

# ========================= УЧЁТ АКТИВНЫХ ПОЛЬЗОВАТЕЛЕЙ =========================

def track_user(user):
    """Отслеживает активного пользователя при каждом взаимодействии."""
    now = datetime.now().isoformat()
    existing = db_query("SELECT user_id FROM active_users WHERE user_id=%s", (user.id,))
    if existing:
        db_execute(
            "UPDATE active_users SET last_active=%s, message_count=message_count+1, username=%s, first_name=%s, last_name=%s WHERE user_id=%s",
            (now, user.username, user.first_name, user.last_name, user.id)
        )
    else:
        logger.info("[USER] Новый пользователь: id=%s, username=%s, name=%s %s", 
                    user.id, user.username, user.first_name, user.last_name)
        db_execute(
            "INSERT INTO active_users (user_id, username, first_name, last_name, first_seen, last_active, message_count) VALUES (%s,%s,%s,%s,%s,%s,1)",
            (user.id, user.username, user.first_name, user.last_name, now, now)
        )

def get_active_users_count():
    rows = db_query("SELECT COUNT(*) FROM active_users")
    return rows[0][0] if rows else 0

def get_all_active_users():
    return db_query("SELECT user_id, username, first_name, last_name FROM active_users ORDER BY last_active DESC")

# ========================= ВРЕМЕННЫЕ ХРАНИЛИЩА =========================

temp_data = {}
view_states = {}
edit_field_states = {}
admin_edit_states = {}
search_states = {}
vpsher_temp = {}
vpsher_edit_states = {}
link_channel_states = {}

# ========================= ВСПОМОГАТЕЛЬНОЕ =========================

def esc(value):
    if value is None or value == "":
        return ""
    return html.escape(str(value))

def main_menu_kb():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("🔍 Найти ВП", "📢 Предложить ВП")
    kb.row("🔎 Поиск по словам", "📂 Категории")
    kb.row("🙋 Анкета впшера", "🕵️ Найти впшера")
    kb.row("📌 Закреп ВП", "🆘 Поддержка")
    kb.row("👤 Мои ВП", "📡 Телеграм канал")
    return kb

def cancel_kb():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("🔙 Отмена")
    return kb

def edit_kb():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("/skip")
    kb.row("🔙 Отмена")
    return kb

def check_cancel(message):
    if message.text and message.text.strip() == "🔙 Отмена":
        uid = message.from_user.id
        temp_data.pop(uid, None)
        edit_field_states.pop(uid, None)
        vpsher_temp.pop(uid, None)
        vpsher_edit_states.pop(uid, None)
        link_channel_states.pop(uid, None)
        bot.send_message(message.chat.id, "🔙 Отменено, возвращаемся в меню", reply_markup=main_menu_kb())
        logger.info("[CANCEL] Пользователь %s отменил действие", uid)
        return True
    return False

def parse_threshold_input(text):
    try:
        thr = int(text.strip())
    except (ValueError, AttributeError):
        return None
    if thr < 0:
        return None
    return thr

def find_thresholds_kb(thresholds, page):
    total_pages = max(1, (len(thresholds) + FIND_PAGE_SIZE - 1) // FIND_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * FIND_PAGE_SIZE
    chunk = thresholds[start:start + FIND_PAGE_SIZE]

    kb = types.InlineKeyboardMarkup()
    for t in chunk:
        kb.row(types.InlineKeyboardButton(f"{t}+", callback_data=f"find_{t}"))

    nav_row = []
    if page > 0:
        nav_row.append(types.InlineKeyboardButton("⬅️", callback_data=f"findpage_{page - 1}"))
    nav_row.append(types.InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(types.InlineKeyboardButton("➡️", callback_data=f"findpage_{page + 1}"))
    kb.row(*nav_row)
    return kb

def get_approved_thresholds():
    rows = db_query("SELECT DISTINCT threshold FROM channels WHERE status='approved' ORDER BY threshold")
    return [r[0] for r in rows]

def build_caption(title, dni, threshold, subs, extra, category=None, time_range=None, schedule=None, internal_id=None, pinned=False):
    prefix = "📌 " if pinned else ""
    text = (
        f"{prefix}<b>{esc(title)}</b>\n\n"
        f"🆔 DNI: {esc(dni)}\n"
        f"📊 Порог: {threshold}+\n"
        f"👥 Подписчиков: {subs}"
    )
    if category:
        cat_emoji = CATEGORIES.get(category, f"📂 {esc(category)}")
        text += f"\n📂 Категория: {cat_emoji}"
    if time_range:
        text += f"\n⏱ Промежуток: {esc(time_range)}"
    if schedule:
        text += f"\n📅 Расписание: {esc(schedule)}"
    text += f"\nℹ️ Дополнительно: {esc(extra)}"
    if internal_id is not None:
        text += f"\n\n🔧 Внутренний ID: {internal_id}"
    return text

def sender_display(user):
    if user.username:
        return f"@{user.username}"
    return f"id{user.id}"

def normalize_url(text):
    if not text:
        return None
    url = text.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        url = "https://" + url
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return None
    if "." not in parsed.netloc:
        return None
    return url

def send_main_menu(chat_id):
    text = (
        "🔎 Выбирай порог подписчиков и смотри подходящие каналы для взаимного пиара.\n\n"
        "🔎 Ищи по ключевым словам (например, «вп кф» или «мп конфа»).\n"
        "📢 Есть свой канал? Предложи его — он появится в базе сразу после заполнения анкеты.\n\n"
        f"🔰 Поддержка: <a href=\"{SUPPORT_USERNAME}\">перейти</a>\n\n"
        f"📡 Наш канал: <a href=\"{CHANNEL_LINK}\">перейти</a>\n\n"
        "Выбирай, с чего начнём 👇"
    )
    bot.send_message(chat_id, text, reply_markup=main_menu_kb())

def get_channel(channel_id):
    rows = db_query("SELECT * FROM channels WHERE id=%s", (channel_id,))
    return rows[0] if rows else None

def get_synonyms_for_query(query):
    query_lower = query.lower()
    found_categories = set()
    all_synonyms = set()
    for category, synonyms in SYNONYMS.items():
        for syn in synonyms:
            if syn in query_lower:
                found_categories.add(category)
                all_synonyms.update(synonyms)
    return list(found_categories), list(all_synonyms)

def search_channels(query, page=0, category=None):
    query_lower = query.lower()
    categories_found, synonyms = get_synonyms_for_query(query_lower)
    conditions = ["status='approved'"]
    params = []
    search_terms = []
    if synonyms:
        for syn in synonyms:
            search_terms.append(f"(LOWER(title) LIKE %s OR LOWER(extra) LIKE %s)")
            params.extend([f"%{syn}%", f"%{syn}%"])
    else:
        words = query_lower.split()
        for word in words:
            search_terms.append(f"(LOWER(title) LIKE %s OR LOWER(extra) LIKE %s)")
            params.extend([f"%{word}%", f"%{word}%"])
    if search_terms:
        conditions.append(f"({' OR '.join(search_terms)})")
    if category and category in CATEGORIES:
        conditions.append("category = %s")
        params.append(category)
    where_clause = " AND ".join(conditions)
    count_sql = f"SELECT COUNT(*) FROM channels WHERE {where_clause}"
    total = db_query(count_sql, params)[0][0]
    offset = page * SEARCH_PAGE_SIZE
    sql = f"SELECT id FROM channels WHERE {where_clause} ORDER BY pinned DESC, RANDOM() LIMIT %s OFFSET %s"
    params.extend([SEARCH_PAGE_SIZE, offset])
    rows = db_query(sql, params)
    return [r[0] for r in rows], total, categories_found


def card_kb(state):
    row = get_channel(state["ids"][state["index"]])
    kb = types.InlineKeyboardMarkup()

    # ИСПРАВЛЕНИЕ #1: Используем is_valid_tg_url вместо startswith("http")
    vp_url = row[5] if is_valid_tg_url(row[5]) else None
    rt_url = row[4] if is_valid_tg_url(row[4]) else None

    if vp_url or rt_url:
        buttons = []
        if vp_url:
            buttons.append(types.InlineKeyboardButton("📤 Вп пост", url=vp_url))
        if rt_url:
            buttons.append(types.InlineKeyboardButton("🔁 Ртпост", url=rt_url))
        kb.row(*buttons)
    total = len(state["ids"])
    idx = state["index"]
    nav_row = []
    if idx > 0:
        nav_row.append(types.InlineKeyboardButton("⬅️", callback_data="nav_prev"))
    nav_row.append(types.InlineKeyboardButton(f"{idx + 1}/{total}", callback_data="noop"))
    if idx < total - 1:
        nav_row.append(types.InlineKeyboardButton("➡️", callback_data="nav_next"))
    kb.row(*nav_row)
    if state.get("search_mode"):
        if state.get("has_more", False):
            kb.row(types.InlineKeyboardButton("🔍 Ещё результаты", callback_data="search_next"))
        if state.get("search_page", 0) > 0:
            kb.row(types.InlineKeyboardButton("🔍 Предыдущие", callback_data="search_prev"))
    kb.row(types.InlineKeyboardButton("🔙 Вернуться в меню", callback_data="back_menu"))
    return kb

def render_card(chat_id, uid):
    state = view_states[uid]
    row = get_channel(state["ids"][state["index"]])
    internal_id = row[0] if uid in ALL_ADMIN_IDS else None
    caption = build_caption(
        row[1], row[2], row[3], row[6], row[7],
        row[13], row[14], row[15], internal_id, pinned=bool(row[12])
    )
    kb = card_kb(state)
    has_photo = bool(row[8])

    prev_message_id = state.get("message_id")
    prev_has_photo = state.get("has_photo")

    if prev_message_id is not None:
        try:
            if has_photo and prev_has_photo:
                media = types.InputMediaPhoto(row[8], caption=caption, parse_mode="HTML")
                bot.edit_message_media(media, chat_id, prev_message_id, reply_markup=kb)
                state["has_photo"] = has_photo
                return
            elif not has_photo and not prev_has_photo:
                bot.edit_message_text(caption, chat_id, prev_message_id, reply_markup=kb)
                state["has_photo"] = has_photo
                return
        except Exception:
            pass
        try:
            bot.delete_message(chat_id, prev_message_id)
        except Exception:
            pass

    if has_photo:
        try:
            msg = bot.send_photo(chat_id, row[8], caption=caption, reply_markup=kb)
        except telebot.apihelper.ApiTelegramException as e:
            # Битый file_id/ссылка на фото — не роняем бота, показываем карточку текстом
            # и чистим невалидное фото в базе, чтобы ошибка не повторялась на этом канале
            logger.warning("[CARD] Битое фото у канала id=%s: %s — отправляю без фото", row[0], e)
            try:
                db_execute("UPDATE channels SET photo_id=NULL WHERE id=%s", (row[0],))
            except Exception as db_err:
                logger.error("[CARD] Не удалось очистить битое photo_id канала id=%s: %s", row[0], db_err)
            has_photo = False
            msg = bot.send_message(chat_id, caption, reply_markup=kb)
    else:
        msg = bot.send_message(chat_id, caption, reply_markup=kb)
    state["message_id"] = msg.message_id
    state["has_photo"] = has_photo

def render_admin_card(chat_id, uid):
    state = view_states[uid]
    row = get_channel(state["ids"][state["index"]])
    if not row:
        return
    total = len(state["ids"])
    idx = state["index"]
    cid, title, thr, pinned = row[0], row[1], row[3], row[12]
    category = row[13] if len(row) > 13 else ""
    rt_post = row[4]
    vp_post = row[5]
    subs = row[6]
    dni = row[2]
    extra = row[7]
    time_range = row[14] if len(row) > 14 else ""
    schedule = row[15] if len(row) > 15 else ""

    prefix = "📌 " if pinned else ""
    cat_text = f" | 📂 {CATEGORIES.get(category, category)}" if category else ""

    text = (
        f"🔧 <b>Админ-просмотр каналов</b>\n"
        f"Канал {idx + 1} из {total}\n\n"
        f"{prefix}#{cid} | <b>{esc(title)}</b>{cat_text}\n"
        f"🆔 DNI: {esc(dni)}\n"
        f"📊 Порог: {thr}+\n"
        f"👥 Подписчиков: {subs}\n"
        f"ℹ️ Доп. инфо: {esc(extra)}\n"
    )
    if time_range:
        text += f"⏱ Промежуток: {esc(time_range)}\n"
    if schedule:
        text += f"📅 Расписание: {esc(schedule)}\n"

    kb = types.InlineKeyboardMarkup()

    # ИСПРАВЛЕНИЕ #1: Используем is_valid_tg_url
    if is_valid_tg_url(rt_post):
        kb.row(types.InlineKeyboardButton("🔁 Ртпост", url=rt_post))
    if is_valid_tg_url(vp_post):
        kb.row(types.InlineKeyboardButton("📤 Вп пост", url=vp_post))
    kb.row(
        types.InlineKeyboardButton("✏️ Порог", callback_data=f"adminedit_{cid}_threshold"),
        types.InlineKeyboardButton("👥 Подписчики", callback_data=f"adminedit_{cid}_subscribers"),
    )
    kb.row(
        types.InlineKeyboardButton("🔗 Вппост", callback_data=f"adminedit_{cid}_vp_post"),
        types.InlineKeyboardButton("🔗 Ртпост", callback_data=f"adminedit_{cid}_rt_post"),
    )
    nav_row = []
    if idx > 0:
        nav_row.append(types.InlineKeyboardButton("⬅️", callback_data="admin_nav_prev"))
    nav_row.append(types.InlineKeyboardButton(f"{idx + 1}/{total}", callback_data="noop"))
    if idx < total - 1:
        nav_row.append(types.InlineKeyboardButton("➡️", callback_data="admin_nav_next"))
    kb.row(*nav_row)
    kb.row(types.InlineKeyboardButton("📋 Показать полный список", callback_data="admin_show_all_list"))
    kb.row(types.InlineKeyboardButton("🔙 Закрыть", callback_data="admin_close_list"))

    prev_message_id = state.get("message_id")
    if prev_message_id:
        try:
            bot.edit_message_text(text, chat_id, prev_message_id, reply_markup=kb, parse_mode="HTML")
            return
        except Exception:
            try:
                bot.delete_message(chat_id, prev_message_id)
            except Exception:
                pass
    msg = bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")
    state["message_id"] = msg.message_id

def get_vpsher(vp_id):
    rows = db_query("SELECT * FROM vpshers WHERE id=%s", (vp_id,))
    return rows[0] if rows else None

def vpsher_card_kb(state):
    total = len(state["ids"])
    idx = state["index"]
    kb = types.InlineKeyboardMarkup()
    nav_row = []
    if idx > 0:
        nav_row.append(types.InlineKeyboardButton("⬅️", callback_data="nav_prev"))
    nav_row.append(types.InlineKeyboardButton(f"{idx + 1}/{total}", callback_data="noop"))
    if idx < total - 1:
        nav_row.append(types.InlineKeyboardButton("➡️", callback_data="nav_next"))
    kb.row(*nav_row)
    kb.row(types.InlineKeyboardButton("🔙 Вернуться в меню", callback_data="back_menu"))
    return kb

def render_vpsher_card(chat_id, uid):
    state = view_states[uid]
    row = get_vpsher(state["ids"][state["index"]])
    if not row:
        bot.send_message(chat_id, "⚠️ Анкета больше недоступна.")
        return
    vp_id, paid_type, frequency, work_days, channel_links, details, price_info, contact = (
        row[0], row[3], row[4], row[5], row[6], row[7], row[8], row[9]
    )
    text = (
        f"🙋 <b>Анкета впшера</b>\n\n"
        f"💰 Тип: {esc(paid_type)}\n"
        f"📅 Частота: {esc(frequency)}\n"
        f"🗓 Дни работы: {esc(work_days)}\n"
        f"🔗 Списки/каналы: {esc(channel_links)}\n"
        f"ℹ️ Подробности: {esc(details)}\n"
    )
    if paid_type == "платно":
        text += f"💵 Цена в неделю: {esc(price_info)}\n"
    text += f"\n📞 Контакт: {esc(contact)}"
    if uid in ALL_ADMIN_IDS:
        text += f"\n🔧 Внутренний ID: {vp_id}"
    kb = vpsher_card_kb(state)

    prev_message_id = state.get("message_id")
    if prev_message_id is not None:
        try:
            bot.edit_message_text(text, chat_id, prev_message_id, reply_markup=kb, parse_mode="HTML")
            return
        except Exception:
            try:
                bot.delete_message(chat_id, prev_message_id)
            except Exception:
                pass
    msg = bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")
    state["message_id"] = msg.message_id

# ========================= АРХИВАЦИЯ / УВЕДОМЛЕНИЕ =========================

def notify_super_admin_deletion(item_id, label, kind, reason=""):
    kind_label = "канал" if kind == "channel" else "анкету впшера"
    restore_prefix = "restore" if kind == "channel" else "restorevpsher"
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("🔄 Вернуть", callback_data=f"{restore_prefix}_{item_id}"))
    text = f"🗑 Удалена(ён) {kind_label} #{item_id}"
    if label:
        text += f" «{esc(label)}»"
    if reason:
        text += f"\nПричина: {reason}"
    text += "\nСохранено в корзине на 24 часа."
    logger.info("[NOTIFY] Уведомление супер-админам об удалении %s #%s", kind_label, item_id)
    for admin_id in SUPER_ADMIN_IDS:
        try:
            bot.send_message(admin_id, text, reply_markup=kb)
        except Exception as e:
            logger.warning("[NOTIFY] Не удалось уведомить админа %s: %s", admin_id, e)

# ИСПРАВЛЕНИЕ #2: Удаляем старую запись перед вставкой (предотвращает duplicate key)
def archive_channel(row, deleted_by, reason=""):
    channel_data = {
        "title": row[1], "dni": row[2], "threshold": row[3],
        "rt_post": row[4], "vp_post": row[5], "subscribers": row[6],
        "extra": row[7], "photo_id": row[8], "status": row[9],
        "submitted_by": row[10], "created_at": row[11], "pinned": row[12],
        "category": row[13] if len(row) > 13 else "чат",
        "time_range": row[14] if len(row) > 14 else "",
        "schedule": row[15] if len(row) > 15 else "",
        "submitted_by_username": row[16] if len(row) > 16 else "",
    }
    logger.info("[ARCHIVE] Архивация канала id=%s, title='%s', удалил=%s", row[0], row[1], deleted_by)
    # Удаляем старую запись с таким же id, если есть (предотвращает UniqueViolation)
    db_execute("DELETE FROM deleted_channels WHERE id=%s", (row[0],))
    db_execute(
        "INSERT INTO deleted_channels (id, channel_data, deleted_by, deleted_at) VALUES (%s,%s,%s,%s)",
        (row[0], json.dumps(channel_data, ensure_ascii=False), deleted_by, datetime.now().isoformat())
    )
    db_execute("DELETE FROM channels WHERE id=%s", (row[0],))
    logger.info("[ARCHIVE] Канал id=%s успешно архивирован и удалён из основной таблицы", row[0])
    return channel_data

# ИСПРАВЛЕНИЕ #2: То же самое для впшеров
def archive_vpsher(row, deleted_by, reason=""):
    vpsher_data = {
        "submitted_by": row[1], "submitted_by_username": row[2], "paid_type": row[3],
        "frequency": row[4], "work_days": row[5], "channel_links": row[6],
        "details": row[7], "price_info": row[8], "contact": row[9], "created_at": row[10],
    }
    logger.info("[ARCHIVE] Архивация анкеты впшера id=%s, удалил=%s", row[0], deleted_by)
    # Удаляем старую запись с таким же id, если есть
    db_execute("DELETE FROM deleted_vpshers WHERE id=%s", (row[0],))
    db_execute(
        "INSERT INTO deleted_vpshers (id, vpsher_data, deleted_by, deleted_at) VALUES (%s,%s,%s,%s)",
        (row[0], json.dumps(vpsher_data, ensure_ascii=False), deleted_by, datetime.now().isoformat())
    )
    db_execute("DELETE FROM vpshers WHERE id=%s", (row[0],))
    logger.info("[ARCHIVE] Анкета впшера id=%s успешно архивирована", row[0])
    return vpsher_data


# ========================= СТАРТ =========================

@bot.message_handler(commands=["start"])
def start(message):
    logger.info("[START] Пользователь %s (username=%s) запустил бота", message.from_user.id, message.from_user.username)
    track_user(message.from_user)
    send_main_menu(message.chat.id)

# ========================= ГЛАВНОЕ МЕНЮ =========================

@bot.message_handler(func=lambda m: m.text == "🔍 Найти ВП")
def h_find(message):
    track_user(message.from_user)
    thresholds = get_approved_thresholds()
    if not thresholds:
        bot.send_message(message.chat.id, "😔 Пока нет ни одного канала в базе.")
        return
    kb = find_thresholds_kb(thresholds, 0)
    bot.send_message(message.chat.id, "📊 Выбери порог подписчиков для поиска ВП:", reply_markup=kb)

@bot.message_handler(func=lambda m: m.text == "🔎 Поиск по словам")
def h_search(message):
    track_user(message.from_user)
    msg = bot.send_message(
        message.chat.id,
        "🔎 Введи ключевые слова для поиска ВП.\n"
        "Например: «вп кф», «мп конфа», «сетка дейли», «confession»\n"
        "Можно на любом языке!",
        reply_markup=cancel_kb()
    )
    bot.register_next_step_handler(msg, step_search_query)

def step_search_query(message):
    if check_cancel(message):
        return
    track_user(message.from_user)
    uid = message.from_user.id
    query = message.text.strip()
    if not query:
        msg = bot.send_message(message.chat.id, "⚠️ Введи ключевые слова для поиска:", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_search_query)
        return
    logger.info("[SEARCH] Пользователь %s ищет: '%s'", uid, query)
    channel_ids, total, categories_found = search_channels(query)
    if not channel_ids:
        bot.send_message(
            message.chat.id,
            "😔 Ничего не найдено по твоему запросу.\n"
            "Попробуй другие ключевые слова или проверь категорию.",
            reply_markup=main_menu_kb()
        )
        return
    search_states[uid] = {
        "query": query, "page": 0, "total": total, "categories_found": categories_found
    }
    view_states[uid] = {
        "threshold": 0, "index": 0, "ids": channel_ids,
        "message_id": None, "has_photo": False, "search_mode": True,
        "has_more": total > SEARCH_PAGE_SIZE,
        "search_page": 0,
        "category": None
    }
    categories_text = ""
    if categories_found:
        categories_text = f"\n📂 Найдены категории: {', '.join(CATEGORIES.get(c, c) for c in categories_found)}"
    bot.send_message(message.chat.id, f"🔎 Найдено каналов: {total}{categories_text}", reply_markup=main_menu_kb())
    render_card(message.chat.id, uid)

@bot.message_handler(func=lambda m: m.text == "📂 Категории")
def h_categories(message):
    track_user(message.from_user)
    kb = types.InlineKeyboardMarkup()
    for cat_key, cat_name in CATEGORIES.items():
        kb.row(types.InlineKeyboardButton(cat_name, callback_data=f"cat_{cat_key}"))
    bot.send_message(message.chat.id, "📂 Выбери категорию каналов:", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("cat_"))
def cb_category(call):
    uid = call.from_user.id
    category = call.data.split("_")[1]
    logger.info("[CATEGORY] Пользователь %s выбрал категорию '%s'", uid, category)
    rows = db_query(
        "SELECT id FROM channels WHERE category=%s AND status='approved' ORDER BY pinned DESC, RANDOM()",
        (category,)
    )
    if not rows:
        bot.answer_callback_query(call.id, f"😔 Нет каналов в категории «{CATEGORIES.get(category, category)}»", show_alert=True)
        return
    view_states[uid] = {
        "threshold": 0, "index": 0, "ids": [r[0] for r in rows],
        "message_id": call.message.message_id, "has_photo": False,
        "search_mode": True, "category": category, "has_more": False, "search_page": 0
    }
    bot.answer_callback_query(call.id)
    render_card(call.message.chat.id, uid)

@bot.callback_query_handler(func=lambda c: c.data.startswith("findpage_"))
def cb_find_page(call):
    page = int(call.data.split("_")[1])
    thresholds = get_approved_thresholds()
    kb = find_thresholds_kb(thresholds, page)
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=kb)
    except Exception:
        pass

@bot.message_handler(func=lambda m: m.text == "📌 Закреп ВП")
def h_pin(message):
    track_user(message.from_user)
    bot.send_message(message.chat.id, f"📌 Чтобы закрепить свой ВП в топе списка, напиши в поддержку и узнай расценки: {SUPPORT_USERNAME}")

@bot.message_handler(func=lambda m: m.text == "🆘 Поддержка")
def h_support(message):
    track_user(message.from_user)
    bot.send_message(message.chat.id, f"🆘 По всем вопросам пиши сюда: {SUPPORT_USERNAME}")

@bot.message_handler(func=lambda m: m.text == "📡 Телеграм канал")
def h_channel(message):
    track_user(message.from_user)
    bot.send_message(message.chat.id, f"📡 Наш канал: {CHANNEL_LINK}")

# ========================= НАВИГАЦИЯ КАРТОЧЕК =========================

@bot.callback_query_handler(func=lambda c: c.data.startswith("find_"))
def cb_find(call):
    uid = call.from_user.id
    thr = int(call.data.split("_")[1])
    logger.info("[FIND] Пользователь %s выбрал порог %s", uid, thr)
    rows = db_query(
        "SELECT id FROM channels WHERE threshold=%s AND status='approved' ORDER BY pinned DESC, RANDOM()",
        (thr,),
    )
    if not rows:
        bot.answer_callback_query(call.id, "😔 Каналов с таким порогом пока нет", show_alert=True)
        return
    view_states[uid] = {
        "threshold": thr, "index": 0, "ids": [r[0] for r in rows],
        "message_id": call.message.message_id, "has_photo": False, "search_mode": False
    }
    bot.answer_callback_query(call.id)
    render_card(call.message.chat.id, uid)

@bot.callback_query_handler(func=lambda c: c.data in ("nav_prev", "nav_next"))
def cb_nav(call):
    uid = call.from_user.id
    state = view_states.get(uid)
    if not state:
        bot.answer_callback_query(call.id, "Сессия устарела, начни поиск заново", show_alert=True)
        return
    total = len(state["ids"])
    if call.data == "nav_next" and state["index"] < total - 1:
        state["index"] += 1
    elif call.data == "nav_prev" and state["index"] > 0:
        state["index"] -= 1
    else:
        bot.answer_callback_query(call.id)
        return
    bot.answer_callback_query(call.id)
    if state.get("mode") == "vpsher":
        render_vpsher_card(call.message.chat.id, uid)
    else:
        render_card(call.message.chat.id, uid)

@bot.callback_query_handler(func=lambda c: c.data == "back_menu")
def cb_back_menu(call):
    uid = call.from_user.id
    view_states.pop(uid, None)
    search_states.pop(uid, None)
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, "🏠 Главное меню", reply_markup=main_menu_kb())

@bot.callback_query_handler(func=lambda c: c.data == "noop")
def cb_noop(call):
    bot.answer_callback_query(call.id)

# ========================= ПАГИНАЦИЯ ПОИСКА =========================

@bot.callback_query_handler(func=lambda c: c.data == "search_next")
def cb_search_next(call):
    uid = call.from_user.id
    sstate = search_states.get(uid)
    if not sstate:
        bot.answer_callback_query(call.id, "Сессия устарела", show_alert=True)
        return
    sstate["page"] += 1
    channel_ids, total, categories_found = search_channels(sstate["query"], page=sstate["page"], category=sstate.get("category"))
    if not channel_ids:
        bot.answer_callback_query(call.id, "Больше результатов нет", show_alert=True)
        return
    view_states[uid] = {
        "threshold": 0, "index": 0, "ids": channel_ids,
        "message_id": None, "has_photo": False, "search_mode": True,
        "has_more": (sstate["page"] + 1) * SEARCH_PAGE_SIZE < total,
        "search_page": sstate["page"],
        "category": sstate.get("category")
    }
    bot.answer_callback_query(call.id)
    render_card(call.message.chat.id, uid)

@bot.callback_query_handler(func=lambda c: c.data == "search_prev")
def cb_search_prev(call):
    uid = call.from_user.id
    sstate = search_states.get(uid)
    if not sstate:
        bot.answer_callback_query(call.id, "Сессия устарела", show_alert=True)
        return
    if sstate["page"] <= 0:
        bot.answer_callback_query(call.id, "Это первая страница", show_alert=True)
        return
    sstate["page"] -= 1
    channel_ids, total, categories_found = search_channels(sstate["query"], page=sstate["page"], category=sstate.get("category"))
    if not channel_ids:
        bot.answer_callback_query(call.id, "Результаты не найдены", show_alert=True)
        return
    view_states[uid] = {
        "threshold": 0, "index": 0, "ids": channel_ids,
        "message_id": None, "has_photo": False, "search_mode": True,
        "has_more": (sstate["page"] + 1) * SEARCH_PAGE_SIZE < total,
        "search_page": sstate["page"],
        "category": sstate.get("category")
    }
    bot.answer_callback_query(call.id)
    render_card(call.message.chat.id, uid)

# ========================= ПРЕДЛОЖИТЬ ВП =========================

@bot.message_handler(func=lambda m: m.text == "📢 Предложить ВП")
def h_suggest(message):
    track_user(message.from_user)
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("✍️ Вручную (один канал)", callback_data="addmode_manual"),
        types.InlineKeyboardButton("📥 По шаблону (несколько)", callback_data="addmode_bulk"),
    )
    bot.send_message(message.chat.id, "Как хочешь добавить канал(ы)?", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "addmode_manual")
def cb_addmode_manual(call):
    bot.answer_callback_query(call.id)
    uid = call.from_user.id
    temp_data[uid] = {}
    msg = bot.send_message(call.message.chat.id, "📝 Введи название канала:", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_title)

@bot.callback_query_handler(func=lambda c: c.data == "addmode_bulk")
def cb_addmode_bulk(call):
    bot.answer_callback_query(call.id)
    start_bulk_add(call.message.chat.id)

def step_title(message):
    if check_cancel(message): return
    uid = message.from_user.id
    temp_data[uid]["title"] = message.text
    msg = bot.send_message(message.chat.id, "🆔 Введи DNI:", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_dni)

def step_dni(message):
    if check_cancel(message): return
    uid = message.from_user.id
    temp_data[uid]["dni"] = message.text
    msg = bot.send_message(message.chat.id, "📊 Введи порог подписчиков числом (например 150):", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_threshold)

def step_threshold(message):
    if check_cancel(message): return
    uid = message.from_user.id
    thr = parse_threshold_input(message.text)
    if thr is None:
        msg = bot.send_message(message.chat.id, "⚠️ Нужно целое неотрицательное число. Введи порог:", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_threshold)
        return
    temp_data[uid]["threshold"] = thr
    msg = bot.send_message(message.chat.id, "🔗 Отправь ссылку на Ртпост:", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_rtpost)

def step_rtpost(message):
    if check_cancel(message): return
    uid = message.from_user.id
    url = normalize_url(message.text)
    if not url:
        msg = bot.send_message(message.chat.id, "⚠️ Это не похоже на ссылку. Отправь ссылку на Ртпост:", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_rtpost)
        return
    temp_data[uid]["rt_post"] = url
    msg = bot.send_message(message.chat.id, "🔗 Отправь ссылку на Вппост:", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_vppost)

def step_vppost(message):
    if check_cancel(message): return
    uid = message.from_user.id
    url = normalize_url(message.text)
    if not url:
        msg = bot.send_message(message.chat.id, "⚠️ Это не похоже на ссылку. Отправь ссылку на Вппост:", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_vppost)
        return
    temp_data[uid]["vp_post"] = url
    msg = bot.send_message(message.chat.id, "👥 Сколько подписчиков в канале? (числом)", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_subs)

def step_subs(message):
    if check_cancel(message): return
    uid = message.from_user.id
    try:
        subs = int(message.text.replace(" ", ""))
    except ValueError:
        msg = bot.send_message(message.chat.id, "⚠️ Нужно число. Сколько подписчиков?", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_subs)
        return
    temp_data[uid]["subscribers"] = subs
    msg = bot.send_message(message.chat.id, "ℹ️ Введи дополнительную информацию о канале:", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_extra)

def step_extra(message):
    if check_cancel(message): return
    uid = message.from_user.id
    temp_data[uid]["extra"] = message.text
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    for cat_name in CATEGORIES.values():
        kb.row(cat_name)
    kb.row("🔙 Отмена")
    msg = bot.send_message(message.chat.id, "📂 Выбери категорию канала:", reply_markup=kb)
    bot.register_next_step_handler(msg, step_category)

def step_category(message):
    if check_cancel(message): return
    uid = message.from_user.id
    category_key = None
    for key, name in CATEGORIES.items():
        if message.text == name:
            category_key = key
            break
    if not category_key:
        msg = bot.send_message(message.chat.id, "⚠️ Выбери категорию из списка:", reply_markup=message.reply_markup)
        bot.register_next_step_handler(msg, step_category)
        return
    temp_data[uid]["category"] = category_key
    msg = bot.send_message(message.chat.id, "⏱ Введи промежуток по времени или «Пропустить»:", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_time_range)

def step_time_range(message):
    if check_cancel(message): return
    uid = message.from_user.id
    if message.text.strip().lower() in ("пропустить", "нет", "skip", "-"):
        temp_data[uid]["time_range"] = ""
    else:
        temp_data[uid]["time_range"] = message.text
    msg = bot.send_message(message.chat.id, "📅 Введи расписание или «Пропустить»:", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_schedule)

def step_schedule(message):
    if check_cancel(message): return
    uid = message.from_user.id
    if message.text.strip().lower() in ("пропустить", "нет", "skip", "-"):
        temp_data[uid]["schedule"] = ""
    else:
        temp_data[uid]["schedule"] = message.text
    msg = bot.send_message(message.chat.id, "📸 Отправь фото (обложку) или «Пропустить»:", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_photo)

def step_photo(message):
    if check_cancel(message): return
    uid = message.from_user.id
    d = temp_data.get(uid)
    if not d:
        bot.send_message(message.chat.id, "⚠️ Сессия устарела", reply_markup=main_menu_kb())
        return
    photo_id = None
    if message.content_type == "photo" and message.photo:
        photo_id = message.photo[-1].file_id
    elif message.content_type == "text" and message.text.strip().lower() in ("пропустить", "нет", "skip", "-", "без фото"):
        photo_id = None
    else:
        msg = bot.send_message(message.chat.id, "⚠️ Пришли фото или «Пропустить»:", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_photo)
        return
    new_id = db_insert_returning_id(
        """INSERT INTO channels
           (title, dni, threshold, rt_post, vp_post, subscribers, extra, photo_id, status, submitted_by, created_at, pinned, category, time_range, schedule, submitted_by_username)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s,%s,%s) RETURNING id""",
        (d["title"], d["dni"], d["threshold"], d["rt_post"], d["vp_post"],
         d["subscribers"], d["extra"], photo_id, "approved", uid, datetime.now().isoformat(),
         d.get("category", "чат"), d.get("time_range", ""), d.get("schedule", ""), sender_display(message.from_user)),
    )
    logger.info("[ADD CHANNEL] Пользователь %s добавил канал id=%s, title='%s'", uid, new_id, d["title"])
    bot.send_message(message.chat.id, "✅ Канал добавлен и уже доступен в поиске ВП!", reply_markup=main_menu_kb())
    caption = "🆕 Новый канал добавлен!\n\n" + build_caption(
        d["title"], d["dni"], d["threshold"], d["subscribers"], d["extra"],
        d.get("category", "чат"), d.get("time_range", ""), d.get("schedule", ""), new_id
    )
    caption += f"\n\n👤 От: {esc(sender_display(message.from_user))} (id {uid})"
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("📤 Вп пост", url=d["vp_post"]),
        types.InlineKeyboardButton("🔁 Ртпост", url=d["rt_post"]),
    )
    kb.row(
        types.InlineKeyboardButton("📌 Закрепить", callback_data=f"pincb_{new_id}"),
        types.InlineKeyboardButton("🗑 Удалить", callback_data=f"delcb_{new_id}"),
    )
    for admin_id in ALL_ADMIN_IDS:
        try:
            if photo_id:
                bot.send_photo(admin_id, photo_id, caption=caption, reply_markup=kb)
            else:
                bot.send_message(admin_id, caption, reply_markup=kb)
        except Exception:
            pass
    temp_data.pop(uid, None)

# ========================= ДОБАВЛЕНИЕ ПО ШАБЛОНУ =========================

def start_bulk_add(chat_id):
    text = (
        "📥 Отправь каналы по шаблону — каждый с новой строки.\n"
        "Поля через « | »:\n\n"
        f"<code>{BULK_TEMPLATE}</code>\n\n"
        "Пример:\n"
        "<code>Мой канал | DNI123 | 100 | https://t.me/rt/1 | https://t.me/vp/1 | 500 | контент | кф | 1к-2к | пн/ср/пт</code>\n\n"
        "⚠️ Фото так добавить нельзя — добавь потом через «👤 Мои ВП»."
    )
    msg = bot.send_message(chat_id, text, reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_bulk_add)

def parse_bulk_line(line):
    parts = [p.strip() for p in line.split("|")]
    if len(parts) < 7:
        raise ValueError(f"нужно минимум 7 полей, сейчас {len(parts)}")
    title = parts[0]
    dni = parts[1] if len(parts) > 1 else ""
    thr_raw = parts[2] if len(parts) > 2 else "0"
    rt_raw = parts[3] if len(parts) > 3 else ""
    vp_raw = parts[4] if len(parts) > 4 else ""
    subs_raw = parts[5] if len(parts) > 5 else "0"
    extra = parts[6] if len(parts) > 6 else ""
    category = parts[7] if len(parts) > 7 else "чат"
    time_range = parts[8] if len(parts) > 8 else ""
    schedule = parts[9] if len(parts) > 9 else ""
    if not title:
        raise ValueError("не указано название")
    thr = parse_threshold_input(thr_raw)
    if thr is None:
        raise ValueError("порог должен быть числом")
    if not subs_raw.isdigit():
        raise ValueError("подписчики должны быть числом")
    rt_url = normalize_url(rt_raw) if rt_raw else ""
    vp_url = normalize_url(vp_raw) if vp_raw else ""
    if category not in CATEGORIES:
        category = "чат"
    return {
        "title": title, "dni": dni, "threshold": thr,
        "rt_post": rt_url, "vp_post": vp_url, "subscribers": int(subs_raw),
        "extra": extra, "category": category, "time_range": time_range, "schedule": schedule,
    }

def step_bulk_add(message):
    if check_cancel(message): return
    uid = message.from_user.id
    if not message.text:
        msg = bot.send_message(message.chat.id, "⚠️ Пришли текст по шаблону:", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_bulk_add)
        return
    lines = [l for l in message.text.splitlines() if l.strip()]
    added = []
    errors = []
    for i, line in enumerate(lines, start=1):
        try:
            d = parse_bulk_line(line)
        except ValueError as e:
            errors.append(f"Строка {i}: {e}")
            continue
        new_id = db_insert_returning_id(
            """INSERT INTO channels
               (title, dni, threshold, rt_post, vp_post, subscribers, extra, photo_id, status, submitted_by, created_at, pinned, category, time_range, schedule, submitted_by_username)
               VALUES (%s,%s,%s,%s,%s,%s,%s,NULL,%s,%s,%s,0,%s,%s,%s,%s) RETURNING id""",
            (d["title"], d["dni"], d["threshold"], d["rt_post"], d["vp_post"],
             d["subscribers"], d["extra"], "approved", uid, datetime.now().isoformat(),
             d["category"], d["time_range"], d["schedule"], sender_display(message.from_user)),
        )
        added.append(new_id)
        logger.info("[BULK ADD] Пользователь %s добавил канал id=%s через шаблон", uid, new_id)
    report = f"✅ Добавлено каналов: {len(added)}\n"
    if errors:
        report += "\n⚠️ Не удалось добавить:\n" + "\n".join(errors)
    logger.info("[BULK ADD] Итог: добавлено=%s, ошибок=%s", len(added), len(errors))
    bot.send_message(message.chat.id, report, reply_markup=main_menu_kb())
    for cid in added:
        row = get_channel(cid)
        if not row: continue
        caption = "🆕 Новый канал (по шаблону)!\n\n" + build_caption(
            row[1], row[2], row[3], row[6], row[7], row[13], row[14], row[15], cid
        )
        caption += f"\n\n👤 От: {esc(sender_display(message.from_user))} (id {uid})"
        kb = types.InlineKeyboardMarkup()
        kb.row(
            types.InlineKeyboardButton("📤 Вп пост", url=row[5]),
            types.InlineKeyboardButton("🔁 Ртпост", url=row[4]),
        )
        kb.row(
            types.InlineKeyboardButton("📌 Закрепить", callback_data=f"pincb_{cid}"),
            types.InlineKeyboardButton("🗑 Удалить", callback_data=f"delcb_{cid}"),
        )
        for admin_id in ALL_ADMIN_IDS:
            try:
                bot.send_message(admin_id, caption, reply_markup=kb)
            except Exception:
                pass

# ========================= АНКЕТА ВПШЕРА =========================

@bot.message_handler(func=lambda m: m.text == "🙋 Анкета впшера")
def h_vpsher_start(message):
    track_user(message.from_user)
    uid = message.from_user.id
    vpsher_temp[uid] = {}
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("💰 Платно", callback_data="vptype_платно"),
        types.InlineKeyboardButton("🆓 Бесплатно", callback_data="vptype_бесплатно"),
    )
    bot.send_message(message.chat.id, "🙋 Заполним анкету впшера.\n\nТы делаешь ВП платно или бесплатно?", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("vptype_"))
def cb_vpsher_type(call):
    uid = call.from_user.id
    if uid not in vpsher_temp:
        bot.answer_callback_query(call.id, "Сессия устарела", show_alert=True)
        return
    paid_type = call.data.split("_", 1)[1]
    vpsher_temp[uid]["paid_type"] = paid_type
    bot.answer_callback_query(call.id, f"Выбрано: {paid_type}")
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    except Exception:
        pass
    msg = bot.send_message(call.message.chat.id, "📅 Сколько ВП ты делаешь в день/неделю?", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_vpsher_frequency)

def step_vpsher_frequency(message):
    if check_cancel(message): return
    uid = message.from_user.id
    vpsher_temp[uid]["frequency"] = message.text
    msg = bot.send_message(message.chat.id, "🗓 В какие дни ты работаешь?", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_vpsher_days)

def step_vpsher_days(message):
    if check_cancel(message): return
    uid = message.from_user.id
    vpsher_temp[uid]["work_days"] = message.text
    msg = bot.send_message(message.chat.id, "🔗 Скинь списки/каналы, с которыми работаешь:", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_vpsher_links)

def step_vpsher_links(message):
    if check_cancel(message): return
    uid = message.from_user.id
    vpsher_temp[uid]["channel_links"] = message.text
    msg = bot.send_message(message.chat.id, "ℹ️ Расскажи подробнее о себе:", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_vpsher_details)

def step_vpsher_details(message):
    if check_cancel(message): return
    uid = message.from_user.id
    vpsher_temp[uid]["details"] = message.text
    if vpsher_temp[uid].get("paid_type") == "платно":
        msg = bot.send_message(message.chat.id, "💵 Сколько нужно платить в неделю? Укажи сумму:", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_vpsher_price)
    else:
        msg = bot.send_message(message.chat.id, "📞 Укажи свой юз для связи (например @username):", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_vpsher_contact)

def step_vpsher_price(message):
    if check_cancel(message): return
    uid = message.from_user.id
    vpsher_temp[uid]["price_info"] = message.text
    msg = bot.send_message(message.chat.id, "📞 Укажи свой юз для связи (например @username):", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_vpsher_contact)

def step_vpsher_contact(message):
    if check_cancel(message): return
    uid = message.from_user.id
    contact = message.text.strip() if message.text else ""
    if not contact:
        msg = bot.send_message(message.chat.id, "⚠️ Юз обязателен. Укажи его:", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_vpsher_contact)
        return
    d = vpsher_temp.get(uid)
    if not d:
        bot.send_message(message.chat.id, "⚠️ Сессия устарела", reply_markup=main_menu_kb())
        return
    d["contact"] = contact
    new_id = db_insert_returning_id(
        """INSERT INTO vpshers
           (submitted_by, submitted_by_username, paid_type, frequency, work_days, channel_links, details, price_info, contact, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
        (uid, sender_display(message.from_user), d.get("paid_type", ""), d.get("frequency", ""),
         d.get("work_days", ""), d.get("channel_links", ""), d.get("details", ""),
         d.get("price_info", ""), contact, datetime.now().isoformat()),
    )
    logger.info("[VPSHER] Пользователь %s создал анкету впшера id=%s", uid, new_id)
    bot.send_message(message.chat.id, "✅ Анкета отправлена! С тобой свяжутся.", reply_markup=main_menu_kb())

    admin_text = (
        f"🙋 <b>Новая анкета впшера #{new_id}</b>\n\n"
        f"💰 Тип: {esc(d.get('paid_type', '—'))}\n"
        f"📅 Частота: {esc(d.get('frequency', '—'))}\n"
        f"🗓 Дни работы: {esc(d.get('work_days', '—'))}\n"
        f"🔗 Списки/каналы: {esc(d.get('channel_links', '—'))}\n"
        f"ℹ️ Подробности: {esc(d.get('details', '—'))}\n"
    )
    if d.get("paid_type") == "платно":
        admin_text += f"💵 Цена в неделю: {esc(d.get('price_info', '—'))}\n"
    admin_text += f"\n📞 Контакт: {esc(contact)}\n👤 Отправитель: {esc(sender_display(message.from_user))} (id {uid})"

    for admin_id in ALL_ADMIN_IDS:
        try:
            kb_admin = types.InlineKeyboardMarkup()
            kb_admin.row(types.InlineKeyboardButton("🗑 Удалить анкету", callback_data=f"vpsherdel_{new_id}"))
            bot.send_message(admin_id, admin_text, reply_markup=kb_admin)
        except Exception:
            pass
    vpsher_temp.pop(uid, None)

@bot.message_handler(commands=["vpshers"])
def admin_vpshers(message):
    if message.from_user.id not in ALL_ADMIN_IDS:
        return
    track_user(message.from_user)
    rows = db_query(
        "SELECT id, paid_type, contact, submitted_by_username, created_at FROM vpshers ORDER BY id DESC LIMIT 30"
    )
    if not rows:
        bot.send_message(message.chat.id, "Пока нет ни одной анкеты впшера.")
        return
    for r in rows:
        vp_id, paid_type, contact, username, created_at = r
        text = f"#{vp_id} | {esc(paid_type)} | {esc(contact)} | от {esc(username)} | {created_at}"
        kb = types.InlineKeyboardMarkup()
        kb.row(types.InlineKeyboardButton("🗑 Удалить анкету", callback_data=f"vpsherdel_{vp_id}"))
        bot.send_message(message.chat.id, text, reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("vpsherdel_"))
def cb_vpsher_delete(call):
    if call.from_user.id not in ALL_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Нет доступа", show_alert=True)
        return
    vp_id = int(call.data.split("_")[1])
    row = get_vpsher(vp_id)
    if not row:
        bot.answer_callback_query(call.id, "Анкета не найдена", show_alert=True)
        return
    vpsher_data = archive_vpsher(row, call.from_user.id)
    log_admin_action(call.from_user.id, "delete_vpsher", f"анкета впшера #{vp_id}")
    bot.answer_callback_query(call.id, "🗑 Анкета удалена")
    try:
        bot.edit_message_text(f"🗑 Анкета впшера #{vp_id} удалена", call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    notify_super_admin_deletion(vp_id, vpsher_data.get("contact"), "vpsher", f"удалено администратором {call.from_user.id}")

@bot.message_handler(func=lambda m: m.text == "🕵️ Найти впшера")
def h_find_vpsher(message):
    track_user(message.from_user)
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("💰 Платные", callback_data="vpfind_платно"),
        types.InlineKeyboardButton("🆓 Бесплатные", callback_data="vpfind_бесплатно"),
    )
    kb.row(types.InlineKeyboardButton("🔎 Все", callback_data="vpfind_все"))
    bot.send_message(message.chat.id, "🕵️ Каких впшеров показать?", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("vpfind_"))
def cb_find_vpsher(call):
    uid = call.from_user.id
    choice = call.data.split("_", 1)[1]
    logger.info("[FIND VPSHER] Пользователь %s ищет: '%s'", uid, choice)
    if choice == "все":
        rows = db_query("SELECT id FROM vpshers ORDER BY RANDOM()")
    else:
        rows = db_query("SELECT id FROM vpshers WHERE paid_type=%s ORDER BY RANDOM()", (choice,))
    if not rows:
        bot.answer_callback_query(call.id, "😔 Пока нет анкет с таким фильтром", show_alert=True)
        return
    view_states[uid] = {
        "ids": [r[0] for r in rows], "index": 0, "message_id": None, "mode": "vpsher"
    }
    bot.answer_callback_query(call.id)
    render_vpsher_card(call.message.chat.id, uid)

# ========================= МОИ ВП =========================

def _render_my_channels(chat_id, user_id, page=0, message_id=None):
    channels = db_query(
        "SELECT id, title, subscribers, threshold, pinned, category FROM channels WHERE submitted_by=%s ORDER BY id",
        (user_id,),
    )
    vpshers = db_query(
        "SELECT id, paid_type, contact, created_at FROM vpshers WHERE submitted_by=%s ORDER BY id",
        (user_id,),
    )

    total_channels = len(channels)
    total_pages = max(1, (total_channels + MY_VP_PAGE_SIZE - 1) // MY_VP_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * MY_VP_PAGE_SIZE
    page_channels = channels[start:start + MY_VP_PAGE_SIZE]

    text = f"👤 <b>Твои записи</b>"
    if total_pages > 1:
        text += f" (страница {page + 1}/{total_pages})"
    text += ":\n\n"

    kb = types.InlineKeyboardMarkup()

    linked = get_linked_channels(user_id)
    if linked:
        text += "🔗 <b>Привязанные Telegram-каналы:</b>\n"
        unlinked_tgks = []
        for idx, link in enumerate(linked, 1):
            vp_channel = db_query("SELECT title FROM channels WHERE linked_chat_id=%s LIMIT 1", (link[3],))
            vp_name = vp_channel[0][0] if vp_channel else None
            if vp_name:
                text += f"{idx}. {esc(link[2] or 'неизвестно')} | 👥 {link[5] or '—'} | 📢 {esc(vp_name)}\n"
            else:
                text += f"{idx}. {esc(link[2] or 'неизвестно')} | 👥 {link[5] or '—'} | ⚠️ <b>не связан с ВП</b>\n"
                unlinked_tgks.append(link)
            kb.row(types.InlineKeyboardButton(f"❌ Отвязать {esc(link[2] or 'канал')}", callback_data=f"unlink_channel_{link[3]}"))
        text += "\n"

        if unlinked_tgks:
            kb.row(types.InlineKeyboardButton("⚠️ Привязать ТГК к каналам ВП", callback_data="migrate_links"))

        kb.row(types.InlineKeyboardButton("➕ Привязать ещё канал", callback_data="link_channel_start"))
    else:
        text += "⚠️ <b>Telegram-каналы не привязаны!</b>\n"
        text += "Привяжи каналы, чтобы подписчики обновлялись автоматически.\n\n"
        kb.row(types.InlineKeyboardButton("🔗 Привязать Telegram-канал", callback_data="link_channel_start"))

    if page_channels:
        text += "<b>📢 Каналы в базе:</b>\n"
        for idx, (cid, title, subs, thr, pinned, category) in enumerate(page_channels, start=start + 1):
            prefix = "📌 " if pinned else ""
            cat_text = f" | 📂 {CATEGORIES.get(category, category)}" if category else ""
            text += f"{idx}. {prefix}<b>{esc(title)}</b>{cat_text}\n   👥 {subs} подписчиков | 📊 порог {thr}+\n\n"
            kb.row(types.InlineKeyboardButton(f"✏️ Канал #{idx}", callback_data=f"editmenu_{cid}"))

    if vpshers:
        text += "\n<b>🙋 Анкеты впшера:</b>\n"
        for idx, (vp_id, paid_type, contact, created_at) in enumerate(vpshers, 1):
            text += f"{idx}. #{vp_id} | {esc(paid_type)} | {esc(contact)} | {created_at}\n\n"
            kb.row(types.InlineKeyboardButton(f"✏️ Анкета #{idx}", callback_data=f"editvpsher_{vp_id}"))

    nav_row = []
    if page > 0:
        nav_row.append(types.InlineKeyboardButton("⬅️", callback_data=f"myvp_page_{page - 1}"))
    if total_pages > 1:
        nav_row.append(types.InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav_row.append(types.InlineKeyboardButton("➡️", callback_data=f"myvp_page_{page + 1}"))
    if nav_row:
        kb.row(*nav_row)

    kb.row(types.InlineKeyboardButton("🗑 Удалить канал", callback_data="mydel_menu"))
    kb.row(types.InlineKeyboardButton("🗑 Удалить анкету впшера", callback_data="myvpsherdel_menu"))

    if message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=kb, parse_mode="HTML")
            return True
        except Exception:
            pass
    bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")
    bot.send_message(chat_id, "🔙 Вернуться в меню можно кнопками ниже", reply_markup=main_menu_kb())
    return True

@bot.message_handler(func=lambda m: m.text == "👤 Мои ВП")
def h_my_channels(message):
    track_user(message.from_user)
    _render_my_channels(message.chat.id, message.from_user.id, page=0)

# ИСПРАВЛЕНИЕ #3: Добавлен декоратор @bot.callback_query_handler
@bot.callback_query_handler(func=lambda c: c.data.startswith("myvp_page_"))
def cb_myvp_page(call):
    page = int(call.data.split("_")[2])
    _render_my_channels(call.message.chat.id, call.from_user.id, page=page, message_id=call.message.message_id)
    bot.answer_callback_query(call.id)

# ========================= ПРИВЯЗКА КАНАЛА =========================

@bot.callback_query_handler(func=lambda c: c.data == "link_channel_start")
def cb_link_channel_start(call):
    uid = call.from_user.id
    link_channel_states[uid] = {"step": "waiting_forward", "edit_message_id": call.message.message_id}
    bot_username = bot.get_me().username or "bot"
    instruction = (
        f"🔗 <b>Привязка Telegram-канала</b>\n\n"
        f"Чтобы привязать свой канал, выполни 3 простых шага:\n\n"
        f"1️⃣ Добавь этого бота (@{bot_username}) в администраторы своего канала.\n"
        f"   → Перейди в настройки канала → Администраторы → Добавить администратора.\n\n"
        f"2️⃣ Дай боту права на публикацию сообщений (или хотя бы чтение).\n\n"
        f"3️⃣ Перешли <b>любой пост</b> из своего канала сюда в личные сообщения боту.\n"
        f"   Это нужно, чтобы бот определил ID твоего канала.\n\n"
        f"⚠️ <b>Важно:</b> после привязки количество подписчиков будет обновляться\n"
        f"автоматически с задержкой примерно 5 минут.\n\n"
        f"🔙 Если передумал — нажми «Отмена»"
    )
    bot.edit_message_text(
        instruction,
        call.message.chat.id, call.message.message_id, parse_mode="HTML"
    )
    msg = bot.send_message(call.message.chat.id, "Жду пересланный пост из твоего канала...", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_link_channel_forward)
    bot.answer_callback_query(call.id)

def step_link_channel_forward(message):
    if check_cancel(message):
        link_channel_states.pop(message.from_user.id, None)
        return
    uid = message.from_user.id
    state = link_channel_states.get(uid)
    if not state or state.get("step") != "waiting_forward":
        bot.send_message(message.chat.id, "⚠️ Сессия привязки устарела. Начни заново через «👤 Мои ВП».", reply_markup=main_menu_kb())
        return

    if not message.forward_from_chat:
        msg = bot.send_message(
            message.chat.id,
            "⚠️ Это не похоже на пересланный пост из канала.\n"
            "Перешли любой пост из своего канала сюда.\n"
            "Убедись, что бот добавлен администратором в твой канал.",
            reply_markup=cancel_kb()
        )
        bot.register_next_step_handler(msg, step_link_channel_forward)
        return

    chat = message.forward_from_chat
    if chat.type not in ("channel", "supergroup"):
        msg = bot.send_message(message.chat.id, "⚠️ Нужен именно канал. Попробуй ещё раз:", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_link_channel_forward)
        return

    try:
        bot_member = bot.get_chat_member(chat.id, bot.get_me().id)
        if bot_member.status not in ("administrator", "creator"):
            msg = bot.send_message(
                message.chat.id,
                "❌ Бот не является администратором в этом канале.\n"
                "Добавь бота в администраторы канала и попробуй снова.",
                reply_markup=cancel_kb()
            )
            bot.register_next_step_handler(msg, step_link_channel_forward)
            return
    except Exception as e:
        msg = bot.send_message(
            message.chat.id,
            f"❌ Не удалось проверить права бота в канале.\n"
            f"Ошибка: {esc(str(e))}\n"
            f"Убедись, что бот добавлен администратором и попробуй снова.",
            reply_markup=cancel_kb()
        )
        bot.register_next_step_handler(msg, step_link_channel_forward)
        return

    try:
        user_member = bot.get_chat_member(chat.id, uid)
        if user_member.status not in ("administrator", "creator"):
            msg = bot.send_message(
                message.chat.id,
                "❌ Ты не являешься администратором этого канала.\n"
                "Привязать можно только свой канал.",
                reply_markup=cancel_kb()
            )
            bot.register_next_step_handler(msg, step_link_channel_forward)
            return
    except Exception:
        msg = bot.send_message(message.chat.id, "❌ Не удалось проверить твои права в канале. Попробуй снова:", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_link_channel_forward)
        return

    channel_username = chat.username
    if not channel_username and chat.invite_link:
        channel_username = chat.invite_link
    elif not channel_username:
        channel_username = str(chat.id)

    link_channel(uid, channel_username, chat.id)
    link_channel_states.pop(uid, None)

    user_channels = db_query(
        "SELECT id, title, subscribers FROM channels WHERE submitted_by=%s ORDER BY id",
        (uid,)
    )

    try:
        count = bot.get_chat_member_count(chat.id)
    except Exception:
        count = "не удалось получить"

    if not user_channels:
        bot.send_message(
            message.chat.id,
            f"✅ <b>Канал успешно привязан!</b>\n\n"
            f"📡 Канал: {esc(channel_username)}\n"
            f"👥 Текущих подписчиков: {count}\n\n"
            f"⚠️ У тебя пока нет каналов в базе ВП.\n"
            f"Добавь канал через «📢 Предложить ВП»,\n"
            f"чтобы подписчики обновлялись автоматически.",
            reply_markup=main_menu_kb(),
            parse_mode="HTML"
        )
        _render_my_channels(message.chat.id, uid, page=0)
        return

    admin_text = (
        f"🔗 <b>Новый ТГК привязан</b>\n"
        f"👤 Пользователь: {esc(sender_display(message.from_user))} (id {uid})\n"
        f"📡 Канал: {esc(channel_username)}\n"
        f"👥 Подписчиков: {count}\n"
        f"⏳ Ожидает выбора канала ВП..."
    )
    for admin_id in ALL_ADMIN_IDS:
        try:
            bot.send_message(admin_id, admin_text, parse_mode="HTML")
        except Exception:
            pass
    log_admin_action(uid, "link_channel", f"привязан ТГК {channel_username} (chat_id: {chat.id}), ожидает выбор ВП")

    kb = types.InlineKeyboardMarkup()
    for cid, title, subs in user_channels:
        kb.row(types.InlineKeyboardButton(
            f"📢 {esc(title)} | 👥 {subs}",
            callback_data=f"linktochannel_{chat.id}_{cid}"
        ))
    kb.row(types.InlineKeyboardButton("❌ Ни к какому", callback_data=f"linktochannel_{chat.id}_0"))

    bot.send_message(
        message.chat.id,
        f"✅ <b>Канал {esc(channel_username)} привязан!</b>\n"
        f"👥 Подписчиков: {count}\n\n"
        f"📌 <b>Выбери, к какому каналу из твоих ВП</b>\n"
        f"привязать этот Telegram-канал для обновления подписчиков:",
        reply_markup=kb,
        parse_mode="HTML"
    )

# ========================= РЕДАКТИРОВАНИЕ КАНАЛОВ =========================

@bot.callback_query_handler(func=lambda c: c.data.startswith("linktochannel_"))
def cb_link_to_channel(call):
    uid = call.from_user.id
    parts = call.data.split("_")
    chat_id = int(parts[1])
    channel_id = int(parts[2])

    if channel_id == 0:
        bot.answer_callback_query(call.id, "✅ ТГК привязан без канала ВП")
        try:
            bot.edit_message_text(
                "✅ Telegram-канал привязан.\nПодписчики не будут обновляться для каналов ВП.",
                call.message.chat.id, call.message.message_id
            )
        except Exception:
            pass
        _render_my_channels(call.message.chat.id, uid, page=0)
        return

    row = get_channel(channel_id)
    if not row or row[10] != uid:
        bot.answer_callback_query(call.id, "⛔ Это не твой канал", show_alert=True)
        return

    db_execute("UPDATE channels SET linked_chat_id=%s WHERE id=%s", (chat_id, channel_id))

    try:
        count = bot.get_chat_member_count(chat_id)
        db_execute("UPDATE channels SET subscribers=%s WHERE id=%s", (count, channel_id))
    except Exception:
        count = "не удалось получить"

    bot.answer_callback_query(call.id, f"✅ Привязано к каналу «{row[1]}»")
    try:
        bot.edit_message_text(
            f"✅ <b>Telegram-канал привязан к ВП!</b>\n\n"
            f"📢 Канал ВП: {esc(row[1])}\n"
            f"👥 Подписчиков обновлено: {count}\n\n"
            f"🔄 Теперь подписчики будут обновляться\n"
            f"автоматически каждые 5 минут.",
            call.message.chat.id, call.message.message_id,
            parse_mode="HTML"
        )
    except Exception:
        pass

    linked_rows = get_linked_channels(uid)
    channel_username = "неизвестно"
    for link in linked_rows:
        if link[3] == chat_id:
            channel_username = link[2] or "неизвестно"
            break

    admin_text = (
        f"🔗 <b>ТГК связан с каналом ВП</b>\n"
        f"👤 Пользователь: {esc(sender_display(call.from_user))} (id {uid})\n"
        f"📡 ТГК: {esc(channel_username)}\n"
        f"📢 Канал ВП: {esc(row[1])} (ID: {channel_id})\n"
        f"👥 Подписчиков: {count}"
    )
    for admin_id in ALL_ADMIN_IDS:
        try:
            bot.send_message(admin_id, admin_text, parse_mode="HTML")
        except Exception:
            pass
    log_admin_action(uid, "link_channel_to_vp", f"ТГК {channel_username} привязан к каналу ВП {row[1]} (ID: {channel_id})")

    _render_my_channels(call.message.chat.id, uid, page=0)

@bot.callback_query_handler(func=lambda c: c.data == "unlink_channel")
def cb_unlink_channel_all(call):
    uid = call.from_user.id
    linked = get_linked_channels(uid)
    if not linked:
        bot.answer_callback_query(call.id, "⚠️ Каналы не привязаны", show_alert=True)
        return
    for link in linked:
        channel_username = link[2] or "неизвестно"
        db_execute("UPDATE channels SET linked_chat_id=NULL WHERE linked_chat_id=%s", (link[3],))
        unlink_channel(uid, link[3])
        admin_text = f"🔗 Канал отвязан\n👤 Пользователь: {esc(sender_display(call.from_user))} (id {uid})\n📡 Канал: {esc(channel_username)}"
        for admin_id in ALL_ADMIN_IDS:
            try:
                bot.send_message(admin_id, admin_text)
            except Exception:
                pass
        log_admin_action(uid, "unlink_channel", f"отвязан канал {channel_username} (chat_id: {link[3]})")
    bot.answer_callback_query(call.id, "✅ Все каналы отвязаны")
    _render_my_channels(call.message.chat.id, uid, page=0)

@bot.callback_query_handler(func=lambda c: c.data.startswith("unlink_channel_"))
def cb_unlink_channel_single(call):
    uid = call.from_user.id
    channel_chat_id = int(call.data.split("_")[2])
    link = get_linked_channel(uid, channel_chat_id)
    if not link:
        bot.answer_callback_query(call.id, "⚠️ Канал не найден", show_alert=True)
        return
    channel_username = link[2] or "неизвестно"
    db_execute("UPDATE channels SET linked_chat_id=NULL WHERE linked_chat_id=%s", (channel_chat_id,))
    unlink_channel(uid, channel_chat_id)
    admin_text = f"🔗 Канал отвязан\n👤 Пользователь: {esc(sender_display(call.from_user))} (id {uid})\n📡 Канал: {esc(channel_username)}"
    for admin_id in ALL_ADMIN_IDS:
        try:
            bot.send_message(admin_id, admin_text)
        except Exception:
            pass
    log_admin_action(uid, "unlink_channel", f"отвязан канал {channel_username} (chat_id: {channel_chat_id})")
    bot.answer_callback_query(call.id, f"✅ Канал {channel_username} отвязан")
    _render_my_channels(call.message.chat.id, uid, page=0)

@bot.callback_query_handler(func=lambda c: c.data.startswith("editmenu_"))
def cb_edit_menu(call):
    uid = call.from_user.id
    cid = int(call.data.split("_")[1])
    row = get_channel(cid)
    if not row or row[10] != uid:
        bot.answer_callback_query(call.id, "⛔ Это не твой канал", show_alert=True)
        return
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("📝 Название", callback_data=f"editfield_{cid}_title"),
        types.InlineKeyboardButton("🆔 DNI", callback_data=f"editfield_{cid}_dni"),
    )
    kb.row(
        types.InlineKeyboardButton("📊 Порог", callback_data=f"editfield_{cid}_threshold"),
        types.InlineKeyboardButton("👥 Подписчики", callback_data=f"editfield_{cid}_subscribers"),
    )
    kb.row(
        types.InlineKeyboardButton("🔗 Ртпост", callback_data=f"editfield_{cid}_rt_post"),
        types.InlineKeyboardButton("🔗 Вппост", callback_data=f"editfield_{cid}_vp_post"),
    )
    kb.row(
        types.InlineKeyboardButton("ℹ️ Доп. инфо", callback_data=f"editfield_{cid}_extra"),
        types.InlineKeyboardButton("📂 Категория", callback_data=f"editfield_{cid}_category"),
    )
    kb.row(
        types.InlineKeyboardButton("⏱ Промежуток", callback_data=f"editfield_{cid}_time_range"),
        types.InlineKeyboardButton("📅 Расписание", callback_data=f"editfield_{cid}_schedule"),
    )
    kb.row(types.InlineKeyboardButton("📸 Фото", callback_data=f"editfield_{cid}_photo"))
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data="back_my_channels"))
    bot.edit_message_text(
        f"✏️ Редактирование «{esc(row[1])}»\nВыбери, что хочешь изменить:",
        call.message.chat.id, call.message.message_id, reply_markup=kb
    )
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data == "back_my_channels")
def cb_back_my_channels(call):
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    _render_my_channels(call.message.chat.id, call.from_user.id, page=0)

@bot.callback_query_handler(func=lambda c: c.data.startswith("editfield_"))
def cb_edit_field(call):
    uid = call.from_user.id
    parts = call.data.split("_")
    cid = int(parts[1])
    field = "_".join(parts[2:])
    row = get_channel(cid)
    if not row or row[10] != uid:
        bot.answer_callback_query(call.id, "⛔ Это не твой канал", show_alert=True)
        return
    edit_field_states[uid] = {"channel_id": cid, "field": field}
    bot.answer_callback_query(call.id)
    if field == "photo":
        msg = bot.send_message(call.message.chat.id, f"📸 Пришли новое фото для канала «{esc(row[1])}»:", reply_markup=edit_kb())
        bot.register_next_step_handler(msg, step_edit_single_field)
    elif field == "category":
        kb = types.InlineKeyboardMarkup()
        for cat_key, cat_name in CATEGORIES.items():
            kb.row(types.InlineKeyboardButton(cat_name, callback_data=f"setcat_{cid}_{cat_key}"))
        bot.send_message(call.message.chat.id, f"📂 Выбери категорию для «{esc(row[1])}»:", reply_markup=kb)
        edit_field_states.pop(uid, None)
    else:
        current_value = ""
        if field == "title": current_value = row[1]
        elif field == "dni": current_value = row[2]
        elif field == "threshold": current_value = row[3]
        elif field == "rt_post": current_value = row[4]
        elif field == "vp_post": current_value = row[5]
        elif field == "subscribers": current_value = row[6]
        elif field == "extra": current_value = row[7]
        elif field == "time_range": current_value = row[14] if len(row) > 14 else ""
        elif field == "schedule": current_value = row[15] if len(row) > 15 else ""
        field_name = FIELD_NAMES_RU.get(field, field)
        msg = bot.send_message(
            call.message.chat.id,
            f"✏️ Введи новое значение для поля «{field_name}»\n"
            f"Текущее: {esc(current_value)}\n\n"
            f"Или /skip чтобы оставить как есть:",
            reply_markup=edit_kb()
        )
        bot.register_next_step_handler(msg, step_edit_single_field)

@bot.callback_query_handler(func=lambda c: c.data.startswith("setcat_"))
def cb_set_category(call):
    uid = call.from_user.id
    parts = call.data.split("_")
    cid = int(parts[1])
    category = parts[2]
    row = get_channel(cid)
    if not row or row[10] != uid:
        bot.answer_callback_query(call.id, "⛔ Это не твой канал", show_alert=True)
        return
    db_execute("UPDATE channels SET category=%s WHERE id=%s", (category, cid))
    bot.answer_callback_query(call.id, f"✅ Категория изменена на «{CATEGORIES.get(category, category)}»")
    try:
        bot.edit_message_text(
            f"✅ Категория для «{esc(row[1])}» изменена на «{CATEGORIES.get(category, category)}»",
            call.message.chat.id, call.message.message_id
        )
    except Exception:
        pass

def step_edit_single_field(message):
    if check_cancel(message):
        return
    uid = message.from_user.id
    state = edit_field_states.get(uid)
    if not state:
        bot.send_message(message.chat.id, "⚠️ Сессия редактирования устарела", reply_markup=main_menu_kb())
        return
    cid = state["channel_id"]
    field = state["field"]
    row = get_channel(cid)
    field_name = FIELD_NAMES_RU.get(field, field)
    if message.text and message.text.strip() == "/skip":
        bot.send_message(message.chat.id, f"✅ Поле «{field_name}» оставлено без изменений", reply_markup=main_menu_kb())
        edit_field_states.pop(uid, None)
        return
    try:
        if field == "threshold":
            value = parse_threshold_input(message.text)
            if value is None:
                raise ValueError("Нужно число")
        elif field == "subscribers":
            value = int(message.text.replace(" ", ""))
        elif field in ("rt_post", "vp_post"):
            value = normalize_url(message.text)
            if not value:
                raise ValueError("Некорректная ссылка")
        elif field == "photo":
            if message.content_type == "photo" and message.photo:
                value = message.photo[-1].file_id
            else:
                raise ValueError("Пришли именно фото")
        else:
            value = message.text
        db_field = field
        if field == "time_range": db_field = "time_range"
        elif field == "schedule": db_field = "schedule"
        elif field == "photo": db_field = "photo_id"
        db_execute(f"UPDATE channels SET {db_field}=%s WHERE id=%s", (value, cid))
        logger.info("[EDIT] Пользователь %s изменил поле '%s' канала id=%s", uid, field, cid)
        bot.send_message(message.chat.id, f"✅ Поле «{field_name}» успешно обновлено!", reply_markup=main_menu_kb())
    except Exception as e:
        msg = bot.send_message(
            message.chat.id,
            f"⚠️ Ошибка: {esc(str(e))}\nПопробуй ещё раз или /skip:",
            reply_markup=edit_kb()
        )
        bot.register_next_step_handler(msg, step_edit_single_field)
        return
    edit_field_states.pop(uid, None)

@bot.callback_query_handler(func=lambda c: c.data == "mydel_menu")
def cb_my_del_menu(call):
    uid = call.from_user.id
    rows = db_query("SELECT id, title FROM channels WHERE submitted_by=%s ORDER BY id", (uid,))
    if not rows:
        bot.answer_callback_query(call.id, "У тебя нет каналов для удаления", show_alert=True)
        return
    kb = types.InlineKeyboardMarkup()
    for cid, title in rows:
        kb.row(types.InlineKeyboardButton(f"🗑 {title}", callback_data=f"mydel_{cid}"))
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data="back_my_channels"))
    bot.edit_message_text("🗑 Выбери канал для удаления:", call.message.chat.id, call.message.message_id, reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("mydel_") and not c.data.startswith("mydelconfirm_"))
def cb_my_delete(call):
    if call.data == "mydel_cancel":
        bot.answer_callback_query(call.id, "Отменено")
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
        return
    uid = call.from_user.id
    cid = int(call.data.split("_")[1])
    row = get_channel(cid)
    if not row or row[10] != uid:
        bot.answer_callback_query(call.id, "⛔ Это не твой канал", show_alert=True)
        return
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("✅ Да, удалить", callback_data=f"mydelconfirm_{cid}"),
        types.InlineKeyboardButton("❌ Отмена", callback_data="mydel_cancel"),
    )
    try:
        bot.edit_message_text(f"⚠️ Ты уверен, что хочешь удалить канал «{esc(row[1])}»?", call.message.chat.id, call.message.message_id, reply_markup=kb)
    except Exception:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=kb)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("mydelconfirm_"))
def cb_my_delete_confirm(call):
    uid = call.from_user.id
    cid = int(call.data.split("_")[1])
    row = get_channel(cid)
    if not row or row[10] != uid:
        bot.answer_callback_query(call.id, "⛔ Это не твой канал", show_alert=True)
        return
    archive_channel(row, uid)
    logger.info("[DELETE] Пользователь %s удалил свой канал id=%s", uid, cid)
    for admin_id in ALL_ADMIN_IDS:
        try:
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("🔄 Вернуть", callback_data=f"restore_{cid}"),
                types.InlineKeyboardButton("🗑 Удалить навсегда", callback_data=f"permadel_{cid}"),
            )
            bot.send_message(
                admin_id,
                f"⚠️ Пользователь {uid} удалил канал #{cid} «{esc(row[1])}»\n"
                f"Канал сохранен в корзине на 24 часа.",
                reply_markup=kb
            )
        except Exception:
            pass
    bot.answer_callback_query(call.id, "🗑 Удалено")
    try:
        bot.edit_message_text(f"🗑 Канал «{esc(row[1])}» удалён", call.message.chat.id, call.message.message_id)
    except Exception:
        pass

# ========================= РЕДАКТИРОВАНИЕ АНКЕТЫ ВПШЕРА =========================

@bot.callback_query_handler(func=lambda c: c.data.startswith("editvpsher_"))
def cb_edit_vpsher(call):
    uid = call.from_user.id
    vp_id = int(call.data.split("_")[1])
    row = get_vpsher(vp_id)
    if not row or row[1] != uid:
        bot.answer_callback_query(call.id, "⛔ Это не твоя анкета", show_alert=True)
        return
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("📅 Частота", callback_data=f"editvpsherfield_{vp_id}_frequency"))
    kb.row(types.InlineKeyboardButton("🗓 Дни работы", callback_data=f"editvpsherfield_{vp_id}_work_days"))
    kb.row(types.InlineKeyboardButton("🔗 Списки/каналы", callback_data=f"editvpsherfield_{vp_id}_channel_links"))
    kb.row(types.InlineKeyboardButton("ℹ️ Подробности", callback_data=f"editvpsherfield_{vp_id}_details"))
    if row[3] == "платно":
        kb.row(types.InlineKeyboardButton("💵 Цена", callback_data=f"editvpsherfield_{vp_id}_price_info"))
    kb.row(types.InlineKeyboardButton("📞 Контакт", callback_data=f"editvpsherfield_{vp_id}_contact"))
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data="back_my_channels"))
    bot.edit_message_text(
        f"✏️ Редактирование анкеты впшера #{vp_id}\nВыбери, что хочешь изменить:",
        call.message.chat.id, call.message.message_id, reply_markup=kb
    )
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("editvpsherfield_"))
def cb_edit_vpsher_field(call):
    uid = call.from_user.id
    parts = call.data.split("_")
    vp_id = int(parts[1])
    field = "_".join(parts[2:])
    row = get_vpsher(vp_id)
    if not row or row[1] != uid:
        bot.answer_callback_query(call.id, "⛔ Это не твоя анкета", show_alert=True)
        return
    vpsher_edit_states[uid] = {"vpsher_id": vp_id, "field": field}
    field_name = VPSHER_FIELD_NAMES_RU.get(field, field)
    current_value = ""
    if field == "frequency": current_value = row[4]
    elif field == "work_days": current_value = row[5]
    elif field == "channel_links": current_value = row[6]
    elif field == "details": current_value = row[7]
    elif field == "price_info": current_value = row[8]
    elif field == "contact": current_value = row[9]
    bot.answer_callback_query(call.id)
    msg = bot.send_message(
        call.message.chat.id,
        f"✏️ Введи новое значение для поля «{field_name}»\n"
        f"Текущее: {esc(current_value)}\n\n"
        f"Или /skip чтобы оставить как есть:",
        reply_markup=edit_kb()
    )
    bot.register_next_step_handler(msg, step_edit_vpsher_field)

def step_edit_vpsher_field(message):
    if check_cancel(message):
        return
    uid = message.from_user.id
    state = vpsher_edit_states.get(uid)
    if not state:
        bot.send_message(message.chat.id, "⚠️ Сессия редактирования устарела", reply_markup=main_menu_kb())
        return
    vp_id = state["vpsher_id"]
    field = state["field"]
    field_name = VPSHER_FIELD_NAMES_RU.get(field, field)
    if message.text and message.text.strip() == "/skip":
        bot.send_message(message.chat.id, f"✅ Поле «{field_name}» оставлено без изменений", reply_markup=main_menu_kb())
        vpsher_edit_states.pop(uid, None)
        return
    value = message.text
    db_execute(f"UPDATE vpshers SET {field}=%s WHERE id=%s", (value, vp_id))
    logger.info("[EDIT VPSHER] Пользователь %s изменил поле '%s' анкеты id=%s", uid, field, vp_id)
    bot.send_message(message.chat.id, f"✅ Поле «{field_name}» успешно обновлено!", reply_markup=main_menu_kb())
    vpsher_edit_states.pop(uid, None)

@bot.callback_query_handler(func=lambda c: c.data == "myvpsherdel_menu")
def cb_my_vpsher_del_menu(call):
    uid = call.from_user.id
    rows = db_query("SELECT id, contact, created_at FROM vpshers WHERE submitted_by=%s ORDER BY id", (uid,))
    if not rows:
        bot.answer_callback_query(call.id, "У тебя нет анкет впшера", show_alert=True)
        return
    kb = types.InlineKeyboardMarkup()
    for vp_id, contact, created_at in rows:
        kb.row(types.InlineKeyboardButton(f"🗑 #{vp_id} | {contact}", callback_data=f"myvpsherdel_{vp_id}"))
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data="back_my_channels"))
    bot.edit_message_text("🗑 Выбери анкету для удаления:", call.message.chat.id, call.message.message_id, reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("myvpsherdel_"))
def cb_my_vpsher_delete(call):
    uid = call.from_user.id
    vp_id = int(call.data.split("_")[1])
    row = get_vpsher(vp_id)
    if not row or row[1] != uid:
        bot.answer_callback_query(call.id, "⛔ Это не твоя анкета", show_alert=True)
        return
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("✅ Да, удалить", callback_data=f"myvpsherdelconfirm_{vp_id}"),
        types.InlineKeyboardButton("❌ Отмена", callback_data="back_my_channels"),
    )
    try:
        bot.edit_message_text(f"⚠️ Удалить анкету впшера #{vp_id}?", call.message.chat.id, call.message.message_id, reply_markup=kb)
    except Exception:
        pass
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("myvpsherdelconfirm_"))
def cb_my_vpsher_delete_confirm(call):
    uid = call.from_user.id
    vp_id = int(call.data.split("_")[1])
    row = get_vpsher(vp_id)
    if not row or row[1] != uid:
        bot.answer_callback_query(call.id, "⛔ Это не твоя анкета", show_alert=True)
        return
    vpsher_data = archive_vpsher(row, uid)
    logger.info("[DELETE VPSHER] Пользователь %s удалил анкету id=%s", uid, vp_id)
    for admin_id in ALL_ADMIN_IDS:
        try:
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("🔄 Вернуть", callback_data=f"restorevpsher_{vp_id}"),
                types.InlineKeyboardButton("🗑 Удалить навсегда", callback_data=f"permadelvpsher_{vp_id}"),
            )
            bot.send_message(admin_id, f"⚠️ Пользователь {uid} удалил анкету #{vp_id}\nСохранена в корзине.", reply_markup=kb)
        except Exception:
            pass
    bot.answer_callback_query(call.id, "🗑 Анкета удалена")
    try:
        bot.edit_message_text(f"🗑 Анкета впшера #{vp_id} удалена", call.message.chat.id, call.message.message_id)
    except Exception:
        pass

# ========================= ВОССТАНОВЛЕНИЕ АНКЕТ ВПШЕРОВ =========================

@bot.callback_query_handler(func=lambda c: c.data.startswith("restorevpsher_"))
def cb_restore_vpsher(call):
    if call.from_user.id not in ALL_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Нет доступа", show_alert=True)
        return
    vp_id = int(call.data.split("_")[1])
    deleted = db_query("SELECT vpsher_data FROM deleted_vpshers WHERE id=%s ORDER BY deleted_at DESC LIMIT 1", (vp_id,))
    if not deleted:
        bot.answer_callback_query(call.id, "Анкета не найдена", show_alert=True)
        return
    vpsher_data = deleted[0][0]
    if isinstance(vpsher_data, str):
        vpsher_data = json.loads(vpsher_data)
    db_execute(
        """INSERT INTO vpshers (id, submitted_by, submitted_by_username, paid_type, frequency, work_days, channel_links, details, price_info, contact, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (id) DO UPDATE SET
               submitted_by=EXCLUDED.submitted_by, submitted_by_username=EXCLUDED.submitted_by_username,
               paid_type=EXCLUDED.paid_type, frequency=EXCLUDED.frequency, work_days=EXCLUDED.work_days,
               channel_links=EXCLUDED.channel_links, details=EXCLUDED.details,
               price_info=EXCLUDED.price_info, contact=EXCLUDED.contact, created_at=EXCLUDED.created_at""",
        (vp_id, vpsher_data["submitted_by"], vpsher_data["submitted_by_username"], vpsher_data["paid_type"],
         vpsher_data["frequency"], vpsher_data["work_days"], vpsher_data["channel_links"],
         vpsher_data["details"], vpsher_data["price_info"], vpsher_data["contact"], vpsher_data["created_at"])
    )
    db_execute("DELETE FROM deleted_vpshers WHERE id=%s", (vp_id,))
    log_admin_action(call.from_user.id, "restore_vpsher", f"анкета впшера #{vp_id}")
    logger.info("[RESTORE] Админ %s восстановил анкету впшера id=%s", call.from_user.id, vp_id)
    bot.answer_callback_query(call.id, "✅ Анкета восстановлена!")
    try:
        bot.edit_message_text(f"✅ Анкета #{vp_id} восстановлена", call.message.chat.id, call.message.message_id)
    except Exception:
        pass

@bot.callback_query_handler(func=lambda c: c.data.startswith("permadelvpsher_"))
def cb_permanent_delete_vpsher(call):
    if call.from_user.id not in SUPER_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Только супер-админ", show_alert=True)
        return
    vp_id = int(call.data.split("_")[1])
    db_execute("DELETE FROM deleted_vpshers WHERE id=%s", (vp_id,))
    log_admin_action(call.from_user.id, "permadelete_vpsher", f"анкета впшера #{vp_id}")
    logger.info("[PERMADEL] Супер-админ %s навсегда удалил анкету id=%s", call.from_user.id, vp_id)
    bot.answer_callback_query(call.id, "🗑 Анкета удалена навсегда")
    try:
        bot.edit_message_text(f"🗑 Анкета #{vp_id} удалена навсегда", call.message.chat.id, call.message.message_id)
    except Exception:
        pass

# ========================= НОВЫЕ CALLBACK'И ДЛЯ АДМИНКИ (КАНАЛЫ) =========================

@bot.callback_query_handler(func=lambda c: c.data in ("admin_nav_prev", "admin_nav_next"))
def cb_admin_nav(call):
    uid = call.from_user.id
    if uid not in ALL_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Нет доступа", show_alert=True)
        return
    state = view_states.get(uid)
    if not state or not state.get("admin_list_mode"):
        bot.answer_callback_query(call.id, "Сессия устарела", show_alert=True)
        return
    total = len(state["ids"])
    if call.data == "admin_nav_next" and state["index"] < total - 1:
        state["index"] += 1
    elif call.data == "admin_nav_prev" and state["index"] > 0:
        state["index"] -= 1
    else:
        bot.answer_callback_query(call.id)
        return
    bot.answer_callback_query(call.id)
    render_admin_card(call.message.chat.id, uid)

@bot.callback_query_handler(func=lambda c: c.data.startswith("adminedit_"))
def cb_admin_edit_field(call):
    """Быстрое редактирование поля канала прямо из админ-просмотра (порог/подписчики/Вппост/Ртпост).
    ИСПРАВЛЕНИЕ: раньше у кнопок adminedit_ не было обработчика вообще, поэтому они 'зависали'."""
    uid = call.from_user.id
    if uid not in ALL_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Нет доступа", show_alert=True)
        return
    parts = call.data.split("_")
    try:
        cid = int(parts[1])
    except (IndexError, ValueError):
        bot.answer_callback_query(call.id, "⚠️ Некорректные данные кнопки", show_alert=True)
        return
    field = "_".join(parts[2:])
    row = get_channel(cid)
    if not row:
        bot.answer_callback_query(call.id, "⚠️ Канал не найден", show_alert=True)
        return
    # Отвечаем на callback сразу, чтобы кнопка не "висела" с крутящимся индикатором
    bot.answer_callback_query(call.id)
    admin_edit_states[uid] = {"channel_id": cid, "field": field, "chat_id": call.message.chat.id}
    current_value = ""
    if field == "threshold": current_value = row[3]
    elif field == "subscribers": current_value = row[6]
    elif field == "rt_post": current_value = row[4]
    elif field == "vp_post": current_value = row[5]
    field_name = FIELD_NAMES_RU.get(field, field)
    msg = bot.send_message(
        call.message.chat.id,
        f"🔧 <b>Админ-редактирование</b>\n"
        f"Канал #{cid} «{esc(row[1])}»\n\n"
        f"Введи новое значение для поля «{field_name}»\n"
        f"Текущее: {esc(current_value)}\n\n"
        f"Или /skip чтобы оставить как есть:",
        reply_markup=edit_kb(), parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, step_admin_edit_field)

def step_admin_edit_field(message):
    if check_cancel(message):
        return
    uid = message.from_user.id
    if uid not in ALL_ADMIN_IDS:
        return
    state = admin_edit_states.get(uid)
    if not state:
        bot.send_message(message.chat.id, "⚠️ Сессия редактирования устарела", reply_markup=main_menu_kb())
        return
    cid = state["channel_id"]
    field = state["field"]
    field_name = FIELD_NAMES_RU.get(field, field)
    if message.text and message.text.strip() == "/skip":
        bot.send_message(message.chat.id, f"✅ Поле «{field_name}» оставлено без изменений", reply_markup=main_menu_kb())
        admin_edit_states.pop(uid, None)
        return
    try:
        if field == "threshold":
            value = parse_threshold_input(message.text)
            if value is None:
                raise ValueError("Нужно число")
        elif field == "subscribers":
            value = int(message.text.replace(" ", ""))
        elif field in ("rt_post", "vp_post"):
            value = normalize_url(message.text)
            if not value:
                raise ValueError("Некорректная ссылка")
        else:
            value = message.text
        db_execute(f"UPDATE channels SET {field}=%s WHERE id=%s", (value, cid))
        logger.info("[ADMIN EDIT] Админ %s изменил поле '%s' канала id=%s -> %s", uid, field, cid, value)
        log_admin_action(uid, "admin_edit_field", f"канал #{cid}: {field}={value}")
        bot.send_message(message.chat.id, f"✅ Поле «{field_name}» канала #{cid} успешно обновлено!", reply_markup=main_menu_kb())
    except Exception as e:
        msg = bot.send_message(
            message.chat.id,
            f"⚠️ Ошибка: {esc(str(e))}\nПопробуй ещё раз или /skip:",
            reply_markup=edit_kb()
        )
        bot.register_next_step_handler(msg, step_admin_edit_field)
        return
    admin_edit_states.pop(uid, None)
    # Если админ смотрел карточку в режиме админ-просмотра, обновляем её на месте
    view_state = view_states.get(uid)
    if view_state and view_state.get("admin_list_mode"):
        try:
            render_admin_card(state["chat_id"], uid)
        except Exception as e:
            logger.warning("[ADMIN EDIT] Не удалось перерисовать карточку: %s", e)

@bot.callback_query_handler(func=lambda c: c.data == "admin_show_all_list")
def cb_admin_show_all(call):
    if call.from_user.id not in ALL_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Нет доступа", show_alert=True)
        return
    rows = db_query("SELECT id, title, threshold, pinned, category FROM channels ORDER BY pinned DESC, id")
    lines = []
    for r in rows:
        cid, title, thr, pinned, cat = r
        p = "📌" if pinned else "  "
        lines.append(f"{p}#{cid} | {esc(title)} | {thr}+ | {CATEGORIES.get(cat, cat)}")
    text = "📋 <b>Полный список каналов:</b>\n\n" + "\n".join(lines) if lines else "База пуста"
    bot.send_message(call.message.chat.id, text, parse_mode="HTML")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data == "admin_close_list")
def cb_admin_close(call):
    if call.from_user.id not in ALL_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Нет доступа", show_alert=True)
        return
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    view_states.pop(call.from_user.id, None)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("admindelconfirm_"))
def cb_admin_del_confirm(call):
    if call.from_user.id not in ALL_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Нет доступа", show_alert=True)
        return
    cid = int(call.data.split("_")[1])
    row = get_channel(cid)
    if not row:
        bot.answer_callback_query(call.id, "Канал не найден", show_alert=True)
        return
    archive_channel(row, call.from_user.id)
    log_admin_action(call.from_user.id, "delete_channel", f"канал #{cid}")
    logger.info("[ADMIN DELETE] Админ %s удалил канал id=%s", call.from_user.id, cid)
    bot.answer_callback_query(call.id, "🗑 Канал удалён")
    try:
        bot.edit_message_text(f"🗑 Канал #{cid} «{esc(row[1])}» удалён", call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    notify_super_admin_deletion(cid, row[1], "channel", f"удалено админом {call.from_user.id}")

@bot.callback_query_handler(func=lambda c: c.data == "delcb_cancel")
def cb_del_cancel(call):
    bot.answer_callback_query(call.id, "Отменено")
    try:
        bot.edit_message_text("❌ Удаление отменено", call.message.chat.id, call.message.message_id)
    except Exception:
        pass

@bot.callback_query_handler(func=lambda c: c.data.startswith("pincb_"))
def cb_pin_channel(call):
    if call.from_user.id not in ALL_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Нет доступа", show_alert=True)
        return
    cid = int(call.data.split("_")[1])
    db_execute("UPDATE channels SET pinned=1 WHERE id=%s", (cid,))
    log_admin_action(call.from_user.id, "pin", f"канал #{cid}")
    logger.info("[PIN] Админ %s закрепил канал id=%s", call.from_user.id, cid)
    bot.answer_callback_query(call.id, "📌 Закреплено")
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    except Exception:
        pass

@bot.callback_query_handler(func=lambda c: c.data.startswith("delcb_"))
def cb_del_channel(call):
    if call.from_user.id not in ALL_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Нет доступа", show_alert=True)
        return
    cid = int(call.data.split("_")[1])
    row = get_channel(cid)
    if not row:
        bot.answer_callback_query(call.id, "Канал не найден", show_alert=True)
        return
    archive_channel(row, call.from_user.id)
    log_admin_action(call.from_user.id, "delete_channel", f"канал #{cid}")
    logger.info("[DELETE] Админ %s удалил канал id=%s", call.from_user.id, cid)
    bot.answer_callback_query(call.id, "🗑 Удалено")
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    except Exception:
        pass
    notify_super_admin_deletion(cid, row[1], "channel", f"удалено админом {call.from_user.id}")

@bot.callback_query_handler(func=lambda c: c.data.startswith("restore_"))
def cb_restore_channel(call):
    if call.from_user.id not in ALL_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Нет доступа", show_alert=True)
        return
    cid = int(call.data.split("_")[1])
    deleted = db_query("SELECT channel_data FROM deleted_channels WHERE id=%s ORDER BY deleted_at DESC LIMIT 1", (cid,))
    if not deleted:
        bot.answer_callback_query(call.id, "Канал не найден в корзине", show_alert=True)
        return
    channel_data = deleted[0][0]
    if isinstance(channel_data, str):
        channel_data = json.loads(channel_data)
    db_execute(
        """INSERT INTO channels (id, title, dni, threshold, rt_post, vp_post, subscribers, extra,
               photo_id, status, submitted_by, created_at, pinned, category, time_range, schedule, submitted_by_username)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (id) DO UPDATE SET
               title=EXCLUDED.title, dni=EXCLUDED.dni, threshold=EXCLUDED.threshold,
               rt_post=EXCLUDED.rt_post, vp_post=EXCLUDED.vp_post, subscribers=EXCLUDED.subscribers,
               extra=EXCLUDED.extra, photo_id=EXCLUDED.photo_id, status=EXCLUDED.status,
               submitted_by=EXCLUDED.submitted_by, created_at=EXCLUDED.created_at, pinned=EXCLUDED.pinned,
               category=EXCLUDED.category, time_range=EXCLUDED.time_range, schedule=EXCLUDED.schedule,
               submitted_by_username=EXCLUDED.submitted_by_username""",
        (cid, channel_data["title"], channel_data["dni"], channel_data["threshold"],
         channel_data["rt_post"], channel_data["vp_post"], channel_data["subscribers"],
         channel_data["extra"], channel_data.get("photo_id"), channel_data["status"],
         channel_data["submitted_by"], channel_data["created_at"], channel_data.get("pinned", 0),
         channel_data.get("category", "чат"), channel_data.get("time_range", ""),
         channel_data.get("schedule", ""), channel_data.get("submitted_by_username", ""))
    )
    db_execute("DELETE FROM deleted_channels WHERE id=%s", (cid,))
    log_admin_action(call.from_user.id, "restore_channel", f"канал #{cid}")
    logger.info("[RESTORE] Админ %s восстановил канал id=%s", call.from_user.id, cid)
    bot.answer_callback_query(call.id, "✅ Канал восстановлен!")
    try:
        bot.edit_message_text(f"✅ Канал #{cid} восстановлен", call.message.chat.id, call.message.message_id)
    except Exception:
        pass

@bot.callback_query_handler(func=lambda c: c.data.startswith("permadel_"))
def cb_permadel_channel(call):
    if call.from_user.id not in SUPER_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Только супер-админ", show_alert=True)
        return
    cid = int(call.data.split("_")[1])
    db_execute("DELETE FROM deleted_channels WHERE id=%s", (cid,))
    log_admin_action(call.from_user.id, "permadelete_channel", f"канал #{cid}")
    logger.info("[PERMADEL] Супер-админ %s навсегда удалил канал id=%s", call.from_user.id, cid)
    bot.answer_callback_query(call.id, "🗑 Канал удалён навсегда")
    try:
        bot.edit_message_text(f"🗑 Канал #{cid} удалён навсегда", call.message.chat.id, call.message.message_id)
    except Exception:
        pass


# ========================= АДМИН-ПАНЕЛЬ =========================

@bot.message_handler(commands=["admin"])
def admin_panel(message):
    if message.from_user.id not in ALL_ADMIN_IDS:
        return
    track_user(message.from_user)
    total = db_query("SELECT COUNT(*) FROM channels")[0][0]
    pinned_total = db_query("SELECT COUNT(*) FROM channels WHERE pinned=1")[0][0]
    vpshers_total = db_query("SELECT COUNT(*) FROM vpshers")[0][0]
    active_users = get_active_users_count()
    text = (
        "🔧 <b>Админ-панель</b>\n\n"
        f"📋 Всего каналов: {total}\n"
        f"📌 Закреплено: {pinned_total}\n"
        f"🙋 Анкет впшеров: {vpshers_total}\n"
        f"👥 Активных пользователей: {active_users}\n\n"
        "/list — просмотр\n"
        "/view ID — быстрый просмотр\n"
        "/del ID — удалить\n"
        "/pin ID — закрепить\n"
        "/unpin ID — открепить\n"
        "/editthreshold ID (новый порог)\n"
        "/editsubscribers ID (новое кол-во)\n"
        "/export — выгрузить\n"
        "/import — загрузить\n"
        "/clean_trash — очистить корзину\n"
        "/vpshers — анкеты\n"
        "/delvpsher ID\n"
        "/delall — удалить ВСЕ каналы"
    )
    if message.from_user.id in SUPER_ADMIN_IDS:
        text += "\n\n👑 /actions — журнал\n👑 /trash — корзина\n👑 /broadcast — рассылка всем\n👑 /exportall — экспорт ВСЕЙ базы данных (все таблицы)\n👑 /importall — импорт ВСЕЙ базы данных из JSON"
    bot.send_message(message.chat.id, text)

# ========================= РАССЫЛКА /broadcast (НОВАЯ ФУНКЦИЯ) =========================

@bot.message_handler(commands=["broadcast"])
def admin_broadcast(message):
    if message.from_user.id not in SUPER_ADMIN_IDS:
        return
    track_user(message.from_user)
    total_users = get_active_users_count()
    msg = bot.send_message(
        message.chat.id,
        f"📢 <b>Рассылка сообщений</b>\n\n"
        f"👥 Всего активных пользователей: <b>{total_users}</b>\n\n"
        f"Отправь текст сообщения, которое нужно разослать всем.\n"
        f"Поддерживается HTML-разметка.\n\n"
        f"🔙 Для отмены нажми «Отмена»",
        reply_markup=cancel_kb(),
        parse_mode="HTML"
    )
    bot.register_next_step_handler(msg, step_broadcast_message)

def step_broadcast_message(message):
    if check_cancel(message):
        return
    if message.from_user.id not in SUPER_ADMIN_IDS:
        return

    broadcast_text = message.text
    if not broadcast_text or not broadcast_text.strip():
        msg = bot.send_message(
            message.chat.id,
            "⚠️ Сообщение не может быть пустым. Отправь текст рассылки:",
            reply_markup=cancel_kb()
        )
        bot.register_next_step_handler(msg, step_broadcast_message)
        return

    # Сохраняем рассылку в БД
    broadcast_id = db_insert_returning_id(
        "INSERT INTO broadcasts (admin_id, message_text, created_at) VALUES (%s,%s,%s) RETURNING id",
        (message.from_user.id, broadcast_text, datetime.now().isoformat())
    )

    # Получаем всех активных пользователей
    users = get_all_active_users()
    total = len(users)
    sent = 0
    failed = 0
    failed_users = []

    logger.info("[BROADCAST] Начало рассылки #%s от админа %s. Всего пользователей: %s", broadcast_id, message.from_user.id, total)

    # Отправляем статус
    status_msg = bot.send_message(
        message.chat.id,
        f"⏳ <b>Рассылка начата...</b>\n"
        f"👥 Всего: {total}\n"
        f"✅ Отправлено: 0\n"
        f"❌ Ошибок: 0",
        parse_mode="HTML"
    )

    for user_row in users:
        user_id = user_row[0]
        try:
            bot.send_message(user_id, broadcast_text, parse_mode="HTML", disable_web_page_preview=False)
            sent += 1
            time.sleep(0.05)  # Небольшая задержка чтобы не спамить API
        except Exception as e:
            failed += 1
            failed_users.append(f"{user_id} ({user_row[1] or 'no_username'})")
            logger.warning("[BROADCAST] Ошибка отправки пользователю %s: %s", user_id, e)

        # Обновляем статус каждые 10 сообщений
        if (sent + failed) % 10 == 0:
            try:
                bot.edit_message_text(
                    f"⏳ <b>Рассылка в процессе...</b>\n"
                    f"👥 Всего: {total}\n"
                    f"✅ Отправлено: {sent}\n"
                    f"❌ Ошибок: {failed}",
                    message.chat.id,
                    status_msg.message_id,
                    parse_mode="HTML"
                )
            except Exception:
                pass

    # Обновляем статистику в БД
    db_execute(
        "UPDATE broadcasts SET sent_count=%s, failed_count=%s, total_users=%s, completed_at=%s WHERE id=%s",
        (sent, failed, total, datetime.now().isoformat(), broadcast_id)
    )

    # Финальный отчёт
    report = (
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"📨 ID рассылки: #{broadcast_id}\n"
        f"👥 Всего пользователей: <b>{total}</b>\n"
        f"✅ Успешно доставлено: <b>{sent}</b>\n"
        f"❌ Не доставлено: <b>{failed}</b>\n"
        f"📊 Процент доставки: <b>{round(sent/total*100, 1) if total > 0 else 0}%</b>"
    )

    if failed_users and len(failed_users) <= 20:
        report += f"\n\n❌ <b>Не доставлено:</b>\n" + "\n".join(failed_users[:20])
    elif failed_users:
        report += f"\n\n❌ Первые 20 из {len(failed_users)} не доставлено:\n" + "\n".join(failed_users[:20])

    logger.info("[BROADCAST] Рассылка #%s завершена. Отправлено: %s, ошибок: %s", broadcast_id, sent, failed)

    try:
        bot.edit_message_text(
            report,
            message.chat.id,
            status_msg.message_id,
            parse_mode="HTML"
        )
    except Exception:
        bot.send_message(message.chat.id, report, parse_mode="HTML")

    bot.send_message(message.chat.id, "🏠 Главное меню", reply_markup=main_menu_kb())

@bot.message_handler(commands=["broadcasts"])
def admin_broadcasts_history(message):
    if message.from_user.id not in SUPER_ADMIN_IDS:
        return
    rows = db_query(
        "SELECT id, message_text, sent_count, failed_count, total_users, created_at, completed_at "
        "FROM broadcasts ORDER BY id DESC LIMIT 10"
    )
    if not rows:
        bot.send_message(message.chat.id, "📭 История рассылок пуста")
        return

    text = "📢 <b>История рассылок:</b>\n\n"
    for r in rows:
        bid, msg_text, sent, failed, total, created, completed = r
        percent = round(sent/total*100, 1) if total > 0 else 0
        status = "✅ Завершена" if completed else "⏳ В процессе"
        text += (
            f"📨 <b>Рассылка #{bid}</b>\n"
            f"📅 {created}\n"
            f"{status}\n"
            f"👥 {total} | ✅ {sent} | ❌ {failed} | 📊 {percent}%\n"
            f"📝 {esc(msg_text[:100])}{'...' if len(msg_text) > 100 else ''}\n\n"
        )
    bot.send_message(message.chat.id, text, parse_mode="HTML")

# ========================= КОМАНДА /delall ДЛЯ СУПЕР-АДМИНА =========================

@bot.message_handler(commands=["delall"])
def admin_del_all(message):
    if message.from_user.id not in SUPER_ADMIN_IDS:
        return
    track_user(message.from_user)
    total = db_query("SELECT COUNT(*) FROM channels")[0][0]
    if total == 0:
        bot.send_message(message.chat.id, "📭 База уже пуста")
        return

    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("✅ Да, удалить ВСЕ", callback_data="delall_confirm"),
        types.InlineKeyboardButton("❌ Отмена", callback_data="delall_cancel"),
    )
    bot.send_message(
        message.chat.id,
        f"⚠️ <b>ВНИМАНИЕ!</b>\n\n"
        f"Ты собираешься удалить <b>ВСЕ</b> каналы из базы.\n"
        f"Количество: <b>{total}</b>\n\n"
        f"Это действие нельзя отменить!\n"
        f"Каналы будут архивированы в корзину.",
        reply_markup=kb,
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda c: c.data == "delall_confirm")
def cb_delall_confirm(call):
    if call.from_user.id not in SUPER_ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ Только супер-админ", show_alert=True)
        return

    rows = db_query("SELECT * FROM channels")
    deleted_count = 0
    for row in rows:
        archive_channel(row, call.from_user.id)
        deleted_count += 1

    log_admin_action(call.from_user.id, "delete_all_channels", f"удалено {deleted_count} каналов")
    logger.info("[DELALL] Супер-админ %s удалил ВСЕ каналы (%s шт.)", call.from_user.id, deleted_count)

    bot.answer_callback_query(call.id, f"🗑 Удалено {deleted_count} каналов")
    try:
        bot.edit_message_text(
            f"✅ <b>Удаление завершено</b>\n\n"
            f"🗑 Удалено каналов: <b>{deleted_count}</b>\n"
            f"📋 Все каналы архивированы в корзину\n"
            f"👤 Админ: <code>{call.from_user.id}</code>",
            call.message.chat.id,
            call.message.message_id,
            parse_mode="HTML"
        )
    except Exception:
        pass

    # Уведомляем остальных супер-админов
    for admin_id in SUPER_ADMIN_IDS:
        if admin_id != call.from_user.id:
            try:
                bot.send_message(
                    admin_id,
                    f"⚠️ <b>Массовое удаление!</b>\n\n"
                    f"👤 Админ: <code>{call.from_user.id}</code>\n"
                    f"🗑 Удалено каналов: <b>{deleted_count}</b>\n"
                    f"📋 Все данные в корзине",
                    parse_mode="HTML"
                )
            except Exception:
                pass

@bot.callback_query_handler(func=lambda c: c.data == "delall_cancel")
def cb_delall_cancel(call):
    bot.answer_callback_query(call.id, "Отменено")
    try:
        bot.edit_message_text("❌ Удаление отменено", call.message.chat.id, call.message.message_id)
    except Exception:
        pass

# ========================= ОСТАЛЬНЫЕ АДМИН-КОМАНДЫ =========================

@bot.message_handler(commands=["delvpsher"])
def admin_del_vpsher(message):
    if message.from_user.id not in ALL_ADMIN_IDS:
        return
    track_user(message.from_user)
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /delvpsher ID")
        return
    vp_id = int(parts[1])
    row = get_vpsher(vp_id)
    if not row:
        bot.send_message(message.chat.id, f"⚠️ Анкета #{vp_id} не найдена")
        return
    vpsher_data = archive_vpsher(row, message.from_user.id)
    log_admin_action(message.from_user.id, "delete_vpsher", f"анкета #{vp_id}")
    logger.info("[ADMIN DELETE] Админ %s удалил анкету id=%s", message.from_user.id, vp_id)
    bot.send_message(message.chat.id, f"🗑 Анкета #{vp_id} удалена")
    notify_super_admin_deletion(vp_id, vpsher_data.get("contact"), "vpsher", f"удалено админом {message.from_user.id}")

@bot.message_handler(commands=["list"])
def admin_list(message):
    if message.from_user.id not in ALL_ADMIN_IDS:
        return
    track_user(message.from_user)
    rows = db_query("SELECT id, title, threshold, pinned, rt_post, vp_post, category FROM channels ORDER BY pinned DESC, id")
    if not rows:
        bot.send_message(message.chat.id, "База пуста")
        return
    uid = message.from_user.id
    view_states[uid] = {
        "ids": [r[0] for r in rows], "index": 0, "admin_list_mode": True, "message_id": None
    }
    render_admin_card(message.chat.id, uid)

@bot.message_handler(commands=["view"])
def admin_view(message):
    if message.from_user.id not in ALL_ADMIN_IDS:
        return
    track_user(message.from_user)
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /view ID")
        return
    cid = int(parts[1])
    row = get_channel(cid)
    if not row:
        bot.send_message(message.chat.id, f"⚠️ Канал #{cid} не найден")
        return
    rows = db_query("SELECT id FROM channels ORDER BY pinned DESC, id")
    all_ids = [r[0] for r in rows]
    try:
        idx = all_ids.index(cid)
    except ValueError:
        idx = 0
    uid = message.from_user.id
    view_states[uid] = {"ids": all_ids, "index": idx, "admin_list_mode": True, "message_id": None}
    render_admin_card(message.chat.id, uid)

@bot.message_handler(commands=["editthreshold"])
def admin_edit_threshold(message):
    if message.from_user.id not in ALL_ADMIN_IDS:
        return
    track_user(message.from_user)
    parts = message.text.split()
    if len(parts) < 3 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /editthreshold ID (новый порог)")
        return
    cid = int(parts[1])
    try:
        new_thr = int(parts[2])
    except ValueError:
        bot.send_message(message.chat.id, "⚠️ Порог должен быть числом")
        return
    row = get_channel(cid)
    if not row:
        bot.send_message(message.chat.id, f"⚠️ Канал #{cid} не найден")
        return
    db_execute("UPDATE channels SET threshold=%s WHERE id=%s", (new_thr, cid))
    log_admin_action(message.from_user.id, "edit_threshold", f"канал #{cid}: порог={new_thr}")
    logger.info("[ADMIN EDIT] Админ %s изменил порог канала id=%s на %s", message.from_user.id, cid, new_thr)
    bot.send_message(message.chat.id, f"✅ Порог канала #{cid} изменен на {new_thr}+")

@bot.message_handler(commands=["editsubscribers"])
def admin_edit_subscribers(message):
    if message.from_user.id not in ALL_ADMIN_IDS:
        return
    track_user(message.from_user)
    parts = message.text.split()
    if len(parts) < 3 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /editsubscribers ID (новое количество)")
        return
    cid = int(parts[1])
    try:
        new_subs = int(parts[2].replace(" ", ""))
    except ValueError:
        bot.send_message(message.chat.id, "⚠️ Число")
        return
    row = get_channel(cid)
    if not row:
        bot.send_message(message.chat.id, f"⚠️ Канал #{cid} не найден")
        return
    db_execute("UPDATE channels SET subscribers=%s WHERE id=%s", (new_subs, cid))
    log_admin_action(message.from_user.id, "edit_subscribers", f"канал #{cid}: подписчики={new_subs}")
    logger.info("[ADMIN EDIT] Админ %s изменил подписчиков канала id=%s на %s", message.from_user.id, cid, new_subs)
    bot.send_message(message.chat.id, f"✅ Подписчики канала #{cid} изменены на {new_subs}")

@bot.message_handler(commands=["del"])
def admin_del(message):
    if message.from_user.id not in ALL_ADMIN_IDS:
        return
    track_user(message.from_user)
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /del ID")
        return
    cid = int(parts[1])
    row = get_channel(cid)
    if not row:
        bot.send_message(message.chat.id, f"⚠️ Канал #{cid} не найден")
        return
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("✅ Да, удалить", callback_data=f"admindelconfirm_{cid}"),
        types.InlineKeyboardButton("❌ Отмена", callback_data="delcb_cancel"),
    )
    bot.send_message(message.chat.id, f"⚠️ Удалить канал #{cid} «{esc(row[1])}»?", reply_markup=kb)

@bot.message_handler(commands=["pin"])
def admin_pin(message):
    if message.from_user.id not in ALL_ADMIN_IDS:
        return
    track_user(message.from_user)
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /pin ID")
        return
    cid = int(parts[1])
    row = get_channel(cid)
    if not row:
        bot.send_message(message.chat.id, f"⚠️ Канал #{cid} не найден")
        return
    db_execute("UPDATE channels SET pinned=1 WHERE id=%s", (cid,))
    log_admin_action(message.from_user.id, "pin", f"канал #{cid}")
    logger.info("[ADMIN PIN] Админ %s закрепил канал id=%s", message.from_user.id, cid)
    bot.send_message(message.chat.id, f"📌 Канал #{cid} закреплён")

@bot.message_handler(commands=["unpin"])
def admin_unpin(message):
    if message.from_user.id not in ALL_ADMIN_IDS:
        return
    track_user(message.from_user)
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /unpin ID")
        return
    cid = int(parts[1])
    row = get_channel(cid)
    if not row:
        bot.send_message(message.chat.id, f"⚠️ Канал #{cid} не найден")
        return
    db_execute("UPDATE channels SET pinned=0 WHERE id=%s", (cid,))
    log_admin_action(message.from_user.id, "unpin", f"канал #{cid}")
    logger.info("[ADMIN UNPIN] Админ %s открепил канал id=%s", message.from_user.id, cid)
    bot.send_message(message.chat.id, f"📌 Канал #{cid} откреплён")

@bot.message_handler(commands=["clean_trash"])
def admin_clean_trash(message):
    if message.from_user.id not in SUPER_ADMIN_IDS:
        return
    track_user(message.from_user)
    cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
    result1 = db_query("SELECT COUNT(*) FROM deleted_channels WHERE deleted_at < %s", (cutoff,))
    result2 = db_query("SELECT COUNT(*) FROM deleted_vpshers WHERE deleted_at < %s", (cutoff,))
    count1 = result1[0][0] if result1 else 0
    count2 = result2[0][0] if result2 else 0
    if count1 > 0:
        db_execute("DELETE FROM deleted_channels WHERE deleted_at < %s", (cutoff,))
    if count2 > 0:
        db_execute("DELETE FROM deleted_vpshers WHERE deleted_at < %s", (cutoff,))
    total = count1 + count2
    logger.info("[CLEAN TRASH] Супер-админ %s очистил корзину: %s каналов, %s анкет", message.from_user.id, count1, count2)
    if total > 0:
        bot.send_message(message.chat.id, f"🗑 Очищено {total} записей")
    else:
        bot.send_message(message.chat.id, "📭 Корзина чиста")

@bot.message_handler(commands=["trash"])
def admin_trash(message):
    if message.from_user.id not in SUPER_ADMIN_IDS:
        return
    track_user(message.from_user)
    channels = db_query("SELECT id, channel_data, deleted_by, deleted_at FROM deleted_channels ORDER BY deleted_at DESC LIMIT 20")
    vpshers = db_query("SELECT id, vpsher_data, deleted_by, deleted_at FROM deleted_vpshers ORDER BY deleted_at DESC LIMIT 20")
    if not channels and not vpshers:
        bot.send_message(message.chat.id, "📭 Корзина пуста")
        return
    if channels:
        bot.send_message(message.chat.id, "📋 <b>Удаленные каналы:</b>", parse_mode="HTML")
        for cid, channel_data, deleted_by, deleted_at in channels:
            if isinstance(channel_data, str):
                channel_data = json.loads(channel_data)
            title = channel_data.get("title", "неизвестно")
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("🔄 Вернуть", callback_data=f"restore_{cid}"),
                types.InlineKeyboardButton("🗑 Удалить навсегда", callback_data=f"permadel_{cid}"),
            )
            bot.send_message(message.chat.id, f"🗑 #{cid} «{esc(title)}»\nУдален: {deleted_by}\nДата: {deleted_at}", reply_markup=kb)
    if vpshers:
        bot.send_message(message.chat.id, "🙋 <b>Удаленные анкеты:</b>", parse_mode="HTML")
        for vp_id, vpsher_data, deleted_by, deleted_at in vpshers:
            if isinstance(vpsher_data, str):
                vpsher_data = json.loads(vpsher_data)
            contact = vpsher_data.get("contact", "неизвестно")
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("🔄 Вернуть", callback_data=f"restorevpsher_{vp_id}"),
                types.InlineKeyboardButton("🗑 Удалить навсегда", callback_data=f"permadelvpsher_{vp_id}"),
            )
            bot.send_message(message.chat.id, f"🗑 #{vp_id} | {esc(contact)}\nУдален: {deleted_by}\nДата: {deleted_at}", reply_markup=kb)

@bot.message_handler(commands=["export"])
def admin_export(message):
    if message.from_user.id not in ALL_ADMIN_IDS:
        return
    track_user(message.from_user)
    rows = db_query(
        "SELECT id, title, dni, threshold, rt_post, vp_post, subscribers, extra, "
        "photo_id, status, submitted_by, created_at, pinned, category, time_range, schedule "
        "FROM channels ORDER BY id"
    )
    if not rows:
        bot.send_message(message.chat.id, "База пуста")
        return
    lines = []
    for r in rows:
        safe = ["" if v is None else str(v).replace("\t", " ").replace("\n", " ") for v in r]
        lines.append("\t".join(safe))
    content = "\n".join(lines)
    buf = io.BytesIO(content.encode("utf-8"))
    buf.name = f"vp_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    bot.send_document(message.chat.id, buf, caption=f"📦 Экспорт: {len(rows)} каналов")
    log_admin_action(message.from_user.id, "export", f"{len(rows)} каналов")
    logger.info("[EXPORT] Админ %s экспортировал %s каналов", message.from_user.id, len(rows))

@bot.message_handler(commands=["import"])
def admin_import(message):
    if message.from_user.id not in ALL_ADMIN_IDS:
        return
    track_user(message.from_user)
    msg = bot.send_message(message.chat.id, "📦 Пришли файл или текст. 🔙 Отмена — прервать.", reply_markup=cancel_kb())
    bot.register_next_step_handler(msg, step_import)

def step_import(message):
    if check_cancel(message): return
    if message.from_user.id not in ALL_ADMIN_IDS: return
    if message.content_type == "document":
        try:
            file_info = bot.get_file(message.document.file_id)
            downloaded = bot.download_file(file_info.file_path)
            content = downloaded.decode("utf-8")
        except Exception as e:
            bot.send_message(message.chat.id, f"⚠️ Ошибка чтения: {e}", reply_markup=main_menu_kb())
            return
    elif message.text:
        content = message.text
    else:
        msg = bot.send_message(message.chat.id, "⚠️ Пришли файл или текст:", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_import)
        return
    lines = [l for l in content.splitlines() if l.strip()]
    imported = 0
    errors = []
    for i, line in enumerate(lines, start=1):
        parts = line.split("\t")
        if len(parts) != len(EXPORT_COLUMNS):
            errors.append(f"Строка {i}: ожидается {len(EXPORT_COLUMNS)} полей")
            continue
        (rid, title, dni, thr, rt_post, vp_post, subs, extra, photo_id,
         status, submitted_by, created_at, pinned, category, time_range, schedule) = parts
        try:
            db_execute(
                """INSERT INTO channels (id, title, dni, threshold, rt_post, vp_post, subscribers, extra,
                       photo_id, status, submitted_by, created_at, pinned, category, time_range, schedule)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (id) DO UPDATE SET
                       title=EXCLUDED.title, dni=EXCLUDED.dni, threshold=EXCLUDED.threshold,
                       rt_post=EXCLUDED.rt_post, vp_post=EXCLUDED.vp_post, subscribers=EXCLUDED.subscribers,
                       extra=EXCLUDED.extra, photo_id=EXCLUDED.photo_id, status=EXCLUDED.status,
                       submitted_by=EXCLUDED.submitted_by, created_at=EXCLUDED.created_at, pinned=EXCLUDED.pinned,
                       category=EXCLUDED.category, time_range=EXCLUDED.time_range, schedule=EXCLUDED.schedule""",
                (int(rid), title, dni, int(thr), rt_post, vp_post, int(subs), extra,
                 photo_id or None, status, int(submitted_by) if submitted_by else None, created_at,
                 int(pinned), category or "чат", time_range or "", schedule or ""),
            )
            imported += 1
        except Exception as e:
            errors.append(f"Строка {i}: {e}")
    db_execute("SELECT setval(pg_get_serial_sequence('channels','id'), COALESCE((SELECT MAX(id) FROM channels), 1))")
    log_admin_action(message.from_user.id, "import", f"{imported} импортировано")
    logger.info("[IMPORT] Админ %s импортировал %s каналов", message.from_user.id, imported)
    report = f"✅ Импортировано/обновлено: {imported}\n"
    if errors:
        report += "\n⚠️ Ошибки:\n" + "\n".join(errors[:20])
    bot.send_message(message.chat.id, report, reply_markup=main_menu_kb())

ALL_DB_TABLES = (
    "channels", "vpshers", "admin_actions", "deleted_channels",
    "deleted_vpshers", "linked_channels", "active_users", "broadcasts",
)

# Первичные ключи каждой таблицы (нужны для ON CONFLICT при импорте)
TABLE_PK = {
    "channels": "id",
    "vpshers": "id",
    "admin_actions": "id",
    "deleted_channels": "id",
    "deleted_vpshers": "id",
    "linked_channels": "id",
    "active_users": "user_id",
    "broadcasts": "id",
}
# Таблицы с автоинкрементным PK — после импорта им нужно подтянуть sequence
SERIAL_TABLES = {"channels", "vpshers", "admin_actions", "linked_channels", "broadcasts"}

def _row_to_json_safe(value):
    """Приводит значение колонки к JSON-совместимому виду (datetime, memoryview, JSONB и т.д.)."""
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            return bytes(value).decode("utf-8")
        except Exception:
            return str(value)
    return value

@bot.message_handler(commands=["exportall"])
def superadmin_export_all(message):
    """Полный экспорт ВСЕХ таблиц базы данных в один JSON-файл. Только для супер-админов."""
    if message.from_user.id not in SUPER_ADMIN_IDS:
        return
    track_user(message.from_user)
    conn, cur = _get_db()
    dump = {}
    total_rows = 0
    try:
        for table in ALL_DB_TABLES:
            cur.execute(f"SELECT * FROM {table} ORDER BY 1")
            col_names = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            dump[table] = [
                {col: _row_to_json_safe(val) for col, val in zip(col_names, row)}
                for row in rows
            ]
            total_rows += len(rows)
    except Exception as e:
        logger.error("[EXPORT ALL] Ошибка экспорта: %s", e)
        bot.send_message(message.chat.id, f"⚠️ Ошибка экспорта: {esc(str(e))}")
        return
    content = json.dumps(dump, ensure_ascii=False, indent=2, default=str)
    buf = io.BytesIO(content.encode("utf-8"))
    buf.name = f"vp_full_db_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    caption = "📦 <b>Полный экспорт базы данных</b>\n" + "\n".join(
        f"• {t}: {len(dump[t])}" for t in ALL_DB_TABLES
    ) + f"\n\nВсего записей: {total_rows}"
    bot.send_document(message.chat.id, buf, caption=caption, parse_mode="HTML")
    log_admin_action(message.from_user.id, "export_all", f"{total_rows} записей из {len(ALL_DB_TABLES)} таблиц")
    logger.info("[EXPORT ALL] Супер-админ %s экспортировал полную БД: %s записей", message.from_user.id, total_rows)

@bot.message_handler(commands=["importall"])
def superadmin_import_all(message):
    """Полный импорт ВСЕХ таблиц БД из JSON-файла, созданного командой /exportall.
    Только для супер-админов. Существующие записи обновляются (по PK), новые — добавляются,
    ничего не удаляется."""
    if message.from_user.id not in SUPER_ADMIN_IDS:
        return
    track_user(message.from_user)
    msg = bot.send_message(
        message.chat.id,
        "📦 Пришли JSON-файл с полным экспортом БД (созданный командой /exportall).\n"
        "⚠️ Существующие записи будут обновлены, новые — добавлены. Ничего не удаляется.\n"
        "🔙 Отмена — прервать.",
        reply_markup=cancel_kb()
    )
    bot.register_next_step_handler(msg, step_import_all)

def step_import_all(message):
    if check_cancel(message):
        return
    if message.from_user.id not in SUPER_ADMIN_IDS:
        return
    if message.content_type != "document":
        msg = bot.send_message(message.chat.id, "⚠️ Пришли именно файл (документ) с JSON:", reply_markup=cancel_kb())
        bot.register_next_step_handler(msg, step_import_all)
        return
    try:
        file_info = bot.get_file(message.document.file_id)
        downloaded = bot.download_file(file_info.file_path)
        data = json.loads(downloaded.decode("utf-8"))
    except Exception as e:
        bot.send_message(message.chat.id, f"⚠️ Ошибка чтения/разбора JSON: {esc(str(e))}", reply_markup=main_menu_kb())
        return
    if not isinstance(data, dict):
        bot.send_message(message.chat.id, "⚠️ Некорректный формат файла: ожидается JSON-объект с таблицами", reply_markup=main_menu_kb())
        return

    report_lines = []
    total_imported = 0
    total_errors = 0

    for table in ALL_DB_TABLES:
        rows = data.get(table)
        if rows is None:
            continue
        if not isinstance(rows, list):
            report_lines.append(f"⚠️ {table}: некорректный формат, пропущено")
            continue
        pk = TABLE_PK[table]
        imported = 0
        errors = 0
        for i, row in enumerate(rows, start=1):
            if not isinstance(row, dict) or pk not in row or row[pk] is None:
                errors += 1
                continue
            try:
                cols = list(row.keys())
                values = []
                for c in cols:
                    v = row[c]
                    if isinstance(v, (dict, list)):
                        v = PgJson(v)
                    values.append(v)
                collist = ", ".join(cols)
                placeholders = ", ".join(["%s"] * len(cols))
                updateset = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != pk)
                sql = f"INSERT INTO {table} ({collist}) VALUES ({placeholders}) ON CONFLICT ({pk}) DO UPDATE SET {updateset}"
                db_execute(sql, tuple(values))
                imported += 1
            except Exception as e:
                errors += 1
                logger.error("[IMPORT ALL] Ошибка импорта строки %s таблицы %s: %s", i, table, e)
        if table in SERIAL_TABLES:
            try:
                db_execute(
                    f"SELECT setval(pg_get_serial_sequence('{table}','{pk}'), "
                    f"COALESCE((SELECT MAX({pk}) FROM {table}), 1))"
                )
            except Exception as e:
                logger.warning("[IMPORT ALL] Не удалось сбросить sequence для %s: %s", table, e)
        report_lines.append(f"• {table}: {imported} ок" + (f", {errors} ошибок" if errors else ""))
        total_imported += imported
        total_errors += errors

    log_admin_action(message.from_user.id, "import_all", f"{total_imported} записей, {total_errors} ошибок")
    logger.info(
        "[IMPORT ALL] Супер-админ %s импортировал полную БД: %s записей, %s ошибок",
        message.from_user.id, total_imported, total_errors
    )
    report = "📦 <b>Импорт полной БД завершён</b>\n\n" + "\n".join(report_lines) + f"\n\nВсего успешно: {total_imported}"
    if total_errors:
        report += f"\n⚠️ Всего ошибок: {total_errors}"
    bot.send_message(message.chat.id, report, reply_markup=main_menu_kb(), parse_mode="HTML")

@bot.message_handler(commands=["actions"])
def superadmin_actions(message):
    if message.from_user.id not in SUPER_ADMIN_IDS:
        return
    track_user(message.from_user)
    rows = db_query("SELECT admin_id, action, details, created_at FROM admin_actions ORDER BY id DESC LIMIT 20")
    if not rows:
        bot.send_message(message.chat.id, "Пока нет действий админов.")
        return
    blocks = []
    for admin_id, action, details, created_at in rows:
        block = f"👤 <code>{admin_id}</code> — <b>{action}</b>"
        if details:
            block += f"\n{details}"
        block += f"\n🕐 {created_at}"
        blocks.append(block)
    bot.send_message(message.chat.id, "🕵️ Последние действия:\n\n" + "\n\n".join(blocks))

# ========================= ФОНОВЫЕ ЗАДАЧИ =========================

# Ошибки, при которых канал считается окончательно недоступным боту (не временный сбой сети)
AUTO_UNLINK_ERROR_CODES = {400, 403}
AUTO_UNLINK_KEYWORDS = (
    "chat not found",
    "kicked",
    "have no rights",
    "not enough rights",
    "bot is not a member",
    "user not found",
    "chat_admin_required",
    "channel_private",
)

def _is_permanent_access_error(exc):
    """
    Отличает окончательную потерю доступа к каналу (бота выгнали / канал удалён / чат не найден)
    от временных сетевых сбоев (502, timeout и т.п.), которые НЕ должны приводить к отвязке.
    """
    if not isinstance(exc, telebot.apihelper.ApiTelegramException):
        return False
    code = getattr(exc, "error_code", None)
    desc = (getattr(exc, "description", "") or str(exc)).lower()
    return code in AUTO_UNLINK_ERROR_CODES and any(kw in desc for kw in AUTO_UNLINK_KEYWORDS)

def update_linked_channel_subscribers():
    """Раз в 5 минут обновляет подписчиков привязанных каналов.
    Если канал стал окончательно недоступен боту (удалён/бот кикнут/чат не найден) —
    привязка автоматически удаляется, чтобы не спамить логи и не пытаться обновлять её вечно."""
    while True:
        try:
            linked = get_all_linked_channels()
            logger.debug("[BG TASK] Обновление подписчиков для %s каналов", len(linked))
            for link_id, user_id, chat_id in linked:
                try:
                    count = bot.get_chat_member_count(chat_id)
                    update_subscribers_count(link_id, count)
                    db_execute(
                        "UPDATE channels SET subscribers=%s WHERE linked_chat_id=%s",
                        (count, chat_id)
                    )
                    logger.debug("[BG TASK] Обновлены подписчики linked_id=%s: %s", link_id, count)
                except Exception as e:
                    if _is_permanent_access_error(e):
                        logger.warning(
                            "[BG TASK] Канал linked_id=%s (chat_id=%s) недоступен боту (%s) — автоматически отвязываю",
                            link_id, chat_id, e
                        )
                        try:
                            unlink_channel(user_id, chat_id)
                            db_execute("UPDATE channels SET linked_chat_id=NULL WHERE linked_chat_id=%s", (chat_id,))
                            logger.info("[BG TASK] Привязка linked_id=%s (user_id=%s) удалена автоматически", link_id, user_id)
                            try:
                                bot.send_message(
                                    user_id,
                                    "⚠️ Бот потерял доступ к одному из твоих привязанных ТГК "
                                    "(канал удалён, бота исключили, или чат не найден).\n"
                                    "Привязка автоматически снята. Если нужно — привяжи канал заново."
                                )
                            except Exception as notify_err:
                                logger.debug("[BG TASK] Не удалось уведомить user_id=%s об отвязке: %s", user_id, notify_err)
                        except Exception as db_err:
                            logger.error("[BG TASK] Не удалось автоматически удалить привязку linked_id=%s: %s", link_id, db_err)
                    else:
                        logger.warning("[BG TASK] Ошибка обновления подписчиков linked_id=%s: %s", link_id, e)
        except Exception as e:
            logger.error("[BG TASK] Ошибка фонового обновления: %s", e)
        time.sleep(CHANNEL_SUBSCRIBERS_UPDATE_INTERVAL)

# ========================= ЗАПУСК =========================

if __name__ == "__main__":
    logger.info("[STARTUP] Запуск бота...")
    threading.Thread(target=update_linked_channel_subscribers, daemon=True).start()
    logger.info("[STARTUP] Фоновый поток обновления подписчиков запущен")
    logger.info("[STARTUP] Бот запущен и готов к работе!")
    bot.infinity_polling(skip_pending=True)