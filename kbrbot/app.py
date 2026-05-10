import asyncio
import html
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from logging.handlers import RotatingFileHandler
import mimetypes
import os
import re
import shutil
import sqlite3
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Awaitable, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlsplit
from urllib.request import ProxyHandler, Request, build_opener, urlopen

from dotenv import load_dotenv
from telethon.errors import FloodWaitError, MediaCaptionTooLongError, MessageNotModifiedError, MessageTooLongError
from telethon import Button, TelegramClient, events


load_dotenv()

APP_STARTED_AT = datetime.now().astimezone().replace(microsecond=0)
APP_ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    session_name: str
    app_name: str
    app_developer: str
    auto_reply_enabled: bool
    reply_once_per_chat: bool
    default_reply: str
    root_requester_ids: tuple[str, ...]
    cleanup_on_start_enabled: bool
    cleanup_logs_on_start: bool
    cleanup_temp_on_start: bool
    openai_api_key: str
    openai_model: str
    openai_base_url: str
    openai_proxy_url: str
    openai_timeout_seconds: float
    openai_max_output_tokens: int
    openai_reasoning_effort: str
    openai_system_prompt: str
    openai_transcribe_model: str
    openai_voice_language: str
    openai_voice_max_bytes: int
    smart_controller_enabled: bool
    admin_bot_username: str
    admin_command: str
    users_button_text: str
    find_user_button_text: str
    subscriptions_button_text: str
    write_user_button_text: str
    mail_next_button_text: str
    promo_button_text: str
    promo_create_button_text: str
    promo_submit_button_text: str
    promo_success_text: str
    promo_budget_rub: str
    promo_amount_rub: str
    promo_mail_text: str
    cancel_button_text: str
    back_button_text: str
    next_page_button_text: str
    report_dir: str
    database_path: str
    mail_text: str
    mail2_send_delay_seconds: float
    log_file: str
    log_max_bytes: int
    log_backup_count: int
    scan_action_delay_seconds: float
    scan_turbo_delay_seconds: float
    bot_response_timeout_seconds: float
    telegram_proxy_enabled: bool
    telegram_proxy_type: str
    telegram_proxy_host: str
    telegram_proxy_port: int
    telegram_proxy_rdns: bool
    telegram_proxy_username: str
    telegram_proxy_password: str
    wizard_target_username: str
    dashboard_brand_name: str
    dashboard_title: str
    dashboard_subtitle: str
    dashboard_hint_primary: str
    dashboard_hint_secondary: str
    dashboard_hint_tertiary: str
    dashboard_logo_path: str
    dashboard_theme_bg: str
    dashboard_theme_panel: str
    dashboard_theme_panel_soft: str
    dashboard_theme_text: str
    dashboard_theme_muted: str
    dashboard_theme_primary: str
    dashboard_theme_good: str
    dashboard_theme_warn: str
    dashboard_theme_bad: str
    dashboard_theme_border: str
    dashboard_http_enabled: bool
    dashboard_http_host: str
    dashboard_http_port: int
    dashboard_public_base_url: str
    dashboard_public_path_prefix: str
    dashboard_public_token: str
    dashboard_public_dir: str
    dashboard_public_retention: int
    dashboard_intro_enabled: bool
    dashboard_intro_seconds: float
    dashboard_intro_template_path: str


@dataclass(frozen=True)
class UserLookupCommand:
    query: str
    use_database: bool

    @property
    def is_username(self) -> bool:
        return bool(normalize_username(self.query))


@dataclass(frozen=True)
class GPTCommand:
    action: str
    prompt: str


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_text(name: str, default: str) -> str:
    value = os.getenv(name)
    if not value:
        return default

    # Windows consoles sometimes save Cyrillic .env values as mojibake.
    if any(marker in value for marker in ("\u0420\u00a0", "\u0420\u040e", "\u0420\u040f", "\u0421\u20ac", "\u0421\u2039")):
        return default

    return value


def env_list(name: str) -> tuple[str, ...]:
    raw = os.getenv(name, "")
    if not raw:
        return ()
    items = []
    for part in re.split(r"[\s,;]+", raw):
        cleaned = part.strip()
        if cleaned:
            items.append(cleaned)
    return tuple(items)


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return float(value.strip().replace(",", "."))
    except ValueError:
        logging.warning("Invalid %s=%r in .env. Using default: %s", name, value, default)
        return default


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value.strip())
    except ValueError:
        logging.warning("Invalid %s=%r in .env. Using default: %s", name, value, default)
        return default


def env_required_int(name: str) -> int:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Set {name} in .env first. See .env.example.")
    try:
        return int(value.strip())
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer. Current value: {value!r}.") from exc


def env_required_text(name: str) -> str:
    value = os.getenv(name)
    if not value or not value.strip():
        raise RuntimeError(f"Set {name} in .env first. See .env.example.")
    return value.strip()


def normalized_positive_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    value = env_float(name, default)
    if value < minimum or value > maximum:
        logging.warning(
            "Invalid %s=%r in .env. Expected %.2f..%.2f. Using default: %s",
            name,
            value,
            minimum,
            maximum,
            default,
        )
        return default
    return value


def sanitize_hex_color(value: str, default: str) -> str:
    raw = (value or "").strip()
    if re.fullmatch(r"#?[0-9a-fA-F]{6}", raw):
        return "#" + raw.lstrip("#")
    return default


def session_file_path(session_name: str) -> Path:
    path = Path(session_name)
    if path.suffix != ".session":
        path = path.with_suffix(".session")
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def build_telegram_proxy():
    if not settings.telegram_proxy_enabled:
        return None
    try:
        import socks
    except ImportError as error:
        raise RuntimeError(
            "TELEGRAM_PROXY_ENABLED=true, but PySocks is not installed. Run: pip install -r requirements.txt"
        ) from error

    proxy_types = {
        "socks5": socks.SOCKS5,
        "socks4": socks.SOCKS4,
        "http": socks.HTTP,
    }
    proxy_type = proxy_types.get(settings.telegram_proxy_type)
    if proxy_type is None:
        raise RuntimeError("Unsupported TELEGRAM_PROXY_TYPE. Use socks5, socks4, or http.")

    return (
        proxy_type,
        settings.telegram_proxy_host,
        settings.telegram_proxy_port,
        settings.telegram_proxy_rdns,
        settings.telegram_proxy_username or None,
        settings.telegram_proxy_password or None,
    )


def repair_telethon_session_if_needed(session_name: str) -> None:
    path = session_file_path(session_name)
    if not path.exists():
        return

    try:
        with sqlite3.connect(path) as conn:
            columns = [row[1] for row in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            expected = ["dc_id", "server_address", "port", "auth_key", "takeout_id"]
            if columns == expected:
                return
            if columns[:5] != expected:
                logging.warning("Unexpected Telethon session schema in %s: %s", path, columns)
                return

            backup_path = path.with_name(
                f"{path.stem}.backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}{path.suffix}"
            )
            shutil.copy2(path, backup_path)
            conn.executescript(
                """
                CREATE TABLE sessions_fixed (
                    dc_id integer primary key,
                    server_address text,
                    port integer,
                    auth_key blob,
                    takeout_id integer
                );
                INSERT INTO sessions_fixed (dc_id, server_address, port, auth_key, takeout_id)
                    SELECT dc_id, server_address, port, auth_key, takeout_id FROM sessions;
                DROP TABLE sessions;
                ALTER TABLE sessions_fixed RENAME TO sessions;
                DELETE FROM version;
                INSERT INTO version VALUES (7);
                """
            )
            conn.commit()
            logging.warning("Repaired Telethon session schema: %s. Backup: %s", path, backup_path)
    except sqlite3.Error:
        logging.exception("Failed to inspect or repair Telethon session file: %s", path)


def load_settings() -> Settings:
    return Settings(
        api_id=env_required_int("API_ID"),
        api_hash=env_required_text("API_HASH"),
        session_name=os.getenv("SESSION_NAME", "my_profile_session"),
        app_name=env_text("APP_NAME", "Vpn_Bot_assist"),
        app_developer=env_text("APP_DEVELOPER", "DevM29"),
        auto_reply_enabled=env_bool("AUTO_REPLY_ENABLED", True),
        reply_once_per_chat=env_bool("REPLY_ONCE_PER_CHAT", True),
        default_reply=os.getenv(
            "DEFAULT_REPLY",
            "\u041f\u0440\u0438\u0432\u0435\u0442! \u042f \u0441\u0435\u0439\u0447\u0430\u0441 \u0437\u0430\u043d\u044f\u0442, \u043e\u0442\u0432\u0435\u0447\u0443 \u043f\u043e\u0437\u0436\u0435.",
        ),
        root_requester_ids=env_list("ROOT_REQUESTER_IDS"),
        cleanup_on_start_enabled=env_bool("CLEANUP_ON_START_ENABLED", True),
        cleanup_logs_on_start=env_bool("CLEANUP_LOGS_ON_START", True),
        cleanup_temp_on_start=env_bool("CLEANUP_TEMP_ON_START", True),
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5.4-mini").strip() or "gpt-5.4-mini",
        openai_base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").strip().rstrip("/")
        or "https://api.openai.com/v1",
        openai_proxy_url=os.getenv("OPENAI_PROXY_URL", "").strip(),
        openai_timeout_seconds=normalized_positive_float(
            "OPENAI_TIMEOUT_SECONDS",
            60.0,
            minimum=5.0,
            maximum=300.0,
        ),
        openai_max_output_tokens=max(128, min(32768, env_int("OPENAI_MAX_OUTPUT_TOKENS", 2048))),
        openai_reasoning_effort=os.getenv("OPENAI_REASONING_EFFORT", "none").strip().casefold(),
        openai_system_prompt=env_text(
            "OPENAI_SYSTEM_PROMPT",
            "РўС‹ РІСЃС‚СЂРѕРµРЅРЅС‹Р№ РїРѕРјРѕС‰РЅРёРє Vpn_Bot_assist. РћС‚РІРµС‡Р°Р№ РєСЂР°С‚РєРѕ, РїРѕРЅСЏС‚РЅРѕ Рё РїРѕ-СЂСѓСЃСЃРєРё, РµСЃР»Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РїРѕРїСЂРѕСЃРёР» РёРЅР°С‡Рµ.",
        ),
        openai_transcribe_model=os.getenv("OPENAI_TRANSCRIBE_MODEL", "gpt-4o-mini-transcribe").strip()
        or "gpt-4o-mini-transcribe",
        openai_voice_language=os.getenv("OPENAI_VOICE_LANGUAGE", "ru").strip() or "ru",
        openai_voice_max_bytes=max(512_000, env_int("OPENAI_VOICE_MAX_BYTES", 25_000_000)),
        smart_controller_enabled=env_bool("SMART_CONTROLLER_ENABLED", False),
        admin_bot_username=os.getenv("ADMIN_BOT_USERNAME", "vpn_kbr_bot"),
        admin_command=os.getenv("ADMIN_COMMAND", "/admin"),
        users_button_text=env_text("USERS_BUTTON_TEXT", "\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0438"),
        find_user_button_text=env_text("FIND_USER_BUTTON_TEXT", "\u041d\u0430\u0439\u0442\u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f"),
        subscriptions_button_text=env_text("SUBSCRIPTIONS_BUTTON_TEXT", "\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f"),
        write_user_button_text=env_text("WRITE_USER_BUTTON_TEXT", "\u041d\u0430\u043f\u0438\u0441\u0430\u0442\u044c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044e"),
        mail_next_button_text=env_text("MAIL_NEXT_BUTTON_TEXT", "\u0414\u0430\u043b\u0435\u0435"),
        promo_button_text=env_text("PROMO_BUTTON_TEXT", "\u041f\u0440\u043e\u043c\u043e\u043a\u043e\u0434\u044b"),
        promo_create_button_text=env_text("PROMO_CREATE_BUTTON_TEXT", "\u0421\u043e\u0437\u0434\u0430\u0442\u044c \u043f\u0440\u043e\u043c\u043e\u043a\u043e\u0434"),
        promo_submit_button_text=env_text("PROMO_SUBMIT_BUTTON_TEXT", "\u0421\u043e\u0437\u0434\u0430\u0442\u044c"),
        promo_success_text=env_text("PROMO_SUCCESS_TEXT", "\u041f\u0440\u043e\u043c\u043e\u043a\u043e\u0434 \u0443\u0441\u043f\u0435\u0448\u043d\u043e \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d"),
        promo_budget_rub=env_text("PROMO_BUDGET_RUB", "100"),
        promo_amount_rub=env_text("PROMO_AMOUNT_RUB", "100"),
        promo_mail_text=env_text(
            "PROMO_MAIL_TEXT",
            "\u0414\u043b\u044f \u0432\u0430\u0441 \u0441\u043e\u0437\u0434\u0430\u043d \u043f\u0440\u043e\u043c\u043e\u043a\u043e\u0434 {promo_code} \u043d\u0430 {promo_amount} \u0440\u0443\u0431.",
        ),
        cancel_button_text=env_text("CANCEL_BUTTON_TEXT", "\u041e\u0442\u043c\u0435\u043d\u0438\u0442\u044c"),
        back_button_text=env_text("BACK_BUTTON_TEXT", "\u041d\u0430\u0437\u0430\u0434"),
        next_page_button_text=env_text("NEXT_PAGE_BUTTON_TEXT", "\u0414\u0430\u043b\u0435\u0435"),
        report_dir=os.getenv("REPORT_DIR", "reports"),
        database_path=os.getenv("DATABASE_PATH", "scan-data.sqlite3"),
        mail_text=env_text("MAIL_TEXT", "\u0417\u0434\u0440\u0430\u0432\u0441\u0442\u0432\u0443\u0439\u0442\u0435!"),
        mail2_send_delay_seconds=normalized_positive_float(
            "MAIL2_SEND_DELAY_SECONDS",
            0.5,
            minimum=0.0,
            maximum=60.0,
        ),
        log_file=os.getenv("LOG_FILE", "userbot.log"),
        log_max_bytes=max(100_000, env_int("LOG_MAX_BYTES", 5_000_000)),
        log_backup_count=max(1, env_int("LOG_BACKUP_COUNT", 5)),
        scan_action_delay_seconds=normalized_positive_float(
            "SCAN_ACTION_DELAY_SECONDS",
            0.08,
            minimum=0.05,
            maximum=30.0,
        ),
        scan_turbo_delay_seconds=normalized_positive_float(
            "SCAN_TURBO_DELAY_SECONDS",
            0.05,
            minimum=0.05,
            maximum=30.0,
        ),
        bot_response_timeout_seconds=normalized_positive_float(
            "BOT_RESPONSE_TIMEOUT_SECONDS",
            45.0,
            minimum=5.0,
            maximum=900.0,
        ),
        telegram_proxy_enabled=env_bool("TELEGRAM_PROXY_ENABLED", False),
        telegram_proxy_type=os.getenv("TELEGRAM_PROXY_TYPE", "socks5").strip().casefold(),
        telegram_proxy_host=os.getenv("TELEGRAM_PROXY_HOST", "127.0.0.1").strip(),
        telegram_proxy_port=max(1, min(65535, env_int("TELEGRAM_PROXY_PORT", 1080))),
        telegram_proxy_rdns=env_bool("TELEGRAM_PROXY_RDNS", True),
        telegram_proxy_username=os.getenv("TELEGRAM_PROXY_USERNAME", "").strip(),
        telegram_proxy_password=os.getenv("TELEGRAM_PROXY_PASSWORD", "").strip(),
        wizard_target_username=os.getenv("WIZARD_TARGET_USERNAME", "wizardvpn_manager"),
        dashboard_brand_name=env_text("DASHBOARD_BRAND_NAME", env_text("APP_NAME", "Vpn_Bot_assist")),
        dashboard_title=env_text("DASHBOARD_TITLE", "РџРѕРЅСЏС‚РЅС‹Р№ РѕС‚С‡С‘С‚ РїРѕ РїРѕРґРїРёСЃРєР°Рј"),
        dashboard_subtitle=env_text(
            "DASHBOARD_SUBTITLE",
            "РџСЂРѕСЃС‚Рѕ СЃРјРѕС‚СЂРё РЅР° С†РёС„СЂС‹: СЃРєРѕР»СЊРєРѕ Р»СЋРґРµР№, СЃРєРѕР»СЊРєРѕ РїРѕРґРїРёСЃРѕРє Рё СЃРєРѕР»СЊРєРѕ РґРµРЅРµРі Р¶РґС‘Рј.",
        ),
        dashboard_hint_primary=env_text(
            "DASHBOARD_HINT_PRIMARY",
            "1) РЎРјРѕС‚СЂРё РєР°СЂС‚РѕС‡РєСѓ В«Р”РѕС…РѕРґ РІ СЃР»РµРґСѓСЋС‰РµРј РјРµСЃСЏС†РµВ» вЂ” СЌС‚Рѕ РіР»Р°РІРЅР°СЏ СЃСѓРјРјР°.",
        ),
        dashboard_hint_secondary=env_text(
            "DASHBOARD_HINT_SECONDARY",
            "2) Р‘Р»РѕРє В«Р—Р°РєР°РЅС‡РёРІР°РµС‚СЃСЏ СЃРєРѕСЂРѕВ» РїРѕРєР°Р·С‹РІР°РµС‚, СЃ РєРµРј СЃРІСЏР·Р°С‚СЊСЃСЏ РІ РїРµСЂРІСѓСЋ РѕС‡РµСЂРµРґСЊ.",
        ),
        dashboard_hint_tertiary=env_text(
            "DASHBOARD_HINT_TERTIARY",
            "3) Р“СЂР°С„РёРєРё РЅРёР¶Рµ РїРѕРєР°Р·С‹РІР°СЋС‚ СЂРѕСЃС‚: СЃРїР»РѕС€РЅР°СЏ Р»РёРЅРёСЏ вЂ” РїСЂРѕС€Р»РѕРµ, РїСѓРЅРєС‚РёСЂ вЂ” РїСЂРѕРіРЅРѕР·.",
        ),
        dashboard_logo_path=os.getenv("DASHBOARD_LOGO_PATH", "").strip(),
        dashboard_theme_bg=env_text("DASHBOARD_THEME_BG", "#0b1020"),
        dashboard_theme_panel=env_text("DASHBOARD_THEME_PANEL", "#141a30"),
        dashboard_theme_panel_soft=env_text("DASHBOARD_THEME_PANEL_SOFT", "#1b2340"),
        dashboard_theme_text=env_text("DASHBOARD_THEME_TEXT", "#edf1ff"),
        dashboard_theme_muted=env_text("DASHBOARD_THEME_MUTED", "#aeb9d6"),
        dashboard_theme_primary=env_text("DASHBOARD_THEME_PRIMARY", "#56d4ff"),
        dashboard_theme_good=env_text("DASHBOARD_THEME_GOOD", "#34d399"),
        dashboard_theme_warn=env_text("DASHBOARD_THEME_WARN", "#f59e0b"),
        dashboard_theme_bad=env_text("DASHBOARD_THEME_BAD", "#f87171"),
        dashboard_theme_border=env_text("DASHBOARD_THEME_BORDER", "#2a3564"),
        dashboard_http_enabled=env_bool("DASHBOARD_HTTP_ENABLED", False),
        dashboard_http_host=os.getenv("DASHBOARD_HTTP_HOST", "0.0.0.0").strip() or "0.0.0.0",
        dashboard_http_port=max(1, min(65535, env_int("DASHBOARD_HTTP_PORT", 8088))),
        dashboard_public_base_url=os.getenv("DASHBOARD_PUBLIC_BASE_URL", "").strip(),
        dashboard_public_path_prefix=os.getenv("DASHBOARD_PUBLIC_PATH_PREFIX", "dashboard").strip().strip("/") or "dashboard",
        dashboard_public_token=os.getenv("DASHBOARD_PUBLIC_TOKEN", "").strip().strip("/"),
        dashboard_public_dir=os.getenv("DASHBOARD_PUBLIC_DIR", "reports/public").strip() or "reports/public",
        dashboard_public_retention=max(1, env_int("DASHBOARD_PUBLIC_RETENTION", 30)),
        dashboard_intro_enabled=env_bool("DASHBOARD_INTRO_ENABLED", True),
        dashboard_intro_seconds=normalized_positive_float(
            "DASHBOARD_INTRO_SECONDS",
            5.0,
            minimum=0.5,
            maximum=30.0,
        ),
        dashboard_intro_template_path=os.getenv(
            "DASHBOARD_INTRO_TEMPLATE_PATH",
            "remotion-plugin-remotion-openai-curated-vpn/index.html",
        ).strip(),
    )


settings = load_settings()
repair_telethon_session_if_needed(settings.session_name)
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

client = TelegramClient(
    settings.session_name,
    settings.api_id,
    settings.api_hash,
    proxy=build_telegram_proxy(),
    loop=loop,
)
already_replied_chat_ids: set[int] = set()
admin_flow_lock = asyncio.Lock()
active_admin_flow: dict[str, object] | None = None
scan_auto_resume_lock = asyncio.Lock()
own_user_id: int | None = None
admin_bot_entity_cache = None
wizard_target_entity_cache = None
SCAN_ACTION_DELAY_SECONDS = settings.scan_action_delay_seconds
active_scan_action_delay_seconds = SCAN_ACTION_DELAY_SECONDS
active_scan_base_delay_seconds = SCAN_ACTION_DELAY_SECONDS
SCAN_CANCEL_CALLBACK_DATA = b"scan_cancel"
POC_REFRESH_CALLBACK_DATA = b"poc:refresh"
POC_SCAN_PAUSE_CALLBACK_DATA = b"poc:scan_pause"
POC_MAIL2_STOP_CALLBACK_DATA = b"poc:mail2_stop"
POC_CLEAR_WIZARD_CALLBACK_DATA = b"poc:clear_wizard"
POC_CLEAR_MAIL2_PENDING_CALLBACK_DATA = b"poc:clear_mail2_pending"
POC_CLEAR_GPT_PENDING_CALLBACK_DATA = b"poc:clear_gpt_pending"
POC_CLEAR_ALL_PENDING_CALLBACK_DATA = b"poc:clear_all_pending"
active_scan_cancel_event: asyncio.Event | None = None
active_scan_owner_id: int | None = None
active_scan_menu_owner_id: int | None = None
active_scan_reset_requested = False
active_scan_auto_resume_task: asyncio.Task | None = None
active_mail2_cancel_event: asyncio.Event | None = None
pending_wizard_requests: dict[int, dict[str, object]] = {}
pending_mail2_requests: dict[int, dict[str, object]] = {}
pending_gpt_requests: dict[int, dict[str, object]] = {}
pending_smart_actions: dict[int, dict[str, object]] = {}
pending_support_requests: dict[int, dict[str, object]] = {}
pending_direct_mail_requests: dict[int, dict[str, object]] = {}
last_reply_sent_at_by_chat: dict[int, float] = {}
last_reply_sent_at_lock = asyncio.Lock()
active_gpt_requests: dict[int, dict[str, object]] = {}
gpt_waiting_request_ids: list[str] = []
gpt_response_cache: dict[str, tuple[float, str]] = {}
gpt_chat_sessions: dict[int, str] = {}
gpt_request_lock = asyncio.Lock()
ProgressCallback = Callable[[str], Awaitable[None]]
logging_is_configured = False
runtime_version_logged = False
startup_cleanup_done = False
dashboard_http_server: ThreadingHTTPServer | None = None
dashboard_http_thread: threading.Thread | None = None
dashboard_intro_template_cache: tuple[Path, float, str] | None = None
dashboard_action_jobs: dict[str, dict[str, object]] = {}
dashboard_action_jobs_lock = threading.Lock()
DASHBOARD_ACTION_JOBS_LIMIT = 300
STATUS_EDIT_MIN_INTERVAL_SECONDS = max(0.25, env_float("STATUS_EDIT_MIN_INTERVAL_SECONDS", 0.7))
status_edit_state: dict[int, tuple[float, str]] = {}
ADMIN_CONVERSATION_MAX_MESSAGES = max(5000, env_int("ADMIN_CONVERSATION_MAX_MESSAGES", 120000))
TELEGRAM_SAFE_TEXT_LIMIT = 3500
SCAN_MAX_CONSECUTIVE_FAILURES = max(1, env_int("SCAN_MAX_CONSECUTIVE_FAILURES", 25))
SCAN_RECOVERY_RETRY_ATTEMPTS = max(1, env_int("SCAN_RECOVERY_RETRY_ATTEMPTS", 3))
SCAN_RECOVERY_RETRY_DELAY_SECONDS = max(0.2, env_float("SCAN_RECOVERY_RETRY_DELAY_SECONDS", 2.0))
SCAN_SESSION_RESTART_DELAY_SECONDS = max(1.0, env_float("SCAN_SESSION_RESTART_DELAY_SECONDS", 5.0))
SCAN_MAX_SESSION_RESTARTS = max(1, env_int("SCAN_MAX_SESSION_RESTARTS", 1000))
BOT_HEALTH_POLL_INTERVAL_SECONDS = max(10.0, env_float("BOT_HEALTH_POLL_INTERVAL_SECONDS", 60.0))
BOT_POLL_INTERVAL_SECONDS = max(0.05, env_float("BOT_POLL_INTERVAL_SECONDS", 0.2))
POST_ACTION_SETTLE_SECONDS = max(0.0, env_float("POST_ACTION_SETTLE_SECONDS", 0.0))
PROMO_CONFIRM_HISTORY_LIMIT = max(100, env_int("PROMO_CONFIRM_HISTORY_LIMIT", 1000))
PROMO_AFTER_SUBMIT_SETTLE_SECONDS = max(0.3, env_float("PROMO_AFTER_SUBMIT_SETTLE_SECONDS", 2.0))
PENDING_REQUEST_TTL_SECONDS = max(300, env_int("PENDING_REQUEST_TTL_SECONDS", 1800))
TELEGRAM_REPLY_MIN_INTERVAL_SECONDS = max(0.0, env_float("TELEGRAM_REPLY_MIN_INTERVAL_SECONDS", 0.45))
LOG_TAIL_DEFAULT_LINES = max(10, env_int("LOG_TAIL_DEFAULT_LINES", 80))
LOG_TAIL_MAX_LINES = max(LOG_TAIL_DEFAULT_LINES, env_int("LOG_TAIL_MAX_LINES", 250))
ADMIN_FLOW_WAIT_NOTICE_SECONDS = max(1.0, env_float("ADMIN_FLOW_WAIT_NOTICE_SECONDS", 2.0))
ADMIN_FLOW_MAX_WAIT_SECONDS = max(30.0, env_float("ADMIN_FLOW_MAX_WAIT_SECONDS", 180.0))
FORECAST_PRICE_PER_SUBSCRIPTION_RUB = env_float("FORECAST_PRICE_PER_SUBSCRIPTION_RUB", 100.0)
FORECAST_RENEWAL_RATE_7_DAYS = env_float("FORECAST_RENEWAL_RATE_7_DAYS", 0.70)
FORECAST_RENEWAL_RATE_30_DAYS = env_float("FORECAST_RENEWAL_RATE_30_DAYS", 0.70)
FORECAST_WINBACK_RATE_EXPIRED = env_float("FORECAST_WINBACK_RATE_EXPIRED", 0.18)
MAX_SCAN_ACTION_DELAY_SECONDS = 2.5
SCAN_CHECKPOINT_USER_INTERVAL = max(1, env_int("SCAN_CHECKPOINT_USER_INTERVAL", 6))
SCAN_CHECKPOINT_MIN_INTERVAL_SECONDS = max(2.0, env_float("SCAN_CHECKPOINT_MIN_INTERVAL_SECONDS", 10.0))
STATUS_COMPACT_MODE = env_bool("STATUS_COMPACT_MODE", True)
ACTION_LOG_PREVIEW_LIMIT = max(120, env_int("ACTION_LOG_PREVIEW_LIMIT", 1200))
GPT_CACHE_TTL_SECONDS = max(0.0, env_float("GPT_CACHE_TTL_SECONDS", 1800.0))
GPT_CACHE_MAX_ITEMS = max(0, env_int("GPT_CACHE_MAX_ITEMS", 300))
GPT_QUEUE_WAIT_SECONDS_PER_REQUEST = max(5.0, env_float("GPT_QUEUE_WAIT_SECONDS_PER_REQUEST", 20.0))


class ScanCancelledError(Exception):
    pass


admin_bot_health = {
    "emoji": "[WAIT]",
    "status": "РїСЂРѕРІРµСЂРєР°",
    "detail": "РµС‰С‘ РЅРµ РїСЂРѕРІРµСЂСЏР»",
    "updated_at": "-",
}


def set_admin_bot_health(emoji: str, status: str, detail: str = "") -> None:
    admin_bot_health.update(
        {
            "emoji": emoji,
            "status": status,
            "detail": detail,
            "updated_at": datetime.now().strftime("%H:%M:%S"),
        }
    )


def format_admin_bot_health() -> str:
    detail = str(admin_bot_health.get("detail") or "")
    suffix = f" - {detail}" if detail else ""
    marker = str(admin_bot_health.get("emoji", "[WAIT]"))
    status = str(admin_bot_health.get("status", "РїСЂРѕРІРµСЂРєР°"))
    if marker in {"[WAIT]", "[WAIT]"}:
        marker = animated_symbol("waiting")
    elif marker in {"[OK]", "[OK]"}:
        marker = animated_symbol("ok")
    elif marker in {"[ERR]", "[ERR]"}:
        marker = animated_symbol("error")
    return (
        f"{marker} "
        f"{status}"
        f"{suffix}"
        f" ({admin_bot_health.get('updated_at', '-')})"
    )


def animated_symbol(kind: str) -> str:
    symbols = {
        "scan": "RUN",
        "waiting": "WAIT",
        "ok": "OK",
        "error": "ERROR",
        "done": "DONE",
        "pause": "PAUSE",
    }
    return f"[{symbols.get(kind, symbols['scan'])}]"


def action_log_path() -> Path:
    log_path = application_log_path()
    suffix = log_path.suffix or ".log"
    return log_path.with_name(f"{log_path.stem}-actions{suffix}.jsonl")


def action_log_preview(value: object, *, limit: int = ACTION_LOG_PREVIEW_LIMIT) -> object:
    if value is None:
        return None
    text = str(value)
    if len(text) <= limit:
        return text
    return text[:limit] + "...<trimmed>"


def append_action_log(entry: dict[str, object]) -> None:
    path = action_log_path()
    if path.parent != Path("."):
        path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def log_action_event(kind: str, **fields: object) -> None:
    entry: dict[str, object] = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "kind": kind,
    }
    for key, value in fields.items():
        if isinstance(value, (str, bytes)):
            entry[key] = action_log_preview(value.decode("utf-8", "replace") if isinstance(value, bytes) else value)
        elif isinstance(value, (int, float, bool)) or value is None:
            entry[key] = value
        else:
            entry[key] = action_log_preview(value)
    try:
        append_action_log(entry)
    except Exception:
        logging.exception("Failed to write action log kind=%s", kind)


def decorate_status_title(title: str, *, done: bool = False, failed: bool = False, paused: bool = False) -> str:
    if failed:
        return f"{animated_symbol('error')} {title}"
    if done:
        return f"{animated_symbol('done')} {title}"
    if paused:
        return f"{animated_symbol('pause')} {title}"
    return title


def note_floodwait(wait_seconds: int) -> None:
    global active_scan_action_delay_seconds
    wait_seconds = max(1, int(wait_seconds))
    target_delay = min(
        MAX_SCAN_ACTION_DELAY_SECONDS,
        active_scan_base_delay_seconds + min(wait_seconds / 180.0, 1.8),
    )
    if target_delay > active_scan_action_delay_seconds:
        active_scan_action_delay_seconds = target_delay
        logging.warning(
            "Adaptive throttle: increased action delay to %.2fs after FloodWait=%ss",
            active_scan_action_delay_seconds,
            wait_seconds,
        )


def note_success_action() -> None:
    global active_scan_action_delay_seconds
    base = active_scan_base_delay_seconds
    if active_scan_action_delay_seconds <= base:
        return
    active_scan_action_delay_seconds = max(base, active_scan_action_delay_seconds - 0.03)


SEARCH_STEPS = [
    "РџРѕРґРєР»СЋС‡Р°СЋСЃСЊ Рє Р°РґРјРёРЅ-Р±РѕС‚Сѓ",
    "РћС‚РєСЂС‹РІР°СЋ СЂР°Р·РґРµР» РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№",
    "С‰Сѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РїРѕ ID",
    "РћС‚РєСЂС‹РІР°СЋ РїРѕРґРїРёСЃРєРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
    "Р¤РѕСЂРјРёСЂСѓСЋ РѕС‚РІРµС‚",
]
INFO_STEPS = [
    "РџРѕРґРєР»СЋС‡Р°СЋСЃСЊ Рє Р°РґРјРёРЅ-Р±РѕС‚Сѓ",
    "РћС‚РєСЂС‹РІР°СЋ СЂР°Р·РґРµР» РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№",
    "С‰Сѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РїРѕ ID",
    "РћС‚РєСЂС‹РІР°СЋ СЃРїРёСЃРѕРє РїРѕРґРїРёСЃРѕРє",
    "Р§РёС‚Р°СЋ РїРѕРґСЂРѕР±РЅРѕСЃС‚Рё РєР°Р¶РґРѕР№ РїРѕРґРїРёСЃРєРё",
    "Р¤РѕСЂРјРёСЂСѓСЋ РїРѕР»РЅС‹Р№ РѕС‚С‡РµС‚",
]
MAIL_STEPS = [
    "РџРѕРґРєР»СЋС‡Р°СЋСЃСЊ Рє Р°РґРјРёРЅ-Р±РѕС‚Сѓ",
    "РћС‚РєСЂС‹РІР°СЋ СЂР°Р·РґРµР» РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№",
    "С‰Сѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РїРѕ ID",
    "РћС‚РєСЂС‹РІР°СЋ С„РѕСЂРјСѓ СЃРѕРѕР±С‰РµРЅРёСЏ",
    "РџРµСЂРµРґР°СЋ С‚РµРєСЃС‚ РїРёСЃСЊРјР°",
    "РџРѕРґС‚РІРµСЂР¶РґР°СЋ РѕС‚РїСЂР°РІРєСѓ",
]
MAIL2_STEPS = [
    "Р§РёС‚Р°СЋ SQLite Р±Р°Р·Сѓ",
    "С‰Сѓ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ Р±РµР· РїРѕРґРїРёСЃРєРё",
    "Р“РѕС‚РѕРІР»СЋ С‚РµРєСЃС‚ СЂР°СЃСЃС‹Р»РєРё",
    "РћС‚РїСЂР°РІР»СЏСЋ СЃРѕРѕР±С‰РµРЅРёСЏ С‡РµСЂРµР· mail",
    "Р¤РѕСЂРјРёСЂСѓСЋ РёС‚РѕРіРѕРІС‹Р№ РѕС‚С‡РµС‚",
]
PROMO_STEPS = [
    "РџРѕРґРєР»СЋС‡Р°СЋСЃСЊ Рє Р°РґРјРёРЅ-Р±РѕС‚Сѓ",
    "РћС‚РєСЂС‹РІР°СЋ СЂР°Р·РґРµР» РїСЂРѕРјРѕРєРѕРґРѕРІ",
    "РћС‚РєСЂС‹РІР°СЋ СЃРѕР·РґР°РЅРёРµ РїСЂРѕРјРѕРєРѕРґР°",
    "Р’РІРѕР¶Сѓ РЅР°Р·РІР°РЅРёРµ РїСЂРѕРјРѕРєРѕРґР°",
    "Р’РІРѕР¶Сѓ Р±СЋРґР¶РµС‚",
    "Р’РІРѕР¶Сѓ СЃСѓРјРјСѓ РїСЂРѕРјРѕРєРѕРґР°",
    "РџРѕРґС‚РІРµСЂР¶РґР°СЋ СЃРѕР·РґР°РЅРёРµ",
    "РћС‚РїСЂР°РІР»СЏСЋ РїСЂРѕРјРѕРєРѕРґ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
]
WIZARD_STEPS = [
    "РџРѕРґРєР»СЋС‡Р°СЋСЃСЊ Рє Р°РґРјРёРЅ-Р±РѕС‚Сѓ",
    "РћС‚РєСЂС‹РІР°СЋ СЂР°Р·РґРµР» РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№",
    "С‰Сѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РїРѕ ID",
    "РћС‚РєСЂС‹РІР°СЋ РїРѕРґРїРёСЃРєРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
    "Р“РѕС‚РѕРІР»СЋ РєР°СЂС‚РѕС‡РєСѓ",
    "Р–РґСѓ РѕС‚РІРµС‚: 1 РѕС‚РїСЂР°РІРёС‚СЊ, 2 РґРѕР±Р°РІРёС‚СЊ, 0 РѕС‚РјРµРЅР°",
    "РћС‚РїСЂР°РІР»СЏСЋ РІ wizard",
]
GPT_STEPS = [
    "РџСЂРѕРІРµСЂСЏСЋ РЅР°СЃС‚СЂРѕР№РєРё OpenAI",
    "РћС‚РїСЂР°РІР»СЏСЋ РІРѕРїСЂРѕСЃ РІ KBR_GPT",
    "Р§РёС‚Р°СЋ РѕС‚РІРµС‚ РјРѕРґРµР»Рё",
    "РћС‚РїСЂР°РІР»СЏСЋ РѕС‚РІРµС‚ РІ С‡Р°С‚",
]
SMART_STEPS = [
    "РџСЂРёРЅРёРјР°СЋ Р·Р°РїСЂРѕСЃ",
    "Р Р°СЃРїРѕР·РЅР°СЋ РіРѕР»РѕСЃ",
    "РџРѕРЅРёРјР°СЋ РЅР°РјРµСЂРµРЅРёРµ С‡РµСЂРµР· KBR_GPT",
    "Р—Р°РїСѓСЃРєР°СЋ РЅСѓР¶РЅРѕРµ РґРµР№СЃС‚РІРёРµ",
]

SUPPORT_OPERATOR_USERNAME = (os.getenv("SUPPORT_OPERATOR_USERNAME", "Aloneinthepluto").strip().lstrip("@") or "Aloneinthepluto")
VIRTUAL_ASSISTANT_NAME = "VPN_KBR"
VIRTUAL_ASSISTANT_INTRO = f"РЇ РІРёСЂС‚СѓР°Р»СЊРЅС‹Р№ РїРѕРјРѕС‰РЅРёРє {VIRTUAL_ASSISTANT_NAME}."
MOJIBAKE_MARKERS = (
    "\u0420\u045f",
    "\u0420\u00a0",
    "\u0420\u045a",
    "\u0420\u040c",
    "\u0420\u0403",
    "\u0420\u2014",
    "\u0420\u201c",
    "\u0420\u201d",
    "\u0420\u045b",
    "\u0420\u02dc",
    "\u0420\u0408",
    "\u0420\u0459",
    "\u0420\u2018",
    "\u0420\u2019",
    "\u0420\u00a4",
    "\u0420\u00a7",
    "\u0420\u0401",
    "\u0420\u0407",
    "\u0420\u00b0",
    "\u0420\u00b5",
    "\u0420\u0451",
    "\u0420\u0455",
    "\u0420\u0405",
    "\u0420\u0457",
    "\u0421\u0402",
    "\u0421\u0453",
    "\u0421\u201a",
    "\u0421\u040a",
    "\u0421\u2039",
    "\u0421\u040f",
    "\u0421\u045a",
    "\u0421\u20ac",
    "\u0421\u2030",
    "\u0432\u0402",
    "\u0432\u201e",
    "\u0432\u201a",
)


def cyrillic_letters_count(text: str) -> int:
    return sum(1 for char in text if ("Рђ" <= char <= "СЏ") or char in {"РЃ", "С‘"})


def mojibake_score(text: str) -> int:
    return sum(text.count(marker) for marker in MOJIBAKE_MARKERS)


def looks_like_mojibake_text(text: str) -> bool:
    sample = str(text or "")
    return mojibake_score(sample) >= 2 or "\u0420\u00a7\u0420\u00b5\u0420\u0458" in sample or "\u0432\u0402\u201d" in sample


def repair_mojibake_text(text: str) -> str:
    original = str(text or "")
    if not original or not looks_like_mojibake_text(original):
        return original

    def repair_piece(piece: str) -> str:
        candidates = [piece]
        for encoding in ("cp1251", "latin1"):
            try:
                candidate = piece.encode(encoding, errors="ignore").decode("utf-8", errors="ignore")
            except Exception:
                continue
            if candidate:
                candidates.append(candidate)

        best = piece
        best_value = (cyrillic_letters_count(piece) * 2) - (mojibake_score(piece) * 8)
        for candidate in candidates[1:]:
            value = (cyrillic_letters_count(candidate) * 2) - (mojibake_score(candidate) * 8)
            if value > best_value:
                best = candidate
                best_value = value
        return best

    repaired_lines = []
    for line in original.splitlines(keepends=True):
        repaired_lines.append(repair_piece(line) if looks_like_mojibake_text(line) else line)
    repaired = "".join(repaired_lines)
    return repair_piece(repaired) if looks_like_mojibake_text(repaired) else repaired


def sanitize_outgoing_text(text: str) -> str:
    repaired = repair_mojibake_text(str(text or ""))
    return repaired.replace("\r\n", "\n").replace("\r", "\n")


def sanitize_outgoing_payload(value):
    if isinstance(value, str):
        return sanitize_outgoing_text(value)
    if isinstance(value, list):
        return [sanitize_outgoing_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize_outgoing_payload(item) for item in value)
    if isinstance(value, dict):
        return {key: sanitize_outgoing_payload(item) for key, item in value.items()}
    return value


def assistant_user_message(text: str) -> str:
    body = sanitize_outgoing_text(text).strip()
    if not body:
        return "Чем могу помочь?"
    if body.startswith(VIRTUAL_ASSISTANT_INTRO):
        return body[len(VIRTUAL_ASSISTANT_INTRO):].strip() or "Чем могу помочь?"
    return body


def assistant_compact_reply(headline: str, detail: str = "") -> str:
    lines = [str(headline or "").strip()]
    detail_text = str(detail or "").strip()
    if detail_text:
        lines.append(detail_text)
    return assistant_user_message("\n".join(line for line in lines if line))


def assistant_list_reply(headline: str, items: list[str], closing: str = "") -> str:
    lines = [str(headline or "").strip()]
    lines.extend(str(item).strip() for item in items if str(item).strip())
    closing_text = str(closing or "").strip()
    if closing_text:
        lines.append(closing_text)
    return assistant_user_message("\n".join(line for line in lines if line))


# Compact, calm user-facing replies inspired by mature support flows.
SUPPORT_TICKET_ACCEPTED_MESSAGE = assistant_compact_reply(
    "РЎРїР°СЃРёР±Рѕ, РѕР±СЂР°С‰РµРЅРёРµ РїСЂРёРЅСЏС‚Рѕ.",
    "РЇ РїРµСЂРµРґР°Р» РµРіРѕ РІ РїРѕРґРґРµСЂР¶РєСѓ.",
)


def support_processing_message() -> str:
    return assistant_compact_reply("РџСЂРёРЅСЏР» Р·Р°РїСЂРѕСЃ.", "РџСЂРѕРІРµСЂСЏСЋ РґР°РЅРЅС‹Рµ.")


def support_voice_processing_message() -> str:
    return assistant_compact_reply("РџСЂРёРЅСЏР» РіРѕР»РѕСЃРѕРІРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ.", "РџРµСЂРµРІРѕР¶Сѓ РµРіРѕ РІ С‚РµРєСЃС‚.")


def gpt_processing_message() -> str:
    return assistant_compact_reply("Р—Р°РїСЂРѕСЃ РїСЂРёРЅСЏС‚.", "Р“РѕС‚РѕРІР»СЋ РѕС‚РІРµС‚.")


def gpt_retry_message(wait_seconds: float) -> str:
    seconds = max(1, int(round(wait_seconds)))
    return assistant_compact_reply(
        "Р—Р°РїСЂРѕСЃ РІ СЂР°Р±РѕС‚Рµ.",
        f"РЎРµСЂРІРёСЃ СЃРµР№С‡Р°СЃ Р·Р°РЅСЏС‚. РћР¶РёРґР°СЋ СЃРІРѕР±РѕРґРЅРѕРµ РѕРєРЅРѕ, СЌС‚Рѕ РјРѕР¶РµС‚ Р·Р°РЅСЏС‚СЊ РѕРєРѕР»Рѕ {seconds} СЃРµРє.",
    )


def format_retry_after_text(seconds: float) -> str:
    seconds_int = max(1, int(round(seconds)))
    minutes, rest = divmod(seconds_int, 60)
    hours, minutes = divmod(minutes, 60)
    parts = []
    if hours:
        parts.append(f"{hours} С‡")
    if minutes:
        parts.append(f"{minutes} РјРёРЅ")
    if rest and not hours:
        parts.append(f"{rest} СЃРµРє")
    return " ".join(parts) or f"{seconds_int} СЃРµРє"


def gpt_daily_limit_message(error_text: str = "") -> str:
    retry_after = parse_retry_seconds_from_error_text(error_text, default_seconds=0)
    detail = "Р”РЅРµРІРЅРѕР№ Р»РёРјРёС‚ Р·Р°РїСЂРѕСЃРѕРІ OpenAI РґР»СЏ С‚РµРєСѓС‰РµРіРѕ РєР»СЋС‡Р° РёСЃС‡РµСЂРїР°РЅ."
    if retry_after > 0:
        detail += f" РќРѕРІС‹Р№ Р·Р°РїСЂРѕСЃ РјРѕР¶РЅРѕ РїСЂРѕР±РѕРІР°С‚СЊ РїСЂРёРјРµСЂРЅРѕ С‡РµСЂРµР· {format_retry_after_text(retry_after)}."
    return assistant_compact_reply(
        "KBR_GPT РІСЂРµРјРµРЅРЅРѕ РЅРµРґРѕСЃС‚СѓРїРµРЅ.",
        detail,
    )


def gpt_timeout_wait_message(wait_seconds: float) -> str:
    seconds = max(1, int(round(wait_seconds)))
    return assistant_compact_reply(
        "РћР¶РёРґР°РЅРёРµ KBR_GPT.",
        f"РўР°Р№РјР°СѓС‚ РѕС‚РІРµС‚Р°. РџРѕРІС‚РѕСЂСЋ Р·Р°РїСЂРѕСЃ С‡РµСЂРµР· {seconds} СЃРµРє.",
    )


def gpt_unavailable_message() -> str:
    return assistant_compact_reply("РЎРµСЂРІРёСЃ РЅРµ РЅР°СЃС‚СЂРѕРµРЅ.", "РћС‚РІРµС‚РёС‚СЊ СЃРµР№С‡Р°СЃ РЅРµ СЃРјРѕРіСѓ.")


def gpt_public_fallback_message() -> str:
    return assistant_compact_reply(
        "РђРІС‚РѕРѕС‚РІРµС‚ СЃРµР№С‡Р°СЃ РЅРµРґРѕСЃС‚СѓРїРµРЅ.",
        "Р•СЃР»Рё РІРѕРїСЂРѕСЃ РїРѕ VPN, РЅР°РїРёС€РёС‚Рµ ID РёР· СЂР°Р·РґРµР»Р° В«РџСЂРѕС„РёР»СЊВ» Рё РєРѕСЂРѕС‚РєРѕ РѕРїРёС€РёС‚Рµ РїСЂРѕР±Р»РµРјСѓ.",
    )


def classify_gpt_failure_reason(error_text: str) -> str:
    lowered = str(error_text or "").casefold()
    if "openai_api_key is not configured" in lowered or "api key" in lowered and "not configured" in lowered:
        return "missing_key"
    if "requests per day" in lowered or " rpd" in lowered or "(rpd)" in lowered:
        return "daily_limit"
    if "rate limit" in lowered or "too many requests" in lowered or "api error 429" in lowered:
        return "rate_limit"
    if "only accessible over https" in lowered or ("http error 403" in lowered and "openai" in lowered):
        return "proxy_https"
    if "tcp_connect_failed" in lowered:
        return "tcp_blocked"
    if "getaddrinfo failed" in lowered or "name or service not known" in lowered or "temporary failure in name resolution" in lowered:
        return "dns"
    if "remote end closed connection without response" in lowered:
        return "connection"
    if "openai connection error" in lowered or "urlopen error" in lowered:
        return "network"
    if "timed out" in lowered or "timeout" in lowered:
        return "timeout"
    return "unknown"


def is_timeout_error_text(error_text: str) -> bool:
    return classify_gpt_failure_reason(error_text) == "timeout"


def is_daily_limit_error_text(error_text: str) -> bool:
    return classify_gpt_failure_reason(error_text) == "daily_limit"


def gpt_failed_message(error_text: str = "") -> str:
    reason = classify_gpt_failure_reason(error_text)
    if reason == "missing_key":
        return assistant_compact_reply(
            "KBR_GPT РЅРµ РЅР°СЃС‚СЂРѕРµРЅ.",
            "РќР° СЃРµСЂРІРµСЂРµ РЅРµ Р·Р°РґР°РЅ OPENAI_API_KEY.",
        )
    if reason == "daily_limit":
        return gpt_daily_limit_message(error_text)
    if reason == "rate_limit":
        return assistant_compact_reply(
            "KBR_GPT РІСЂРµРјРµРЅРЅРѕ РїРµСЂРµРіСЂСѓР¶РµРЅ.",
            "РЎРµР№С‡Р°СЃ СѓРїРµСЂР»РёСЃСЊ РІ Р»РёРјРёС‚ Р·Р°РїСЂРѕСЃРѕРІ. РџРѕРїСЂРѕР±СѓР№С‚Рµ С‡СѓС‚СЊ РїРѕР·Р¶Рµ.",
        )
    if reason == "proxy_https":
        return assistant_compact_reply(
            "РћС€РёР±РєР° СЃРѕРµРґРёРЅРµРЅРёСЏ KBR_GPT.",
            "Р—Р°РїСЂРѕСЃ РґРѕС€РµР» РґРѕ OpenAI, РЅРѕ РїСЂРѕРєСЃРё РёР»Рё РјР°СЂС€СЂСѓС‚ РІРµСЂРЅСѓР» РѕС€РёР±РєСѓ HTTPS. РџСЂРѕРІРµСЂСЊС‚Рµ xray Рё OPENAI_PROXY_URL.",
        )
    if reason == "dns":
        return assistant_compact_reply(
            "РћС€РёР±РєР° СЃРѕРµРґРёРЅРµРЅРёСЏ KBR_GPT.",
            "РЎРµР№С‡Р°СЃ РїСЂРѕР±Р»РµРјР° СЃ DNS РёР»Рё РґРѕСЃС‚СѓРїРѕРј Рє СЃРµС‚Рё РЅР° СЃРµСЂРІРµСЂРµ.",
        )
    if reason == "tcp_blocked":
        return assistant_compact_reply(
            "РћС€РёР±РєР° СЃРѕРµРґРёРЅРµРЅРёСЏ KBR_GPT.",
            "DNS СЂР°Р±РѕС‚Р°РµС‚, РЅРѕ СЃРµСЂРІРµСЂ РЅРµ РјРѕР¶РµС‚ РѕС‚РєСЂС‹С‚СЊ HTTPS-СЃРѕРµРґРёРЅРµРЅРёРµ СЃ OpenAI. РџСЂРѕРІРµСЂСЊС‚Рµ xray РёР»Рё РїСЂРѕРєСЃРё.",
        )
    if reason == "connection":
        return assistant_compact_reply(
            "РћС€РёР±РєР° СЃРѕРµРґРёРЅРµРЅРёСЏ KBR_GPT.",
            "РЎРѕРµРґРёРЅРµРЅРёРµ СЃ OpenAI РѕР±РѕСЂРІР°Р»РѕСЃСЊ РІРѕ РІСЂРµРјСЏ РѕС‚РІРµС‚Р°. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕРІС‚РѕСЂРёС‚СЊ Р·Р°РїСЂРѕСЃ С‡РµСЂРµР· РЅРµСЃРєРѕР»СЊРєРѕ СЃРµРєСѓРЅРґ.",
        )
    if reason == "network":
        return assistant_compact_reply(
            "РћС€РёР±РєР° СЃРѕРµРґРёРЅРµРЅРёСЏ KBR_GPT.",
            "РЎРµР№С‡Р°СЃ РїСЂРѕР±Р»РµРјР° СЃ РїРѕРґРєР»СЋС‡РµРЅРёРµРј СЃРµСЂРІРµСЂР° Рє OpenAI.",
        )
    if reason == "timeout":
        return assistant_compact_reply(
            "KBR_GPT РѕС‚РІРµС‡Р°РµС‚ СЃР»РёС€РєРѕРј РґРѕР»РіРѕ.",
            "РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕРІС‚РѕСЂРёС‚СЊ Р·Р°РїСЂРѕСЃ С‡СѓС‚СЊ РїРѕР·Р¶Рµ.",
        )
    return assistant_compact_reply("РћС€РёР±РєР° KBR_GPT.", "РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕРІС‚РѕСЂРёС‚СЊ Р·Р°РїСЂРѕСЃ С‡СѓС‚СЊ РїРѕР·Р¶Рµ.")


def gpt_escalated_message() -> str:
    return assistant_compact_reply(
        "РќРµ СѓРґР°Р»РѕСЃСЊ Р±С‹СЃС‚СЂРѕ РїРѕР»СѓС‡РёС‚СЊ РѕС‚РІРµС‚.",
        f"РџРµСЂРµРґР°Р» РІРѕРїСЂРѕСЃ РІ РїРѕРґРґРµСЂР¶РєСѓ. Р•СЃР»Рё РЅСѓР¶РЅРѕ СЃСЂРѕС‡РЅРѕ, РЅР°РїРёС€РёС‚Рµ @{SUPPORT_OPERATOR_USERNAME}.",
    )


def requester_mail_text_prompt(user_id: str) -> str:
    return assistant_compact_reply(
        "РџРѕРЅСЏР» Р·Р°РґР°С‡Сѓ.",
        f"РќР°РїРёС€РёС‚Рµ С‚РµРєСЃС‚ СЃРѕРѕР±С‰РµРЅРёСЏ РґР»СЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {user_id}. Р”Р»СЏ РѕС‚РјРµРЅС‹ РѕС‚РїСЂР°РІСЊС‚Рµ `0`.",
    )


def support_thanks_message() -> str:
    return assistant_compact_reply(
        "РџРѕР¶Р°Р»СѓР№СЃС‚Р°.",
        "Р•СЃР»Рё Р±СѓРґРµС‚ РЅСѓР¶РЅРѕ, РїРѕРјРѕРіСѓ СЃ VPN, РѕРїР»Р°С‚РѕР№ РёР»Рё Р»СЋР±С‹Рј РѕР±С‰РёРј РІРѕРїСЂРѕСЃРѕРј.",
    )


def requester_greeting_message() -> str:
    return assistant_compact_reply(
        "Р—РґСЂР°РІСЃС‚РІСѓР№С‚Рµ.",
        "Р§РµРј РјРѕРіСѓ РїРѕРјРѕС‡СЊ?",
    )


def make_progress_bar(done_units: int, total_units: int, width: int = 16) -> tuple[str, int]:
    total_units = max(total_units, 1)
    done_units = max(0, min(done_units, total_units))
    if width <= 0:
        width = max(12, min(30, 12 + len(str(total_units)) * 2))
    percent = int(round((done_units / total_units) * 100))
    filled = int(round((done_units / total_units) * width))
    return f"[{'#' * filled}{'-' * (width - filled)}]", percent


async def emit_process_progress(
    progress_callback: ProgressCallback | None,
    title: str,
    steps: list[str],
    active_step: int,
    *,
    user_id: str | None = None,
    target: str | None = None,
    extra_lines: list[str] | None = None,
    done: bool = False,
    failed: bool = False,
) -> None:
    if not progress_callback:
        return
    await progress_callback(
        build_process_status(
            title,
            steps,
            active_step,
            user_id=user_id,
            target=target,
            extra_lines=extra_lines,
            done=done,
            failed=failed,
        )
    )


def build_process_status(
    title: str,
    steps: list[str],
    active_step: int,
    *,
    user_id: str | None = None,
    target: str | None = None,
    extra_lines: list[str] | None = None,
    done: bool = False,
    failed: bool = False,
) -> str:
    title = sanitize_outgoing_text(title)
    steps = [sanitize_outgoing_text(step) for step in steps]
    status = "РћРЁРР‘РљРђ" if failed else "Р“РћРўРћР’Рћ" if done else "Р’ Р РђР‘РћРўР•"
    total_steps = max(len(steps), 1)
    current_step = max(1, min(active_step, total_steps))
    done_units = total_steps if done else max(current_step - 1 if failed else current_step, 0)
    bar, percent = make_progress_bar(done_units, total_steps, width=0)
    title_text = decorate_status_title(title, done=done, failed=failed)

    lines = [
        title_text,
        f"{bar} {percent}% | РЁРђР“ {current_step}/{total_steps}",
        f"РЎРўРђРўРЈРЎ: {status}",
    ]
    if user_id:
        lines.append(f"ID: {user_id}")
    if target:
        lines.append(f"РљРѕРјСѓ: {target}")

    if not STATUS_COMPACT_MODE:
        step_text = steps[current_step - 1] if steps else title
        lines.append(f"Р”РµР№СЃС‚РІРёРµ: {step_text}")
        if extra_lines:
            lines.extend(sanitize_outgoing_text(str(line)) for line in extra_lines if str(line).strip())
    return sanitize_outgoing_text("\n".join(lines))


def active_admin_flow_text() -> str:
    if not active_admin_flow:
        return "СЃРІРѕР±РѕРґРµРЅ"
    name = str(active_admin_flow.get("name") or "admin")
    user_id = str(active_admin_flow.get("user_id") or "").strip()
    started_at = active_admin_flow.get("started_at")
    try:
        age = format_duration(now_timestamp() - float(started_at))
    except (TypeError, ValueError):
        age = "-"
    suffix = f", user {user_id}" if user_id else ""
    return f"{name}{suffix}, {age}"


@asynccontextmanager
async def admin_flow_context(
    name: str,
    *,
    user_id: str | None = None,
    progress_callback: ProgressCallback | None = None,
    progress_title: str | None = None,
    progress_steps: list[str] | None = None,
    progress_step: int = 1,
):
    global active_admin_flow
    wait_started = loop.time()
    last_notice_at = 0.0
    while admin_flow_lock.locked():
        now = loop.time()
        waited = now - wait_started
        if waited >= ADMIN_FLOW_MAX_WAIT_SECONDS:
            raise RuntimeError(
                f"Admin process is still busy after {ADMIN_FLOW_MAX_WAIT_SECONDS:.0f}s: {active_admin_flow_text()}"
            )
        if now - last_notice_at >= ADMIN_FLOW_WAIT_NOTICE_SECONDS:
            await emit_process_progress(
                progress_callback,
                progress_title or name,
                progress_steps or [name],
                progress_step,
                user_id=user_id,
                extra_lines=[
                    "РђРґРјРёРЅ-РїСЂРѕС†РµСЃСЃ Р·Р°РЅСЏС‚, РѕСЃРІРѕР±РѕР¶РґР°СЋ РѕС‡РµСЂРµРґСЊ.",
                    f"РЎРµР№С‡Р°СЃ РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ: {active_admin_flow_text()}",
                    f"Р–РґСѓ: {format_duration(waited)} / РјР°РєСЃРёРјСѓРј {format_duration(ADMIN_FLOW_MAX_WAIT_SECONDS)}",
                ],
            )
            last_notice_at = now
        await asyncio.sleep(0.25)

    await admin_flow_lock.acquire()
    active_admin_flow = {
        "name": name,
        "user_id": user_id or "",
        "started_at": now_timestamp(),
    }
    logging.info("Admin flow acquired name=%s user_id=%s", name, user_id or "")
    try:
        yield
    finally:
        logging.info("Admin flow released name=%s user_id=%s", name, user_id or "")
        active_admin_flow = None
        admin_flow_lock.release()


def is_final_status_text(text: str) -> bool:
    markers = (
        "РЎРўРђРўРЈРЎ: Р“РћРўРћР’Рћ",
        "РЎРўРђРўРЈРЎ: РћРЁРР‘РљРђ",
        "РЎРўРђРўРЈРЎ: РџРђРЈР—Рђ",
        "Р—Р°СЏРІРєР° РїСЂРёРЅСЏС‚Р° Рё РїРµСЂРµРґР°РЅР° РІ РїРѕРґРґРµСЂР¶РєСѓ",
        "Р—Р°СЏРІРєСѓ РїСЂРёРЅСЏР» Рё РїРµСЂРµРґР°Р» РІ РїРѕРґРґРµСЂР¶РєСѓ",
        "РЎРїР°СЃРёР±Рѕ, РѕР±СЂР°С‰РµРЅРёРµ РїСЂРёРЅСЏС‚Рѕ",
        "РЇ РїРµСЂРµРґР°Р» РµРіРѕ РІ РїРѕРґРґРµСЂР¶РєСѓ",
        "Р“РѕС‚РѕРІРѕ. РћС‚РїСЂР°РІР»СЏСЋ РѕС‚РІРµС‚",
        "РћС‚РІРµС‚ РіРѕС‚РѕРІ.",
        "РќРµ СѓРґР°Р»РѕСЃСЊ Р±С‹СЃС‚СЂРѕ РїРѕР»СѓС‡РёС‚СЊ РѕС‚РІРµС‚.",
        "РќРµ СѓРґР°Р»РѕСЃСЊ",
        "Scan Р·Р°РІРµСЂС€РµРЅ",
        "Scan РЅР° РїР°СѓР·Рµ",
        "Scan СЃР±СЂРѕС€РµРЅ",
    )
    return any(marker in text for marker in markers)


def is_status_like_text(text: str) -> bool:
    cleaned = str(text or "")
    if not cleaned.strip():
        return False
    markers = (
        "РЎРўРђРўРЈРЎ:",
        "РЎС‚Р°С‚СѓСЃ:",
        "РЁРђР“ ",
        "STEP ",
        "Scan РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№",
        "[STATUS]",
        "РѕР¶РёРґР°Р№С‚Рµ",
        "РїРѕРґРѕР¶РґРёС‚Рµ",
        "РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РЅРµРјРЅРѕРіРѕ РїРѕРґРѕР¶РґРёС‚Рµ",
        "РџСЂРёРЅСЏР» Р·Р°РїСЂРѕСЃ.",
        "РџСЂРёРЅСЏР» РіРѕР»РѕСЃРѕРІРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ.",
        "РџСЂРёРЅСЏР» РІРѕРїСЂРѕСЃ.",
        "РЎРµСЂРІРёСЃ СЃРµР№С‡Р°СЃ Р·Р°РЅСЏС‚.",
        "РћС‚РІРµС‚ РіРѕС‚РѕРІ.",
        "РЎРѕР±РёСЂР°СЋ dashboard",
        "Р—Р°СЏРІРєР° РїСЂРёРЅСЏС‚Р° Рё РїРµСЂРµРґР°РЅР° РІ РїРѕРґРґРµСЂР¶РєСѓ",
        "Р—Р°СЏРІРєСѓ РїСЂРёРЅСЏР» Рё РїРµСЂРµРґР°Р» РІ РїРѕРґРґРµСЂР¶РєСѓ",
        "РЎРїР°СЃРёР±Рѕ, РѕР±СЂР°С‰РµРЅРёРµ РїСЂРёРЅСЏС‚Рѕ.",
        "Р—Р°РїСЂРѕСЃ РїРѕРґРґРµСЂР¶РєРё РѕС‚РјРµРЅРµРЅ",
    )
    return any(marker in cleaned for marker in markers)


def extract_scan_position(text: str) -> tuple[int, int] | None:
    patterns = (
        r"РЎРєР°РЅРёСЂРѕРІР°РЅРёРµ РїРѕ ID:\s*(\d+)\s*/\s*(\d+)",
        r"ID\s*(\d+)\s*/\s*(\d+)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        try:
            current = int(match.group(1))
            total = int(match.group(2))
        except ValueError:
            continue
        if current > 0 and total > 0:
            return current, total
    return None


async def edit_status_message(message, text: str, *, buttons=None, parse_mode=None, force: bool = False) -> bool:
    if not message:
        return False
    text = sanitize_outgoing_text(text)
    key = int(getattr(message, "id", 0) or id(message))
    now_monotonic = loop.time()
    last_at, last_text = status_edit_state.get(key, (0.0, ""))
    should_force = force or is_final_status_text(text)
    if not should_force:
        if text == last_text:
            return
        if now_monotonic < last_at:
            return
        if now_monotonic - last_at < STATUS_EDIT_MIN_INTERVAL_SECONDS:
            return
    try:
        await message.edit(text, buttons=buttons, parse_mode=parse_mode)
        status_edit_state[key] = (loop.time(), text)
        log_action_event(
            "status_edit",
            message_id=getattr(message, "id", None),
            text=text,
            parse_mode=parse_mode,
            buttons=bool(buttons),
            result="edited",
        )
        return True
    except MessageNotModifiedError:
        status_edit_state[key] = (loop.time(), text)
        log_action_event(
            "status_edit",
            message_id=getattr(message, "id", None),
            text=text,
            parse_mode=parse_mode,
            buttons=bool(buttons),
            result="not_modified",
        )
        return True
    except FloodWaitError as error:
        wait_seconds = int(getattr(error, "seconds", 1) or 1)
        note_floodwait(wait_seconds)
        status_edit_state[key] = (loop.time() + wait_seconds, text)
        logging.warning("FloodWait on status edit: skipping edits for %ss", wait_seconds)
        log_action_event(
            "status_edit",
            message_id=getattr(message, "id", None),
            text=text,
            parse_mode=parse_mode,
            buttons=bool(buttons),
            result="floodwait",
            wait_seconds=wait_seconds,
        )
        return False
    except Exception:
        logging.exception("Failed to edit status message")
        log_action_event(
            "status_edit",
            message_id=getattr(message, "id", None),
            text=text,
            parse_mode=parse_mode,
            buttons=bool(buttons),
            result="error",
        )
        return False


async def safe_event_reply(event, *args, **kwargs):
    text_arg = sanitize_outgoing_text(args[0]) if args and isinstance(args[0], str) else ""
    if args and isinstance(args[0], str):
        args = (text_arg, *args[1:])
    if args and isinstance(args[0], str) and len(args[0]) > TELEGRAM_SAFE_TEXT_LIMIT and "file" not in kwargs:
        log_action_event(
            "reply",
            chat_id=getattr(event, "chat_id", None),
            sender_id=getattr(event, "sender_id", None),
            text=text_arg,
            parse_mode=kwargs.get("parse_mode"),
            buttons=bool(kwargs.get("buttons")),
            result="reply_as_file",
        )
        return await reply_with_text_file(event, args[0], **kwargs)

    try:
        chat_id = int(getattr(event, "chat_id", 0) or 0)
        if chat_id and TELEGRAM_REPLY_MIN_INTERVAL_SECONDS > 0:
            async with last_reply_sent_at_lock:
                now_ts = now_timestamp()
                last_ts = float(last_reply_sent_at_by_chat.get(chat_id) or 0.0)
                wait_for = TELEGRAM_REPLY_MIN_INTERVAL_SECONDS - (now_ts - last_ts)
                if wait_for > 0:
                    await asyncio.sleep(min(wait_for, 1.5))
                last_reply_sent_at_by_chat[chat_id] = now_timestamp()
        sent = await event.reply(*args, **kwargs)
        log_action_event(
            "reply",
            chat_id=getattr(event, "chat_id", None),
            sender_id=getattr(event, "sender_id", None),
            text=text_arg,
            parse_mode=kwargs.get("parse_mode"),
            buttons=bool(kwargs.get("buttons")),
            result="sent",
            reply_message_id=getattr(sent, "id", None),
        )
        return sent
    except MessageTooLongError:
        if args and isinstance(args[0], str):
            logging.warning("Reply text is too long; sending it as a txt file")
            log_action_event(
                "reply",
                chat_id=getattr(event, "chat_id", None),
                sender_id=getattr(event, "sender_id", None),
                text=text_arg,
                parse_mode=kwargs.get("parse_mode"),
                buttons=bool(kwargs.get("buttons")),
                result="too_long_reply_as_file",
            )
            return await reply_with_text_file(event, args[0], **kwargs)
        logging.exception("Failed to send reply: message is too long")
        log_action_event(
            "reply",
            chat_id=getattr(event, "chat_id", None),
            sender_id=getattr(event, "sender_id", None),
            text=text_arg,
            parse_mode=kwargs.get("parse_mode"),
            buttons=bool(kwargs.get("buttons")),
            result="too_long_error",
        )
        return None
    except FloodWaitError as error:
        wait_seconds = int(getattr(error, "seconds", 1) or 1)
        note_floodwait(wait_seconds)
        logging.warning("FloodWait on reply: message suppressed for %ss", wait_seconds)
        log_action_event(
            "reply",
            chat_id=getattr(event, "chat_id", None),
            sender_id=getattr(event, "sender_id", None),
            text=text_arg,
            parse_mode=kwargs.get("parse_mode"),
            buttons=bool(kwargs.get("buttons")),
            result="floodwait",
            wait_seconds=wait_seconds,
        )
        return None
    except Exception:
        logging.exception("Failed to send reply")
        log_action_event(
            "reply",
            chat_id=getattr(event, "chat_id", None),
            sender_id=getattr(event, "sender_id", None),
            text=text_arg,
            parse_mode=kwargs.get("parse_mode"),
            buttons=bool(kwargs.get("buttons")),
            result="error",
        )
        return None


def remove_file_quietly(path: Path) -> bool:
    try:
        if path.exists() and path.is_file():
            path.unlink()
            return True
    except OSError:
        return False
    return False


def remove_dir_quietly(path: Path) -> bool:
    try:
        if path.exists() and path.is_dir():
            shutil.rmtree(path)
            return True
    except OSError:
        return False
    return False


def startup_cleanup() -> dict[str, int]:
    global startup_cleanup_done
    if startup_cleanup_done or not settings.cleanup_on_start_enabled:
        return {"files": 0, "dirs": 0}
    startup_cleanup_done = True

    removed_files = 0
    removed_dirs = 0
    root = APP_ROOT.resolve()

    def inside_app(path: Path) -> bool:
        try:
            resolved = path.resolve()
        except OSError:
            return False
        return resolved == root or root in resolved.parents

    if settings.cleanup_logs_on_start:
        log_path = Path(settings.log_file)
        if not log_path.is_absolute():
            log_path = root / log_path
        crash_log_path = log_path.with_name(f"{log_path.stem}-crash{log_path.suffix or '.log'}")
        log_candidates = {
            log_path,
            crash_log_path,
            root / "userbot.log",
            root / "userbot-crash.log",
        }
        for base_path in list(log_candidates):
            log_candidates.update(base_path.parent.glob(f"{base_path.name}*"))
        for path in sorted(log_candidates):
            if inside_app(path) and remove_file_quietly(path):
                removed_files += 1

    if settings.cleanup_temp_on_start:
        for path in (
            root / "__pycache__",
            root / ".pytest_cache",
            root / ".mypy_cache",
            root / ".ruff_cache",
        ):
            if inside_app(path) and remove_dir_quietly(path):
                removed_dirs += 1

        for pattern in (
            "*.pyc",
            "*.pyo",
            "*.tmp",
            "*.temp",
            "*.part",
            "*.swp",
            "*.swo",
            "runtime-version.txt",
        ):
            for path in root.glob(pattern):
                if inside_app(path) and remove_file_quietly(path):
                    removed_files += 1

    if removed_files or removed_dirs:
        print(f"Startup cleanup: removed files={removed_files}, dirs={removed_dirs}")
    return {"files": removed_files, "dirs": removed_dirs}


async def reply_with_text_file(event, text: str, **kwargs):
    text = sanitize_outgoing_text(text)
    file_kwargs = dict(kwargs)
    file_kwargs.pop("buttons", None)
    file_kwargs.pop("parse_mode", None)

    report_dir = Path(settings.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    path = report_dir / f"telegram-long-message-{stamp}.txt"
    atomic_write_text(path, text)

    preview = " ".join(text.split())
    if len(preview) > 520:
        preview = preview[:520].rstrip() + "..."
    short_text = "\n".join(
        (
            "РџРѕР»РЅС‹Р№ С‚РµРєСЃС‚ СЃР»РёС€РєРѕРј Р±РѕР»СЊС€РѕР№ РґР»СЏ Telegram. РћС‚РїСЂР°РІР»СЏСЋ С„Р°Р№Р»РѕРј.",
            f"Р¤Р°Р№Р»: {path.name}",
            "",
            preview,
        )
    )
    try:
        return await event.reply(short_text, file=str(path), **file_kwargs)
    except MediaCaptionTooLongError:
        logging.warning("File caption is too long; retrying with minimal caption")
        try:
            return await event.reply(f"РџРѕР»РЅС‹Р№ С‚РµРєСЃС‚ РІ С„Р°Р№Р»Рµ: {path.name}", file=str(path), **file_kwargs)
        except MediaCaptionTooLongError:
            logging.warning("Minimal file caption is too long; retrying without caption")
            return await event.reply(file=str(path), **file_kwargs)
    except FloodWaitError as error:
        wait_seconds = int(getattr(error, "seconds", 1) or 1)
        note_floodwait(wait_seconds)
        logging.warning("FloodWait on file reply: message suppressed for %ss", wait_seconds)
        return None
    except Exception:
        logging.exception("Failed to send long reply as file")
        return None


def build_scan_status(
    text: str,
    *,
    checkpoint_text: str = "РЅРµС‚",
    done: bool = False,
    failed: bool = False,
    paused: bool = False,
) -> str:
    if failed:
        status = "РѕС€РёР±РєР°"
    elif paused:
        status = "РїР°СѓР·Р°"
    elif done:
        status = "Р·Р°РІРµСЂС€РµРЅРѕ"
    else:
        status = "РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ"

    short_text = " ".join(text.split())
    if len(short_text) > 120:
        short_text = short_text[:117].rstrip() + "..."
    position = extract_scan_position(text)
    if position:
        current, total = position
        bar, percent = make_progress_bar(current, total, width=0)
    elif done or paused:
        bar, percent = make_progress_bar(1, 1, width=0)
    elif failed:
        bar, percent = make_progress_bar(0, 1, width=0)
    else:
        bar, percent = make_progress_bar(1, 2, width=0)
    title_text = decorate_status_title(
        "Scan РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№",
        done=done,
        failed=failed,
        paused=paused,
    )
    status_icon = animated_symbol(
        "error" if failed else "done" if done else "pause" if paused else "waiting"
    )

    lines = [
        title_text,
        f"{bar} {percent}%",
        f"{status_icon} РЎС‚Р°С‚СѓСЃ: {status}",
        f"BOT: {format_admin_bot_health()}",
        f"CHECKPOINT: {checkpoint_text}",
    ]
    if not done and not failed and not paused:
        lines.append(f"SPEED: delay {active_scan_action_delay_seconds:.2f}s")
    if position:
        lines.append(f"USER: {position[0]}/{position[1]}")
    lines.append(f"EVENT: {short_text}")
    return "\n".join(lines)


def configure_logging() -> None:
    global logging_is_configured
    if logging_is_configured:
        return

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    log_path = Path(settings.log_file)
    if log_path.parent != Path("."):
        log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=settings.log_max_bytes,
        backupCount=settings.log_backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.WARNING)

    crash_log_path = log_path.with_name(f"{log_path.stem}-crash{log_path.suffix or '.log'}")
    crash_file_handler = RotatingFileHandler(
        crash_log_path,
        maxBytes=settings.log_max_bytes,
        backupCount=settings.log_backup_count,
        encoding="utf-8",
    )
    crash_file_handler.setLevel(logging.ERROR)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    crash_file_handler.setFormatter(formatter)

    logging.basicConfig(
        level=logging.INFO,
        handlers=[console_handler, file_handler, crash_file_handler],
        force=True,
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging_is_configured = True


def application_log_path() -> Path:
    path = Path(settings.log_file)
    if not path.is_absolute():
        path = APP_ROOT / path
    return path


def run_git_metadata_command(args: list[str]) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(APP_ROOT), *args],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except Exception:
        return ""
    if completed.returncode != 0:
        return ""
    return completed.stdout.strip()


def collect_runtime_version_info() -> dict[str, str]:
    full_commit = run_git_metadata_command(["rev-parse", "HEAD"])
    short_commit = run_git_metadata_command(["rev-parse", "--short=12", "HEAD"])
    branch = run_git_metadata_command(["branch", "--show-current"]) or "unknown"
    commit_date = run_git_metadata_command(["log", "-1", "--format=%cd", "--date=iso-strict"])
    tracked_changes = run_git_metadata_command(["status", "--porcelain", "--untracked-files=no"])
    dirty_suffix = "+local" if tracked_changes else ""
    version = f"{short_commit or 'nogit'}{dirty_suffix}"
    return {
        "app": settings.app_name or "Vpn_Bot_assist",
        "developer": settings.app_developer or "DevM29",
        "version": version,
        "branch": branch,
        "commit": full_commit or "unknown",
        "commit_short": short_commit or "unknown",
        "commit_date": commit_date or "unknown",
        "started_at": APP_STARTED_AT.isoformat(sep=" ", timespec="seconds"),
        "project_dir": str(APP_ROOT),
    }


def build_runtime_version_text() -> str:
    info = collect_runtime_version_info()
    return "\n".join(
        (
            f"{info['app']} runtime version",
            f"Developer: {info['developer']}",
            f"Version: {info['version']}",
            f"Branch: {info['branch']}",
            f"Commit: {info['commit']}",
            f"Commit date: {info['commit_date']}",
            f"Started at: {info['started_at']}",
            f"Project: {info['project_dir']}",
        )
    )


def now_timestamp() -> float:
    return datetime.now().timestamp()


def format_bytes(size: int | float | None) -> str:
    value = float(size or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{value:.1f} GB"


def pending_request_age_seconds(data: dict[str, object]) -> float | None:
    created_at = data.get("created_at")
    try:
        return max(0.0, now_timestamp() - float(created_at))
    except (TypeError, ValueError):
        return None


def format_duration(seconds: float | int | None) -> str:
    if seconds is None:
        return "-"
    seconds = max(0, int(seconds))
    minutes, rest = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}С‡ {minutes}Рј"
    if minutes:
        return f"{minutes}Рј {rest}СЃ"
    return f"{rest}СЃ"



def prune_expired_pending_requests() -> dict[str, int]:
    removed = {"wizard": 0, "mail2": 0, "gpt": 0, "smart": 0, "support": 0, "mail": 0}
    for sender_id, data in list(pending_wizard_requests.items()):
        age = pending_request_age_seconds(data)
        if age is not None and age > PENDING_REQUEST_TTL_SECONDS:
            pending_wizard_requests.pop(sender_id, None)
            removed["wizard"] += 1
    for sender_id, data in list(pending_mail2_requests.items()):
        age = pending_request_age_seconds(data)
        if age is not None and age > PENDING_REQUEST_TTL_SECONDS:
            pending_mail2_requests.pop(sender_id, None)
            removed["mail2"] += 1
    for sender_id, data in list(pending_gpt_requests.items()):
        age = pending_request_age_seconds(data)
        if age is not None and age > PENDING_REQUEST_TTL_SECONDS:
            pending_gpt_requests.pop(sender_id, None)
            removed["gpt"] += 1
    for sender_id, data in list(pending_smart_actions.items()):
        age = pending_request_age_seconds(data)
        if age is not None and age > PENDING_REQUEST_TTL_SECONDS:
            pending_smart_actions.pop(sender_id, None)
            removed["smart"] += 1
    for sender_id, data in list(pending_support_requests.items()):
        age = pending_request_age_seconds(data)
        if age is not None and age > PENDING_REQUEST_TTL_SECONDS:
            pending_support_requests.pop(sender_id, None)
            removed["support"] += 1
    for sender_id, data in list(pending_direct_mail_requests.items()):
        age = pending_request_age_seconds(data)
        if age is not None and age > PENDING_REQUEST_TTL_SECONDS:
            pending_direct_mail_requests.pop(sender_id, None)
            removed["mail"] += 1
    if removed["wizard"] or removed["mail2"] or removed["gpt"] or removed["smart"] or removed["support"] or removed["mail"]:
        logging.info(
            "Pruned expired pending requests wizard=%s mail2=%s gpt=%s smart=%s support=%s mail=%s ttl=%ss",
            removed["wizard"],
            removed["mail2"],
            removed["gpt"],
            removed["smart"],
            removed["support"],
            removed["mail"],
            PENDING_REQUEST_TTL_SECONDS,
        )
    return removed


def read_text_tail(path: Path, lines: int) -> str:
    lines = max(1, min(LOG_TAIL_MAX_LINES, int(lines)))
    if not path.exists() or not path.is_file():
        return f"Р›РѕРі-С„Р°Р№Р» РЅРµ РЅР°Р№РґРµРЅ: {path}"

    chunk_size = 8192
    max_bytes = 512_000
    data = b""
    with path.open("rb") as file:
        file.seek(0, os.SEEK_END)
        position = file.tell()
        while position > 0 and data.count(b"\n") <= lines and len(data) < max_bytes:
            read_size = min(chunk_size, position)
            position -= read_size
            file.seek(position)
            data = file.read(read_size) + data

    text = data.decode("utf-8", errors="replace")
    return "\n".join(text.splitlines()[-lines:]) or "[Р»РѕРі РїСѓСЃС‚]"


def command_alias_pattern(*aliases: str) -> str:
    return "|".join(re.escape(alias) for alias in aliases)


def parse_logs_command(text: str) -> int | None:
    match = re.match(
        rf"^\s*/?(?:{command_alias_pattern('logs', 'log', 'tail', 'Р»РѕРіРё', 'Р»РѕРі')})(?:\s+(\d{{1,3}}))?\s*$",
        text or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    if not match.group(1):
        return LOG_TAIL_DEFAULT_LINES
    return max(1, min(LOG_TAIL_MAX_LINES, int(match.group(1))))


def parse_unresolved_command(text: str) -> tuple[str, int | None, str] | None:
    match = re.match(
        rf"^\s*/?(?:{command_alias_pattern('unresolved', 'unsolved', 'unanswered', 'РЅРµСЂРµС€РµРЅРЅС‹Рµ', 'РЅРµРѕС‚РІРµС‡РµРЅРЅС‹Рµ')})(?:\s+([\s\S]+))?\s*$",
        text or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    tail = (match.group(1) or "").strip()
    if not tail or tail.casefold() in {"list", "open", "СЃРїРёСЃРѕРє", "РЅРѕРІС‹Рµ"}:
        return ("list", None, "")
    if tail.casefold() in {"all", "РІСЃРµ"}:
        return ("all", None, "")
    resolve_match = re.match(r"^(?:done|close|resolve|РіРѕС‚РѕРІРѕ|Р·Р°РєСЂС‹С‚СЊ)\s+(\d+)(?:\s+([\s\S]+))?$", tail, flags=re.IGNORECASE)
    if resolve_match:
        return ("resolve", int(resolve_match.group(1)), (resolve_match.group(2) or "").strip())
    if re.fullmatch(r"\d+", tail):
        return ("view", int(tail), "")
    return ("list", None, "")


def build_recent_logs_text(lines: int) -> str:
    log_path = application_log_path()
    content = read_text_tail(log_path, lines)
    header = f"РџРѕСЃР»РµРґРЅРёРµ {lines} СЃС‚СЂРѕРє Р»РѕРіР°: {log_path}"
    return f"{header}\n\n{content}"


def build_diagnostics_text() -> str:
    prune_expired_pending_requests()
    version = collect_runtime_version_info()
    db_path = database_path()
    checkpoint = load_scan_checkpoint()
    try:
        latest_stats = load_latest_scan_stats_from_database()
    except Exception:
        logging.exception("Diagnostics failed to load latest scan stats")
        latest_stats = None
    try:
        requesters_total = requester_count()
    except Exception:
        logging.exception("Diagnostics failed to count requesters")
        requesters_total = -1
    try:
        unresolved_open = unresolved_requests_count(status="open")
    except Exception:
        logging.exception("Diagnostics failed to count unresolved requests")
        unresolved_open = -1
    public_dir = dashboard_public_dir()
    scan_running = bool(active_scan_cancel_event and not active_scan_cancel_event.is_set())
    mail2_running = bool(active_mail2_cancel_event and not active_mail2_cancel_event.is_set())

    db_status = "РЅРµС‚"
    if db_path.exists():
        db_status = f"РµСЃС‚СЊ, {format_bytes(db_path.stat().st_size)}"

    checkpoint_text = "РЅРµС‚"
    if checkpoint:
        next_user_id = int(checkpoint.get("next_user_id") or checkpoint.get("page_number") or 1)
        total_users_hint = int(checkpoint.get("total_users_hint") or 0)
        range_text = f"{next_user_id}" if total_users_hint <= 0 else f"{next_user_id}/{total_users_hint}"
        checkpoint_text = (
            f"{checkpoint.get('status', 'saved')}, ID {range_text}, "
            f"records {len(checkpoint.get('records') or [])}, saved {checkpoint.get('saved_at', '-')}"
        )

    stats_text = "РЅРµС‚"
    if latest_stats:
        stats_text = (
            f"generated {str(latest_stats.get('generated_at') or '-').replace('T', ' ')}, "
            f"users {int(latest_stats.get('total_users') or 0)}, "
            f"paid {int(latest_stats.get('paid_users') or 0)}, "
            f"subs {int(latest_stats.get('total_subscriptions') or 0)}"
        )

    return "\n".join(
        (
            "Р”РёР°РіРЅРѕСЃС‚РёРєР° Vpn_Bot_assist",
            "",
            f"Version: {version['version']}",
            f"Commit: {version['commit_short']}",
            f"Started: {version['started_at']}",
            f"Admin bot: {format_admin_bot_health()}",
            f"Admin flow: {active_admin_flow_text()}",
            "",
            f"SQLite: {db_status}",
            f"SQLite path: {db_path}",
            f"Requesters: {requesters_total if requesters_total >= 0 else 'РѕС€РёР±РєР°'}",
            f"Unresolved: {unresolved_open if unresolved_open >= 0 else 'РѕС€РёР±РєР°'}",
            f"OpenAI: {'РЅР°СЃС‚СЂРѕРµРЅ' if settings.openai_api_key else 'РЅРµС‚ РєР»СЋС‡Р°'} ({settings.openai_model})",
            "",
            f"Scan active: {'РґР°' if scan_running else 'РЅРµС‚'}",
            f"Scan owner: {active_scan_owner_id or '-'}",
            f"Scan checkpoint: {checkpoint_text}",
            f"Scan delay: {active_scan_action_delay_seconds:.2f}s",
            "",
            f"Mail2 active: {'РґР°' if mail2_running else 'РЅРµС‚'}",
            f"Wizard pending: {len(pending_wizard_requests)}",
            f"Mail2 pending: {len(pending_mail2_requests)}",
            f"Mail pending: {len(pending_direct_mail_requests)}",
            f"GPT active: {len(active_gpt_requests)}",
            f"GPT pending: {len(pending_gpt_requests)}",
            f"Smart pending: {len(pending_smart_actions)}",
            "",
            f"Latest stats: {stats_text}",
            f"Dashboard public: {settings.dashboard_public_base_url.rstrip('/')}/{settings.dashboard_public_path_prefix.strip('/')}",
            f"Dashboard dir: {public_dir} ({len(list(public_dir.glob('*.html')))} html)",
        )
    )


def describe_pending_processes(pending: dict[int, dict[str, object]], *, limit: int = 5) -> list[str]:
    if not pending:
        return ["РЅРµС‚"]
    lines: list[str] = []
    for index, (sender_id, data) in enumerate(pending.items(), start=1):
        if index > limit:
            lines.append(f"... РµС‰Рµ {len(pending) - limit}")
            break
        stage = str(data.get("stage") or "РѕР¶РёРґР°РЅРёРµ")
        user_id = str(data.get("user_id") or "-")
        age = pending_request_age_seconds(data)
        lines.append(f"{sender_id}: {stage}, user {user_id}, age {format_duration(age)}")
    return lines


def build_poc_text() -> str:
    prune_expired_pending_requests()
    scan_running = bool(active_scan_cancel_event and not active_scan_cancel_event.is_set())
    mail2_running = bool(active_mail2_cancel_event and not active_mail2_cancel_event.is_set())
    auto_resume_running = bool(active_scan_auto_resume_task and not active_scan_auto_resume_task.done())
    lines = [
        "РџСЂРѕС†РµСЃСЃС‹ Vpn_Bot_assist",
        "",
        f"Admin flow: {active_admin_flow_text()}",
        f"Admin bot: {format_admin_bot_health()}",
        "",
        f"Scan: {'Р°РєС‚РёРІРµРЅ' if scan_running else 'РЅРµ Р·Р°РїСѓС‰РµРЅ'}",
        f"Scan owner: {active_scan_owner_id or '-'}",
        f"Scan checkpoint: {format_scan_checkpoint_text()}",
        f"Scan auto-resume: {'РѕР¶РёРґР°РµС‚' if auto_resume_running else 'РЅРµС‚'}",
        "",
        f"Mail2: {'Р°РєС‚РёРІРЅР°' if mail2_running else 'РЅРµ Р·Р°РїСѓС‰РµРЅР°'}",
        f"Wizard pending: {len(pending_wizard_requests)}",
        *[f"  - {line}" for line in describe_pending_processes(pending_wizard_requests)],
        f"Mail2 pending: {len(pending_mail2_requests)}",
        *[f"  - {line}" for line in describe_pending_processes(pending_mail2_requests)],
        f"Mail pending: {len(pending_direct_mail_requests)}",
        *[f"  - {line}" for line in describe_pending_processes(pending_direct_mail_requests)],
        f"GPT active: {len(active_gpt_requests)}",
        *[f"  - {line}" for line in describe_pending_processes(active_gpt_requests)],
        f"GPT pending: {len(pending_gpt_requests)}",
        *[f"  - {line}" for line in describe_pending_processes(pending_gpt_requests)],
        f"Smart pending: {len(pending_smart_actions)}",
        *[f"  - {line}" for line in describe_pending_processes(pending_smart_actions)],
        "",
        f"Pending TTL: {format_duration(PENDING_REQUEST_TTL_SECONDS)}",
        "РљРЅРѕРїРєРё РЅРёР¶Рµ РІС‹РїРѕР»РЅСЏСЋС‚ РјСЏРіРєРѕРµ СѓРїСЂР°РІР»РµРЅРёРµ: scan СЃС‚Р°РІРёС‚СЃСЏ РЅР° РїР°СѓР·Сѓ, mail2 РїСЂРѕСЃРёС‚ РѕСЃС‚Р°РЅРѕРІРєСѓ, РѕР¶РёРґР°РЅРёСЏ РѕС‡РёС‰Р°СЋС‚СЃСЏ.",
    ]
    return "\n".join(lines)


def build_poc_buttons():
    rows = []
    if active_scan_cancel_event and not active_scan_cancel_event.is_set():
        rows.append([Button.inline("РџР°СѓР·Р° scan", data=POC_SCAN_PAUSE_CALLBACK_DATA)])
    if active_mail2_cancel_event and not active_mail2_cancel_event.is_set():
        rows.append([Button.inline("РћСЃС‚Р°РЅРѕРІРёС‚СЊ mail2", data=POC_MAIL2_STOP_CALLBACK_DATA)])
    if pending_wizard_requests:
        rows.append([Button.inline("РћС‡РёСЃС‚РёС‚СЊ wizard pending", data=POC_CLEAR_WIZARD_CALLBACK_DATA)])
    if pending_mail2_requests:
        rows.append([Button.inline("РћС‡РёСЃС‚РёС‚СЊ mail2 pending", data=POC_CLEAR_MAIL2_PENDING_CALLBACK_DATA)])
    if pending_direct_mail_requests:
        rows.append([Button.inline("РћС‡РёСЃС‚РёС‚СЊ mail pending", data=b"poc:clear_mail_pending")])
    if pending_gpt_requests:
        rows.append([Button.inline("РћС‡РёСЃС‚РёС‚СЊ GPT pending", data=POC_CLEAR_GPT_PENDING_CALLBACK_DATA)])
    if pending_smart_actions:
        rows.append([Button.inline("РћС‡РёСЃС‚РёС‚СЊ smart pending", data=b"poc:clear_smart_pending")])
    if pending_wizard_requests or pending_mail2_requests or pending_direct_mail_requests or pending_gpt_requests or pending_smart_actions:
        rows.append([Button.inline("РћС‡РёСЃС‚РёС‚СЊ РІСЃРµ pending", data=POC_CLEAR_ALL_PENDING_CALLBACK_DATA)])
    rows.append([Button.inline("РћР±РЅРѕРІРёС‚СЊ РїСЂРѕС†РµСЃСЃС‹", data=POC_REFRESH_CALLBACK_DATA)])
    return rows


def log_runtime_version() -> None:
    global runtime_version_logged
    if runtime_version_logged:
        return

    version_text = build_runtime_version_text()
    logging.warning("STARTUP VERSION\n%s", version_text)
    try:
        (APP_ROOT / "runtime-version.txt").write_text(version_text + "\n", encoding="utf-8")
    except Exception:
        logging.exception("Failed to write runtime-version.txt")
    runtime_version_logged = True


def extract_user_id(text: str) -> str | None:
    cleaned = text.strip()
    if re.fullmatch(r"\d{1,20}", cleaned):
        return cleaned

    match = re.search(r"\d{1,20}", cleaned)
    if match:
        return match.group(0)

    return None


def normalize_username(value: str) -> str:
    cleaned = (value or "").strip()
    if cleaned.startswith("@"):
        cleaned = cleaned[1:]
    cleaned = cleaned.strip().casefold()
    if not re.fullmatch(r"[a-z0-9_]{3,32}", cleaned):
        return ""
    return cleaned


def extract_username_from_text(text: str) -> str:
    ignored = {
        normalize_username(settings.admin_bot_username),
        normalize_username(settings.wizard_target_username),
    }
    for match in re.finditer(r"(?<![A-Za-z0-9_])@([A-Za-z0-9_]{3,32})", text or ""):
        username = normalize_username(match.group(1))
        if username and username not in ignored:
            return username

    label_match = re.search(
        r"(?:username|user\s*name|login|telegram)\s*[:=\-]\s*@?([A-Za-z0-9_]{3,32})",
        text or "",
        flags=re.IGNORECASE,
    )
    if label_match:
        username = normalize_username(label_match.group(1))
        if username and username not in ignored:
            return username

    return ""


def extract_username_from_record(record: dict) -> str:
    explicit = normalize_username(str(record.get("username") or ""))
    if explicit:
        return explicit
    return extract_username_from_text(str(record.get("user_text") or ""))


def parse_user_lookup_command(command: str | tuple[str, ...], text: str) -> UserLookupCommand | None:
    aliases = (command,) if isinstance(command, str) else command
    match = re.match(rf"^\s*/?(?:{command_alias_pattern(*aliases)})\s+(.+?)\s*$", text or "", flags=re.IGNORECASE)
    if not match:
        return None

    parts = [part.strip() for part in match.group(1).split() if part.strip()]
    if not parts:
        return None

    use_database = False
    query_parts: list[str] = []
    for part in parts:
        if part.casefold() in {"-b", "--base", "--db", "db", "base"}:
            use_database = True
            continue
        query_parts.append(part)

    if len(query_parts) != 1:
        return None

    query = query_parts[0].strip()
    if re.fullmatch(r"\d{1,20}", query) or normalize_username(query):
        return UserLookupCommand(query=query, use_database=use_database)

    return None


def parse_mail_command(text: str) -> tuple[str, str] | None:
    match = re.match(
        rf"^\s*/?(?:{command_alias_pattern('mail', 'send', 'message', 'msg', 'РїРёСЃСЊРјРѕ')})\s+(\d{{1,20}})(?:\s+([\s\S]+))?\s*$",
        text or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    user_id = match.group(1)
    message_text = (match.group(2) or "").strip() or settings.mail_text
    return user_id, message_text


def parse_requester_mail_target_only(text: str) -> str | None:
    raw_text = str(text or "").strip()
    if not raw_text or raw_text.startswith("/"):
        return None

    patterns = (
        r"^(?:РѕС‚РїСЂР°РІ(?:СЊ|РёС‚СЊ)|РїРѕС€Р»Рё|РЅР°РїРёС€Рё)\s+(?:СЃРѕРѕР±С‰РµРЅРёРµ|РїРёСЃСЊРјРѕ|mail)\s+(?:РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ|СЋР·РµСЂСѓ|user)\s+(?P<user_id>\d{1,20})\s*$",
        r"^(?:РѕС‚РїСЂР°РІ(?:СЊ|РёС‚СЊ)|РїРѕС€Р»Рё|РЅР°РїРёС€Рё)\s+(?:РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ|СЋР·РµСЂСѓ|user)\s+(?P<user_id>\d{1,20})\s+(?:СЃРѕРѕР±С‰РµРЅРёРµ|РїРёСЃСЊРјРѕ|mail)\s*$",
    )
    for pattern in patterns:
        match = re.match(pattern, raw_text, flags=re.IGNORECASE)
        if match:
            return str(match.group("user_id") or "").strip()
    return None


def parse_mail2_command(text: str) -> str | None:
    match = re.match(
        rf"^\s*/?(?:{command_alias_pattern('mail2', 'broadcast', 'massmail', 'СЂР°СЃСЃС‹Р»РєР°')})(?:\s+([\s\S]+))?\s*$",
        text or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return (match.group(1) or "").strip()


def parse_gpt_command(text: str) -> GPTCommand | None:
    return None


def extract_openai_response_text(response_data: dict) -> str:
    direct_text = str(response_data.get("output_text") or "").strip()
    if direct_text:
        return direct_text

    chunks: list[str] = []
    for item in response_data.get("output") or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            if content.get("type") in {"output_text", "text"}:
                text = str(content.get("text") or "").strip()
                if text:
                    chunks.append(text)
    return "\n\n".join(chunks).strip()


def openai_urlopen(request: Request, *, use_proxy: bool = True):
    proxy_url = settings.openai_proxy_url.strip()
    if use_proxy and proxy_url:
        normalized_proxy_url = proxy_url if "://" in proxy_url else f"http://{proxy_url}"
        opener = build_opener(
            ProxyHandler(
                {
                    "http": normalized_proxy_url,
                    "https": normalized_proxy_url,
                }
            )
        )
        return opener.open(request, timeout=settings.openai_timeout_seconds)
    opener = build_opener(ProxyHandler({}))
    return opener.open(request, timeout=settings.openai_timeout_seconds)


OPENAI_MAX_RETRY_ATTEMPTS = 3
OPENAI_MAX_RETRY_DELAY_SECONDS = 90.0
OPENAI_MIN_RETRY_DELAY_SECONDS = 1.0
GPT_RATE_LIMIT_RETRY_WINDOW_SECONDS = 120.0
GPT_RATE_LIMIT_FALLBACK_DELAY_SECONDS = 10.0
GPT_TIMEOUT_RETRY_WINDOW_SECONDS = 120.0
GPT_TIMEOUT_RETRY_DELAY_SECONDS = 15.0


def parse_openai_retry_delay(error: HTTPError, error_message: str, attempt: int) -> float:
    retry_after = ""
    try:
        retry_after = str(error.headers.get("Retry-After") or "").strip()
    except Exception:
        retry_after = ""

    if retry_after:
        try:
            parsed = float(retry_after)
            if parsed > 0:
                return max(OPENAI_MIN_RETRY_DELAY_SECONDS, min(parsed, OPENAI_MAX_RETRY_DELAY_SECONDS))
        except ValueError:
            pass

    text = (error_message or "").casefold()
    match = re.search(r"try again in\s+(\d+(?:\.\d+)?)s", text)
    if not match:
        match = re.search(r"in\s+(\d+(?:\.\d+)?)\s+seconds?", text)
    if match:
        try:
            parsed = float(match.group(1))
            if parsed > 0:
                return max(OPENAI_MIN_RETRY_DELAY_SECONDS, min(parsed, OPENAI_MAX_RETRY_DELAY_SECONDS))
        except ValueError:
            pass

    fallback = min(OPENAI_MAX_RETRY_DELAY_SECONDS, OPENAI_MIN_RETRY_DELAY_SECONDS * (2 ** attempt))
    return max(OPENAI_MIN_RETRY_DELAY_SECONDS, fallback)


def parse_retry_seconds_from_error_text(error_text: str, default_seconds: float = GPT_RATE_LIMIT_FALLBACK_DELAY_SECONDS) -> float:
    text = str(error_text or "").casefold()
    match = re.search(r"try again in\s+(\d+(?:\.\d+)?)s", text)
    if not match:
        match = re.search(r"in\s+(\d+(?:\.\d+)?)\s+seconds?", text)
    if match:
        try:
            parsed = float(match.group(1))
            if parsed > 0:
                return max(OPENAI_MIN_RETRY_DELAY_SECONDS, min(parsed, OPENAI_MAX_RETRY_DELAY_SECONDS))
        except ValueError:
            pass
    compact_match = re.search(r"try again in\s+(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", text)
    if compact_match and any(compact_match.groups()):
        hours = int(compact_match.group(1) or 0)
        minutes = int(compact_match.group(2) or 0)
        seconds = int(compact_match.group(3) or 0)
        parsed = hours * 3600 + minutes * 60 + seconds
        if parsed > 0:
            return parsed
    minute_match = re.search(r"in\s+(\d+(?:\.\d+)?)\s+minutes?", text)
    if minute_match:
        try:
            parsed = float(minute_match.group(1)) * 60
            if parsed > 0:
                return parsed
        except ValueError:
            pass
    if default_seconds <= 0:
        return 0.0
    return max(OPENAI_MIN_RETRY_DELAY_SECONDS, min(default_seconds, OPENAI_MAX_RETRY_DELAY_SECONDS))


def diagnose_openai_connectivity() -> str:
    parsed = urlsplit(settings.openai_base_url or "https://api.openai.com/v1")
    host = parsed.hostname or "api.openai.com"
    port = parsed.port or (443 if (parsed.scheme or "https").casefold() == "https" else 80)
    parts: list[str] = []
    proxy_url = settings.openai_proxy_url.strip()
    parts.append(f"proxy={'configured' if proxy_url else 'not_configured'}")
    try:
        addr_info = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        addresses = sorted({item[4][0] for item in addr_info if item and len(item) > 4 and item[4]})
        parts.append(f"dns_ok host={host} addresses={len(addresses)}")
    except OSError as error:
        parts.append(f"dns_failed host={host} error={error}")
        return "; ".join(parts)

    last_tcp_error = ""
    for address in addresses[:4]:
        try:
            with socket.create_connection((address, port), timeout=min(5.0, settings.openai_timeout_seconds)):
                parts.append(f"tcp_ok address={address} port={port}")
                return "; ".join(parts)
        except OSError as error:
            last_tcp_error = str(error)
    parts.append(f"tcp_connect_failed port={port} error={last_tcp_error or 'unknown'}")
    return "; ".join(parts)


def is_rate_limit_error_text(error_text: str) -> bool:
    text = str(error_text or "").casefold()
    return "rate limit" in text or "too many requests" in text or "api error 429" in text


def is_openai_https_proxy_error(error_code: int, error_text: str) -> bool:
    lowered = str(error_text or "").casefold()
    return error_code == 403 and "only accessible over https" in lowered


def call_openai_response_payload(payload: dict[str, object]) -> tuple[str, str]:
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def make_request() -> Request:
        return Request(
            f"{settings.openai_base_url}/responses",
            data=body,
            headers={
                "Authorization": f"Bearer {settings.openai_api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

    response_data: dict[str, object] = {}
    last_error_text = ""
    direct_fallback_attempted = False
    for attempt in range(OPENAI_MAX_RETRY_ATTEMPTS):
        try:
            with openai_urlopen(make_request()) as response:
                response_data = json.loads(response.read().decode("utf-8", errors="replace"))
            break
        except HTTPError as error:
            error_body = error.read().decode("utf-8", errors="replace")
            try:
                error_data = json.loads(error_body)
                error_message = str((error_data.get("error") or {}).get("message") or error_body)
            except Exception:
                error_message = error_body or str(error)
            last_error_text = f"OpenAI API error {error.code}: {error_message[:500]}"

            if is_daily_limit_error_text(last_error_text):
                raise RuntimeError(last_error_text) from error

            if settings.openai_proxy_url.strip() and not direct_fallback_attempted and is_openai_https_proxy_error(error.code, error_message):
                logging.warning("OpenAI proxy returned HTTPS 403, retrying direct without proxy")
                direct_fallback_attempted = True
                try:
                    with openai_urlopen(make_request(), use_proxy=False) as response:
                        response_data = json.loads(response.read().decode("utf-8", errors="replace"))
                    break
                except HTTPError as direct_error:
                    direct_error_body = direct_error.read().decode("utf-8", errors="replace")
                    try:
                        direct_error_data = json.loads(direct_error_body)
                        direct_error_message = str((direct_error_data.get("error") or {}).get("message") or direct_error_body)
                    except Exception:
                        direct_error_message = direct_error_body or str(direct_error)
                    last_error_text = f"OpenAI direct fallback error {direct_error.code}: {direct_error_message[:500]}"
                    error = direct_error
                    error_message = direct_error_message
                except URLError as direct_error:
                    connectivity = diagnose_openai_connectivity()
                    last_error_text = f"OpenAI direct fallback connection error: {direct_error.reason}; {connectivity}"
                    has_next_attempt = attempt + 1 < OPENAI_MAX_RETRY_ATTEMPTS
                    if has_next_attempt:
                        wait_seconds = min(OPENAI_MAX_RETRY_DELAY_SECONDS, OPENAI_MIN_RETRY_DELAY_SECONDS * (2 ** attempt))
                        logging.warning(
                            "OpenAI direct fallback connection problem, retry in %.1fs (%s/%s): %s",
                            wait_seconds,
                            attempt + 1,
                            OPENAI_MAX_RETRY_ATTEMPTS,
                            last_error_text,
                        )
                        time.sleep(wait_seconds)
                        continue
                    raise RuntimeError(last_error_text) from direct_error

            is_retryable = error.code == 429 or error.code in {408, 500, 502, 503, 504}
            has_next_attempt = attempt + 1 < OPENAI_MAX_RETRY_ATTEMPTS
            if is_retryable and has_next_attempt:
                wait_seconds = parse_openai_retry_delay(error, error_message, attempt)
                logging.warning(
                    "OpenAI temporary error %s, retry in %.1fs (%s/%s)",
                    error.code,
                    wait_seconds,
                    attempt + 1,
                    OPENAI_MAX_RETRY_ATTEMPTS,
                )
                time.sleep(wait_seconds)
                continue
            raise RuntimeError(last_error_text) from error
        except URLError as error:
            connectivity = diagnose_openai_connectivity()
            last_error_text = f"OpenAI connection error: {error.reason}; {connectivity}"
            has_next_attempt = attempt + 1 < OPENAI_MAX_RETRY_ATTEMPTS
            if has_next_attempt:
                wait_seconds = min(OPENAI_MAX_RETRY_DELAY_SECONDS, OPENAI_MIN_RETRY_DELAY_SECONDS * (2 ** attempt))
                logging.warning(
                    "OpenAI connection problem, retry in %.1fs (%s/%s): %s",
                    wait_seconds,
                    attempt + 1,
                    OPENAI_MAX_RETRY_ATTEMPTS,
                    last_error_text,
                )
                time.sleep(wait_seconds)
                continue
            raise RuntimeError(last_error_text) from error
    else:
        raise RuntimeError(last_error_text or "OpenAI request failed after retries")

    response_text = extract_openai_response_text(response_data)
    response_id = str(response_data.get("id") or "").strip()
    if not response_text:
        raise RuntimeError("OpenAI returned an empty response")
    return response_text, response_id


def call_openai_response(prompt: str, previous_response_id: str | None = None) -> tuple[str, str]:
    payload: dict[str, object] = {
        "model": settings.openai_model,
        "input": prompt,
        "instructions": settings.openai_system_prompt,
        "max_output_tokens": settings.openai_max_output_tokens,
    }
    if previous_response_id:
        payload["previous_response_id"] = previous_response_id
    if settings.openai_reasoning_effort and settings.openai_reasoning_effort not in {"none", "off", "false", "0"}:
        payload["reasoning"] = {"effort": settings.openai_reasoning_effort}
    return call_openai_response_payload(payload)


async def ask_chatgpt(prompt: str, previous_response_id: str | None = None) -> tuple[str, str]:
    return await asyncio.to_thread(call_openai_response, prompt, previous_response_id)


def multipart_form_data(fields: dict[str, str], files: dict[str, tuple[str, bytes, str]]) -> tuple[bytes, str]:
    boundary = f"----VpnBotAssist{uuid.uuid4().hex}"
    body = bytearray()
    for name, value in fields.items():
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"))
        body.extend(str(value).encode("utf-8"))
        body.extend(b"\r\n")
    for name, (filename, content, content_type) in files.items():
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(
            f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'.encode("utf-8")
        )
        body.extend(f"Content-Type: {content_type}\r\n\r\n".encode("utf-8"))
        body.extend(content)
        body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode("utf-8"))
    return bytes(body), f"multipart/form-data; boundary={boundary}"


def call_openai_transcription(audio_path: Path) -> str:
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured")
    content = audio_path.read_bytes()
    if len(content) > settings.openai_voice_max_bytes:
        raise RuntimeError(f"Voice file is too large: {format_bytes(len(content))}")

    content_type = mimetypes.guess_type(str(audio_path))[0] or "audio/ogg"
    fields = {
        "model": settings.openai_transcribe_model,
        "response_format": "json",
    }
    if settings.openai_voice_language:
        fields["language"] = settings.openai_voice_language
    body, content_type_header = multipart_form_data(
        fields,
        {"file": (audio_path.name, content, content_type)},
    )
    request = Request(
        f"{settings.openai_base_url}/audio/transcriptions",
        data=body,
        headers={
            "Authorization": f"Bearer {settings.openai_api_key}",
            "Content-Type": content_type_header,
        },
        method="POST",
    )
    try:
        with openai_urlopen(request) as response:
            response_data = json.loads(response.read().decode("utf-8", errors="replace"))
    except HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        try:
            error_data = json.loads(error_body)
            error_message = str((error_data.get("error") or {}).get("message") or error_body)
        except Exception:
            error_message = error_body or str(error)
        raise RuntimeError(f"OpenAI transcription error {error.code}: {error_message[:500]}") from error
    except URLError as error:
        connectivity = diagnose_openai_connectivity()
        raise RuntimeError(f"OpenAI transcription connection error: {error.reason}; {connectivity}") from error

    transcript = str(response_data.get("text") or "").strip()
    if not transcript:
        raise RuntimeError("OpenAI returned an empty transcription")
    return transcript


async def transcribe_telegram_voice(event: events.NewMessage.Event) -> str:
    with tempfile.TemporaryDirectory(prefix="vpn-bot-voice-") as temp_dir:
        audio_path = Path(temp_dir) / "voice.ogg"
        downloaded = await event.download_media(file=str(audio_path))
        path = Path(downloaded) if downloaded else audio_path
        if not path.exists():
            raise RuntimeError("РќРµ СѓРґР°Р»РѕСЃСЊ СЃРєР°С‡Р°С‚СЊ РіРѕР»РѕСЃРѕРІРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ")
        return await asyncio.to_thread(call_openai_transcription, path)


def is_voice_or_audio_message(event: events.NewMessage.Event) -> bool:
    message = getattr(event, "message", None)
    if not message:
        return False
    if getattr(message, "voice", None) or getattr(message, "audio", None):
        return True
    document = getattr(message, "document", None)
    mime_type = str(getattr(document, "mime_type", "") or "")
    return mime_type.startswith("audio/")


PROBLEM_REPORT_KEYWORDS = (
    "РїСЂРѕР±Р»РµРј",
    "РѕС€РёР±",
    "РЅРµ СЂР°Р±РѕС‚Р°РµС‚",
    "РЅРµ РјРѕРіСѓ",
    "РЅРµ РїРѕР»СѓС‡Р°РµС‚СЃСЏ",
    "РЅРµ РїРѕРґРєР»СЋС‡",
    "РЅРµ РѕС‚РєСЂС‹",
    "РЅРµ Р·Р°С…РѕРґРёС‚",
    "РЅРµ РїСЂРёС…РѕРґ",
    "Р·Р°РІРёСЃ",
    "СЃР»РѕРјР°РЅ",
    "РєР»СЋС‡",
    "РїРѕРґРїРёСЃРє",
    "vpn",
    "впн",
    "не работает впн",
    "vpn не работает",
    "отвал",
    "отвалился",
    "не грузит",
    "не открывает",
    "не подключается",
    "подключение не удалось",
    "connection failed",
    "timed out",
    "timeout",
    "таймаут",
    "ошибка сети",
    "сеть недоступна",
    "нет доступа",
    "не пускает",
    "не получается войти",
    "пропал интернет",
    "не проходит",
    "не активируется",
    "невалидный ключ",
    "invalid key",
    "ключ не подходит",
    "ключ не работает",
    "конфиг не работает",
    "не видит подписку",
    "подписка не активна",
    "подписка истекла",
    "списали деньги",
    "деньги списали",
    "оплата не прошла",
    "платеж не прошел",
    "платеж не прошёл",
    "чек есть",
    "нужна помощь",
    "помогите",
)

SUPPORT_KEY_ISSUE_KEYWORDS = (
    "РєР»СЋС‡",
    "key",
    "РєРѕРЅС„РёРі",
    "РєРѕРЅС„РёРіСѓСЂР°С†",
    "vpn РЅРµ СЂР°Р±РѕС‚Р°РµС‚",
    "РЅРµ РїРѕРґРєР»СЋС‡",
    "РЅРµ РѕС‚РєСЂС‹",
)

SUPPORT_PAYMENT_ISSUE_KEYWORDS = (
    "РїР»Р°С‚РµР¶",
    "РѕРїР»Р°С‚",
    "СЃРїРёСЃР°Р»",
    "СЃРїРёСЃР°Р»Рё",
    "С‡РµРє",
    "РЅРµ РїСЂРѕС€РµР» РїР»Р°С‚РµР¶",
    "РЅРµ РїСЂРѕС€Р»Р° РѕРїР»Р°С‚Р°",
    "С‚СЂР°РЅР·Р°РєС†",
)

SUPPORT_VAGUE_ISSUE_ROOTS = (
    "РєР»СЋС‡",
    "РїСЂРѕР±Р»РµРј",
    "РѕРїР»Р°С‚",
    "РїР»Р°С‚РµР¶",
    "РїРѕРґРїРёСЃ",
    "vpn",
    "РІРїРЅ",
    "РєРѕРЅС„РёРі",
    "РѕС€РёР±",
    "РїРѕРјРѕРі",
    "РЅРµСЂР°Р±РѕС‚",
)

SUPPORT_DETAIL_HINT_ROOTS = (
    "РєРѕРіРґР°",
    "РїРѕСЃР»Рµ",
    "РѕС€РёР±",
    "РєРѕРґ",
    "РїРёС€РµС‚",
    "СЃРєСЂРёРЅ",
    "РїСЂРёР»РѕР¶",
    "android",
    "iphone",
    "ios",
    "windows",
    "mac",
    "pc",
    "Р»РѕРєР°С†",
    "СЃРµСЂРІРµСЂ",
    "РѕРїР»Р°С‚РёР»",
    "С‡РµРє",
    "С‚СЂР°РЅР·Р°Рє",
    "С‚Р°Р№РјР°СѓС‚",
    "timeout",
)

NON_REQUESTER_GREETING_KEYWORDS = (
    "РїСЂРёРІРµС‚",
    "Р·РґСЂР°РІСЃС‚РІСѓР№С‚Рµ",
    "РґРѕР±СЂС‹Р№ РґРµРЅСЊ",
    "РґРѕР±СЂС‹Р№ РІРµС‡РµСЂ",
    "СЃР°Р»Р°Рј",
    "hello",
    "hi",
    "здравствуйте",
    "доброе утро",
    "доброй ночи",
    "ку",
    "дарова",
    "салам алейкум",
    "здарова",
    "хай",
    "приветик",
    "yo",
    "hey",
)

NON_REQUESTER_THANKS_KEYWORDS = (
    "СЃРїР°СЃРёР±Рѕ",
    "Р±Р»Р°РіРѕРґР°СЂСЋ",
    "thanks",
    "thx",
    "благодарен",
    "спс",
    "спасибо большое",
    "от души",
    "огромное спасибо",
    "мерси",
)

NON_REQUESTER_VPN_SETUP_KEYWORDS = (
    "РєР°Рє РїРѕРґРєР»СЋС‡РёС‚СЊ vpn",
    "РєР°Рє РїРѕРґРєР»СЋС‡РёС‚СЊ РІРїРЅ",
    "РєР°Рє РЅР°СЃС‚СЂРѕРёС‚СЊ vpn",
    "РєР°Рє РЅР°СЃС‚СЂРѕРёС‚СЊ РІРїРЅ",
    "РєР°Рє РІРєР»СЋС‡РёС‚СЊ vpn",
    "РєР°Рє РІРєР»СЋС‡РёС‚СЊ РІРїРЅ",
    "РёРЅСЃС‚СЂСѓРєС†РёСЏ",
    "РёРЅСЃС‚СЂСѓРєС†",
    "РЅР°СЃС‚СЂРѕР№РєР° vpn",
    "РЅР°СЃС‚СЂРѕР№РєР° РІРїРЅ",
    "РїРѕРґРєР»СЋС‡РµРЅРёРµ vpn",
    "РїРѕРґРєР»СЋС‡РµРЅРёРµ РІРїРЅ",
    "РіРґРµ РёРЅСЃС‚СЂСѓРєС†РёСЏ",
    "как подключить",
    "как настроить",
    "как включить",
    "как пользоваться vpn",
    "как запустить vpn",
    "как зайти через vpn",
    "настройка впн",
    "настройка vpn",
    "помощь с vpn",
    "инструкция по vpn",
    "мануал",
    "гайд",
    "guide",
    "setup vpn",
    "vpn setup",
    "как добавить ключ",
    "куда вставить ключ",
    "как импортировать ключ",
    "какое приложение",
    "какой клиент",
    "где скачать",
)

NON_REQUESTER_PROFILE_ID_HELP_KEYWORDS = (
    "РєР°Рє СѓР·РЅР°С‚СЊ id",
    "РіРґРµ РјРѕР№ id",
    "РіРґРµ СѓР·РЅР°С‚СЊ id",
    "РєР°Рє РїРѕСЃРјРѕС‚СЂРµС‚СЊ id",
    "РєР°Рє РЅР°Р№С‚Рё id",
    "СЃРІРѕР№ id",
    "РјРѕР№ id",
    "id РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
    "id РІ РїСЂРѕС„РёР»Рµ",
    "где id",
    "мой айди",
    "мой id",
    "как узнать айди",
    "как узнать id",
    "как найти айди",
    "нужен айди",
    "покажи id",
    "покажи айди",
    "id где смотреть",
    "id в боте",
    "telegram id или bot id",
    "мой номер пользователя",
)


# Clean Russian intent layer. It intentionally duplicates some legacy mojibake
# tuples above so user-facing recognition keeps working even when old literals
# in the file were saved with a broken console encoding.
RU_OPERATOR_REQUEST_PHRASES = (
    "позови оператора",
    "позовите оператора",
    "нужен оператор",
    "живой оператор",
    "нужен человек",
    "позови человека",
    "соедини с оператором",
    "соедините с оператором",
    "свяжи с поддержкой",
    "свяжите с поддержкой",
    "нужна поддержка",
    "админ",
    "администратор",
    "позови админа",
    "оператор",
    "человек",
    "хочу поговорить",
)

RU_GREETING_PHRASES = (
    "привет",
    "здравствуйте",
    "добрый день",
    "добрый вечер",
    "доброе утро",
    "доброй ночи",
    "салам",
    "салам алейкум",
    "ку",
    "дарова",
    "здарова",
    "хай",
    "hello",
    "hi",
    "hey",
)

RU_THANKS_PHRASES = (
    "спасибо",
    "спс",
    "благодарю",
    "спасибо большое",
    "огромное спасибо",
    "от души",
    "все ок",
    "всё ок",
    "все работает",
    "всё работает",
    "получилось",
    "решилось",
    "thanks",
    "thx",
)

RU_VPN_SETUP_PHRASES = (
    "как подключить",
    "как подключить vpn",
    "как подключить впн",
    "как настроить",
    "как настроить vpn",
    "как настроить впн",
    "как включить",
    "как включить vpn",
    "как пользоваться",
    "как пользоваться vpn",
    "инструкция",
    "инструкция по подключению",
    "инструкция vpn",
    "где инструкция",
    "пришлите инструкцию",
    "дай инструкцию",
    "мануал",
    "гайд",
    "setup vpn",
    "vpn setup",
    "как добавить ключ",
    "куда вставить ключ",
    "куда ввести ключ",
    "как импортировать ключ",
    "как установить",
    "какое приложение",
    "какой клиент",
    "где скачать приложение",
    "v2rayng",
    "streisand",
    "hiddify",
    "ios подключить",
    "iphone подключить",
    "android подключить",
    "windows подключить",
    "mac подключить",
)

RU_PROFILE_ID_PHRASES = (
    "где id",
    "где айди",
    "где мой id",
    "где мой айди",
    "как узнать id",
    "как узнать айди",
    "как найти id",
    "как найти айди",
    "как посмотреть id",
    "покажи id",
    "покажи айди",
    "мой id",
    "мой айди",
    "нужен id",
    "нужен айди",
    "номер пользователя",
    "мой номер пользователя",
    "id пользователя",
    "id в профиле",
    "айди в профиле",
)

RU_SELF_INFO_PHRASES = (
    "моя подписка",
    "мои подписки",
    "мой статус",
    "мой профиль",
    "моя инфа",
    "информация обо мне",
    "покажи мою подписку",
    "покажи мои подписки",
    "какая у меня подписка",
    "когда закончится",
    "когда истекает",
    "сколько осталось",
    "до какого числа",
    "мой баланс",
    "мой vpn",
)

RU_KEY_ISSUE_PHRASES = (
    "ключ не работает",
    "ключ плохо работает",
    "ключ не подходит",
    "ключ не открывается",
    "ключ не подключается",
    "ключ слетел",
    "ключ пропал",
    "ключ устарел",
    "ключ истек",
    "ключ истёк",
    "невалидный ключ",
    "invalid key",
    "конфиг не работает",
    "конфигурация не работает",
    "ссылка не работает",
    "не импортируется",
    "не добавляется ключ",
    "не копируется ключ",
    "vless не работает",
    "vmess не работает",
)

RU_PAYMENT_ISSUE_PHRASES = (
    "оплата не прошла",
    "платеж не прошел",
    "платеж не прошёл",
    "платёж не прошел",
    "платёж не прошёл",
    "деньги списали",
    "списали деньги",
    "оплатил но не работает",
    "оплатил а подписки нет",
    "оплатила а подписки нет",
    "чек есть",
    "есть чек",
    "транзакция",
    "не пришла оплата",
    "не зачислилось",
    "баланс не пополнился",
    "пополнение не пришло",
    "промокод не работает",
    "promo не работает",
)

RU_CONNECTION_ISSUE_PHRASES = (
    "vpn не работает",
    "впн не работает",
    "не работает vpn",
    "не работает впн",
    "не подключается",
    "не соединяется",
    "подключение не удалось",
    "connection failed",
    "не открывает сайты",
    "не открывается",
    "не грузит",
    "не заходит",
    "нет доступа",
    "нет интернета",
    "пропал интернет",
    "ошибка сети",
    "таймаут",
    "timeout",
    "timed out",
    "отвалился",
    "обрывается",
    "постоянно отключается",
    "не пускает",
    "не проходит подключение",
)

RU_SPEED_ISSUE_PHRASES = (
    "медленно",
    "очень медленно",
    "низкая скорость",
    "скорость низкая",
    "тормозит",
    "лагает",
    "пинг высокий",
    "большой пинг",
    "плохо грузит",
    "ютуб тормозит",
    "youtube тормозит",
    "инстаграм не грузит",
    "telegram не грузит",
    "телеграм не грузит",
)

RU_SUBSCRIPTION_ISSUE_PHRASES = (
    "подписка пропала",
    "подписка не активна",
    "подписка истекла",
    "подписка истёкла",
    "не вижу подписку",
    "нет подписки",
    "продление не прошло",
    "не продлилась",
    "не активировалась",
    "доступ закончился",
)

RU_PROBLEM_PHRASES = (
    *RU_KEY_ISSUE_PHRASES,
    *RU_PAYMENT_ISSUE_PHRASES,
    *RU_CONNECTION_ISSUE_PHRASES,
    *RU_SPEED_ISSUE_PHRASES,
    *RU_SUBSCRIPTION_ISSUE_PHRASES,
    "проблема",
    "ошибка",
    "не могу",
    "не получается",
    "помогите",
    "нужна помощь",
    "что делать",
    "сломалось",
)


def looks_like_problem_report(text: str) -> bool:
    cleaned = (text or "").strip().casefold()
    if len(cleaned) < 6:
        return False
    return any(keyword in cleaned for keyword in PROBLEM_REPORT_KEYWORDS) or any(
        phrase in cleaned for phrase in RU_PROBLEM_PHRASES
    )


def is_operator_request_text(text: str) -> bool:
    cleaned = (text or "").strip().casefold()
    if not cleaned:
        return False
    markers = (
        "РїРѕР·РѕРІРё РѕРїРµСЂР°С‚РѕСЂР°",
        "РїРѕР·РѕРІРёС‚Рµ РѕРїРµСЂР°С‚РѕСЂР°",
        "РЅСѓР¶РµРЅ РѕРїРµСЂР°С‚РѕСЂ",
        "Р¶РёРІРѕР№ РѕРїРµСЂР°С‚РѕСЂ",
        "РїРѕР·РѕРІРё Р°РґРјРёРЅР°",
        "СЃРІСЏР¶Рё СЃ РѕРїРµСЂР°С‚РѕСЂРѕРј",
        "РѕРїРµСЂР°С‚РѕСЂ",
    )
    return any(marker in cleaned for marker in markers) or any(phrase in cleaned for phrase in RU_OPERATOR_REQUEST_PHRASES)


def support_operator_contact_text() -> str:
    return assistant_compact_reply(
        "РџРѕРґРєР»СЋС‡Р°СЋ РѕРїРµСЂР°С‚РѕСЂР° РїРѕРґРґРµСЂР¶РєРё.",
        f"Р•СЃР»Рё РЅСѓР¶РЅРѕ СЃСЂРѕС‡РЅРѕ, РЅР°РїРёС€РёС‚Рµ @{SUPPORT_OPERATOR_USERNAME}.",
    )


def is_vpn_setup_request_text(text: str) -> bool:
    cleaned = (text or "").strip().casefold()
    if not cleaned:
        return False
    return any(keyword in cleaned for keyword in NON_REQUESTER_VPN_SETUP_KEYWORDS) or any(
        phrase in cleaned for phrase in RU_VPN_SETUP_PHRASES
    )


def is_profile_id_help_text(text: str) -> bool:
    cleaned = (text or "").strip().casefold()
    if not cleaned:
        return False
    return any(keyword in cleaned for keyword in NON_REQUESTER_PROFILE_ID_HELP_KEYWORDS) or any(
        phrase in cleaned for phrase in RU_PROFILE_ID_PHRASES
    )


def vpn_setup_help_message() -> str:
    return assistant_list_reply(
        "РљР°Рє РїРѕРґРєР»СЋС‡РёС‚СЊ VPN:",
        [
            "1) РћС‚РєСЂРѕР№С‚Рµ VPN_KBR_BOT.",
            "2) РџРµСЂРµР№РґРёС‚Рµ РІ СЂР°Р·РґРµР» СЃ РёРЅСЃС‚СЂСѓРєС†РёРµР№ РїРѕ РїРѕРґРєР»СЋС‡РµРЅРёСЋ.",
            "3) РЎРєРѕРїРёСЂСѓР№С‚Рµ РєР»СЋС‡ Рё РѕС‚РєСЂРѕР№С‚Рµ РµРіРѕ РІ VPN-РїСЂРёР»РѕР¶РµРЅРёРё.",
            "4) РќР°Р¶РјРёС‚Рµ В«РџРѕРґРєР»СЋС‡РёС‚СЊВ».",
        ],
        "Р•СЃР»Рё РЅРµ РїРѕР»СѓС‡Р°РµС‚СЃСЏ, РїСЂРёС€Р»РёС‚Рµ ID РёР· В«РџСЂРѕС„РёР»СЊВ» Рё С‚РµРєСЃС‚ РѕС€РёР±РєРё.",
    )


def profile_id_help_message() -> str:
    return assistant_list_reply(
        "РљР°Рє СѓР·РЅР°С‚СЊ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ:",
        [
            "1) РћС‚РєСЂРѕР№С‚Рµ VPN_KBR_BOT.",
            "2) РџРµСЂРµР№РґРёС‚Рµ РІ СЂР°Р·РґРµР» В«РџСЂРѕС„РёР»СЊВ».",
            "3) РЎРєРѕРїРёСЂСѓР№С‚Рµ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ Рё РѕС‚РїСЂР°РІСЊС‚Рµ РµРіРѕ СЃСЋРґР°.",
        ],
        "Р’Р°Р¶РЅРѕ: РЅСѓР¶РµРЅ РёРјРµРЅРЅРѕ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РІ Р±РѕС‚Рµ, Р° РЅРµ Telegram ID Рё РЅРµ ID РїРѕРґРїРёСЃРєРё.",
    )


def detect_non_requester_intent(text: str) -> str:
    cleaned = (text or "").strip().casefold()
    if not cleaned:
        return "empty"
    if is_operator_request_text(cleaned):
        return "operator"
    if is_vpn_setup_request_text(cleaned):
        return "vpn_setup_help"
    if is_profile_id_help_text(cleaned):
        return "profile_id_help"
    if detect_support_issue_types(cleaned) or looks_like_problem_report(cleaned):
        return "support_issue"
    words_count = len(re.findall(r"\S+", cleaned))
    if words_count <= 3 and (
        any(keyword in cleaned for keyword in NON_REQUESTER_GREETING_KEYWORDS)
        or any(phrase in cleaned for phrase in RU_GREETING_PHRASES)
    ):
        return "greeting"
    if words_count <= 5 and (
        any(keyword in cleaned for keyword in NON_REQUESTER_THANKS_KEYWORDS)
        or any(phrase in cleaned for phrase in RU_THANKS_PHRASES)
    ):
        return "thanks"
    return "unknown"


def support_intake_message() -> str:
    return assistant_list_reply(
        "Р§РµРј РјРѕРіСѓ РїРѕРјРѕС‡СЊ:",
        [
            "Р•СЃР»Рё РІРѕРїСЂРѕСЃ РїРѕ VPN, РїСЂРёС€Р»РёС‚Рµ ID РёР· В«РџСЂРѕС„РёР»СЊВ» Рё РєРѕСЂРѕС‚РєРѕ РѕРїРёС€РёС‚Рµ РїСЂРѕР±Р»РµРјСѓ.",
            "Р•СЃР»Рё РІРѕРїСЂРѕСЃ РѕР±С‰РёР№, РїСЂРѕСЃС‚Рѕ РЅР°РїРёС€РёС‚Рµ РµРіРѕ РѕР±С‹С‡РЅС‹Рј СЃРѕРѕР±С‰РµРЅРёРµРј.",
        ],
        "Р•СЃР»Рё РЅСѓР¶РµРЅ С‡РµР»РѕРІРµРє, РЅР°РїРёС€РёС‚Рµ: В«РїРѕР·РѕРІРё РѕРїРµСЂР°С‚РѕСЂР°В».",
    )


def assistant_capabilities_message() -> str:
    return assistant_list_reply(
        "Р§С‚Рѕ СЏ СѓРјРµСЋ:",
        [
            "РџРѕРјРѕС‡СЊ РїРѕРґРєР»СЋС‡РёС‚СЊ VPN Рё РЅР°Р№С‚Рё ID РІ СЂР°Р·РґРµР»Рµ В«РџСЂРѕС„РёР»СЊВ».",
            "Р Р°Р·РѕР±СЂР°С‚СЊ РїСЂРѕР±Р»РµРјСѓ СЃ РєР»СЋС‡РѕРј, РѕРїР»Р°С‚РѕР№, СЃРєРѕСЂРѕСЃС‚СЊСЋ РёР»Рё РїРѕРґРєР»СЋС‡РµРЅРёРµРј.",
            "РџРѕРґСЃРєР°Р·Р°С‚СЊ РїРѕ С‡Р°СЃС‚С‹Рј РІРѕРїСЂРѕСЃР°Рј Рё РїРµСЂРµРґР°С‚СЊ СЃР»РѕР¶РЅС‹Р№ СЃР»СѓС‡Р°Р№ РѕРїРµСЂР°С‚РѕСЂСѓ.",
            "Р•СЃР»Рё РЅСѓР¶РЅР° РїРѕРґРґРµСЂР¶РєР°, РїРѕРґРіРѕС‚РѕРІР»СЋ РѕР±СЂР°С‰РµРЅРёРµ РёР»Рё РґР°Рј РєРѕРЅС‚Р°РєС‚ РѕРїРµСЂР°С‚РѕСЂР°.",
            "Р”Р»СЏ Р·Р°РїСЂРѕСЃРЅРёРєРѕРІ РґРѕСЃС‚СѓРїРЅС‹ РєРѕРјР°РЅРґС‹: РїРѕР»СЊР·РѕРІР°С‚РµР»Рё, РїРѕРґРїРёСЃРєРё, wizard, СЂР°СЃСЃС‹Р»РєРё, РїСЂРѕРјРѕРєРѕРґС‹, scan, Р»РѕРіРё Рё dashboard.",
        ],
        "РњРѕР¶РЅРѕ РїРёСЃР°С‚СЊ РѕР±С‹С‡РЅС‹РјРё СЃР»РѕРІР°РјРё, РЅР°РїСЂРёРјРµСЂ: В«РїРѕРєР°Р¶Рё РїРѕРґРїРёСЃРєРё СЋР·РµСЂР° 1232В» РёР»Рё В«РЅР°РїРёС€Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ 1231 РїСЂРёРІРµС‚В».",
    )


def payment_help_message() -> str:
    return assistant_list_reply(
        "Р•СЃР»Рё РїР»Р°С‚РµР¶ РЅРµ РїСЂРѕС€РµР»:",
        [
            "1) РџСЂРѕРІРµСЂСЊС‚Рµ, СЃРїРёСЃР°Р»РёСЃСЊ Р»Рё РґРµРЅСЊРіРё РІ Р±Р°РЅРєРµ.",
            "2) Р•СЃР»Рё РґРµРЅСЊРіРё СЃРїРёСЃР°Р»РёСЃСЊ, РїРѕРґРѕР¶РґРёС‚Рµ РЅРµСЃРєРѕР»СЊРєРѕ РјРёРЅСѓС‚.",
            "3) РџСЂРёС€Р»РёС‚Рµ ID РёР· В«РџСЂРѕС„РёР»СЊВ», СЃСѓРјРјСѓ Рё РїСЂРёРјРµСЂРЅРѕРµ РІСЂРµРјСЏ РѕРїР»Р°С‚С‹.",
        ],
        "РЇ РїСЂРѕРІРµСЂСЋ РґР°РЅРЅС‹Рµ Рё РїРµСЂРµРґР°Рј РѕР±СЂР°С‰РµРЅРёРµ РІ РїРѕРґРґРµСЂР¶РєСѓ, РµСЃР»Рё РѕРїР»Р°С‚Р° РЅРµ РїРѕРґС‚СЏРЅСѓР»Р°СЃСЊ.",
    )


def key_problem_help_message() -> str:
    return assistant_list_reply(
        "Р•СЃР»Рё РєР»СЋС‡ РЅРµ СЂР°Р±РѕС‚Р°РµС‚:",
        [
            "1) РџСЂРёС€Р»РёС‚Рµ ID РёР· СЂР°Р·РґРµР»Р° В«РџСЂРѕС„РёР»СЊВ».",
            "2) РќР°РїРёС€РёС‚Рµ, С‡С‚Рѕ РёРјРµРЅРЅРѕ РїСЂРѕРёСЃС…РѕРґРёС‚: РЅРµ РїРѕРґРєР»СЋС‡Р°РµС‚СЃСЏ, РЅРµС‚ РёРЅС‚РµСЂРЅРµС‚Р°, РѕС€РёР±РєР° РїСЂРёР»РѕР¶РµРЅРёСЏ РёР»Рё РЅРёР·РєР°СЏ СЃРєРѕСЂРѕСЃС‚СЊ.",
            "3) Р•СЃР»Рё РїРѕРґРїРёСЃРѕРє РЅРµСЃРєРѕР»СЊРєРѕ, СѓС‚РѕС‡РЅРёС‚Рµ, СЃ РєР°РєРѕР№ РїСЂРѕР±Р»РµРјР°.",
        ],
        "РџРѕСЃР»Рµ СЌС‚РѕРіРѕ СЏ СЃРјРѕРіСѓ РїСЂРѕРІРµСЂРёС‚СЊ РїРѕРґРїРёСЃРєСѓ Рё СЃРѕСЃС‚Р°РІРёС‚СЊ С‚РѕС‡РЅРѕРµ РѕР±СЂР°С‰РµРЅРёРµ.",
    )


def speed_problem_help_message() -> str:
    return assistant_list_reply(
        "Р•СЃР»Рё VPN СЂР°Р±РѕС‚Р°РµС‚ РјРµРґР»РµРЅРЅРѕ:",
        [
            "1) РџРµСЂРµРїРѕРґРєР»СЋС‡РёС‚Рµ VPN.",
            "2) РџРѕРїСЂРѕР±СѓР№С‚Рµ РґСЂСѓРіСѓСЋ СЃРµС‚СЊ: Wi-Fi РёР»Рё РјРѕР±РёР»СЊРЅС‹Р№ РёРЅС‚РµСЂРЅРµС‚.",
            "3) РџСЂРёС€Р»РёС‚Рµ ID РёР· В«РџСЂРѕС„РёР»СЊВ», РіРѕСЂРѕРґ/РѕРїРµСЂР°С‚РѕСЂР° Рё РіРґРµ РёРјРµРЅРЅРѕ РЅРёР·РєР°СЏ СЃРєРѕСЂРѕСЃС‚СЊ.",
        ],
        "Р•СЃР»Рё РїСЂРѕР±Р»РµРјР° РїРѕРґС‚РІРµСЂРґРёС‚СЃСЏ, РїРµСЂРµРґР°Рј РґР°РЅРЅС‹Рµ РІ РїРѕРґРґРµСЂР¶РєСѓ.",
    )


def subscription_help_message() -> str:
    return assistant_list_reply(
        "РљР°Рє РїСЂРѕРІРµСЂРёС‚СЊ РїРѕРґРїРёСЃРєСѓ:",
        [
            "1) РћС‚РєСЂРѕР№С‚Рµ VPN_KBR_BOT.",
            "2) РџРµСЂРµР№РґРёС‚Рµ РІ В«РџСЂРѕС„РёР»СЊВ».",
            "3) РўР°Рј РІРёРґРЅС‹ РІР°С€ ID, РїРѕРґРїРёСЃРєРё Рё СЃСЂРѕРєРё.",
        ],
        "Р•СЃР»Рё С…РѕС‚РёС‚Рµ, РїСЂРёС€Р»РёС‚Рµ СЃРІРѕР№ ID, Рё СЏ РїРѕРјРѕРіСѓ СЂР°Р·РѕР±СЂР°С‚СЊСЃСЏ РїРѕ РІР°С€РµРјСѓ РїСЂРѕС„РёР»СЋ.",
    )


REQUESTER_ACTION_HINT_KEYWORDS = (
    "/",
    "menu",
    "РјРµРЅСЋ",
    "dashboard",
    "РґР°С€Р±РѕСЂРґ",
    "adminsite",
    "Р°РґРјРёРЅ",
    "status",
    "СЃС‚Р°С‚СѓСЃ",
    "process",
    "РїСЂРѕС†РµСЃСЃ",
    "diag",
    "РґРёР°Рі",
    "logs",
    "Р»РѕРі",
    "version",
    "РІРµСЂСЃРёСЏ",
    "help ",
    "info ",
    "user ",
    "subs ",
    "wizard",
    "РІРёР·Р°СЂРґ",
    "mail",
    "send",
    "СЃРѕРѕР±С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
    "РѕС‚РїСЂР°РІСЊ СЃРѕРѕР±С‰РµРЅРёРµ",
    "РЅР°РїРёС€Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
    "broadcast",
    "СЂР°СЃСЃС‹Р»РєР°",
    "promo",
    "РїСЂРѕРјРѕРєРѕРґ",
    "coupon",
    "scan",
    "СЃРєР°РЅ",
    "roots",
    "unresolved",
    "tail",
    "РЅР°Р№РґРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
    "РїРѕРєР°Р¶Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
    "РїРѕРґРїРёСЃРєРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
    "РѕС‚РєСЂРѕР№",
    "Р·Р°РїСѓСЃС‚Рё",
    "РѕСЃС‚Р°РЅРѕРІРё",
    "РїРѕСЃС‚Р°РІСЊ РЅР° РїР°СѓР·Сѓ",
    "СЃР±СЂРѕСЃ",
)


def looks_like_requester_action_text(text: str) -> bool:
    cleaned = (text or "").strip().casefold()
    if not cleaned:
        return False
    if cleaned.startswith("/"):
        return True
    if detect_direct_smart_action(cleaned) is not None:
        return True
    return any(keyword in cleaned for keyword in REQUESTER_ACTION_HINT_KEYWORDS)


def support_clarification_message() -> str:
    return assistant_list_reply(
        "РќСѓР¶РЅРѕ РЅРµРјРЅРѕРіРѕ Р±РѕР»СЊС€Рµ РґРµС‚Р°Р»РµР№:",
        [
            "1) Р§С‚Рѕ РёРјРµРЅРЅРѕ РЅРµ СЂР°Р±РѕС‚Р°РµС‚.",
            "2) Р“РґРµ СЌС‚Рѕ РїСЂРѕРёСЃС…РѕРґРёС‚: РїСЂРёР»РѕР¶РµРЅРёРµ Рё СѓСЃС‚СЂРѕР№СЃС‚РІРѕ.",
            "3) РљР°РєРѕР№ С‚РµРєСЃС‚ РѕС€РёР±РєРё РёР»Рё С‡С‚Рѕ РІС‹ СѓР¶Рµ РїСЂРѕР±РѕРІР°Р»Рё.",
        ],
        "РџСЂРёРјРµСЂ: ID 123456, РєР»СЋС‡ РЅРµ РїРѕРґРєР»СЋС‡Р°РµС‚СЃСЏ РІ v2ray РЅР° Android, РѕС€РёР±РєР° timeout.",
    )


SUPPORT_QUICK_TEMPLATES: dict[str, str] = {
    "key_not_working": (
        "Р‘С‹СЃС‚СЂР°СЏ РїСЂРѕРІРµСЂРєР° РєР»СЋС‡Р°:\n"
        "1) РћС‚РєСЂРѕР№С‚Рµ РїРѕРґРїРёСЃРєСѓ Рё Р·Р°РЅРѕРІРѕ СЃРєРѕРїРёСЂСѓР№С‚Рµ РєР»СЋС‡ С†РµР»РёРєРѕРј.\n"
        "2) РЈРґР°Р»РёС‚Рµ СЃС‚Р°СЂС‹Р№ РїСЂРѕС„РёР»СЊ РІ РїСЂРёР»РѕР¶РµРЅРёРё Рё РёРјРїРѕСЂС‚РёСЂСѓР№С‚Рµ РЅРѕРІС‹Р№ РєР»СЋС‡.\n"
        "3) РџСЂРѕРІРµСЂСЊС‚Рµ РґР°С‚Сѓ Рё РІСЂРµРјСЏ РЅР° СѓСЃС‚СЂРѕР№СЃС‚РІРµ (РґРѕР»Р¶РЅС‹ Р±С‹С‚СЊ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРёРјРё).\n"
        "4) РџРµСЂРµРєР»СЋС‡РёС‚Рµ СЃРµС‚СЊ: WiвЂ‘Fi в†” РјРѕР±РёР»СЊРЅР°СЏ.\n"
        "Р•СЃР»Рё РЅРµ РїРѕРјРѕР¶РµС‚ вЂ” РЅР°РїРёС€РёС‚Рµ: `РЅРµ РїРѕРјРѕРіР»Рѕ` Рё РјС‹ РїРµСЂРµРґР°РґРёРј РІ РїРѕРґРґРµСЂР¶РєСѓ."
    ),
    "payment_not_applied": (
        "РџСЂРѕРІРµСЂРєР° РѕРїР»Р°С‚С‹:\n"
        "1) РЈРєР°Р¶РёС‚Рµ ID РёР· СЂР°Р·РґРµР»Р° В«РџСЂРѕС„РёР»СЊВ».\n"
        "2) Р’СЂРµРјСЏ Рё СЃСѓРјРјСѓ РїР»Р°С‚РµР¶Р°.\n"
        "3) РџРѕСЃР»РµРґРЅРёРµ С†РёС„СЂС‹ РѕРїРµСЂР°С†РёРё РёР»Рё С‡РµРє (РµСЃР»Рё РµСЃС‚СЊ).\n"
        "РџРѕСЃР»Рµ СЌС‚РѕРіРѕ СЃСЂР°Р·Сѓ РїРµСЂРµРґР°Рј Р·Р°СЏРІРєСѓ РѕРїРµСЂР°С‚РѕСЂСѓ."
    ),
    "vpn_slow": (
        "Р•СЃР»Рё VPN РјРµРґР»РµРЅРЅРѕ СЂР°Р±РѕС‚Р°РµС‚:\n"
        "1) РЎРјРµРЅРёС‚Рµ СЃРµСЂРІРµСЂ/Р»РѕРєР°С†РёСЋ РІ РїРѕРґРїРёСЃРєРµ.\n"
        "2) РџРµСЂРµР·Р°РїСѓСЃС‚РёС‚Рµ РїСЂРёР»РѕР¶РµРЅРёРµ Рё РїРѕРґРєР»СЋС‡РµРЅРёРµ.\n"
        "3) РџСЂРѕРІРµСЂСЊС‚Рµ Р±РµР· VPN СЃРєРѕСЂРѕСЃС‚СЊ РёРЅС‚РµСЂРЅРµС‚Р°.\n"
        "4) РџРѕРїСЂРѕР±СѓР№С‚Рµ РґСЂСѓРіСѓСЋ СЃРµС‚СЊ.\n"
        "Р•СЃР»Рё РїСЂРѕР±Р»РµРјР° РѕСЃС‚Р°РµС‚СЃСЏ вЂ” РЅР°РїРёС€РёС‚Рµ `РЅРµ РїРѕРјРѕРіР»Рѕ`."
    ),
}


def detect_support_template_key(text: str) -> str | None:
    cleaned = (text or "").casefold()
    if any(token in cleaned for token in ("РєР»СЋС‡", "key", "РєРѕРЅС„РёРі", "vless", "vmess", "trojan")):
        return "key_not_working"
    if any(token in cleaned for token in ("РѕРїР»Р°С‚", "РїР»Р°С‚РµР¶", "С‡РµРє", "payment", "РїРµСЂРµРІРѕРґ")):
        return "payment_not_applied"
    if any(token in cleaned for token in ("РјРµРґР»РµРЅ", "СЃРєРѕСЂРѕСЃС‚", "С‚РѕСЂРјРѕР·", "slow", "lag")):
        return "vpn_slow"
    return None


def support_quick_template_message(issue_text: str) -> str | None:
    key = detect_support_template_key(issue_text)
    if not key:
        return None
    return SUPPORT_QUICK_TEMPLATES.get(key)


def build_template_help_text() -> str:
    return (
        "РЁР°Р±Р»РѕРЅС‹ РїРѕРґРґРµСЂР¶РєРё:\n"
        "- /tpl key\n"
        "- /tpl payment\n"
        "- /tpl slow\n"
        "- /tpl auto <С‚РµРєСЃС‚ РїСЂРѕР±Р»РµРјС‹>\n\n"
        "РџСЂРёРјРµСЂ: /tpl auto РЅРµ СЂР°Р±РѕС‚Р°РµС‚ РєР»СЋС‡ РЅР° iphone"
    )


def resolve_template_text(command_key: str, command_rest: str) -> str:
    key = (command_key or "").strip().casefold()
    if not key:
        return build_template_help_text()
    if key == "auto":
        template = support_quick_template_message(command_rest)
        return template or "РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕРґРѕР±СЂР°С‚СЊ С€Р°Р±Р»РѕРЅ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё. " + build_template_help_text()
    if key in {"key", "РєР»СЋС‡"}:
        return SUPPORT_QUICK_TEMPLATES["key_not_working"]
    if key in {"payment", "pay", "РѕРїР»Р°С‚Р°"}:
        return SUPPORT_QUICK_TEMPLATES["payment_not_applied"]
    if key in {"slow", "speed", "РјРµРґР»РµРЅРЅРѕ", "СЃРєРѕСЂРѕСЃС‚СЊ"}:
        return SUPPORT_QUICK_TEMPLATES["vpn_slow"]
    return build_template_help_text()


def support_payment_clarification_message() -> str:
    return assistant_list_reply(
        "Р§С‚РѕР±С‹ РїСЂРѕРІРµСЂРёС‚СЊ РѕРїР»Р°С‚Сѓ, РїСЂРёС€Р»РёС‚Рµ:",
        [
            "1) ID РёР· В«РџСЂРѕС„РёР»СЊВ».",
            "2) РљРѕРіРґР° Р±С‹Р»Р° РѕРїР»Р°С‚Р°.",
            "3) РЎСѓРјРјСѓ РѕРїР»Р°С‚С‹.",
            "4) Р§РµРє РёР»Рё РїРѕСЃР»РµРґРЅРёРµ С†РёС„СЂС‹ РїР»Р°С‚РµР¶Р°, РµСЃР»Рё РѕРЅРё РµСЃС‚СЊ.",
        ],
    )


def support_issue_clarification_message(text: str) -> str:
    issue_types = detect_support_issue_types(text)
    if "РїСЂРѕР±Р»РµРјР° СЃ РѕРїР»Р°С‚РѕР№/РїР»Р°С‚РµР¶РѕРј" in issue_types:
        return support_payment_clarification_message()
    return support_clarification_message()


def support_user_not_found_message(lookup: str) -> str:
    lookup_text = str(lookup or "").strip() or "СѓРєР°Р·Р°РЅРЅС‹Р№ ID"
    return assistant_list_reply(
        f"РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ `{lookup_text}` РЅРµ РЅР°Р№РґРµРЅ РІ Р±Р°Р·Рµ VPN_KBR.",
        [
            "Р’РѕР·РјРѕР¶РЅС‹Рµ РїСЂРёС‡РёРЅС‹:",
            "1) Р’С‹ РѕС‚РїСЂР°РІРёР»Рё Telegram ID, Р° РЅРµ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ Р±РѕС‚Р°.",
            "2) Р’С‹ РѕС‚РїСЂР°РІРёР»Рё ID РїРѕРґРїРёСЃРєРё, Р° РЅРµ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.",
            "3) РџСЂРѕС„РёР»СЊ РµС‰Рµ РЅРµ РїРѕРїР°Р» РІ Р±Р°Р·Сѓ РїРѕСЃР»Рµ РїРѕСЃР»РµРґРЅРµРіРѕ scan.",
        ],
        "РџСЂРѕРІРµСЂСЊС‚Рµ ID РІ СЂР°Р·РґРµР»Рµ В«РџСЂРѕС„РёР»СЊВ» Рё РѕС‚РїСЂР°РІСЊС‚Рµ РµРіРѕ РµС‰Рµ СЂР°Р·.",
    )


def is_support_issue_too_vague(text: str) -> bool:
    cleaned = (text or "").strip().casefold()
    if not cleaned:
        return True
    cleaned = re.sub(r"@[\w]{3,32}", " ", cleaned)
    cleaned = re.sub(r"\b\d{4,20}\b", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return True

    words = re.findall(r"[a-zР°-СЏС‘0-9_]+", cleaned, flags=re.IGNORECASE)
    meaningful_words = [word for word in words if len(word) >= 2 and not word.isdigit()]
    if len(meaningful_words) <= 2:
        return True

    has_detail = any(root in cleaned for root in SUPPORT_DETAIL_HINT_ROOTS)
    if len(meaningful_words) <= 4 and not has_detail:
        return True

    if all(any(word.startswith(root) for root in SUPPORT_VAGUE_ISSUE_ROOTS) for word in meaningful_words):
        return True
    return False


def detect_support_issue_types(text: str) -> list[str]:
    cleaned = (text or "").casefold()
    issue_types: list[str] = []
    if any(keyword in cleaned for keyword in SUPPORT_KEY_ISSUE_KEYWORDS) or any(
        phrase in cleaned for phrase in RU_KEY_ISSUE_PHRASES
    ):
        issue_types.append("РїСЂРѕР±Р»РµРјР° СЃ РєР»СЋС‡РѕРј/РєРѕРЅС„РёРіРѕРј")
    if any(keyword in cleaned for keyword in SUPPORT_PAYMENT_ISSUE_KEYWORDS) or any(
        phrase in cleaned for phrase in RU_PAYMENT_ISSUE_PHRASES
    ):
        issue_types.append("РїСЂРѕР±Р»РµРјР° СЃ РѕРїР»Р°С‚РѕР№/РїР»Р°С‚РµР¶РѕРј")
    if any(phrase in cleaned for phrase in RU_SPEED_ISSUE_PHRASES):
        issue_types.append("проблема со скоростью/стабильностью")
    if any(phrase in cleaned for phrase in RU_SUBSCRIPTION_ISSUE_PHRASES):
        issue_types.append("проблема с подпиской/доступом")
    if not issue_types and looks_like_problem_report(text):
        issue_types.append("РѕР±С‰Р°СЏ С‚РµС…РЅРёС‡РµСЃРєР°СЏ РїСЂРѕР±Р»РµРјР°")
    return issue_types


def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = str(value or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def extract_username_candidates_without_at(text: str) -> list[str]:
    candidates: list[str] = []
    for token in re.findall(r"\b[A-Za-z][A-Za-z0-9_]{2,31}\b", text or ""):
        username = normalize_username(token)
        if username:
            candidates.append(username)
    return unique_preserve_order(candidates)


def support_lookup_candidates(text: str, sender) -> list[str]:
    candidates: list[str] = []
    direct = extract_problem_lookup(text)
    if direct:
        candidates.append(direct)
    candidates.extend(extract_username_candidates_without_at(text))
    sender_user = sender_username(sender)
    if sender_user:
        candidates.append(sender_user)
        candidates.append(f"@{sender_user}")
    sender_id = str(getattr(sender, "id", "") or "").strip()
    if sender_id:
        candidates.append(sender_id)
    return unique_preserve_order(candidates)[:20]


def resolve_support_record(text: str, sender) -> tuple[dict | None, str]:
    for lookup in support_lookup_candidates(text, sender):
        record = load_latest_record_by_lookup_from_database(lookup)
        if record:
            return record, lookup
    return None, ""


NON_REQUESTER_SELF_INFO_KEYWORDS = (
    "РјРѕР№ СЃС‚Р°С‚СѓСЃ",
    "РјРѕСЏ РїРѕРґРїРёСЃРєР°",
    "РјРѕРё РїРѕРґРїРёСЃРєРё",
    "РјРѕР№ РїСЂРѕС„РёР»СЊ",
    "РјРѕР№ id",
    "РјРѕСЏ РёРЅС„Р°",
    "РёРЅС„РѕСЂРјР°С†РёСЏ РѕР±Рѕ РјРЅРµ",
    "РёРЅС„РѕСЂРјР°С†РёСЏ Рѕ РјРЅРµ",
    "РїРѕРєР°Р¶Рё РјРѕР№",
    "РїРѕРєР°Р¶Рё РјРѕРё РїРѕРґРїРёСЃРєРё",
    "РјРѕР№ vpn",
)


NON_REQUESTER_RESTRICTED_ACTION_KEYWORDS = (
    "/help",
    "/info",
    "/user",
    "/subs",
    "/send",
    "/mail",
    "/broadcast",
    "/coupon",
    "/wizard",
    "/scan",
    "/roots",
    "/dashboard",
    "/adminsite",
    "/diag",
    "/processes",
    "wizard",
    "РІРёР·Р°СЂРґ",
    "mail",
    "send",
    "broadcast",
    "promo",
    "promocode",
    "РїСЂРѕРјРѕРєРѕРґ",
    "РєСѓРїРѕРЅ",
    "scan",
    "СЃРєР°РЅ",
    "roots",
    "СЂР°СЃСЃС‹Р»РєР°",
    "РѕС‚РїСЂР°РІСЊ СЃРѕРѕР±С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
    "РЅР°РїРёС€Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
    "РєР°СЂС‚РѕС‡РєСѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
    "РѕС‚РїСЂР°РІСЊ РІ wizard",
    "РїРѕРєР°Р¶Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
    "РЅР°Р№РґРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
    "РїРѕРґРїРёСЃРєРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
    "РёРЅС„Рѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
    "СЃС‚Р°С‚СѓСЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
)


def is_non_requester_self_info_text(text: str) -> bool:
    cleaned = (text or "").strip().casefold()
    if not cleaned:
        return False
    return any(marker in cleaned for marker in NON_REQUESTER_SELF_INFO_KEYWORDS) or any(
        phrase in cleaned for phrase in RU_SELF_INFO_PHRASES
    )


def is_non_requester_restricted_action_text(text: str) -> bool:
    cleaned = (text or "").strip().casefold()
    if not cleaned:
        return False
    if is_non_requester_self_info_text(cleaned):
        return False
    return any(marker in cleaned for marker in NON_REQUESTER_RESTRICTED_ACTION_KEYWORDS)


def resolve_non_requester_self_record(text: str, sender) -> tuple[dict | None, str]:
    candidates: list[str] = []
    direct = extract_problem_lookup(text)
    if direct:
        candidates.append(direct)
    direct_username = extract_username_from_text(text or "")
    if direct_username:
        candidates.append(direct_username)
        candidates.append(f"@{direct_username}")
    sender_user = sender_username(sender)
    if sender_user:
        candidates.append(sender_user)
        candidates.append(f"@{sender_user}")
    sender_id = str(getattr(sender, "id", "") or "").strip()
    if sender_id:
        candidates.append(sender_id)
    for lookup in unique_preserve_order(candidates):
        record = load_latest_record_by_lookup_from_database(lookup)
        if record:
            return record, lookup
    return None, ""


def non_requester_restricted_action_message() -> str:
    return assistant_list_reply(
        "Р­С‚Р° С„СѓРЅРєС†РёСЏ РґРѕСЃС‚СѓРїРЅР° С‚РѕР»СЊРєРѕ Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂР°Рј.",
        [
            "РћР±С‹С‡РЅС‹Рј РїРѕР»СЊР·РѕРІР°С‚РµР»СЏРј РЅРµРґРѕСЃС‚СѓРїРЅС‹ РїРѕРёСЃРє РґСЂСѓРіРёС… РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№, СЂР°СЃСЃС‹Р»РєРё, wizard, РїСЂРѕРјРѕРєРѕРґС‹ Рё СЃР»СѓР¶РµР±РЅС‹Рµ РєРѕРјР°РЅРґС‹.",
            "РЇ РјРѕРіСѓ РїРѕРјРѕС‡СЊ С‚РѕР»СЊРєРѕ РїРѕ РІР°С€РµРјСѓ РїСЂРѕС„РёР»СЋ Рё РІР°С€РёРј РїРѕРґРїРёСЃРєР°Рј.",
        ],
        "Р•СЃР»Рё РЅСѓР¶РµРЅ РІР°С€ СЃС‚Р°С‚СѓСЃ, РЅР°РїРёС€РёС‚Рµ ID РёР· СЂР°Р·РґРµР»Р° В«РџСЂРѕС„РёР»СЊВ» РёР»Рё РѕРїРёС€РёС‚Рµ РїСЂРѕР±Р»РµРјСѓ СЃ VPN.",
    )


def non_requester_self_info_not_found_message() -> str:
    return assistant_list_reply(
        "РќРµ СЃРјРѕРі РЅР°Р№С‚Рё РІР°С€ РїСЂРѕС„РёР»СЊ РІ Р±Р°Р·Рµ.",
        [
            "РћС‚РєСЂРѕР№С‚Рµ СЂР°Р·РґРµР» В«РџСЂРѕС„РёР»СЊВ» РІ VPN_KBR_BOT Рё РїСЂРёС€Р»РёС‚Рµ РІР°С€ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.",
            "РџРѕСЃР»Рµ СЌС‚РѕРіРѕ СЏ СЃРјРѕРіСѓ РїРѕРјРѕС‡СЊ РїРѕ РІР°С€РµР№ РїРѕРґРїРёСЃРєРµ РёР»Рё РїРµСЂРµРґР°С‚СЊ РІРѕРїСЂРѕСЃ РІ РїРѕРґРґРµСЂР¶РєСѓ.",
        ],
    )


def non_requester_self_info_message(record: dict) -> str:
    return assistant_user_message(
        "РЅС„РѕСЂРјР°С†РёСЏ РїРѕ РІР°С€РµРјСѓ РїСЂРѕС„РёР»СЋ:\n" + format_user_summary_from_record(record)
    )


def support_pick_subscriptions(record: dict, text: str) -> list[dict]:
    subscriptions = list(record.get("subscriptions") or [])
    if not subscriptions:
        return []
    if len(subscriptions) == 1:
        return subscriptions
    cleaned = (text or "").strip()
    lowered = cleaned.casefold()
    if not lowered:
        return []

    if any(marker in lowered for marker in ("РІСЃРµ", "РѕР±Рµ", "РѕР±Р°", "all")):
        return subscriptions

    selected_indexes: set[int] = set()

    for index, subscription in enumerate(subscriptions):
        subscription_id = str(subscription.get("subscription_id") or "").strip()
        if subscription_id and re.search(rf"\b{re.escape(subscription_id)}\b", cleaned):
            selected_indexes.add(index)

    numbers = re.findall(r"\b\d{1,20}\b", cleaned)
    for number in numbers:
        try:
            value = int(number)
        except ValueError:
            continue
        if 1 <= value <= len(subscriptions):
            selected_indexes.add(value - 1)

    for index, subscription in enumerate(subscriptions):
        location = str(subscription.get("location") or "").strip()
        button_text = str(subscription.get("button_text") or "").strip()
        if location and location.casefold() in lowered:
            selected_indexes.add(index)
        if button_text and button_text.casefold() in lowered:
            selected_indexes.add(index)

    if not selected_indexes:
        return []
    return [subscriptions[index] for index in range(len(subscriptions)) if index in selected_indexes]


def support_no_subscriptions_message() -> str:
    return assistant_list_reply(
        "РџРѕ РІР°С€РµРјСѓ РїСЂРѕС„РёР»СЋ СЏ РЅРµ РЅР°С€РµР» Р°РєС‚РёРІРЅС‹С… РїРѕРґРїРёСЃРѕРє.",
        [
            "РџСЂРѕРІРµСЂСЊС‚Рµ СЂР°Р·РґРµР» В«РџРѕРґРїРёСЃРєРёВ» РІ Р±РѕС‚Рµ.",
            "Р•СЃР»Рё РѕРїР»Р°С‚Р° Р±С‹Р»Р° РЅРµРґР°РІРЅРѕ, РїСЂРёС€Р»РёС‚Рµ ID РёР· В«РџСЂРѕС„РёР»СЊВ» Рё РІСЂРµРјСЏ РѕРїР»Р°С‚С‹ РёР»Рё С‡РµРє.",
        ],
    )


def support_subscriptions_question(record: dict) -> str:
    subscriptions = list(record.get("subscriptions") or [])
    if len(subscriptions) <= 1:
        return ""
    lines = [assistant_compact_reply("РќР°С€РµР» РЅРµСЃРєРѕР»СЊРєРѕ РїРѕРґРїРёСЃРѕРє.", "РЈС‚РѕС‡РЅРёС‚Рµ, РїРѕ РєР°РєРѕР№ РёРјРµРЅРЅРѕ РІРѕР·РЅРёРє РІРѕРїСЂРѕСЃ:")]
    for index, subscription in enumerate(subscriptions, start=1):
        sub_id = str(subscription.get("subscription_id") or "").strip() or f"sub-{index}"
        location = str(subscription.get("location") or "").strip()
        label = str(subscription.get("button_text") or "").strip()
        lines.append(
            f"{index}) {sub_id}"
            + (f" | {location}" if location else "")
            + (f" | {label}" if label and label != location else "")
        )
    lines.append("РњРѕР¶РЅРѕ СѓРєР°Р·Р°С‚СЊ РЅРµСЃРєРѕР»СЊРєРѕ: РЅР°РїСЂРёРјРµСЂ `1 3` РёР»Рё `12345 98765`.")
    lines.append("Р•СЃР»Рё РІРѕРїСЂРѕСЃ РїРѕ РІСЃРµРј РїРѕРґРїРёСЃРєР°Рј, РѕС‚РІРµС‚СЊС‚Рµ `РІСЃРµ` РёР»Рё `РѕР±Рµ`.")
    return "\n".join(lines)


def build_support_wizard_report(
    *,
    sender_id: int,
    sender_username_value: str,
    sender_full_name: str,
    issue_text: str,
    record: dict | None,
    lookup_used: str,
    selected_subscriptions: list[dict] | None,
) -> str:
    card_text = format_user_summary_from_record(record) if record else ""
    selected_text = ""
    selected_items = list(selected_subscriptions or [])
    if selected_items:
        selected_lines = ["Р’С‹Р±СЂР°РЅРЅС‹Рµ РїРѕРґРїРёСЃРєРё:"]
        for item in selected_items:
            selected_lines.append(
                f"- ID: {str(item.get('subscription_id') or '').strip() or '-'}"
                f" | Р›РѕРєР°С†РёСЏ: {str(item.get('location') or '').strip() or '-'}"
                f" | РљРЅРѕРїРєР°: {str(item.get('button_text') or '').strip() or '-'}"
            )
        selected_text = "\n".join(selected_lines)
    report_lines = [
        "Р—Р°СЏРІРєР° РїРѕРґРґРµСЂР¶РєРё VPN_KBR",
        f"Р’СЂРµРјСЏ: {datetime.now().isoformat(timespec='seconds')}",
        f"РћС‚РїСЂР°РІРёС‚РµР»СЊ Telegram ID: {sender_id}",
        f"РћС‚РїСЂР°РІРёС‚РµР»СЊ username: @{sender_username_value}" if sender_username_value else "РћС‚РїСЂР°РІРёС‚РµР»СЊ username: РЅРµС‚",
        f"РћС‚РїСЂР°РІРёС‚РµР»СЊ РёРјСЏ: {sender_full_name}" if sender_full_name else "РћС‚РїСЂР°РІРёС‚РµР»СЊ РёРјСЏ: РЅРµС‚",
        f"Lookup: {lookup_used or 'РЅРµ РѕРїСЂРµРґРµР»РµРЅ'}",
        "",
        "РўРµРєСЃС‚ РѕР±СЂР°С‰РµРЅРёСЏ:",
        issue_text.strip() or "[РїСѓСЃС‚Рѕ]",
    ]
    if selected_text:
        report_lines.extend(("", selected_text))
    if card_text:
        report_lines.extend(("", "РљР°СЂС‚РѕС‡РєР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ:", card_text))
    return "\n".join(report_lines)


async def forward_support_issue_to_wizard(
    *,
    sender,
    sender_id: int,
    issue_text: str,
    record: dict | None,
    lookup_used: str,
    selected_subscriptions: list[dict] | None,
) -> None:
    sender_username_value = sender_username(sender)
    sender_full_name_value = sender_full_name(sender)
    report_text = build_support_wizard_report(
        sender_id=sender_id,
        sender_username_value=sender_username_value,
        sender_full_name=sender_full_name_value,
        issue_text=issue_text,
        record=record,
        lookup_used=lookup_used,
        selected_subscriptions=selected_subscriptions,
    )
    await send_to_wizard_target(report_text)


async def update_or_reply_text(event, status_message, text: str, *, force: bool = True) -> None:
    if status_message:
        await edit_status_message(status_message, text, force=force)
    else:
        await safe_event_reply(event, text)


async def handle_pending_support_request(event, sender, sender_id: int, incoming_text: str) -> bool:
    pending_support = pending_support_requests.get(sender_id)
    if not pending_support:
        return False

    lowered_reply = incoming_text.casefold()
    if lowered_reply in {"0", "РѕС‚РјРµРЅР°", "cancel", "/cancel"}:
        pending_support_requests.pop(sender_id, None)
        await safe_event_reply(
            event,
            assistant_compact_reply("Р—Р°РїСЂРѕСЃ РѕС‚РјРµРЅРµРЅ.", "РџСЂРёС€Р»РёС‚Рµ РЅРѕРІРѕРµ РѕРїРёСЃР°РЅРёРµ, РєРѕРіРґР° Р±СѓРґРµС‚Рµ РіРѕС‚РѕРІС‹."),
        )
        return True

    pending_stage = str(pending_support.get("stage") or "await_subscription")
    if pending_stage == "await_issue_details":
        if is_support_issue_too_vague(incoming_text):
            await safe_event_reply(
                event,
                support_issue_clarification_message(str(pending_support.get("issue_text") or incoming_text)),
            )
            return True
        pending_support_requests.pop(sender_id, None)
        await forward_support_issue_to_wizard(
            sender=sender,
            sender_id=sender_id,
            issue_text=incoming_text,
            record=dict(pending_support.get("record") or {}),
            lookup_used=str(pending_support.get("lookup") or ""),
            selected_subscriptions=(
                list(pending_support.get("selected_subscriptions") or [])
                if isinstance(pending_support.get("selected_subscriptions"), list)
                else []
            ),
        )
        await safe_event_reply(event, SUPPORT_TICKET_ACCEPTED_MESSAGE)
        return True

    record = dict(pending_support.get("record") or {})
    if not list(record.get("subscriptions") or []):
        pending_support_requests.pop(sender_id, None)
        await safe_event_reply(event, support_no_subscriptions_message())
        return True

    selected_subscriptions = support_pick_subscriptions(record, incoming_text)
    if not selected_subscriptions:
        await safe_event_reply(event, support_subscriptions_question(record))
        return True

    original_issue_text = str(pending_support.get("issue_text") or "")
    if is_support_issue_too_vague(original_issue_text):
        pending_support["stage"] = "await_issue_details"
        pending_support["selected_subscriptions"] = selected_subscriptions
        pending_support_requests[sender_id] = pending_support
        await safe_event_reply(event, support_clarification_message())
        return True

    pending_support_requests.pop(sender_id, None)
    await forward_support_issue_to_wizard(
        sender=sender,
        sender_id=sender_id,
        issue_text=original_issue_text,
        record=record,
        lookup_used=str(pending_support.get("lookup") or ""),
        selected_subscriptions=selected_subscriptions,
    )
    await safe_event_reply(event, SUPPORT_TICKET_ACCEPTED_MESSAGE)
    return True


async def handle_support_issue_flow(
    event,
    sender,
    sender_id: int,
    issue_text: str,
    *,
    status_message=None,
    self_only: bool = False,
) -> None:
    if self_only:
        record, lookup_used = resolve_non_requester_self_record(issue_text, sender)
    else:
        record, lookup_used = resolve_support_record(issue_text, sender)
    if record:
        subscriptions = list(record.get("subscriptions") or [])
        if not subscriptions:
            await update_or_reply_text(event, status_message, support_no_subscriptions_message())
            return

        selected_subscriptions = support_pick_subscriptions(record, issue_text)
        if len(subscriptions) > 1 and not selected_subscriptions:
            pending_support_requests[sender_id] = {
                "record": record,
                "lookup": lookup_used,
                "issue_text": issue_text,
                "stage": "await_subscription",
                "created_at": now_timestamp(),
            }
            await update_or_reply_text(event, status_message, support_subscriptions_question(record))
            return

        lowered_issue = (issue_text or "").casefold()
        quick_template = support_quick_template_message(issue_text)
        if quick_template and "РЅРµ РїРѕРјРѕРіР»Рѕ" not in lowered_issue and "РѕРїРµСЂР°С‚РѕСЂ" not in lowered_issue:
            pending_support_requests[sender_id] = {
                "record": record,
                "lookup": lookup_used,
                "selected_subscriptions": selected_subscriptions,
                "issue_text": issue_text,
                "stage": "await_issue_details",
                "created_at": now_timestamp(),
            }
            await update_or_reply_text(event, status_message, quick_template)
            return

        if is_support_issue_too_vague(issue_text):
            if selected_subscriptions:
                pending_support_requests[sender_id] = {
                    "record": record,
                    "lookup": lookup_used,
                    "selected_subscriptions": selected_subscriptions,
                    "stage": "await_issue_details",
                    "created_at": now_timestamp(),
                }
            await update_or_reply_text(event, status_message, support_issue_clarification_message(issue_text))
            return

        save_unresolved_from_event(
            event,
            sender,
            source="support",
            reason="support_escalation",
            question_text=issue_text,
            payload={
                "lookup_used": lookup_used,
                "subscriptions": [
                    {
                        "subscription_id": str(item.get("subscription_id") or ""),
                        "location": str(item.get("location") or ""),
                    }
                    for item in list(selected_subscriptions or [])
                ],
            },
        )
        await forward_support_issue_to_wizard(
            sender=sender,
            sender_id=sender_id,
            issue_text=issue_text,
            record=record,
            lookup_used=lookup_used,
            selected_subscriptions=selected_subscriptions,
        )
        await update_or_reply_text(event, status_message, SUPPORT_TICKET_ACCEPTED_MESSAGE)
        return

    lookup_guess = extract_problem_lookup(issue_text)
    if lookup_guess:
        if is_support_issue_too_vague(issue_text):
            await update_or_reply_text(event, status_message, support_issue_clarification_message(issue_text))
            return
        await update_or_reply_text(event, status_message, support_user_not_found_message(lookup_guess))
        return

    await update_or_reply_text(event, status_message, support_intake_message())


async def handle_non_requester_voice_message(event, sender, sender_id: int, incoming_text: str) -> None:
    await safe_event_reply(
        event,
        assistant_compact_reply(
            "Голосовые команды отключены.",
            "Опишите вопрос текстом, я передам его в поддержку VPN_KBR.",
        ),
    )
    return

    status_message = await safe_event_reply(
        event,
        support_voice_processing_message(),
    )
    try:
        transcript = await transcribe_telegram_voice(event)
        if is_operator_request_text(transcript):
            await update_or_reply_text(event, status_message, support_operator_contact_text())
            return
        voice_intent = detect_non_requester_intent(transcript)
        if voice_intent == "greeting":
            await update_or_reply_text(event, status_message, support_intake_message())
            return
        if voice_intent == "vpn_setup_help":
            await update_or_reply_text(event, status_message, vpn_setup_help_message())
            return
        if voice_intent == "profile_id_help":
            await update_or_reply_text(event, status_message, profile_id_help_message())
            return
        if voice_intent == "thanks":
            await update_or_reply_text(event, status_message, support_thanks_message())
            return
        if is_non_requester_self_info_text(transcript):
            record, _ = resolve_non_requester_self_record(transcript, sender)
            await update_or_reply_text(
                event,
                status_message,
                non_requester_self_info_message(record) if record else non_requester_self_info_not_found_message(),
            )
            return
        if voice_intent == "support_issue":
            await handle_support_issue_flow(
                event,
                sender,
                sender_id,
                transcript,
                status_message=status_message,
                self_only=True,
            )
            return
        if is_non_requester_restricted_action_text(transcript):
            await update_or_reply_text(event, status_message, non_requester_restricted_action_message())
            return
        await handle_gpt_prompt(
            event,
            sender_id,
            transcript,
            status_message=status_message,
            compact_status=True,
            reveal_unavailable=False,
        )
    except Exception:
        logging.exception("Non-requester voice GPT mode failed sender_id=%s", sender_id)
        record_voice_failure(event, sender, incoming_text, sender_id=sender_id)
        await update_or_reply_text(
            event,
            status_message,
            assistant_compact_reply(
                "    .",
                "  .     VPN,  ID   .",
            ),
        )


async def handle_non_requester_text_message(event, sender, sender_id: int, incoming_text: str) -> None:
    if not incoming_text:
        await safe_event_reply(event, support_intake_message())
        return

    text_intent = detect_non_requester_intent(incoming_text)
    if text_intent == "greeting":
        await safe_event_reply(event, support_intake_message())
        return
    if text_intent == "vpn_setup_help":
        await safe_event_reply(event, vpn_setup_help_message())
        return
    if text_intent == "profile_id_help":
        await safe_event_reply(event, profile_id_help_message())
        return
    if text_intent == "thanks":
        await safe_event_reply(event, support_thanks_message())
        return
    if is_non_requester_self_info_text(incoming_text):
        record, _ = resolve_non_requester_self_record(incoming_text, sender)
        await safe_event_reply(
            event,
            non_requester_self_info_message(record) if record else non_requester_self_info_not_found_message(),
        )
        return
    if text_intent == "support_issue":
        status_message = await safe_event_reply(
            event,
            support_processing_message(),
        )
        await handle_support_issue_flow(
            event,
            sender,
            sender_id,
            incoming_text,
            status_message=status_message,
            self_only=True,
        )
        return
    if is_non_requester_restricted_action_text(incoming_text):
        await safe_event_reply(event, non_requester_restricted_action_message())
        return

    await safe_event_reply(event, support_operator_contact_text())


async def handle_non_requester_message(event, sender, sender_id: int, incoming_text: str) -> bool:
    logging.info(
        "Non-requester GPT mode sender_id=%s username=%s text=%r",
        sender_id,
        sender_username(sender),
        incoming_text,
    )
    log_action_event(
        "non_requester_message",
        sender_id=sender_id,
        chat_id=getattr(event, "chat_id", None),
        username=sender_username(sender),
        text=incoming_text,
        is_voice=is_voice_or_audio_message(event),
    )
    if is_operator_request_text(incoming_text):
        log_action_event("non_requester_route", sender_id=sender_id, route="operator_contact")
        await safe_event_reply(event, support_operator_contact_text())
        return True
    if await handle_pending_support_request(event, sender, sender_id, incoming_text):
        log_action_event("non_requester_route", sender_id=sender_id, route="pending_support")
        return True
    if is_voice_or_audio_message(event):
        log_action_event("non_requester_route", sender_id=sender_id, route="voice")
        await handle_non_requester_voice_message(event, sender, sender_id, incoming_text)
        return True
    log_action_event("non_requester_route", sender_id=sender_id, route="text")
    await handle_non_requester_text_message(event, sender, sender_id, incoming_text)
    return True


def extract_problem_lookup(text: str) -> str:
    cleaned = str(text or "")
    for match in re.finditer(r"(?<![A-Za-z0-9_])@([A-Za-z0-9_]{3,32})", cleaned):
        username = normalize_username(match.group(1))
        if username and username not in {
            normalize_username(settings.admin_bot_username),
            normalize_username(settings.wizard_target_username),
        }:
            return f"@{username}"
    id_match = re.search(r"\b\d{1,20}\b", cleaned)
    if id_match:
        return id_match.group(0)
    return ""


def build_problem_report_text(
    *,
    sender_id: int,
    sender_username_value: str,
    sender_full_name: str,
    user_lookup: str,
    user_card: str,
    problem_text: str,
) -> str:
    lines = [
        "РџСЂРѕР±Р»РµРјР° РѕС‚ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
        f"Р’СЂРµРјСЏ: {datetime.now().isoformat(timespec='seconds')}",
        f"РћС‚РїСЂР°РІРёС‚РµР»СЊ ID: {sender_id}",
        f"РћС‚РїСЂР°РІРёС‚РµР»СЊ username: @{sender_username_value}" if sender_username_value else "РћС‚РїСЂР°РІРёС‚РµР»СЊ username: РЅРµС‚",
        f"РћС‚РїСЂР°РІРёС‚РµР»СЊ РёРјСЏ: {sender_full_name}" if sender_full_name else "РћС‚РїСЂР°РІРёС‚РµР»СЊ РёРјСЏ: РЅРµС‚",
        f"РџРѕРёСЃРє РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: {user_lookup or 'РЅРµ СѓРєР°Р·Р°РЅ'}",
        "",
        "РўРµРєСЃС‚ РїСЂРѕР±Р»РµРјС‹:",
        problem_text.strip() or "[РїСѓСЃС‚Рѕ]",
    ]
    if user_card:
        lines.extend(("", "РљР°СЂС‚РѕС‡РєР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ:", user_card))
    return "\n".join(lines)


async def forward_problem_report_to_wizard(event, sender, text: str) -> bool:
    if not looks_like_problem_report(text):
        return False

    sender_id = int(event.sender_id or 0)
    lookup = extract_problem_lookup(text)
    if not lookup:
        return False
    user_card = ""
    resolved_lookup = ""
    if lookup:
        record = load_latest_record_by_lookup_from_database(lookup)
        if record:
            user_card = format_user_summary_from_record(record)
            resolved_lookup = str(record.get("user_id") or "").strip() or lookup
        else:
            resolved_lookup = lookup

    sender_username_value = sender_username(sender)
    sender_full_name_value = sender_full_name(sender)
    report_text = build_problem_report_text(
        sender_id=sender_id,
        sender_username_value=sender_username_value,
        sender_full_name=sender_full_name_value,
        user_lookup=resolved_lookup or lookup,
        user_card=user_card,
        problem_text=text,
    )

    await send_to_wizard_target(report_text)
    logging.info(
        "Problem report forwarded to wizard sender_id=%s lookup=%s has_card=%s",
        sender_id,
        resolved_lookup or lookup,
        bool(user_card),
    )
    return True


SMART_ACTION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "action": {
            "type": "string",
            "enum": [
                "chat",
                "menu",
                "dashboard",
                "processes",
                "diag",
                "logs",
                "version",
                "user_summary",
                "user_subs",
                "wizard",
                "mail",
                "broadcast",
                "promo",
                "scan_menu",
                "scan_new",
                "scan_continue",
                "scan_results",
                "scan_pause",
                "scan_reset",
                "gpt_reset",
            ],
        },
        "query": {"type": "string"},
        "user_id": {"type": "string"},
        "text": {"type": "string"},
        "use_database": {"type": "boolean"},
        "lines": {"type": "integer"},
        "confidence": {"type": "number"},
        "explanation": {"type": "string"},
    },
    "required": ["action", "query", "user_id", "text", "use_database", "lines", "confidence", "explanation"],
}


SMART_CONTROLLER_INSTRUCTIONS = """
РўС‹ РґРёСЃРїРµС‚С‡РµСЂ РєРѕРјР°РЅРґ Telegram-Р±РѕС‚Р° Vpn_Bot_assist.
РќСѓР¶РЅРѕ РїРѕРЅСЏС‚СЊ СЃРІРѕР±РѕРґРЅС‹Р№ СЂСѓСЃСЃРєРёР№ Р·Р°РїСЂРѕСЃ РІР»Р°РґРµР»СЊС†Р° Рё РІРµСЂРЅСѓС‚СЊ С‚РѕР»СЊРєРѕ JSON РїРѕ СЃС…РµРјРµ.

Р”РѕСЃС‚СѓРїРЅС‹Рµ РґРµР№СЃС‚РІРёСЏ Рё РёС… СЃРјС‹СЃР»:
- menu: РѕС‚РєСЂС‹С‚СЊ РјРµРЅСЋ РєРѕРјР°РЅРґ.
- dashboard: РѕС‚РєСЂС‹С‚СЊ admin system / dashboard.
- processes: РїРѕРєР°Р·Р°С‚СЊ Р°РєС‚РёРІРЅС‹Рµ РїСЂРѕС†РµСЃСЃС‹.
- diag: РїРѕРєР°Р·Р°С‚СЊ РґРёР°РіРЅРѕСЃС‚РёРєСѓ.
- logs: РїРѕРєР°Р·Р°С‚СЊ РїРѕСЃР»РµРґРЅРёРµ СЃС‚СЂРѕРєРё Р»РѕРіР°; С‡РёСЃР»Рѕ СЃС‚СЂРѕРє РїРѕР»РѕР¶Рё РІ lines.
- version: РїРѕРєР°Р·Р°С‚СЊ РІРµСЂСЃРёСЋ.
- user_summary: РєСЂР°С‚РєР°СЏ РєР°СЂС‚РѕС‡РєР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.
- user_subs: РїРѕРґСЂРѕР±РЅР°СЏ РёРЅС„РѕСЂРјР°С†РёСЏ Рё РїРѕРґРїРёСЃРєРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.
- wizard: РїРѕРґРіРѕС‚РѕРІРёС‚СЊ РєР°СЂС‚РѕС‡РєСѓ Рё РѕС‚РїСЂР°РІРєСѓ РІ wizard.
- mail: РѕС‚РїСЂР°РІРёС‚СЊ СЃРѕРѕР±С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ.
- broadcast: СЂР°СЃСЃС‹Р»РєР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏРј Р±РµР· РїРѕРґРїРёСЃРєРё.
- promo: СЃРѕР·РґР°С‚СЊ РїСЂРѕРјРѕРєРѕРґ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ.
- scan_menu: РѕС‚РєСЂС‹С‚СЊ РјРµРЅСЋ scan.
- scan_new: РЅРѕРІС‹Р№ scan.
- scan_continue: РїСЂРѕРґРѕР»Р¶РёС‚СЊ scan.
- scan_results: РїРѕРєР°Р·Р°С‚СЊ СЂРµР·СѓР»СЊС‚Р°С‚С‹ scan.
- scan_pause: РїРѕСЃС‚Р°РІРёС‚СЊ scan РЅР° РїР°СѓР·Сѓ.
- scan_reset: СЃР±СЂРѕСЃРёС‚СЊ scan.
- gpt_reset: РѕС‡РёСЃС‚РёС‚СЊ РєРѕРЅС‚РµРєСЃС‚ KBR_GPT.
- chat: РѕР±С‹С‡РЅС‹Р№ РѕС‚РІРµС‚ KBR_GPT, РµСЃР»Рё СЌС‚Рѕ РЅРµ РєРѕРјР°РЅРґР°.

РџСЂР°РІРёР»Р° РІС‹Р±РѕСЂР°:
- Р•СЃР»Рё СЌС‚Рѕ РѕР±С‹С‡РЅС‹Р№ РІРѕРїСЂРѕСЃ, РїСЂРѕСЃСЊР±Р° РїРѕРґСѓРјР°С‚СЊ, РѕР±СЉСЏСЃРЅРёС‚СЊ РёР»Рё РЅР°РїРёСЃР°С‚СЊ С‚РµРєСЃС‚: action=chat.
- Р•СЃР»Рё РїСЂРѕСЃСЏС‚ РѕС‚РєСЂС‹С‚СЊ РјРµРЅСЋ, Р°РґРјРёРЅ СЃР°Р№С‚, СЃС‚Р°С‚СѓСЃ, РїСЂРѕС†РµСЃСЃС‹, РґРёР°РіРЅРѕСЃС‚РёРєСѓ, Р»РѕРіРё, РІРµСЂСЃРёСЋ: РІС‹Р±РµСЂРё С‚РѕС‡РЅРѕРµ РґРµР№СЃС‚РІРёРµ.
- Р•СЃР»Рё РїСЂРѕСЃСЏС‚ РЅР°Р№С‚Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РєСЂР°С‚РєРѕ: user_summary.
- Р•СЃР»Рё РїСЂРѕСЃСЏС‚ РїРѕРґРїРёСЃРєРё, РїРѕРґСЂРѕР±РЅРѕСЃС‚Рё, СЃС‚Р°С‚СѓСЃ РїРѕРґРїРёСЃРѕРє: user_subs.
- Р•СЃР»Рё СЃРєР°Р·Р°РЅРѕ "РёР· Р±Р°Р·С‹", "РїРѕ Р±Р°Р·Рµ", "Р±С‹СЃС‚СЂРѕ", "Р±РµР· Р°РґРјРёРЅ-Р±РѕС‚Р°": use_database=true.
- Р•СЃР»Рё РїСЂРѕСЃСЏС‚ РѕС‚РїСЂР°РІРёС‚СЊ РІ wizard: action=wizard, user_id РѕР±СЏР·Р°С‚РµР»РµРЅ, РґРѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Р№ С‚РµРєСЃС‚ РїРѕР»РѕР¶Рё РІ text.
- Р•СЃР»Рё РїСЂРѕСЃСЏС‚ РЅР°РїРёСЃР°С‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ: action=mail, user_id Рё text РѕР±СЏР·Р°С‚РµР»СЊРЅС‹.
- Р•СЃР»Рё РїСЂРѕСЃСЏС‚ СЂР°СЃСЃС‹Р»РєСѓ РІСЃРµРј Р±РµР· РїРѕРґРїРёСЃРєРё: action=broadcast, text РѕР±СЏР·Р°С‚РµР»РµРЅ.
- Р•СЃР»Рё РїСЂРѕСЃСЏС‚ РїСЂРѕРјРѕРєРѕРґ РёР»Рё РєСѓРїРѕРЅ: action=promo, user_id РѕР±СЏР·Р°С‚РµР»РµРЅ, text РѕРїС†РёРѕРЅР°Р»РµРЅ.
- Р”Р»СЏ scan РІС‹Р±РµСЂРё scan_menu / scan_new / scan_continue / scan_results / scan_pause / scan_reset.
- Р•СЃР»Рё ID РЅРµСЏСЃРµРЅ, РЅРµ РІС‹РґСѓРјС‹РІР°Р№ РµРіРѕ: РІС‹Р±РµСЂРё chat Рё РїРѕРїСЂРѕСЃРё СѓС‚РѕС‡РЅРёС‚СЊ ID.
- Р•СЃР»Рё РІРёРґРёС€СЊ С‚РѕС‡РЅСѓСЋ РєРѕРјР°РЅРґСѓ РІСЂРѕРґРµ /send, /user, /subs, /wizard, /broadcast, /coupon, /gpt reset, scan new, scan results вЂ” РІС‹Р±РµСЂРё СЃРѕРѕС‚РІРµС‚СЃС‚РІСѓСЋС‰РµРµ РґРµР№СЃС‚РІРёРµ.
"""


def parse_json_object(text: str) -> dict:
    cleaned = text.strip()
    try:
        value = json.loads(cleaned)
        return value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", cleaned)
        if not match:
            return {}
        try:
            value = json.loads(match.group(0))
            return value if isinstance(value, dict) else {}
        except json.JSONDecodeError:
            return {}


def classify_smart_request_text(text: str) -> dict:
    direct_action = detect_direct_smart_action(text)
    if direct_action is not None:
        return direct_action
    payload: dict[str, object] = {
        "model": settings.openai_model,
        "instructions": SMART_CONTROLLER_INSTRUCTIONS,
        "input": text,
        "max_output_tokens": 700,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "vpn_bot_smart_action",
                "strict": True,
                "schema": SMART_ACTION_SCHEMA,
            }
        },
    }
    response_text, _ = call_openai_response_payload(payload)
    action = parse_json_object(response_text)
    if not action:
        raise RuntimeError("Smart controller returned invalid JSON")
    return action


async def classify_smart_request(text: str) -> dict:
    return await asyncio.to_thread(classify_smart_request_text, text)


def detect_direct_smart_action(text: str) -> dict[str, object] | None:
    raw_text = str(text or "").strip()
    if not raw_text:
        return None

    lowered = raw_text.casefold()

    simple_keyword_actions = (
        (("menu", "РјРµРЅСЋ", "РєРѕРјР°РЅРґС‹", "РїРѕРєР°Р¶Рё РєРѕРјР°РЅРґС‹"), "menu", "", "", False, 0),
        (("dashboard", "РґР°С€Р±РѕСЂРґ", "РѕС‚С‡РµС‚", "РѕС‚С‡С‘С‚"), "dashboard", "", "", False, 0),
        (("adminsite", "admin site", "Р°РґРјРёРЅ СЃР°Р№С‚", "Р°РґРјРёРЅ РїР°РЅРµР»СЊ", "Р°РґРјРёРЅРєР°"), "dashboard", "", "", False, 0),
        (("processes", "РїСЂРѕС†РµСЃСЃС‹", "РїСЂРѕС†РµСЃСЃС‹ Р±РѕС‚Р°"), "processes", "", "", False, 0),
        (("diag", "РґРёР°РіРЅРѕСЃС‚РёРєР°", "РґРёР°РіРЅРѕСЃС‚РёРєСѓ"), "diag", "", "", False, 0),
        (("version", "РІРµСЂСЃРёСЏ", "РєР°РєР°СЏ РІРµСЂСЃРёСЏ"), "version", "", "", False, 0),
        (("scan results", "СЂРµР·СѓР»СЊС‚Р°С‚С‹ scan", "СЂРµР·СѓР»СЊС‚Р°С‚С‹ СЃРєР°РЅР°"), "scan_results", "", "", False, 0),
        (("scan continue", "РїСЂРѕРґРѕР»Р¶РёС‚СЊ scan", "РїСЂРѕРґРѕР»Р¶РёС‚СЊ СЃРєР°РЅ"), "scan_continue", "", "", False, 0),
        (("scan new", "РЅРѕРІС‹Р№ scan", "РЅРѕРІС‹Р№ СЃРєР°РЅ", "Р·Р°РїСѓСЃС‚Рё scan"), "scan_new", "", "", False, 0),
        (("scan reset", "СЃР±СЂРѕСЃ scan", "СЃР±СЂРѕСЃ СЃРєР°РЅР°"), "scan_reset", "", "", False, 0),
        (("stop scan", "pause scan", "СЃС‚РѕРї СЃРєР°РЅ", "РїР°СѓР·Р° scan", "РїР°СѓР·Р° СЃРєР°РЅ"), "scan_pause", "", "", False, 0),
        (("gpt reset", "СЃР±СЂРѕСЃ gpt", "РѕС‡РёСЃС‚Рё gpt", "РѕС‡РёСЃС‚Рё РєРѕРЅС‚РµРєСЃС‚ gpt"), "gpt_reset", "", "", False, 0),
    )
    for keywords, action_name, query, action_text, use_database, lines in simple_keyword_actions:
        if any(keyword in lowered for keyword in keywords):
            return {
                "action": action_name,
                "query": query,
                "user_id": "",
                "text": action_text,
                "use_database": use_database,
                "lines": lines,
                "confidence": 0.98,
                "explanation": "Р›РѕРєР°Р»СЊРЅРѕ СЂР°СЃРїРѕР·РЅР°РЅ С‚РёРїРѕРІРѕР№ Р·Р°РїСЂРѕСЃ РєРѕРјР°РЅРґС‹.",
            }

    log_match = re.match(r"^(?:Р»РѕРіРё|Р»РѕРі|tail)\s*(?P<lines>\d{1,4})?\s*$", raw_text, flags=re.IGNORECASE)
    if log_match:
        lines = int(log_match.group("lines") or LOG_TAIL_DEFAULT_LINES)
        return {
            "action": "logs",
            "query": "",
            "user_id": "",
            "text": "",
            "use_database": False,
            "lines": max(1, min(LOG_TAIL_MAX_LINES, lines)),
            "confidence": 0.98,
            "explanation": "Р›РѕРєР°Р»СЊРЅРѕ СЂР°СЃРїРѕР·РЅР°РЅ Р·Р°РїСЂРѕСЃ РїСЂРѕСЃРјРѕС‚СЂР° Р»РѕРіРѕРІ.",
        }

    db_hint = any(token in lowered for token in (" -b", " РёР· Р±Р°Р·С‹", " РїРѕ Р±Р°Р·Рµ", " РёР· sql", " РёР· sqlite", " Р±С‹СЃС‚СЂРѕ"))

    user_lookup_patterns = (
        (r"^(?:РїРѕРєР°Р¶Рё|РЅР°Р№РґРё|РѕС‚РєСЂРѕР№)?\s*(?:РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ|СЋР·РµСЂР°|user)\s+(?P<query>@?[A-Za-z0-9_]{3,32}|\d{1,20})\s*$", "user_summary"),
        (r"^(?:СЃС‚Р°С‚СѓСЃ|РєР°СЂС‚РѕС‡РєР°)\s+(?P<query>@?[A-Za-z0-9_]{3,32}|\d{1,20})\s*$", "user_summary"),
        (r"^(?:РїРѕРґРїРёСЃРєРё|subs|РёРЅС„Рѕ|РёРЅС„РѕСЂРјР°С†РёСЏ)\s+(?P<query>@?[A-Za-z0-9_]{3,32}|\d{1,20})\s*$", "user_subs"),
        (r"^(?:wizard|РІРёР·Р°СЂРґ)\s+(?P<query>\d{1,20})\s*$", "wizard"),
        (r"^(?:РїРѕРєР°Р¶Рё|РЅР°Р№РґРё|РѕС‚РєСЂРѕР№)?\s*(?:РїРѕРґРїРёСЃРє(?:Сѓ|Рё)?|subs|РёРЅС„Рѕ(?:СЂРјР°С†РёСЋ)?)\s+(?:РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ|РїРѕР»СЊР·Р°РєСѓ|СЋР·РµСЂР°|user)\s+(?P<query>@?[A-Za-z0-9_]{3,32}|\d{1,20})\s*$", "user_subs"),
        (r"^(?:РїРѕРєР°Р¶Рё|РЅР°Р№РґРё|РѕС‚РєСЂРѕР№)?\s*(?:РїРѕРґРїРёСЃРє(?:Сѓ|Рё)?|subs|РёРЅС„Рѕ(?:СЂРјР°С†РёСЋ)?)\s+(?P<query>@?[A-Za-z0-9_]{3,32}|\d{1,20})\s*$", "user_subs"),
    )
    for pattern, action_name in user_lookup_patterns:
        match = re.match(pattern, raw_text, flags=re.IGNORECASE)
        if not match:
            continue
        query = str(match.group("query") or "").strip()
        return {
            "action": action_name,
            "query": query,
            "user_id": query.lstrip("@") if action_name == "wizard" else "",
            "text": "",
            "use_database": db_hint,
            "lines": 0,
            "confidence": 0.97,
            "explanation": "Р›РѕРєР°Р»СЊРЅРѕ СЂР°СЃРїРѕР·РЅР°РЅ Р·Р°РїСЂРѕСЃ РїРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ.",
        }

    mail_patterns = (
        r"^(?:РѕС‚РїСЂР°РІ(?:СЊ|РёС‚СЊ)|РїРѕС€Р»Рё|РЅР°РїРёС€Рё)\s+(?:СЃРѕРѕР±С‰РµРЅРёРµ|РїРёСЃСЊРјРѕ|mail)\s+(?:РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ|СЋР·РµСЂСѓ|user)\s+(?P<user_id>\d{1,20})\s*(?:СЃ\s+С‚РµРєСЃС‚РѕРј)?\s*[,:\-]?\s*(?P<text>.+)$",
        r"^(?:РѕС‚РїСЂР°РІ(?:СЊ|РёС‚СЊ)|РїРѕС€Р»Рё|РЅР°РїРёС€Рё)\s+(?:РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ|СЋР·РµСЂСѓ|user)\s+(?P<user_id>\d{1,20})\s*(?:СЃ\s+С‚РµРєСЃС‚РѕРј)?\s*[,:\-]?\s*(?P<text>.+)$",
        r"^(?:РЅР°РїРёС€Рё|РѕС‚РїСЂР°РІ(?:СЊ|РёС‚СЊ)|РїРѕС€Р»Рё)\s+(?:СЋР·РµСЂСѓ|РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ|user)\s+(?P<user_id>\d{1,20})\s+(?P<text>.+)$",
        r"^(?:РѕС‚РїСЂР°РІ(?:СЊ|РёС‚СЊ)|РїРѕС€Р»Рё)\s+(?P<user_id>\d{1,20})\s*(?:СЃ\s+С‚РµРєСЃС‚РѕРј)?\s*[,:\-]?\s*(?P<text>.+)$",
    )
    for pattern in mail_patterns:
        match = re.match(pattern, raw_text, flags=re.IGNORECASE)
        if not match:
            continue
        user_id = str(match.group("user_id") or "").strip()
        message_text = str(match.group("text") or "").strip(" \t\r\n,;:-")
        if not user_id or not message_text:
            continue
        return {
            "action": "mail",
            "query": user_id,
            "user_id": user_id,
            "text": message_text,
            "use_database": False,
            "lines": 0,
            "confidence": 0.99,
            "explanation": "РџСЂСЏРјР°СЏ РєРѕРјР°РЅРґР° РѕС‚РїСЂР°РІРєРё СЃРѕРѕР±С‰РµРЅРёСЏ СЂР°СЃРїРѕР·РЅР°РЅР° Р±РµР· РѕР±СЂР°С‰РµРЅРёСЏ Рє KBR_GPT.",
        }

    broadcast_patterns = (
        r"^(?:СЃРґРµР»Р°Р№|Р·Р°РїСѓСЃС‚Рё|РѕС‚РїСЂР°РІСЊ)?\s*(?:СЂР°СЃСЃС‹Р»РєСѓ|broadcast|mail2)\s*(?:РІСЃРµРј\s+Р±РµР·\s+РїРѕРґРїРёСЃРєРё)?\s*[,:\-]?\s*(?P<text>.+)$",
    )
    for pattern in broadcast_patterns:
        match = re.match(pattern, raw_text, flags=re.IGNORECASE)
        if not match:
            continue
        message_text = str(match.group("text") or "").strip(" \t\r\n,;:-")
        if not message_text:
            continue
        return {
            "action": "broadcast",
            "query": "",
            "user_id": "",
            "text": message_text,
            "use_database": False,
            "lines": 0,
            "confidence": 0.97,
            "explanation": "Р›РѕРєР°Р»СЊРЅРѕ СЂР°СЃРїРѕР·РЅР°РЅ Р·Р°РїСЂРѕСЃ СЂР°СЃСЃС‹Р»РєРё.",
        }

    promo_match = re.match(
        r"^(?:СЃРѕР·РґР°Р№|СЃРґРµР»Р°Р№)?\s*(?:РїСЂРѕРјРѕРєРѕРґ|РєСѓРїРѕРЅ|promo)\s+(?:РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ\s+)?(?P<user_id>\d{1,20})(?:\s*[,:\-]?\s*(?P<text>.+))?$",
        raw_text,
        flags=re.IGNORECASE,
    )
    if promo_match:
        user_id = str(promo_match.group("user_id") or "").strip()
        message_text = str(promo_match.group("text") or "").strip(" \t\r\n,;:-")
        return {
            "action": "promo",
            "query": user_id,
            "user_id": user_id,
            "text": message_text,
            "use_database": False,
            "lines": 0,
            "confidence": 0.97,
            "explanation": "Р›РѕРєР°Р»СЊРЅРѕ СЂР°СЃРїРѕР·РЅР°РЅ Р·Р°РїСЂРѕСЃ РїСЂРѕРјРѕРєРѕРґР°.",
        }

    wizard_patterns = (
        r"^(?:РѕС‚РїСЂР°РІ(?:СЊ|РёС‚СЊ)|СЃРґРµР»Р°Р№|СЃРѕР·РґР°Р№|РїРѕРґРіРѕС‚РѕРІСЊ)\s+(?:РІ\s+)?(?:wizard|РІРёР·Р°СЂРґ|РІРёР·Р°РґСЂ)\s+(?:СЋР·РµСЂР°|РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ|user)?\s*(?P<user_id>\d{1,20})\s*(?P<text>.*)$",
        r"^(?:wizard|РІРёР·Р°СЂРґ|РІРёР·Р°РґСЂ)\s+(?:СЋР·РµСЂР°|РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ|user)?\s*(?P<user_id>\d{1,20})\s*(?P<text>.*)$",
    )
    for pattern in wizard_patterns:
        match = re.match(pattern, raw_text, flags=re.IGNORECASE)
        if not match:
            continue
        user_id = str(match.group("user_id") or "").strip()
        extra_text = str(match.group("text") or "").strip(" \t\r\n,;:-")
        if not user_id:
            continue
        if extra_text.casefold().startswith("СЃ РїСЂРѕР±Р»РµРј"):
            extra_text = extra_text[10:].strip(" \t\r\n,;:-")
        return {
            "action": "wizard",
            "query": user_id,
            "user_id": user_id,
            "text": extra_text,
            "use_database": False,
            "lines": 0,
            "confidence": 0.99,
            "explanation": "Р›РѕРєР°Р»СЊРЅРѕ СЂР°СЃРїРѕР·РЅР°РЅ Р·Р°РїСЂРѕСЃ РѕС‚РїСЂР°РІРєРё РєР°СЂС‚РѕС‡РєРё РІ wizard.",
        }

    return None


class TextCommandEvent:
    def __init__(self, base_event, raw_text: str):
        self._base_event = base_event
        self.raw_text = raw_text
        self.text = raw_text
        self.message = None

    def __getattr__(self, name: str):
        return getattr(self._base_event, name)


async def execute_text_command(event, command_text: str) -> None:
    log_action_event(
        "execute_text_command",
        sender_id=getattr(event, "sender_id", None),
        chat_id=getattr(event, "chat_id", None),
        command_text=command_text,
    )
    await handle_private_message(TextCommandEvent(event, command_text))


def command_from_smart_action(action: dict) -> tuple[str, bool, str]:
    name = str(action.get("action") or "chat").strip()
    query = str(action.get("query") or "").strip()
    user_id = str(action.get("user_id") or "").strip()
    text = str(action.get("text") or "").strip()
    use_database = bool(action.get("use_database"))
    suffix = " -b" if use_database else ""
    if name == "menu":
        return "menu", False, "РћС‚РєСЂС‹С‚СЊ РјРµРЅСЋ"
    if name == "dashboard":
        return "/dashboard", False, "РћС‚РєСЂС‹С‚СЊ admin system"
    if name == "processes":
        return "/processes", False, "РџРѕРєР°Р·Р°С‚СЊ РїСЂРѕС†РµСЃСЃС‹"
    if name == "diag":
        return "/diag", False, "РџРѕРєР°Р·Р°С‚СЊ РґРёР°РіРЅРѕСЃС‚РёРєСѓ"
    if name == "version":
        return "/version", False, "РџРѕРєР°Р·Р°С‚СЊ РІРµСЂСЃРёСЋ"
    if name == "logs":
        lines = max(1, min(LOG_TAIL_MAX_LINES, int(action.get("lines") or LOG_TAIL_DEFAULT_LINES)))
        return f"/tail {lines}", False, f"РџРѕРєР°Р·Р°С‚СЊ РїРѕСЃР»РµРґРЅРёРµ {lines} СЃС‚СЂРѕРє Р»РѕРіР°"
    if name == "user_summary":
        lookup = query or user_id
        if not lookup:
            return "", False, ""
        return f"/user {lookup}{suffix}", False, f"РљСЂР°С‚РєР°СЏ РєР°СЂС‚РѕС‡РєР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {lookup}"
    if name == "user_subs":
        lookup = query or user_id
        if not lookup:
            return "", False, ""
        return f"/subs {lookup}{suffix}", False, f"РџРѕРґРїРёСЃРєРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {lookup}"
    if name == "wizard":
        if not user_id:
            return "", False, ""
        return f"/wizard {user_id}", True, f"РџРѕРґРіРѕС‚РѕРІРёС‚СЊ wizard РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ {user_id}"
    if name == "mail":
        if not user_id or not text:
            return "", False, ""
        return f"/send {user_id} {text}".strip(), True, f"РћС‚РїСЂР°РІРёС‚СЊ СЃРѕРѕР±С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ {user_id}"
    if name == "broadcast":
        if not text:
            return "", False, ""
        return f"/broadcast {text}".strip(), True, "Р—Р°РїСѓСЃС‚РёС‚СЊ СЂР°СЃСЃС‹Р»РєСѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏРј Р±РµР· РїРѕРґРїРёСЃРєРё"
    if name == "promo":
        if not user_id:
            return "", False, ""
        return f"/coupon {user_id} {text}".strip(), True, f"РЎРѕР·РґР°С‚СЊ РїСЂРѕРјРѕРєРѕРґ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ {user_id}"
    if name == "scan_menu":
        return "scan", False, "РћС‚РєСЂС‹С‚СЊ scan"
    if name == "scan_new":
        return "scan new", True, "Р—Р°РїСѓСЃС‚РёС‚СЊ РЅРѕРІС‹Р№ scan"
    if name == "scan_continue":
        return "scan continue", False, "РџСЂРѕРґРѕР»Р¶РёС‚СЊ scan"
    if name == "scan_results":
        return "scan results", False, "РџРѕРєР°Р·Р°С‚СЊ СЂРµР·СѓР»СЊС‚Р°С‚С‹ scan"
    if name == "scan_pause":
        return "stop СЃРєР°РЅ", False, "РџРѕСЃС‚Р°РІРёС‚СЊ scan РЅР° РїР°СѓР·Сѓ"
    if name == "scan_reset":
        return "scan reset", True, "РЎР±СЂРѕСЃРёС‚СЊ scan"
    if name == "gpt_reset":
        return "/gpt reset", False, "РћС‡РёСЃС‚РёС‚СЊ РєРѕРЅС‚РµРєСЃС‚ KBR_GPT"
    return "", False, ""


async def apply_wizard_note_after_command(event, sender_id: int, note: str) -> None:
    if not note.strip():
        return
    pending = pending_wizard_requests.get(sender_id)
    if not pending:
        return
    base_text = str(pending.get("base_text") or "")
    final_text = "\n\n".join((base_text, f"Р”РѕРїРѕР»РЅРµРЅРёРµ РїРѕ РіРѕР»РѕСЃРѕРІРѕР№/СѓРјРЅРѕР№ РєРѕРјР°РЅРґРµ:\n{note.strip()}"))
    pending["extra_text"] = note.strip()
    pending["final_text"] = final_text
    pending["stage"] = "await_final_choice"
    await safe_event_reply(event, f"РћР±РЅРѕРІР»РµРЅРЅС‹Р№ РїСЂРµРґРїСЂРѕСЃРјРѕС‚СЂ wizard:\n\n{final_text}")
    await safe_event_reply(
        event,
        "РћС‚РїСЂР°РІР»СЏС‚СЊ СЌС‚РѕС‚ РІР°СЂРёР°РЅС‚?",
        buttons=[
            [Button.text("1 РѕС‚РїСЂР°РІРёС‚СЊ"), Button.text("2 РёР·РјРµРЅРёС‚СЊ РґРѕРїРёСЃРєСѓ")],
            [Button.text("0 РѕС‚РјРµРЅР°")],
        ],
    )


async def execute_smart_action(event, sender_id: int, action: dict, *, confirmed: bool = False, status_message=None) -> None:
    action_name = str(action.get("action") or "chat").strip()
    original_text = str(action.get("text") or "").strip()
    log_action_event(
        "smart_action",
        sender_id=sender_id,
        chat_id=getattr(event, "chat_id", None),
        action=action_name,
        confirmed=confirmed,
        original_text=original_text,
        query=str(action.get("query") or ""),
        user_id=str(action.get("user_id") or ""),
    )
    if action_name == "chat":
        await handle_gpt_prompt(
            event,
            sender_id,
            original_text or str(action.get("query") or ""),
            status_message=status_message,
            compact_status=True,
            reveal_unavailable=False,
        )
        return

    command_text, requires_confirmation, title = command_from_smart_action(action)
    if not command_text:
        await handle_gpt_prompt(
            event,
            sender_id,
            original_text or str(action.get("query") or ""),
            status_message=status_message,
            compact_status=True,
            reveal_unavailable=False,
        )
        return

    if requires_confirmation and not confirmed:
        pending_smart_actions[sender_id] = {
            "stage": "await_confirm",
            "action": action,
            "command_text": command_text,
            "created_at": now_timestamp(),
        }
        log_action_event(
            "smart_action_pending_confirm",
            sender_id=sender_id,
            chat_id=getattr(event, "chat_id", None),
            action=action_name,
            command_text=command_text,
            title=title,
        )
        details = [
            "РЇ РїРѕРЅСЏР» С‚Р°Рє:",
            title,
            f"РљРѕРјР°РЅРґР°: {command_text}",
        ]
        if original_text:
            details.append(f"РўРµРєСЃС‚: {original_text}")
        details.append("")
        details.append("1 РІС‹РїРѕР»РЅРёС‚СЊ")
        details.append("0 РѕС‚РјРµРЅР°")
        await safe_event_reply(
            event,
            "\n".join(details),
            buttons=[[Button.text("1 РІС‹РїРѕР»РЅРёС‚СЊ"), Button.text("0 РѕС‚РјРµРЅР°")]],
        )
        return

    await execute_text_command(event, command_text)
    if action_name == "wizard":
        await apply_wizard_note_after_command(event, sender_id, original_text)


async def handle_smart_request(event, sender_id: int, request_text: str, *, source: str, compact_status: bool = False) -> None:
    log_action_event(
        "smart_request_start",
        sender_id=sender_id,
        chat_id=getattr(event, "chat_id", None),
        source=source,
        compact_status=compact_status,
        request_text=request_text,
    )
    if not settings.smart_controller_enabled:
        log_action_event(
            "smart_request_bypass",
            sender_id=sender_id,
            chat_id=getattr(event, "chat_id", None),
            reason="smart_controller_disabled",
        )
        await handle_gpt_prompt(event, sender_id, request_text, compact_status=True, reveal_unavailable=False)
        return
    if compact_status:
        status_message = await safe_event_reply(
            event,
            assistant_compact_reply("РџРѕРЅСЏР» Р·Р°РїСЂРѕСЃ.", "РћРїСЂРµРґРµР»СЏСЋ, С‡С‚Рѕ Р»СѓС‡С€Рµ СЃРґРµР»Р°С‚СЊ."),
        )
    else:
        status_message = await safe_event_reply(
            event,
            build_process_status(
                "РЈРјРЅС‹Р№ РїРѕРјРѕС‰РЅРёРє",
                SMART_STEPS,
                3,
                extra_lines=[f"СЃС‚РѕС‡РЅРёРє: {source}", f"РўРµРєСЃС‚: {len(request_text)} СЃРёРјРІРѕР»РѕРІ"],
            ),
        )
    try:
        action = await classify_smart_request(request_text)
        action_name = str(action.get("action") or "chat").strip()
        log_action_event(
            "smart_request_classified",
            sender_id=sender_id,
            chat_id=getattr(event, "chat_id", None),
            action=action_name,
            explanation=str(action.get("explanation") or ""),
            query=str(action.get("query") or ""),
            user_id=str(action.get("user_id") or ""),
            text=str(action.get("text") or ""),
        )
        if compact_status:
            explanation = str(action.get("explanation") or "").strip()
            if action_name == "chat":
                await edit_status_message(
                    status_message,
                    assistant_compact_reply("РџРѕРЅСЏР» Р·Р°РїСЂРѕСЃ.", "Р“РѕС‚РѕРІР»СЋ РѕС‚РІРµС‚."),
                    force=True,
                )
            else:
                _, _, title = command_from_smart_action(action)
                detail = explanation[:160] if explanation else (title or "РџРѕРґРіРѕС‚Р°РІР»РёРІР°СЋ РґРµР№СЃС‚РІРёРµ.")
                await edit_status_message(
                    status_message,
                    assistant_compact_reply("РџРѕРЅСЏР» Р·Р°РґР°С‡Сѓ.", detail),
                    force=True,
                )
        else:
            await edit_status_message(
                status_message,
                build_process_status(
                    "РЈРјРЅС‹Р№ РїРѕРјРѕС‰РЅРёРє",
                    SMART_STEPS,
                    4,
                    extra_lines=[
                        f"Р Р°СЃРїРѕР·РЅР°РЅРѕ РґРµР№СЃС‚РІРёРµ: {action.get('action', 'chat')}",
                        str(action.get("explanation") or "").strip()[:200],
                    ],
                    done=True,
                ),
                force=True,
            )
        await execute_smart_action(event, sender_id, action, status_message=status_message if compact_status else None)
    except Exception as error:
        fallback_action = detect_direct_smart_action(request_text)
        if fallback_action is not None:
            logging.warning(
                "Smart request fallback sender_id=%s source=%s reason=%s action=%s",
                sender_id,
                source,
                str(error)[:300],
                fallback_action.get("action"),
            )
            log_action_event(
                "smart_request_fallback",
                sender_id=sender_id,
                chat_id=getattr(event, "chat_id", None),
                source=source,
                error=str(error),
                fallback_action=str(fallback_action.get("action") or ""),
            )
            await edit_status_message(
                status_message,
                assistant_compact_reply(
                    "РџРѕРЅСЏР» Р·Р°РґР°С‡Сѓ.",
                    "Р’С‹РїРѕР»РЅСЏСЋ РµРµ Р±РµР· РѕР±СЂР°С‰РµРЅРёСЏ Рє KBR_GPT.",
                ),
                force=True,
            )
            await execute_smart_action(event, sender_id, fallback_action, status_message=status_message)
            return
        logging.exception("Smart request failed sender_id=%s source=%s", sender_id, source)
        log_action_event(
            "smart_request_error",
            sender_id=sender_id,
            chat_id=getattr(event, "chat_id", None),
            source=source,
            error=str(error),
        )
        await edit_status_message(
            status_message,
            assistant_compact_reply(
                "РќРµ СѓРґР°Р»РѕСЃСЊ СЃСЂР°Р·Сѓ СЂР°СЃРїРѕР·РЅР°С‚СЊ РґРµР№СЃС‚РІРёРµ.",
                "РџСЂРѕР±СѓСЋ РѕС‚РІРµС‚РёС‚СЊ РєР°Рє KBR_GPT.",
            ),
            force=True,
        )
        await handle_gpt_prompt(
            event,
            sender_id,
            request_text,
            status_message=status_message,
            compact_status=True,
            reveal_unavailable=False,
        )


def format_promo_mail_text(user_id: str, promo_code: str) -> str:
    try:
        return settings.promo_mail_text.format(
            user_id=user_id,
            promo_code=promo_code,
            promo_budget=settings.promo_budget_rub,
            promo_amount=settings.promo_amount_rub,
        )
    except Exception:
        logging.exception("Failed to format PROMO_MAIL_TEXT; using fallback text")
        return f"Р”Р»СЏ РІР°СЃ СЃРѕР·РґР°РЅ РїСЂРѕРјРѕРєРѕРґ {promo_code} РЅР° {settings.promo_amount_rub} СЂСѓР±."


def parse_promo_command(text: str) -> tuple[str, str, str] | None:
    match = re.match(
        rf"^\s*/?(?:{command_alias_pattern('promo', 'coupon', 'promocode', 'РїСЂРѕРјРѕ', 'РїСЂРѕРјРѕРєРѕРґ')})\s+(\d{{1,20}})(?:\s+([\s\S]+))?\s*$",
        text or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    user_id = match.group(1)
    promo_code = f"{user_id}nPromo"
    message_text = (match.group(2) or "").strip() or format_promo_mail_text(user_id, promo_code)
    return user_id, promo_code, message_text


def parse_help_command(text: str) -> UserLookupCommand | None:
    return parse_user_lookup_command(("help", "user", "find", "РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ", "РЅР°Р№С‚Рё"), text)


def is_help_overview_command(text: str) -> bool:
    return bool(re.match(r"^\s*/?help\s*$", text, flags=re.IGNORECASE))


def is_command_menu_command(text: str) -> bool:
    return bool(re.match(r"^\s*/?(?:menu|commands|cmd|РєРѕРјР°РЅРґС‹|РјРµРЅСЋ)\s*$", text, flags=re.IGNORECASE))


def is_status_command(text: str) -> bool:
    return bool(re.match(r"^\s*/?(?:dashboard|dash|status|report|РґР°С€Р±РѕСЂРґ|РѕС‚С‡РµС‚|РѕС‚С‡С‘С‚|СЃС‚Р°С‚СѓСЃ)\s*$", text, flags=re.IGNORECASE))


def is_admin_site_command(text: str) -> bool:
    return bool(
        re.match(
            r"^\s*/?(?:adminsite|admin_site|liveadmin|adminpanel|Р°РґРјРёРЅСЃР°Р№С‚|Р°РґРјРёРЅ\s*СЃР°Р№С‚)\s*$",
            text,
            flags=re.IGNORECASE,
        )
    )


def is_root_panel_command(text: str) -> bool:
    return bool(
        re.match(
            r"^\s*/?(?:root|rootpanel|root_site|РѕРїРµСЂР°С‚РѕСЂ|operator)\s*$",
            text,
            flags=re.IGNORECASE,
        )
    )


def is_version_command(text: str) -> bool:
    return bool(re.match(r"^\s*/?(?:version|РІРµСЂСЃРёСЏ|v)\s*$", text, flags=re.IGNORECASE))


def is_diagnostics_command(text: str) -> bool:
    return bool(re.match(r"^\s*/?(?:diag|diagnostics|doctor|health|РґРёР°РіРЅРѕСЃС‚РёРєР°)\s*$", text, flags=re.IGNORECASE))


def is_poc_command(text: str) -> bool:
    return bool(
        re.match(
            r"^\s*/?(?:poc|proc|process|processes|tasks|jobs|ps|РїСЂРѕС†РµСЃСЃС‹|РїСЂРѕС†РµСЃСЃ|РїСЂРѕС†РµСЃ|Р·Р°РґР°С‡Рё|Рїoc)\s*$",
            text,
            flags=re.IGNORECASE,
        )
    )


def is_roots_command(text: str) -> bool:
    return bool(re.match(r"^\s*/?roots(?:\s+.*)?$", text or "", flags=re.IGNORECASE))


def parse_template_command(text: str) -> tuple[str, str] | None:
    match = re.match(r"^\s*/?(?:tpl|template|С€Р°Р±Р»РѕРЅ)(?:\s+([\w\-]+)(?:\s+([\s\S]+))?)?\s*$", text or "", flags=re.IGNORECASE)
    if not match:
        return None
    key = str(match.group(1) or "").strip().casefold()
    rest = str(match.group(2) or "").strip()
    return key, rest


def parse_scan_menu_action(text: str, allow_numeric: bool = False) -> str | None:
    cleaned = text.strip().casefold()
    if cleaned in {"/scan", "scan", "СЃРєР°РЅ"}:
        return "menu"
    if cleaned in {"/scan_new", "scan new", "new scan", "РЅРѕРІС‹Р№ scan", "РЅРѕРІС‹Р№ СЃРєР°РЅ", "РЅР°С‡Р°С‚СЊ СЃРєР°РЅ", "Р·Р°РїСѓСЃС‚Рё СЃРєР°РЅ"}:
        return "new"
    if cleaned in {
        "/scan_start",
        "/scan_continue",
        "scan start",
        "scan continue",
        "continue scan",
        "start scan",
        "РїСЂРѕРґРѕР»Р¶РёС‚СЊ scan",
        "РїСЂРѕРґРѕР»Р¶РёС‚СЊ СЃРєР°РЅ",
        "РїСЂРѕРґРѕР»Р¶Рё СЃРєР°РЅ",
    }:
        return "continue"
    if cleaned in {
        "/stopscan",
        "stop scan",
        "stop СЃРєР°РЅ",
        "СЃС‚РѕРї СЃРєР°РЅ",
        "scan stop",
        "scan pause",
        "pause scan",
        "РїР°СѓР·Р° scan",
        "РїР°СѓР·Р° СЃРєР°РЅ",
        "РѕСЃС‚Р°РЅРѕРІРёС‚СЊ scan",
        "РѕСЃС‚Р°РЅРѕРІРёС‚СЊ СЃРєР°РЅ",
    }:
        return "pause_results"
    mapping = {
        "/scan": "start",
        "/scanmenu": "menu",
        "scan menu": "menu",
        "scan status": "menu",
        "СЃС‚Р°С‚СѓСЃ СЃРєР°РЅР°": "menu",
        "СЃС‚Р°С‚СѓСЃ scan": "menu",
        "СЃРєР°РЅС‹": "menu",
        "РјРµРЅСЋ СЃРєР°РЅ": "menu",
        "РјРµРЅСЋ СЃРєР°РЅРѕРІ": "menu",
        "/scan_start": "start",
        "scan": "start",
        "СЃРєР°РЅ": "start",
        "scan start": "start",
        "scan new": "start",
        "start scan": "start",
        "РЅРѕРІС‹Р№ СЃРєР°РЅ": "start",
        "РЅРѕРІС‹Р№ scan": "start",
        "Р·Р°РїСѓСЃС‚Рё СЃРєР°РЅ": "start",
        "РЅР°С‡Р°С‚СЊ СЃРєР°РЅ": "start",
        "РїСЂРѕРґРѕР»Р¶РёС‚СЊ СЃРєР°РЅ": "start",
        "РїСЂРѕРґРѕР»Р¶Рё СЃРєР°РЅ": "start",
        "/stopscan": "pause",
        "/scan_pause": "pause",
        "scan pause": "pause",
        "scan stop": "pause",
        "pause scan": "pause",
        "stop scan": "pause",
        "РїР°СѓР·Р° СЃРєР°РЅ": "pause",
        "РїРѕСЃС‚Р°РІСЊ СЃРєР°РЅ РЅР° РїР°СѓР·Сѓ": "pause",
        "РѕСЃС‚Р°РЅРѕРІРёС‚СЊ СЃРєР°РЅ": "pause",
        "/scan_reset": "reset",
        "scan reset": "reset",
        "reset scan": "reset",
        "СЃР±СЂРѕСЃ СЃРєР°РЅР°": "reset",
        "СЃР±СЂРѕСЃРёС‚СЊ СЃРєР°РЅ": "reset",
        "СЃР±СЂРѕСЃ scan": "reset",
        "/scan_results": "results",
        "scan results": "results",
        "scan result": "results",
        "results scan": "results",
        "СЂРµР·СѓР»СЊС‚Р°С‚С‹ СЃРєР°РЅР°": "results",
        "РїРѕРєР°Р·Р°С‚СЊ СЂРµР·СѓР»СЊС‚Р°С‚С‹ СЃРєР°РЅР°": "results",
    }
    action = mapping.get(cleaned)
    if action:
        return action

    if allow_numeric:
        numeric_mapping = {
            "1": "new",
            "2": "continue",
            "3": "pause_results",
            "4": "results",
            "5": "reset",
            "6": "menu",
        }
        return numeric_mapping.get(cleaned)

    return None


def parse_info_command(text: str) -> UserLookupCommand | None:
    return parse_user_lookup_command(("info", "subs", "subscriptions", "РїРѕРґРїРёСЃРєРё"), text)


def parse_wizard_command(text: str) -> str | None:
    match = re.match(
        rf"^\s*/?(?:{command_alias_pattern('wizard', 'card', 'РєР°СЂС‚РѕС‡РєР°')})\s+(\d{{1,20}})\s*$",
        text or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1)


def parse_wizard_reply_choice(text: str) -> str | None:
    cleaned = text.strip().casefold()
    first_token = cleaned.split(maxsplit=1)[0] if cleaned else ""
    if first_token == "1":
        return "send_now"
    if first_token == "2":
        return "add_text"
    if first_token == "0":
        return "cancel"
    if cleaned in {"1", "РЅРµС‚", "no", "n", "РѕС‚РїСЂР°РІРёС‚СЊ", "send"}:
        return "send_now"
    if cleaned in {"2", "РґР°", "yes", "y", "РґРѕР±Р°РІРёС‚СЊ", "add"}:
        return "add_text"
    if cleaned in {"0", "РѕС‚РјРµРЅР°", "cancel", "/cancel"}:
        return "cancel"
    return None


def is_control_reply_text(text: str) -> bool:
    cleaned = str(text or "").strip().casefold()
    if not cleaned:
        return False
    if re.fullmatch(r"\d{1,3}", cleaned):
        return True
    return cleaned in {
        "РґР°",
        "РЅРµС‚",
        "yes",
        "no",
        "y",
        "n",
        "send",
        "cancel",
        "/cancel",
        "РѕС‚РјРµРЅР°",
        "РІС‹РїРѕР»РЅРёС‚СЊ",
        "РѕС‚РїСЂР°РІРёС‚СЊ",
    }


def current_pending_workflow_name(sender_id: int) -> str:
    if sender_id in pending_wizard_requests:
        return "wizard"
    if sender_id in pending_mail2_requests:
        return "mail2"
    if sender_id in pending_direct_mail_requests:
        return "mail"
    if sender_id in pending_gpt_requests:
        return "gpt"
    if sender_id in pending_smart_actions:
        return "smart"
    if sender_id in pending_support_requests:
        return "support"
    return ""


def is_explicit_requester_command_input(text: str, sender_id: int) -> bool:
    raw_text = str(text or "").strip()
    if not raw_text:
        return False
    if raw_text.startswith("/"):
        return True
    if is_command_menu_command(raw_text):
        return True
    if is_help_overview_command(raw_text):
        return True
    if is_requester_capabilities_question(raw_text):
        return True
    if is_version_command(raw_text) or is_diagnostics_command(raw_text) or is_status_command(raw_text):
        return True
    if is_admin_site_command(raw_text) or is_poc_command(raw_text) or is_roots_command(raw_text):
        return True
    if parse_logs_command(raw_text) is not None or parse_unresolved_command(raw_text) is not None:
        return True
    if parse_gpt_command(raw_text) is not None:
        return True
    if parse_mail_command(raw_text) is not None or parse_mail2_command(raw_text) is not None:
        return True
    if parse_promo_command(raw_text) is not None:
        return True
    if parse_help_command(raw_text) is not None or parse_info_command(raw_text) is not None:
        return True
    if parse_wizard_command(raw_text):
        return True
    if parse_scan_menu_action(raw_text, allow_numeric=active_scan_menu_owner_id == sender_id):
        return True
    if parse_scan_command(raw_text):
        return True
    return False


def mark_active_gpt_request(
    sender_id: int,
    *,
    canceled: bool = False,
    suppress_output: bool = False,
    reason: str = "",
) -> bool:
    request_state = active_gpt_requests.get(sender_id)
    if not request_state:
        return False
    if canceled:
        request_state["canceled"] = True
    if suppress_output:
        request_state["suppress_output"] = True
    if reason:
        request_state["reason"] = reason
    return True


def current_command_execution_name(sender_id: int) -> str:
    pending_name = current_pending_workflow_name(sender_id)
    if pending_name:
        return pending_name
    if active_scan_cancel_event and not active_scan_cancel_event.is_set() and active_scan_owner_id == sender_id:
        return "scan"
    if active_mail2_cancel_event and not active_mail2_cancel_event.is_set():
        return "mail2"
    if active_admin_flow:
        flow_user_id = str(active_admin_flow.get("user_id") or "").strip()
        if flow_user_id and flow_user_id == str(sender_id):
            return str(active_admin_flow.get("name") or "command")
    return ""


def command_reply_guard_message(workflow_name: str = "") -> str:
    workflow_label = workflow_name.strip() or "С‚РµРєСѓС‰РµР№ РєРѕРјР°РЅРґС‹"
    return assistant_compact_reply(
        "РЎРµР№С‡Р°СЃ Р¶РґСѓ РѕС‚РІРµС‚ РґР»СЏ Р°РєС‚РёРІРЅРѕР№ РєРѕРјР°РЅРґС‹.",
        f"РљРѕСЂРѕС‚РєРёРµ РѕС‚РІРµС‚С‹ РІСЂРѕРґРµ `1`, `2`, `0`, `РґР°`, `РЅРµС‚` РѕР±СЂР°Р±Р°С‚С‹РІР°СЋС‚СЃСЏ С‚РѕР»СЊРєРѕ РІРЅСѓС‚СЂРё {workflow_label}.",
    )


def unknown_slash_command_message() -> str:
    return assistant_compact_reply(
        "РќРµРёР·РІРµСЃС‚РЅР°СЏ РєРѕРјР°РЅРґР°.",
        "РќР°РїРёС€РёС‚Рµ `menu`, С‡С‚РѕР±С‹ СѓРІРёРґРµС‚СЊ РґРѕСЃС‚СѓРїРЅС‹Рµ РєРѕРјР°РЅРґС‹.",
    )


def gpt_queue_message(position: int = 1, estimated_wait_seconds: float = 0.0) -> str:
    position = max(1, int(position or 1))
    wait_seconds = max(0, int(round(estimated_wait_seconds or 0)))
    detail = f"РџРѕР·РёС†РёСЏ: {position}."
    if wait_seconds > 0:
        detail += f" РџСЂРёРјРµСЂРЅРѕРµ РѕР¶РёРґР°РЅРёРµ: {wait_seconds} СЃРµРє."
    detail += " Р—Р°РїСЂРѕСЃ РІС‹РїРѕР»РЅРёС‚СЃСЏ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё."
    return assistant_compact_reply("Р—Р°РїСЂРѕСЃ РІ РѕС‡РµСЂРµРґРё.", detail)


def normalize_gpt_cache_key(prompt: str) -> str:
    cleaned = re.sub(r"\s+", " ", sanitize_outgoing_text(prompt).strip().casefold())
    return cleaned[:500]


def get_cached_gpt_answer(prompt: str) -> str | None:
    if GPT_CACHE_TTL_SECONDS <= 0 or GPT_CACHE_MAX_ITEMS <= 0:
        return None
    key = normalize_gpt_cache_key(prompt)
    if not key:
        return None
    cached = gpt_response_cache.get(key)
    if not cached:
        return None
    created_at, answer = cached
    if time.monotonic() - created_at > GPT_CACHE_TTL_SECONDS:
        gpt_response_cache.pop(key, None)
        return None
    return answer


def store_cached_gpt_answer(prompt: str, answer: str) -> None:
    if GPT_CACHE_TTL_SECONDS <= 0 or GPT_CACHE_MAX_ITEMS <= 0:
        return
    key = normalize_gpt_cache_key(prompt)
    if not key:
        return
    gpt_response_cache[key] = (time.monotonic(), sanitize_outgoing_text(answer).strip())
    while len(gpt_response_cache) > GPT_CACHE_MAX_ITEMS:
        oldest_key = min(gpt_response_cache, key=lambda cache_key: gpt_response_cache[cache_key][0])
        gpt_response_cache.pop(oldest_key, None)


def local_gpt_answer(prompt: str) -> str | None:
    cleaned = sanitize_outgoing_text(prompt).strip()
    lowered = cleaned.casefold()
    if not cleaned:
        return None

    intent = detect_non_requester_intent(cleaned)
    if any(phrase in lowered for phrase in ("С‡С‚Рѕ С‚С‹ СѓРјРµРµС€СЊ", "С‡С‚Рѕ СѓРјРµРµС€СЊ", "РєРѕРјР°РЅРґС‹", "РєР°Рє РїРѕР»СЊР·РѕРІР°С‚СЊСЃСЏ", "РїРѕРјРѕС‰СЊ", "help")):
        return assistant_capabilities_message()
    if intent == "operator":
        return support_operator_contact_text()
    if intent == "vpn_setup_help":
        return vpn_setup_help_message()
    if intent == "profile_id_help":
        return profile_id_help_message()
    if any(word in lowered for word in ("РѕРїР»Р°С‚Р°", "РѕРїР»Р°С‚РёР»", "РїР»Р°С‚РµР¶", "РїР»Р°С‚С‘Р¶", "РґРµРЅСЊРіРё", "С‡РµРє", "РЅРµ РїСЂРѕС€Р»Р° РѕРїР»Р°С‚Р°")):
        return payment_help_message()
    if any(phrase in lowered for phrase in ("РєР»СЋС‡ РЅРµ СЂР°Р±РѕС‚Р°РµС‚", "РЅРµ СЂР°Р±РѕС‚Р°РµС‚ РєР»СЋС‡", "РЅРµС‚ РёРЅС‚РµСЂРЅРµС‚Р°", "РЅРµ РїРѕРґРєР»СЋС‡Р°РµС‚СЃСЏ", "vpn РЅРµ СЂР°Р±РѕС‚Р°РµС‚", "РІРїРЅ РЅРµ СЂР°Р±РѕС‚Р°РµС‚")):
        return key_problem_help_message()
    if any(word in lowered for word in ("РјРµРґР»РµРЅРЅРѕ", "СЃРєРѕСЂРѕСЃС‚СЊ", "С‚РѕСЂРјРѕР·РёС‚", "РїРёРЅРі", "Р»Р°РіР°РµС‚")):
        return speed_problem_help_message()
    if any(word in lowered for word in ("РїРѕРґРїРёСЃРєР°", "РїРѕРґРїРёСЃРєСѓ", "РїСЂРѕРґР»РёС‚СЊ", "РёСЃС‚РµРєР°РµС‚", "Р·Р°РєРѕРЅС‡РёР»Р°СЃСЊ")):
        return subscription_help_message()
    if any(phrase in lowered for phrase in ("Р°РґРјРёРЅ СЃР°Р№С‚", "Р°РґРјРёРЅРєР°", "dashboard", "РґР°С€Р±РѕСЂРґ", "РїР°РЅРµР»СЊ")):
        return assistant_compact_reply(
            "РђРґРјРёРЅ-РїР°РЅРµР»СЊ РѕС‚РєСЂС‹РІР°РµС‚СЃСЏ РєРѕРјР°РЅРґРѕР№ `/adminsite`.",
            "РўР°Рј РјРѕР¶РЅРѕ СЃРјРѕС‚СЂРµС‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№, РїРѕРґРїРёСЃРєРё, СЃС‚Р°С‚РёСЃС‚РёРєСѓ, Р»РѕРіРё Рё РІС‹РїРѕР»РЅСЏС‚СЊ РґРµР№СЃС‚РІРёСЏ.",
        )
    if any(phrase in lowered for phrase in ("РєР°Рє РѕС‚РїСЂР°РІРёС‚СЊ СЃРѕРѕР±С‰РµРЅРёРµ", "РЅР°РїРёСЃР°С‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ", "РѕС‚РїСЂР°РІРёС‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ")):
        return assistant_compact_reply(
            "РЎРѕРѕР±С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ РјРѕР¶РЅРѕ РѕС‚РїСЂР°РІРёС‚СЊ С‚Р°Рє:",
            "`/send 1231 С‚РµРєСЃС‚ СЃРѕРѕР±С‰РµРЅРёСЏ` РёР»Рё РѕР±С‹С‡РЅС‹РјРё СЃР»РѕРІР°РјРё: В«РЅР°РїРёС€Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ 1231 РїСЂРёРІРµС‚В».",
        )
    if any(phrase in lowered for phrase in ("РєР°Рє РѕС‚РїСЂР°РІРёС‚СЊ РІ wizard", "РѕС‚РїСЂР°РІРёС‚СЊ РІ wizard", "РѕС‚РїСЂР°РІСЊ РІ РІРёР·Р°СЂРґ", "РѕС‚РїСЂР°РІСЊ РІ РІРёР·Р°РґСЂ")):
        return assistant_compact_reply(
            "РљР°СЂС‚РѕС‡РєР° РІ wizard РѕС‚РїСЂР°РІР»СЏРµС‚СЃСЏ С‚Р°Рє:",
            "`/wizard 1231` РёР»Рё РѕР±С‹С‡РЅС‹РјРё СЃР»РѕРІР°РјРё: В«РѕС‚РїСЂР°РІСЊ РІ wizard СЋР·РµСЂР° 1231 СЃ РїСЂРѕР±Р»РµРјРѕР№ РЅРµ СЂР°Р±РѕС‚Р°РµС‚ РєР»СЋС‡В».",
        )
    if any(phrase in lowered for phrase in ("РїСЂРѕРјРѕРєРѕРґ", "РїСЂРѕРјРѕ", "РєСѓРїРѕРЅ")):
        return assistant_compact_reply(
            "РџСЂРѕРјРѕРєРѕРґ РјРѕР¶РЅРѕ СЃРѕР·РґР°С‚СЊ РєРѕРјР°РЅРґРѕР№ `/coupon 1231`.",
            "Р‘РѕС‚ СЃРѕР·РґР°СЃС‚ РєРѕРґ РІРёРґР° `1231nPromo`, РґРѕР¶РґРµС‚СЃСЏ РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ Рё РѕС‚РїСЂР°РІРёС‚ РµРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ.",
        )
    if any(phrase in lowered for phrase in ("scan", "СЃРєР°РЅ", "СЃРєР°РЅРёСЂРѕРІР°РЅРёРµ")):
        return assistant_compact_reply(
            "Scan СѓРїСЂР°РІР»СЏРµС‚СЃСЏ РёР· РјРµРЅСЋ `scan`.",
            "Р”РѕСЃС‚СѓРїРЅРѕ: РЅРѕРІС‹Р№ scan, РїСЂРѕРґРѕР»Р¶РёС‚СЊ, РїР°СѓР·Р°, СЃР±СЂРѕСЃ Рё РїСЂРѕСЃРјРѕС‚СЂ СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ.",
        )
    if intent == "thanks":
        return support_thanks_message()

    words_count = len(re.findall(r"\S+", lowered))
    if intent == "greeting":
        return requester_greeting_message()
    if words_count <= 5 and any(phrase in lowered for phrase in ("РєР°Рє РґРµР»Р°", "РєР°Рє С‚С‹", "С‡С‚Рѕ РЅРѕРІРѕРіРѕ")):
        return assistant_compact_reply("Р’СЃРµ С…РѕСЂРѕС€Рѕ, СЃРїР°СЃРёР±Рѕ.", "Р§РµРј РјРѕРіСѓ РїРѕРјРѕС‡СЊ?")
    if words_count <= 4 and lowered in {"РЅРѕСЂРјР°Р»СЊРЅРѕ", "РЅРѕСЂРј", "С…РѕСЂРѕС€Рѕ", "РїРѕРЅСЏР»", "СЏСЃРЅРѕ", "РѕРє", "РѕРєРµР№"}:
        return assistant_compact_reply("РћС‚Р»РёС‡РЅРѕ.", "Р•СЃР»Рё РїРѕРЅР°РґРѕР±РёС‚СЃСЏ РїРѕРјРѕС‰СЊ, РЅР°РїРёС€РёС‚Рµ РІРѕРїСЂРѕСЃ.")
    if words_count <= 4 and lowered in {"РЅСѓ", "РЅРµ РїРѕРЅСЏР»", "С‡С‚Рѕ РґР°Р»СЊС€Рµ", "РґР°Р»СЊС€Рµ"}:
        return assistant_compact_reply(
            "РњРѕРіСѓ РїРѕРјРѕС‡СЊ.",
            "РќР°РїРёС€РёС‚Рµ РІРѕРїСЂРѕСЃ РїРѕР»РЅРѕСЃС‚СЊСЋ РёР»Рё РїСЂРёС€Р»РёС‚Рµ ID РёР· СЂР°Р·РґРµР»Р° В«РџСЂРѕС„РёР»СЊВ», РµСЃР»Рё РїСЂРѕР±Р»РµРјР° РїРѕ VPN.",
        )
    return None


def parse_scan_command(text: str) -> str | None:
    return parse_scan_menu_action(text, allow_numeric=False)


def build_command_menu_text() -> str:
    return "\n".join(
        (
            "РњРµРЅСЋ Vpn_Bot_assist",
            "",
            "Р“Р»Р°РІРЅС‹Рµ РєРѕРјР°РЅРґС‹:",
            "/dashboard - СЃСЃС‹Р»РєР° РЅР° Р°РЅР°Р»РёС‚РёС‡РµСЃРєРёР№ dashboard РёР· SQLite Р±Р°Р·С‹",
            "/adminsite - РѕС‚РєСЂС‹С‚СЊ live admin system",
            "/processes - Р°РєС‚РёРІРЅС‹Рµ Р·Р°РґР°С‡Рё: scan, mail2, wizard Рё РѕР¶РёРґР°РЅРёСЏ",
            "/diag - РґРёР°РіРЅРѕСЃС‚РёРєР° Р±РѕС‚Р°, Р±Р°Р·С‹, scan Рё dashboard",
            "/unresolved - РѕР±СЂР°С‰РµРЅРёСЏ, РЅР° РєРѕС‚РѕСЂС‹Рµ Р±РѕС‚ РЅРµ РѕС‚РІРµС‚РёР» СЃР°Рј",
            "/tail [СЃС‚СЂРѕРє] - РїРѕСЃР»РµРґРЅРёРµ СЃС‚СЂРѕРєРё userbot.log",
            "/version - РІРµСЂСЃРёСЏ, commit Рё РґР°С‚Р° Р·Р°РїСѓСЃРєР°",
            "",
            "РџРѕР»СЊР·РѕРІР°С‚РµР»Рё:",
            "/user <id|username> - РєСЂР°С‚РєР°СЏ РєР°СЂС‚РѕС‡РєР° С‡РµСЂРµР· Р°РґРјРёРЅ-Р±РѕС‚",
            "/user <id|username> -b - РєСЂР°С‚РєР°СЏ РєР°СЂС‚РѕС‡РєР° РёР· SQLite Р±Р°Р·С‹",
            "/subs <id|username> - РїРѕРґСЂРѕР±РЅР°СЏ РёРЅС„РѕСЂРјР°С†РёСЏ Рё РїРѕРґРїРёСЃРєРё С‡РµСЂРµР· Р°РґРјРёРЅ-Р±РѕС‚",
            "/subs <id|username> -b - РїРѕРґСЂРѕР±РЅР°СЏ РёРЅС„РѕСЂРјР°С†РёСЏ Рё РїРѕРґРїРёСЃРєРё РёР· SQLite Р±Р°Р·С‹",
            "/wizard <id> - РїРѕРґРіРѕС‚РѕРІРёС‚СЊ РєР°СЂС‚РѕС‡РєСѓ, РїСЂРѕРІРµСЂРёС‚СЊ Рё РѕС‚РїСЂР°РІРёС‚СЊ РІ wizard",
            "",
            "РЎРѕРѕР±С‰РµРЅРёСЏ Рё РїСЂРѕРјРѕ:",
            "/send <id> <С‚РµРєСЃС‚> - РѕС‚РїСЂР°РІРёС‚СЊ СЃРѕРѕР±С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
            "/send <id> - РѕС‚РїСЂР°РІРёС‚СЊ С‚РµРєСЃС‚ РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ РёР· MAIL_TEXT",
            "/broadcast <С‚РµРєСЃС‚> - СЂР°СЃСЃС‹Р»РєР° РІСЃРµРј РїРѕР»СЊР·РѕРІР°С‚РµР»СЏРј Р±РµР· РїРѕРґРїРёСЃРєРё РёР· SQLite Р±Р°Р·С‹",
            "/broadcast - РїРѕРїСЂРѕСЃРёС‚СЊ С‚РµРєСЃС‚ СЃР»РµРґСѓСЋС‰РёРј СЃРѕРѕР±С‰РµРЅРёРµРј",
            "/coupon <id> - СЃРѕР·РґР°С‚СЊ РїСЂРѕРјРѕРєРѕРґ <id>nPromo Рё РѕС‚РїСЂР°РІРёС‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
            "",
            "Scan:",
            "scan - РјРµРЅСЋ СЃРєР°РЅР°",
            "scan new - РЅРѕРІС‹Р№ СЃРєР°РЅ СЃ РїРµСЂРІРѕРіРѕ ID",
            "scan continue - РїСЂРѕРґРѕР»Р¶РёС‚СЊ СЃРѕС…СЂР°РЅРµРЅРЅС‹Р№ СЃРєР°РЅ",
            "stop СЃРєР°РЅ - РїР°СѓР·Р° Рё РїСЂРѕСЃРјРѕС‚СЂ СЂРµР·СѓР»СЊС‚Р°С‚РѕРІ",
            "scan results - СЂРµР·СѓР»СЊС‚Р°С‚С‹ scan Рё dashboard",
            "scan reset - СЃР±СЂРѕСЃ СЃРѕС…СЂР°РЅРµРЅРЅРѕРіРѕ scan",
            "",
            "Р”РѕСЃС‚СѓРї:",
            "/roots - СЃРїРёСЃРѕРє Р·Р°РїСЂРѕСЃРЅРёРєРѕРІ",
            "/roots add <user_id|@username|me> - РґРѕР±Р°РІРёС‚СЊ Р·Р°РїСЂРѕСЃРЅРёРєР°",
            "/roots del <user_id|@username> - СѓРґР°Р»РёС‚СЊ Р·Р°РїСЂРѕСЃРЅРёРєР°",
            "",
            "РЎС‚Р°СЂС‹Рµ РЅР°Р·РІР°РЅРёСЏ С‚РѕР¶Рµ СЂР°Р±РѕС‚Р°СЋС‚: status, help, info, mail, mail2, promo, poc, logs.",
        )
    )


def build_command_menu_buttons():
    return [
        [Button.text("scan"), Button.text("scan results")],
        [Button.text("/dashboard"), Button.text("/adminsite"), Button.text("/diag")],
        [Button.text("/processes"), Button.text("/tail")],
        [Button.text("/gpt"), Button.text("/gpt reset")],
        [Button.text("menu")],
        [Button.text("scan new"), Button.text("scan continue")],
        [Button.text("stop СЃРєР°РЅ"), Button.text("scan reset")],
        [Button.text("/user 123456789"), Button.text("/subs 123456789")],
        [Button.text("/user username -b"), Button.text("/subs username -b")],
        [Button.text("/wizard 123456789"), Button.text("/coupon 123456789")],
        [Button.text("/send 123456789"), Button.text("/broadcast")],
        [Button.text("/roots"), Button.text("/roots add me")],
    ]


def is_requester_capabilities_question(text: str) -> bool:
    cleaned = (text or "").strip().casefold()
    if not cleaned:
        return False
    patterns = (
        "С‡С‚Рѕ С‚С‹ СѓРјРµРµС€СЊ",
        "С‡С‚Рѕ СѓРјРµРµС€СЊ",
        "С‡С‚Рѕ РјРѕР¶РµС€СЊ",
        "С‚РІРѕРё РІРѕР·РјРѕР¶РЅРѕСЃС‚Рё",
        "РІРѕР·РјРѕР¶РЅРѕСЃС‚Рё Р±РѕС‚Р°",
        "С‡С‚Рѕ С‚С‹ РјРѕР¶РµС€СЊ",
        "С‡С‚Рѕ СѓРјРµРµС‚ Р±РѕС‚",
        "РїРѕРєР°Р¶Рё РІРѕР·РјРѕР¶РЅРѕСЃС‚Рё",
    )
    return any(marker in cleaned for marker in patterns)


def build_requester_capabilities_text() -> str:
    return "\n".join(
        (
            "РЇ РІРёСЂС‚СѓР°Р»СЊРЅС‹Р№ РїРѕРјРѕС‰РЅРёРє VPN_KBR. РќРёР¶Рµ - С‡С‚Рѕ СЏ СѓРјРµСЋ Рё РєР°Рє СЌС‚РёРј РїРѕР»СЊР·РѕРІР°С‚СЊСЃСЏ.",
            "",
            "1) Р Р°Р±РѕС‚Р° СЃ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏРјРё:",
            "- /help 123456789 - РєРѕСЂРѕС‚РєР°СЏ РєР°СЂС‚РѕС‡РєР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
            "- /help username -b - РєР°СЂС‚РѕС‡РєР° РёР· SQLite",
            "- /info 123456789 - РїРѕР»РЅС‹Р№ РѕС‚С‡РµС‚ РїРѕ РїРѕРґРїРёСЃРєР°Рј",
            "- /info username -b - РїРѕР»РЅС‹Р№ РѕС‚С‡РµС‚ РёР· SQLite",
            "- /user ... Рё /subs ... - С‚Рµ Р¶Рµ РґРµР№СЃС‚РІРёСЏ РІ РЅРѕРІС‹С… РєРѕСЂРѕС‚РєРёС… РЅР°Р·РІР°РЅРёСЏС…",
            "",
            "2) Р Р°Р±РѕС‚Р° СЃ Wizard Рё РїРѕРґРґРµСЂР¶РєРѕР№:",
            "- /wizard 123456789 - СЃРѕР±СЂР°С‚СЊ РєР°СЂС‚РѕС‡РєСѓ, РїРѕРєР°Р·Р°С‚СЊ С‚РµР±Рµ РЅР° РїСЂРѕРІРµСЂРєСѓ Рё Р·Р°С‚РµРј РѕС‚РїСЂР°РІРёС‚СЊ РІ wizard",
            "- РјРѕРіСѓ РїСЂРёРЅСЏС‚СЊ РїСЂРѕР±Р»РµРјСѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ Рё СЃС„РѕСЂРјРёСЂРѕРІР°С‚СЊ РєР°СЂС‚РѕС‡РєСѓ РґР»СЏ wizard",
            "",
            "3) Р Р°СЃСЃС‹Р»РєРё Рё РїСЂРѕРјРѕ:",
            "- /send 123456789 РўРµРєСЃС‚ - РѕС‚РїСЂР°РІРёС‚СЊ СЃРѕРѕР±С‰РµРЅРёРµ РєРѕРЅРєСЂРµС‚РЅРѕРјСѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
            "- /mail2 РўРµРєСЃС‚ РёР»Рё /broadcast РўРµРєСЃС‚ - СЂР°СЃСЃС‹Р»РєР° РїРѕ Р±Р°Р·Рµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏРј Р±РµР· РїРѕРґРїРёСЃРєРё",
            "- /promo 123456789 РёР»Рё /coupon 123456789 - СЃРѕР·РґР°С‚СЊ РїСЂРѕРјРѕРєРѕРґ Рё РѕС‚РїСЂР°РІРёС‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
            "",
            "4) Scan Рё Р°РЅР°Р»РёС‚РёРєР°:",
            "- scan - РѕС‚РєСЂС‹С‚СЊ РјРµРЅСЋ СЃРєР°РЅР°",
            "- scan new - РЅРѕРІС‹Р№ РїРѕР»РЅС‹Р№ СЃРєР°РЅ",
            "- scan continue - РїСЂРѕРґРѕР»Р¶РёС‚СЊ СЃ СЃРѕС…СЂР°РЅРµРЅРЅРѕРіРѕ РјРµСЃС‚Р°",
            "- stop СЃРєР°РЅ - РїРѕСЃС‚Р°РІРёС‚СЊ scan РЅР° РїР°СѓР·Сѓ Рё СЃРѕС…СЂР°РЅРёС‚СЊ РїСЂРѕРіСЂРµСЃСЃ",
            "- scan results - РїРѕРєР°Р·Р°С‚СЊ СЂРµР·СѓР»СЊС‚Р°С‚С‹ Рё dashboard",
            "",
            "5) Dashboard Рё Р°РґРјРёРЅ-СЃР°Р№С‚:",
            "- /dashboard - Р°РЅР°Р»РёС‚РёС‡РµСЃРєРёР№ dashboard РїРѕ Р±Р°Р·Рµ",
            "- /adminsite - РѕС‚РєСЂС‹С‚СЊ live admin РїР°РЅРµР»СЊ",
            "- /status - Р±С‹СЃС‚СЂРѕ РѕС‚РєСЂС‹С‚СЊ СЃС‚Р°С‚СѓСЃ Рё dashboard",
            "",
            "6) РљРѕРЅС‚СЂРѕР»СЊ Рё РґРёР°РіРЅРѕСЃС‚РёРєР°:",
            "- /processes РёР»Рё /poc - Р°РєС‚РёРІРЅС‹Рµ РїСЂРѕС†РµСЃСЃС‹, РїР°СѓР·Р° Рё СЃРЅСЏС‚РёРµ Р·Р°РґР°С‡",
            "- /diag - РґРёР°РіРЅРѕСЃС‚РёРєР° Р±РѕС‚Р°, Р±Р°Р·С‹ Рё СЃРµСЂРІРёСЃРѕРІ",
            "- /tail 100 - РїРѕСЃР»РµРґРЅРёРµ СЃС‚СЂРѕРєРё Р»РѕРіР°",
            "- /version - РІРµСЂСЃРёСЏ, commit Рё РІСЂРµРјСЏ Р·Р°РїСѓСЃРєР°",
            "",
            "7) Р”РѕСЃС‚СѓРїС‹ Р·Р°РїСЂРѕСЃРЅРёРєРѕРІ:",
            "- /roots - СЃРїРёСЃРѕРє Р·Р°РїСЂРѕСЃРЅРёРєРѕРІ",
            "- /roots add <id|@username|me> - РґРѕР±Р°РІРёС‚СЊ Р·Р°РїСЂРѕСЃРЅРёРєР°",
            "- /roots del <id|@username> - СѓРґР°Р»РёС‚СЊ Р·Р°РїСЂРѕСЃРЅРёРєР°",
            "",
            "РџРѕРґСЃРєР°Р·РєР°: РЅР°РїРёС€Рё РїСЂРѕСЃС‚Рѕ `menu`, Рё СЏ РїРѕРєР°Р¶Сѓ РєРЅРѕРїРєРё РІСЃРµС… РѕСЃРЅРѕРІРЅС‹С… РєРѕРјР°РЅРґ.",
        )
    )


def extract_labeled_value(text: str, labels: tuple[str, ...]) -> str | None:
    for label in labels:
        match = re.search(rf"{re.escape(label)}\s*[:\-]?\s*([^\n\r]+)", text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def extract_user_number(user_text: str, subscriptions_text: str) -> str | None:
    vpn_match = re.search(
        r"\u0423\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u0435\s+VPN\s+\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f\s*\u2116\s*(\d+)",
        subscriptions_text,
        flags=re.IGNORECASE,
    )
    if vpn_match:
        return vpn_match.group(1)

    return extract_labeled_value(
        user_text,
        (
            "\u043d\u043e\u043c\u0435\u0440 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f",
            "\u043d\u043e\u043c\u0435\u0440",
            "user number",
            "number",
        ),
    )


def extract_subscription_numbers(message) -> list[str]:
    if not message.buttons:
        return []

    numbers: list[str] = []
    for row in message.buttons:
        for button in row:
            match = re.search(r"\[(\d+)\]", button.text)
            if match:
                numbers.append(match.group(1))
    return numbers


def extract_subscription_buttons(message) -> list[dict[str, int | str]]:
    if not message.buttons:
        return []

    subscriptions: list[dict[str, int | str]] = []
    for row_index, row in enumerate(message.buttons):
        for column_index, button in enumerate(row):
            match = re.search(r"\[(\d+)\]", button.text)
            if match:
                subscriptions.append(
                    {
                        "id": match.group(1),
                        "text": button.text,
                        "row": row_index,
                        "column": column_index,
                    }
                )
    return subscriptions


def extract_numbered_buttons(message) -> list[dict[str, int | str]]:
    if not message.buttons:
        return []

    result: list[dict[str, int | str]] = []
    for row_index, row in enumerate(message.buttons):
        for column_index, button in enumerate(row):
            match = re.search(r"\d{1,20}", button.text)
            if not match:
                continue
            result.append(
                {
                    "id": match.group(0),
                    "text": button.text,
                    "row": row_index,
                    "column": column_index,
                }
            )
    return result


def is_navigation_button_text(text: str) -> bool:
    lowered = text.casefold()
    words = (
        settings.back_button_text.casefold(),
        settings.next_page_button_text.casefold(),
        settings.cancel_button_text.casefold(),
        "\u043d\u0430\u0437\u0430\u0434",
        "\u0434\u0430\u043b\u0435\u0435",
        "\u0432\u043f\u0435\u0440\u0435\u0434",
        "\u043e\u0442\u043c\u0435\u043d\u0438\u0442\u044c",
    )
    return any(word and word in lowered for word in words) or text.strip() in {"вћЎ", "вћЎпёЏ", "В»", ">>", "вЏ­"}


def extract_user_buttons(message) -> list[dict[str, int | str]]:
    return [
        button
        for button in extract_numbered_buttons(message)
        if not is_navigation_button_text(str(button["text"]))
    ]


def extract_all_buttons(message) -> list[dict[str, int | str]]:
    if not message.buttons:
        return []
    result: list[dict[str, int | str]] = []
    for row_index, row in enumerate(message.buttons):
        for column_index, button in enumerate(row):
            result.append(
                {
                    "text": button.text,
                    "row": row_index,
                    "column": column_index,
                }
            )
    return result


def get_back_page_button(message) -> dict[str, int | str] | None:
    candidates = {"в¬…", "в¬…пёЏ", "В«", "<<", "вЏ®"}
    tokens = (
        settings.back_button_text.casefold(),
        settings.cancel_button_text.casefold(),
        "РЅР°Р·Р°Рґ",
        "back",
        "return",
    )
    for button in extract_all_buttons(message):
        text = str(button["text"]).strip()
        lowered = text.casefold()
        if text in candidates or any(token and token in lowered for token in tokens):
            return button

    nav_buttons = [button for button in extract_all_buttons(message) if is_navigation_button_text(str(button["text"]))]
    if nav_buttons:
        nav_buttons.sort(key=lambda button: (int(button["row"]), int(button["column"])))
        return nav_buttons[0]
    return None


def is_users_page_message(message) -> bool:
    return bool(extract_user_buttons(message))


def score_users_menu_button(text: str) -> int:
    lowered = text.casefold()
    score = 0
    if any(token in lowered for token in ("РїРѕР»СЊР·", "user", "users", "РєР»РёРµРЅС‚", "Р°Р±РѕРЅРµРЅС‚", "СѓС‡Р°СЃС‚")):
        score += 30
    if any(symbol in text for symbol in ("рџ‘¤", "рџ‘Ґ", "рџ§‘", "рџ™Ќ")):
        score += 10
    if re.search(r"\d{1,20}", text):
        score += 2
    if is_navigation_button_text(text):
        score -= 100
    return score


def get_users_menu_candidates(message) -> list[dict[str, int | str]]:
    weighted: list[tuple[int, dict[str, int | str]]] = []
    for button in extract_all_buttons(message):
        score = score_users_menu_button(str(button["text"]))
        if score <= -100:
            continue
        weighted.append((score, button))
    weighted.sort(key=lambda item: item[0], reverse=True)
    return [button for _, button in weighted]


def get_statistics_menu_button(message) -> dict[str, int | str] | None:
    weighted: list[tuple[int, dict[str, int | str]]] = []
    for button in extract_all_buttons(message):
        text = sanitize_outgoing_text(str(button["text"]))
        lowered = text.casefold()
        score = 0
        if "стат" in lowered or "stat" in lowered or "аналит" in lowered:
            score += 40
        if any(symbol in text for symbol in ("📊", "📈", "📉", "🧾")):
            score += 10
        if is_navigation_button_text(text):
            score -= 100
        if score > 0:
            weighted.append((score, button))
    if not weighted:
        return None
    weighted.sort(key=lambda item: item[0], reverse=True)
    return weighted[0][1]


def extract_total_users_from_statistics_text(text: str) -> int | None:
    text = sanitize_outgoing_text(text)
    patterns = (
        r"всего\s+пользовател[еяй]\s*[:\-]?\s*(\d+)",
        r"пользовател[еяй]\s+всего\s*[:\-]?\s*(\d+)",
        r"total\s+users\s*[:\-]?\s*(\d+)",
        r"users\s+total\s*[:\-]?\s*(\d+)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            try:
                value = int(match.group(1))
            except ValueError:
                continue
            if 0 < value < 10_000_000:
                return value

    candidate_values: list[int] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        lowered = line.casefold()
        if "польз" not in lowered and "user" not in lowered:
            continue
        for match in re.finditer(r"\d+", line):
            try:
                value = int(match.group(0))
            except ValueError:
                continue
            if 0 < value < 10_000_000:
                candidate_values.append(value)
    if candidate_values:
        return max(candidate_values)
    return None


def parse_float_number(text: str) -> float | None:
    cleaned = re.sub(r"[^\d,.\s]", "", text).strip()
    if not cleaned:
        return None
    cleaned = cleaned.replace(" ", "")
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def extract_money_from_line(line: str) -> float | None:
    pattern = re.compile(r"(\d[\d\s.,]*)\s*(?:в‚Ѕ|СЂСѓР±|СЂ\b|rub)", flags=re.IGNORECASE)
    match = pattern.search(line)
    if match:
        return parse_float_number(match.group(1))

    if any(token in line.casefold() for token in ("РїСЂРёР±", "РґРѕС…РѕРґ", "РІС‹СЂСѓС‡", "profit", "revenue")):
        match = re.search(r"(\d[\d\s.,]*)", line)
        if match:
            return parse_float_number(match.group(1))
    return None


def detect_period_key(line: str) -> str | None:
    lowered = line.casefold()
    if any(token in lowered for token in ("СЃРµРіРѕРґРЅСЏ", "Р·Р° РґРµРЅСЊ", "РґРµРЅСЊ", "day", "daily")):
        return "day"
    if any(token in lowered for token in ("РЅРµРґРµР»", "week", "weekly")):
        return "week"
    if any(token in lowered for token in ("3 РјРµСЃ", "3 month", "РєРІР°СЂС‚", "quarter")):
        return "quarter"
    if any(token in lowered for token in ("6 РјРµСЃ", "РїРѕР»РіРѕРґ", "half-year", "half year")):
        return "half_year"
    if any(token in lowered for token in ("РјРµСЃСЏС†", "month", "monthly")):
        return "month"
    if any(token in lowered for token in ("РіРѕРґ", "year", "yearly", "annual")):
        return "year"
    if any(token in lowered for token in ("РІСЃРµ РІСЂРµРјСЏ", "РІСЃС‘ РІСЂРµРјСЏ", "all time", "total")):
        return "all_time"
    return None


def extract_admin_statistics_snapshot(text: str) -> dict:
    users_total = extract_total_users_from_statistics_text(text) or 0
    users_by_period: dict[str, int] = {}
    profit_by_period: dict[str, float] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        period_key = detect_period_key(line)
        if not period_key:
            continue

        lowered = line.casefold()
        if any(token in lowered for token in ("РїРѕР»СЊР·", "user")):
            user_match = re.search(r"(\d{1,9})", line)
            if user_match:
                try:
                    users_by_period[period_key] = int(user_match.group(1))
                except ValueError:
                    pass

        if any(token in lowered for token in ("РїСЂРёР±", "РґРѕС…РѕРґ", "РІС‹СЂСѓС‡", "profit", "revenue", "СЂСѓР±", "в‚Ѕ", "rub")):
            money_value = extract_money_from_line(line)
            if money_value is not None:
                profit_by_period[period_key] = money_value

    return {
        "captured_at": datetime.now().isoformat(timespec="seconds"),
        "users_total": users_total,
        "users_by_period": users_by_period,
        "profit_by_period": profit_by_period,
        "raw_text": text,
    }


def format_user_summary(user_id: str, user_text: str, subscriptions_message) -> str:
    subscriptions_text = subscriptions_message.raw_text or ""
    user_number = extract_user_number(user_text, subscriptions_text)
    subscription_numbers = extract_subscription_numbers(subscriptions_message)
    subscriptions = ", ".join(subscription_numbers) if subscription_numbers else "\u043f\u043e\u0434\u043f\u0438\u0441\u043e\u043a \u043d\u0435\u0442"

    return "\n".join(
        (
            f"1. Username \u0431\u043e\u0442\u0430: @{settings.admin_bot_username}",
            f"2. ID \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439: {user_number or user_id}",
            f"3. \u0410\u0439\u0434\u0438 \u043f\u043e\u0434\u043f\u0438\u0441\u043a\u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439: {subscriptions}",
        )
    )


def format_user_summary_from_record(record: dict) -> str:
    user_id = str(record.get("user_id") or "")
    user_text = str(record.get("user_text") or "")
    username = extract_username_from_record(record)
    subscriptions = list(record.get("subscriptions") or [])
    subscriptions_text_for_number = "\n".join(
        str(subscription.get("button_text") or "") for subscription in subscriptions
    )
    subscription_numbers = [
        str(subscription.get("subscription_id") or "").strip()
        for subscription in subscriptions
        if str(subscription.get("subscription_id") or "").strip()
    ]
    subscriptions_text = ", ".join(subscription_numbers) if subscription_numbers else "РїРѕРґРїРёСЃРѕРє РЅРµС‚"
    user_number = extract_user_number(user_text, subscriptions_text_for_number)

    return "\n".join(
        (
            f"1. Username Р±РѕС‚Р°: @{settings.admin_bot_username}",
            f"2. ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: {user_number or user_id}",
            f"3. Username РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: @{username}" if username else "3. Username РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: РЅРµС‚ РІ Р±Р°Р·Рµ",
            f"4. РђР№РґРё РїРѕРґРїРёСЃРѕРє: {subscriptions_text}",
            "5. СЃС‚РѕС‡РЅРёРє: SQLite Р±Р°Р·Р°",
        )
    )


def format_subscription_info_from_record_html(record: dict) -> str:
    user_id = str(record.get("user_id") or "")
    user_text = str(record.get("user_text") or "")
    username = extract_username_from_record(record)
    subscriptions = list(record.get("subscriptions") or [])
    subscriptions_text_for_number = "\n".join(
        str(subscription.get("button_text") or "") for subscription in subscriptions
    )
    user_number = extract_user_number(user_text, subscriptions_text_for_number)
    registration_date = str(record.get("registration_date") or "").strip()
    if not registration_date:
        parsed_registration_date = extract_registration_date(user_text)
        registration_date = parsed_registration_date.strftime("%Y-%m-%d") if parsed_registration_date else ""

    lines = [
        f"1. Username Р±РѕС‚Р°: @{html.escape(settings.admin_bot_username)}",
        f"2. ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: {html.escape(user_number or user_id)}",
        (
            f"3. Username РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: @{html.escape(username)}"
            if username
            else "3. Username РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: РЅРµС‚ РІ Р±Р°Р·Рµ"
        ),
        (
            f"4. Р”Р°С‚Р° СЂРµРіРёСЃС‚СЂР°С†РёРё: {html.escape(registration_date)}"
            if registration_date
            else "4. Р”Р°С‚Р° СЂРµРіРёСЃС‚СЂР°С†РёРё: РЅРµС‚ РІ Р±Р°Р·Рµ"
        ),
        f"5. РџРѕРґРїРёСЃРѕРє РІ Р±Р°Р·Рµ: {len(subscriptions)}",
    ]

    if user_text.strip():
        lines.extend(("", "6. РљР°СЂС‚РѕС‡РєР° РёР· Р±Р°Р·С‹:", html.escape(user_text.strip())))

    if not subscriptions:
        lines.append("\n7. РЅС„Рѕ РїРѕРґРїРёСЃРѕРє: РїРѕРґРїРёСЃРѕРє РЅРµС‚")
        return "\n".join(lines)

    lines.append("\n7. РЅС„Рѕ РїРѕРґРїРёСЃРѕРє:")
    for subscription in subscriptions:
        subscription_id = str(subscription.get("subscription_id") or "")
        button_text = str(subscription.get("button_text") or "")
        detail_text = str(subscription.get("detail_text") or "").strip()
        lines.append("")
        lines.append(f"[{html.escape(subscription_id)}] {html.escape(button_text)}")
        lines.append(make_keys_copyable_html(detail_text or "[empty subscription response]"))

    lines.append("\n8. СЃС‚РѕС‡РЅРёРє: SQLite Р±Р°Р·Р°")
    return "\n".join(lines)


def collect_message_text_variants(message) -> list[str]:
    variants: list[str] = []
    for attribute in ("raw_text", "message", "text"):
        value = getattr(message, attribute, None)
        if value:
            variants.append(str(value))

    action = getattr(message, "action", None)
    if action is not None:
        variants.append(type(action).__name__)
        variants.append(repr(action))
        for attribute in ("message", "title", "reason"):
            value = getattr(action, attribute, None)
            if value:
                variants.append(str(value))

    reply_to = getattr(message, "reply_to", None)
    if reply_to is not None:
        variants.append(repr(reply_to))
    return variants


def log_message(label: str, message) -> None:
    action = getattr(message, "action", None)
    logging.info(
        "%s message_id=%s text=%r action=%s",
        label,
        getattr(message, "id", None),
        message.raw_text or "",
        type(action).__name__ if action is not None else None,
    )
    if action is not None:
        logging.info("%s action_repr=%r", label, action)
    if message.buttons:
        button_texts = []
        for row in message.buttons:
            button_texts.append([button.text for button in row])
        logging.info("%s buttons=%s", label, button_texts)


async def retry_async(label: str, action, *, attempts: int | None = None, delay_seconds: float | None = None):
    attempts = attempts or SCAN_RECOVERY_RETRY_ATTEMPTS
    delay_seconds = delay_seconds if delay_seconds is not None else SCAN_RECOVERY_RETRY_DELAY_SECONDS
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await action()
        except FloodWaitError as error:
            wait_seconds = int(getattr(error, "seconds", 1) or 1)
            note_floodwait(wait_seconds)
            last_error = error
            logging.warning("%s failed with FloodWait=%ss on attempt %s/%s", label, wait_seconds, attempt, attempts)
            await asyncio.sleep(wait_seconds + 1)
        except Exception as error:
            last_error = error
            logging.warning("%s failed on attempt %s/%s: %s", label, attempt, attempts, error)
            if attempt < attempts:
                await asyncio.sleep(delay_seconds * attempt)
    assert last_error is not None
    raise last_error


async def get_admin_bot_entity():
    global admin_bot_entity_cache
    if admin_bot_entity_cache is not None:
        return admin_bot_entity_cache
    set_admin_bot_health("[WAIT]", "РїСЂРѕРІРµСЂРєР°", "РїРѕР»СѓС‡Р°СЋ entity")
    admin_bot_entity_cache = await client.get_entity(settings.admin_bot_username)
    set_admin_bot_health("[OK]", "РѕС‚РІРµС‡Р°РµС‚", "entity РїРѕР»СѓС‡РµРЅ")
    return admin_bot_entity_cache


async def get_wizard_target_entity():
    global wizard_target_entity_cache
    if wizard_target_entity_cache is not None:
        return wizard_target_entity_cache
    target_username = settings.wizard_target_username.lstrip("@")
    wizard_target_entity_cache = await client.get_entity(target_username)
    return wizard_target_entity_cache


def admin_conversation(bot):
    return client.conversation(
        bot,
        timeout=None,
        max_messages=ADMIN_CONVERSATION_MAX_MESSAGES,
    )


def is_incoming_bot_message(message) -> bool:
    return not bool(getattr(message, "out", False)) and not is_intermediate_message(message)


async def latest_bot_message(bot, *, limit: int = 12):
    for attempt in range(2):
        try:
            set_admin_bot_health("[WAIT]", "РїСЂРѕРІРµСЂРєР°", "С‡РёС‚Р°СЋ РїРѕСЃР»РµРґРЅРµРµ СЃРѕРѕР±С‰РµРЅРёРµ")
            messages = await client.get_messages(bot, limit=limit)
            set_admin_bot_health("[OK]", "РѕС‚РІРµС‡Р°РµС‚", "РёСЃС‚РѕСЂРёСЏ РґРѕСЃС‚СѓРїРЅР°")
            break
        except FloodWaitError as error:
            wait_seconds = int(getattr(error, "seconds", 1) or 1)
            note_floodwait(wait_seconds)
            set_admin_bot_health("[WAIT]", "РѕР¶РёРґР°РЅРёРµ", f"FloodWait {wait_seconds}s")
            if attempt:
                raise
            logging.warning("FloodWait on latest_bot_message: sleeping %ss", wait_seconds)
            await asyncio.sleep(wait_seconds + 1)
    for message in messages:
        if is_incoming_bot_message(message):
            return message
    set_admin_bot_health("[ERR]", "РѕС€РёР±РєР°", "РЅРµС‚ РІС…РѕРґСЏС‰РёС… СЃРѕРѕР±С‰РµРЅРёР№")
    raise RuntimeError("No incoming messages found in admin bot chat.")


async def monitor_admin_bot_health() -> None:
    while True:
        try:
            bot = await get_admin_bot_entity()
            await latest_bot_message(bot)
        except asyncio.CancelledError:
            raise
        except FloodWaitError as error:
            wait_seconds = int(getattr(error, "seconds", 1) or 1)
            set_admin_bot_health("[WAIT]", "РѕР¶РёРґР°РЅРёРµ", f"FloodWait {wait_seconds}s")
            await asyncio.sleep(min(wait_seconds + 1, BOT_HEALTH_POLL_INTERVAL_SECONDS * 2))
            continue
        except Exception as error:
            set_admin_bot_health("[ERR]", "РѕС€РёР±РєР°", str(error)[:80])
            logging.warning("Admin bot health check failed: %s", error)
        await asyncio.sleep(BOT_HEALTH_POLL_INTERVAL_SECONDS)


def message_snapshot(message) -> tuple[int | None, str, tuple[tuple[str, ...], ...]]:
    buttons: tuple[tuple[str, ...], ...] = ()
    if message.buttons:
        buttons = tuple(tuple(button.text for button in row) for row in message.buttons)
    return getattr(message, "id", None), message.raw_text or "", buttons


def is_intermediate_message(message) -> bool:
    return (message.raw_text or "").strip() == "\u23f3" and not message.buttons


async def wait_bot_update(bot, previous_snapshot=None, ready=None, timeout_seconds: float | None = None):
    future = loop.create_future()
    timeout_seconds = timeout_seconds or settings.bot_response_timeout_seconds
    set_admin_bot_health("[WAIT]", "РѕР¶РёРґР°РЅРёРµ", "Р¶РґСѓ РѕС‚РІРµС‚")

    def is_usable_message(message) -> bool:
        if not is_incoming_bot_message(message):
            return False
        if is_intermediate_message(message):
            return False
        if previous_snapshot is not None and message_snapshot(message) == previous_snapshot:
            return False
        if ready is not None and not ready(message):
            return False
        return True

    async def handler(event):
        if future.done():
            return
        if event.chat_id != bot.id or event.out:
            return
        if not is_usable_message(event.message):
            return
        future.set_result(event.message)

    async def poll_latest_message():
        while True:
            latest_message = await latest_bot_message(bot)
            if is_usable_message(latest_message):
                return latest_message
            await asyncio.sleep(BOT_POLL_INTERVAL_SECONDS)

    new_message_event = events.NewMessage(chats=bot)
    edited_message_event = events.MessageEdited(chats=bot)
    client.add_event_handler(handler, new_message_event)
    client.add_event_handler(handler, edited_message_event)
    poll_task = asyncio.create_task(poll_latest_message())

    try:
        done, pending = await asyncio.wait(
            [future, poll_task],
            timeout=timeout_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done:
            set_admin_bot_health("[ERR]", "Р·Р°РІРёСЃ", f"РЅРµС‚ РѕС‚РІРµС‚Р° {timeout_seconds:.0f}s")
            raise TimeoutError(
                f"Admin bot @{settings.admin_bot_username} did not send an expected update "
                f"within {timeout_seconds:.0f}s."
            )
        for task in pending:
            task.cancel()
        result = done.pop().result()
        set_admin_bot_health("[OK]", "РѕС‚РІРµС‡Р°РµС‚", "РїРѕР»СѓС‡РµРЅ РѕС‚РІРµС‚")
        return result
    finally:
        poll_task.cancel()
        client.remove_event_handler(handler, new_message_event)
        client.remove_event_handler(handler, edited_message_event)


async def click_button_by_text(message, expected_text: str):
    expected = expected_text.casefold()
    if not message.buttons:
        raise RuntimeError(f"Message has no buttons. Cannot click {expected_text!r}.")

    for row_index, row in enumerate(message.buttons):
        for column_index, button in enumerate(row):
            if expected in button.text.casefold():
                logging.info("Clicking button %r at row=%s column=%s", button.text, row_index, column_index)
                try:
                    result = await message.click(row_index, column_index)
                    note_success_action()
                    return result
                except FloodWaitError as error:
                    wait_seconds = int(getattr(error, "seconds", 1) or 1)
                    note_floodwait(wait_seconds)
                    logging.warning(
                        "FloodWait on click_button_by_text: waiting %ss and retrying button %r",
                        wait_seconds,
                        button.text,
                    )
                    await asyncio.sleep(wait_seconds + 1)
                    result = await message.click(row_index, column_index)
                    note_success_action()
                    return result

    available = [[button.text for button in row] for row in message.buttons]
    raise RuntimeError(f"Button {expected_text!r} not found. Available buttons: {available}")


async def wait_for_click_or_update(click_task, update_task):
    done, pending = await asyncio.wait(
        [click_task, update_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    if update_task in done:
        if not click_task.done():
            click_task.add_done_callback(lambda task: task.exception() if not task.cancelled() else None)
        return update_task.result()

    click_task.result()
    return await update_task


def ensure_parent_dir(path: Path) -> None:
    parent = path.parent
    if parent != Path("."):
        parent.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    ensure_parent_dir(path)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(text, encoding=encoding)
    tmp_path.replace(path)


def database_path() -> Path:
    path = Path(settings.database_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    ensure_parent_dir(path)
    return path


def dashboard_public_dir() -> Path:
    path = Path(settings.dashboard_public_dir)
    if not path.is_absolute():
        path = Path.cwd() / path
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_dashboard_public_url(file_name: str) -> str:
    base_url = settings.dashboard_public_base_url.strip().rstrip("/")
    if not base_url:
        return ""

    parts = [settings.dashboard_public_path_prefix.strip("/")]
    if settings.dashboard_public_token:
        parts.append(settings.dashboard_public_token.strip("/"))
    parts.append(quote(file_name, safe=""))
    return f"{base_url}/{'/'.join(part for part in parts if part)}"


def dashboard_intro_template_path() -> Path:
    raw_path = settings.dashboard_intro_template_path.strip()
    path = Path(raw_path) if raw_path else Path("remotion-plugin-remotion-openai-curated-vpn/index.html")
    if not path.is_absolute():
        path = APP_ROOT / path
    return path


def dashboard_loader_file_name(dashboard_file_name: str) -> str:
    stem = Path(dashboard_file_name).stem
    return f"{stem}-loader.html"


def safe_dashboard_public_file_name(file_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", Path(file_name).name)


def direct_dashboard_file_name_from_loader(loader_file_name: str) -> str | None:
    if not loader_file_name.endswith("-loader.html"):
        return None
    return f"{loader_file_name[:-len('-loader.html')]}.html"


def read_dashboard_intro_template() -> str:
    global dashboard_intro_template_cache
    template_path = dashboard_intro_template_path()
    try:
        mtime = template_path.stat().st_mtime
    except OSError:
        logging.exception("Dashboard intro template is not available: %s", template_path)
        return fallback_dashboard_loader_html()

    if (
        dashboard_intro_template_cache is not None
        and dashboard_intro_template_cache[0] == template_path
        and dashboard_intro_template_cache[1] == mtime
    ):
        return dashboard_intro_template_cache[2]

    try:
        template = template_path.read_text(encoding="utf-8")
    except OSError:
        logging.exception("Dashboard intro template read failed: %s", template_path)
        return fallback_dashboard_loader_html()

    dashboard_intro_template_cache = (template_path, mtime, template)
    return template


def publish_dashboard_intro_asset(asset_name: str) -> str:
    safe_name = safe_dashboard_public_file_name(asset_name)
    if not safe_name:
        return asset_name

    source_path = (dashboard_intro_template_path().parent / "public" / asset_name).resolve()
    source_root = (dashboard_intro_template_path().parent / "public").resolve()
    if source_root not in source_path.parents or not source_path.is_file():
        logging.warning("Dashboard intro asset is not available: %s", source_path)
        return asset_name

    target_path = dashboard_public_dir() / safe_name
    try:
        if not target_path.exists() or source_path.stat().st_mtime > target_path.stat().st_mtime:
            shutil.copy2(source_path, target_path)
    except OSError:
        logging.exception("Failed to publish dashboard intro asset: %s", source_path)
        return asset_name
    return build_dashboard_public_url(safe_name) or safe_name


def rewrite_dashboard_intro_asset_urls(template: str) -> str:
    def replace(match: re.Match) -> str:
        prefix = match.group(1)
        asset_name = match.group(2)
        return f"{prefix}{publish_dashboard_intro_asset(asset_name)}"

    return re.sub(r'((?:src|href)=["\'])public/([^"\']+)', replace, template)


def fallback_dashboard_loader_html() -> str:
    return """<!doctype html>
<html lang="ru">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>VPN_KBR_BOT loader</title>
    <style>
      :root { --bg: #f4f1ea; --ink: #151515; }
      * { box-sizing: border-box; }
      html, body { width: 100%; height: 100%; margin: 0; overflow: hidden; }
      body {
        display: grid;
        place-items: center;
        background: var(--bg);
        color: var(--ink);
        font-family: "IBM Plex Mono", Consolas, monospace;
      }
      .stage { width: min(100vw, 100vh); height: min(100vw, 100vh); display: grid; place-items: center; position: relative; }
      .frame { position: absolute; inset: 42px; border: 1px solid var(--ink); opacity: .16; }
      .ring { width: min(390px, 72vw); height: min(390px, 72vw); border: 3px solid var(--ink); border-radius: 50%; animation: spin 2.4s linear infinite; clip-path: polygon(50% 50%, 100% 0, 100% 84%, 0 100%, 0 0); }
      .caption { position: absolute; bottom: 14%; width: min(520px, 72vw); }
      .row { display: flex; justify-content: space-between; font-size: clamp(20px, 3vw, 28px); font-weight: 600; }
      .bar { height: 2px; margin-top: 24px; background: rgb(21 21 21 / 14%); overflow: hidden; }
      .fill { height: 100%; width: 0; background: var(--ink); animation: load 5s linear forwards; }
      .hint { margin-top: 18px; font-size: clamp(12px, 1.6vw, 16px); text-transform: uppercase; opacity: .54; }
      @keyframes spin { to { transform: rotate(360deg); } }
      @keyframes load { to { width: 100%; } }
    </style>
  </head>
  <body>
    <main class="stage">
      <div class="frame"></div>
      <div class="ring"></div>
      <section class="caption">
        <div class="row"><span>vpn_kbr_</span><span id="percent">000%</span></div>
        <div class="bar"><div class="fill"></div></div>
        <div class="hint">opening dashboard</div>
      </section>
    </main>
    <script>
      const percent = document.getElementById("percent");
      const startedAt = performance.now();
      const tick = () => {
        const elapsed = Math.min(5, (performance.now() - startedAt) / 1000);
        percent.textContent = `${{Math.round((elapsed / 5) * 100).toString().padStart(3, "0")}}%`;
        requestAnimationFrame(tick);
      };
      tick();
    </script>
  </body>
</html>"""


def build_dashboard_loader_html(target_url: str) -> str:
    template = rewrite_dashboard_intro_asset_urls(read_dashboard_intro_template())
    target_json = json.dumps(target_url, ensure_ascii=False)
    delay_ms = int(settings.dashboard_intro_seconds * 1000)
    redirect_script = f"""
    <script>
      (() => {{
        const dashboardTarget = {target_json};
        const delayMs = {delay_ms};
        const openDashboard = () => {{
          if (dashboardTarget) {{
            window.location.replace(dashboardTarget);
          }}
        }};
        document.body.style.cursor = "pointer";
        document.body.addEventListener("click", openDashboard, {{once: true}});
        window.setTimeout(openDashboard, delayMs);
      }})();
    </script>
"""
    note_html = """
    <div style="position:fixed;left:50%;bottom:24px;transform:translateX(-50%);font:600 12px 'IBM Plex Mono',Consolas,monospace;letter-spacing:0;text-transform:uppercase;opacity:.58;color:#f7f5ee;white-space:nowrap;">
      dashboard РѕС‚РєСЂРѕРµС‚СЃСЏ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё
    </div>
"""
    if "</body>" in template:
        return template.replace("</body>", f"{note_html}{redirect_script}</body>", 1)
    return f"{template}{note_html}{redirect_script}"


def publish_dashboard_loader_file(dashboard_file_name: str) -> str:
    target_url = build_dashboard_public_url(dashboard_file_name)
    if not target_url or not settings.dashboard_intro_enabled:
        return target_url

    loader_name = dashboard_loader_file_name(dashboard_file_name)
    loader_path = dashboard_public_dir() / loader_name
    atomic_write_text(loader_path, build_dashboard_loader_html(target_url))
    return build_dashboard_public_url(loader_name)


def prune_dashboard_public_files() -> None:
    public_dir = dashboard_public_dir()
    files = [
        path
        for path in public_dir.glob("*.html")
        if path.is_file()
        and not path.name.startswith("latest-")
        and direct_dashboard_file_name_from_loader(path.name) is None
    ]
    files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    for old_path in files[settings.dashboard_public_retention:]:
        try:
            old_path.unlink()
            loader_path = public_dir / dashboard_loader_file_name(old_path.name)
            if loader_path.exists():
                loader_path.unlink()
        except OSError:
            logging.exception("Failed to prune old public dashboard: %s", old_path)

    direct_names = {path.name for path in public_dir.glob("*.html") if path.is_file()}
    for loader_path in public_dir.glob("*-loader.html"):
        target_name = direct_dashboard_file_name_from_loader(loader_path.name)
        if target_name and target_name not in direct_names and not loader_path.name.startswith("latest-"):
            try:
                loader_path.unlink()
            except OSError:
                logging.exception("Failed to prune orphan dashboard loader: %s", loader_path)


def publish_dashboard_file(source_path: Path, latest_name: str | None = None) -> tuple[Path, str]:
    source_path = Path(source_path)
    public_dir = dashboard_public_dir()
    public_name = safe_dashboard_public_file_name(source_path.name)
    if not public_name.endswith(".html"):
        public_name = f"{public_name}.html"
    public_path = public_dir / public_name
    shutil.copy2(source_path, public_path)

    if latest_name:
        latest_name = safe_dashboard_public_file_name(latest_name)
        if not latest_name.endswith(".html"):
            latest_name = f"{latest_name}.html"
        latest_path = public_dir / latest_name
        shutil.copy2(source_path, latest_path)
        publish_dashboard_loader_file(latest_name)

    prune_dashboard_public_files()
    return public_path, publish_dashboard_loader_file(public_name)


def ensure_dashboard_public_url(report_path: Path, latest_name: str | None = None) -> str:
    public_name = safe_dashboard_public_file_name(Path(report_path).name)
    public_path = dashboard_public_dir() / public_name
    if not public_path.exists() and Path(report_path).exists():
        publish_dashboard_file(Path(report_path), latest_name=latest_name)
    elif latest_name and Path(report_path).exists():
        latest_path = dashboard_public_dir() / safe_dashboard_public_file_name(latest_name)
        if not latest_path.exists():
            shutil.copy2(report_path, latest_path)
        publish_dashboard_loader_file(latest_path.name)
    return publish_dashboard_loader_file(public_name)


def build_dashboard_empty_admin_html(message: str) -> str:
    safe_message = html.escape(message)
    brand = html.escape(settings.dashboard_brand_name or settings.app_name or "Vpn_Bot_assist")
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{brand} - Admin</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; min-height: 100vh; display: grid; place-items: center; background: #0b1020; color: #edf1ff; font-family: "Segoe UI", Arial, sans-serif; }}
    main {{ width: min(720px, calc(100vw - 32px)); border: 1px solid #2a3564; border-radius: 10px; padding: 24px; background: linear-gradient(180deg, #141a30, #1b2340); }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    p {{ margin: 0; color: #aeb9d6; line-height: 1.55; }}
    code {{ color: #56d4ff; }}
  </style>
</head>
<body>
  <main>
    <h1>{brand}: admin dashboard</h1>
    <p>{safe_message}</p>
    <p>Р—Р°РїСѓСЃС‚Рё <code>/scan</code>, РґРѕР¶РґРёСЃСЊ Р·Р°РїРёСЃРё РґР°РЅРЅС‹С… РІ SQLite, Р·Р°С‚РµРј РѕР±РЅРѕРІРё СЌС‚Сѓ СЃС‚СЂР°РЅРёС†Сѓ.</p>
  </main>
</body>
</html>"""


def build_live_admin_dashboard_html() -> str:
    stats = load_latest_scan_stats_from_database()
    if not stats:
        return build_dashboard_empty_admin_html("Р’ SQL Р±Р°Р·Рµ РїРѕРєР° РЅРµС‚ РїРѕСЃР»РµРґРЅРµРіРѕ scan РґР»СЏ РїРѕСЃС‚СЂРѕРµРЅРёСЏ Р¶РёРІРѕР№ Р°РґРјРёРЅ-РїР°РЅРµР»Рё.")
    stats["database"] = {
        "path": str(database_path()),
        "source": "sqlite-live",
    }
    stats["business_analysis"] = analyze_business_status(stats)
    return build_scan_dashboard_html(stats)


def build_live_root_panel_html() -> str:
    records = load_latest_records_from_database()
    stats = records
    if not stats:
        return build_dashboard_empty_admin_html("Р’ SQL Р±Р°Р·Рµ РїРѕРєР° РЅРµС‚ РґР°РЅРЅС‹С… РґР»СЏ root-РїР°РЅРµР»Рё. РЎРЅР°С‡Р°Р»Р° Р·Р°РїСѓСЃС‚Рё scan.")
    if not records:
        return build_dashboard_empty_admin_html("Р’ SQL Р±Р°Р·Рµ РїРѕРєР° РЅРµС‚ РґР°РЅРЅС‹С… РґР»СЏ root-РїР°РЅРµР»Рё. РЎРЅР°С‡Р°Р»Р° Р·Р°РїСѓСЃС‚Рё scan.")
    users_json = admin_user_rows_json(records)
    brand = html.escape(settings.dashboard_brand_name)
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{brand} вЂ” Root Panel</title>
  <style>
    :root {{
      --bg:#f5f5f7; --panel:#ffffff; --border:#c7c7cc; --text:#1d1d1f; --muted:#6e6e73;
      --primary:#0071e3; --good:#34c759; --warn:#ff9f0a;
    }}
    * {{ box-sizing:border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text','Segoe UI',Roboto,sans-serif; background:var(--bg); color:var(--text); -webkit-font-smoothing:antialiased; text-rendering:optimizeLegibility; }}
    .top-tabs {{ position: sticky; top: 0; z-index: 20; background: rgba(245,245,247,.92); backdrop-filter: blur(8px); border-bottom: 2px solid var(--border); }}
    .wrap {{ display:grid; grid-template-columns: 340px 1fr; gap:12px; padding:12px; min-height:100vh; }}
    .panel {{ background:var(--panel); border:2px solid var(--border); border-radius:12px; padding:12px; box-shadow:0 1px 2px rgba(0,0,0,.04), 0 6px 20px rgba(0,0,0,.03); transition: box-shadow .18s ease, border-color .18s ease; }}
    .panel:has(.item.active) {{ border-color: rgba(0,113,227,.55); box-shadow:0 1px 2px rgba(0,0,0,.04), 0 10px 24px rgba(0,113,227,.10); }}
    .panel + .panel {{ margin-top: 10px; }}
    h1 {{ margin:0 0 10px; font-size:17px; font-weight:600; letter-spacing:0; }}
    h1 {{
      padding: 8px 10px;
      border: 2px solid var(--border);
      border-radius: 10px;
      background: #fafafc;
    }}
    input, textarea, button, select {{ width:100%; border:1px solid var(--border); border-radius:10px; padding:10px; font:inherit; background:#fff; color:var(--text); }}
    input:focus, textarea:focus, select:focus {{
      outline: none;
      border-color: rgba(0,113,227,.5);
      box-shadow: 0 0 0 3px rgba(0,113,227,.12);
    }}
    .search-wrap {{
      margin-top: 8px;
      padding: 10px;
      border: 2px solid rgba(0,113,227,.45);
      border-radius: 12px;
      background: #f7faff;
      box-shadow: 0 6px 18px rgba(0,113,227,.12);
    }}
    .search-label {{
      font-size: 12px;
      font-weight: 700;
      color: #0a5bc4;
      margin-bottom: 6px;
    }}
    #search {{
      border: 2px solid #9fc2f4;
      background: #fff;
      box-shadow: inset 0 0 0 1px rgba(0,113,227,.06);
      font-weight: 600;
    }}
    #search:focus {{
      border-color: #0071e3;
      box-shadow: 0 0 0 3px rgba(0,113,227,.14);
    }}
    #search::placeholder {{
      color: #6d7b90;
    }}
    textarea {{ min-height:88px; resize:vertical; }}
    .list {{ margin-top:10px; max-height:52vh; overflow:auto; border:2px solid var(--border); border-radius:10px; background:#fff; }}
    .item {{ padding:10px; border-bottom:2px solid #e3e3e8; cursor:pointer; transition:background .16s ease; }}
    .item:last-child {{ border-bottom:none; }}
    .item:hover {{ background:#f7f7f9; }}
    .item.active {{ background:#eef4ff; border-left:4px solid var(--primary); }}
    .item .chips {{ display:flex; gap:6px; flex-wrap:wrap; margin-top:6px; }}
    .chip {{ border:2px solid var(--border); border-radius:999px; padding:2px 7px; font-size:11px; font-weight:600; background:#fff; color:#555; }}
    .chip.good {{ border-color: rgba(52,199,89,.6); color:#127a3a; }}
    .chip.warn {{ border-color: rgba(255,159,10,.65); color:#9a5a00; }}
    .chip.bad {{ border-color: rgba(255,59,48,.55); color:#a12822; }}
    .muted {{ color:var(--muted); font-size:12px; }}
    .grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; }}
    .card {{ border:2px solid var(--border); border-radius:10px; padding:10px; background:#fbfbfd; }}
    .actions {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:8px; margin-top:10px; }}
    .scenarios {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; margin-top:10px; }}
    .btn-primary {{ background:var(--primary); color:#fff; border-color:var(--primary); }}
    .btn-good {{ background:var(--good); color:#fff; border-color:var(--good); }}
    .btn-warn {{ background:var(--warn); color:#fff; border-color:var(--warn); }}
    button {{ transition:transform .06s ease, box-shadow .16s ease, background .16s ease; }}
    button:hover {{ box-shadow:0 2px 10px rgba(0,0,0,.06); }}
    button:active {{ transform:translateY(1px); }}
    .status {{ margin-top:10px; white-space:pre-wrap; font-size:13px; color:var(--muted); border:2px solid var(--border); border-radius:10px; padding:10px; background:#fafafa; }}
    .section-tag {{
      display:inline-block;
      margin:0 0 8px;
      padding:4px 8px;
      border:2px solid var(--border);
      border-radius:999px;
      font-size:11px;
      font-weight:700;
      color:#4f4f55;
      background:#fff;
      text-transform:uppercase;
    }}
    .quick-row {{ display:flex; gap:8px; flex-wrap:wrap; margin-top:8px; }}
    .quick-btn {{ width:auto; padding:8px 10px; font-size:12px; font-weight:700; }}
    .event-log {{
      margin-top:8px;
      border:2px solid var(--border);
      border-radius:10px;
      background:#fff;
      padding:8px;
      max-height:120px;
      overflow:auto;
      font-size:12px;
      color:#4a4a50;
      line-height:1.4;
    }}
    .risk-preview {{
      margin-top:8px;
      border:2px solid var(--border);
      border-radius:10px;
      background:#fff;
      padding:8px;
      font-size:12px;
      color:#45454b;
    }}
    .risk-preview b {{ color:#9a5a00; }}
    .quick-dock {{
      position: fixed;
      left: 10px;
      right: 10px;
      bottom: 10px;
      z-index: 40;
      display: none;
      gap: 8px;
      padding: 8px;
      border: 2px solid var(--border);
      border-radius: 12px;
      background: rgba(255,255,255,.96);
      backdrop-filter: blur(8px);
      box-shadow: 0 10px 26px rgba(0,0,0,.12);
    }}
    .quick-dock button {{ min-height: 42px; font-weight: 700; }}
    .selection-hint {{
      margin: 8px 12px 0;
      padding: 8px 10px;
      border: 2px solid var(--border);
      border-radius: 10px;
      background:#fff;
      color:#3a3a3f;
      font-size:12px;
      font-weight:600;
    }}
    .mini-kpis {{
      margin: 8px 12px 0;
      display: grid;
      grid-template-columns: repeat(4, minmax(0,1fr));
      gap: 8px;
    }}
    .mini-kpi {{
      border: 2px solid var(--border);
      border-radius: 10px;
      background:#fff;
      padding:8px;
      text-align:center;
      font-size:11px;
      color:#63636a;
    }}
    .mini-kpi b {{ display:block; margin-top:4px; font-size:16px; color:#1d1d1f; }}
    #tabUsers, #tabServices, #tabState, #tabConsole {{
      width: auto;
      min-width: 140px;
      background: #fff;
      border: 2px solid var(--border);
      border-radius: 10px;
      font-weight: 600;
    }}
    #tabUsers.active, #tabServices.active, #tabState.active, #tabConsole.active {{
      border-color: rgba(0,113,227,.9);
      color: var(--primary);
      background: #eef4ff;
    }}
    @media (max-width: 980px) {{
      .wrap {{ grid-template-columns:1fr; }}
      .actions {{ grid-template-columns:1fr; }}
      .list {{ max-height: 40vh; }}
      .top-tabs {{ padding-top: 2px; }}
      #tabUsers, #tabServices, #tabState, #tabConsole {{ min-height: 44px; font-size: 14px; }}
      .btn-primary, .btn-good, .btn-warn {{ min-height: 44px; font-weight: 700; }}
    }}
    @media (max-width: 640px) {{
      .mini-kpis {{ grid-template-columns: 1fr 1fr; }}
      .list {{ max-height: 32vh; }}
      .search-wrap {{
        position: sticky;
        top: 0;
        z-index: 4;
      }}
      .actions {{
        position: sticky;
        bottom: 0;
        background: var(--panel);
        padding-top: 8px;
        border-top: 2px solid var(--border);
      }}
      .status {{ margin-bottom: 72px; }}
      .quick-dock {{ display: grid; grid-template-columns: repeat(3, minmax(0,1fr)); }}
    }}
  </style>
</head>
<body>
  <div class="top-tabs" style="display:flex;gap:8px;padding:12px 12px 10px">
    <button id="tabUsers" type="button">РџРѕР»СЊР·РѕРІР°С‚РµР»Рё</button>
    <button id="tabServices" type="button">РЎРµСЂРІРёСЃС‹ РЅР° СЃРµСЂРІРµСЂРµ</button>
    <button id="tabState" type="button">РЎРѕСЃС‚РѕСЏРЅРёРµ СЃР»СѓР¶Р±</button>
  </div>
  <div id="selectionHint" class="selection-hint">Р’С‹Р±РµСЂРёС‚Рµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ СЃР»РµРІР°, С‡С‚РѕР±С‹ РѕС‚РєСЂС‹С‚СЊ РґРµС‚Р°Р»СЊРЅСѓСЋ РєР°СЂС‚РѕС‡РєСѓ Рё Р±С‹СЃС‚СЂС‹Рµ РґРµР№СЃС‚РІРёСЏ.</div>
  <div id="scanNotice" class="selection-hint" style="display:none"></div>
  <div id="miniKpis" class="mini-kpis"></div>
  <div class="wrap" id="viewUsers">
    <section class="panel">
      <div class="section-tag">Users</div>
      <h1>РџРѕР»СЊР·РѕРІР°С‚РµР»Рё</h1>
      <div class="grid" style="margin-top:8px" id="userKpis"></div>
      <div id="riskPreview" class="risk-preview"></div>
      <div class="search-wrap">
        <div class="search-label">Поиск пользователя</div>
        <input id="search" placeholder="Введите ID или @username">
      </div>
      <div class="list" id="list"></div>
    </section>
    <section class="panel">
      <div class="section-tag">Profile</div>
      <h1>РљР°СЂС‚РѕС‡РєР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ</h1>
      <div class="grid" id="meta"></div>
      <div class="panel" style="margin-top:10px;padding:10px">
        <div class="muted" style="margin-bottom:8px">РўРµРєСЃС‚ РґР»СЏ Mail / Wizard</div>
        <textarea id="message" placeholder="Р’РІРµРґРёС‚Рµ С‚РµРєСЃС‚ СЃРѕРѕР±С‰РµРЅРёСЏ"></textarea>
        <div class="scenarios">
          <button id="scKey" type="button">РљР»СЋС‡ РЅРµ СЂР°Р±РѕС‚Р°РµС‚</button>
          <button id="scPay" type="button">РџР»Р°С‚РµР¶ РЅРµ РїСЂРѕС€РµР»</button>
          <button id="scIos" type="button">РџРѕРґРєР»СЋС‡РµРЅРёРµ iOS</button>
          <button id="scAndroid" type="button">РџРѕРґРєР»СЋС‡РµРЅРёРµ Android</button>
        </div>
        <div class="actions">
          <button class="btn-good" id="btnWizard">РћС‚РїСЂР°РІРёС‚СЊ РІ Wizard</button>
          <button class="btn-primary" id="btnMail">РќР°РїРёСЃР°С‚СЊ Mail</button>
          <button class="btn-warn" id="btnPromo">РџРѕРґРіРѕС‚РѕРІРёС‚СЊ Promo</button>
        </div>
        <div class="actions" style="margin-top:8px">
          <button id="btnCopyId" type="button">РљРѕРїРёСЂРѕРІР°С‚СЊ ID</button>
          <button id="btnCopyUsername" type="button">РљРѕРїРёСЂРѕРІР°С‚СЊ @username</button>
          <button id="btnFillTemplate" type="button">РЁР°Р±Р»РѕРЅ РѕС‚РІРµС‚Р°</button>
        </div>
        <div class="actions" style="margin-top:8px">
          <button class="btn-good" id="btnWizardFromTemplate" type="button">Wizard РёР· С€Р°Р±Р»РѕРЅР°</button>
          <button id="btnClearText" type="button">РћС‡РёСЃС‚РёС‚СЊ С‚РµРєСЃС‚</button>
          <button id="btnToTop" type="button">РќР°РІРµСЂС…</button>
        </div>
        <div class="status" id="status">Р’С‹Р±РµСЂРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ СЃР»РµРІР°.</div>
        <div id="eventLog" class="event-log"></div>
        <div class="panel" style="margin-top:10px;padding:10px">
          <div class="muted" style="margin-bottom:6px">История действий</div>
          <div id="actionsLog" class="event-log"></div>
        </div>
        <div class="panel" style="margin-top:10px;padding:10px">
          <div class="muted" style="margin-bottom:6px">Последние ошибки</div>
          <div id="errorsLog" class="event-log"></div>
        </div>
      </div>
    </section>
  </div>
  <div class="panel" id="viewServices" style="display:none;margin:12px;">
    <div class="section-tag">Services</div>
    <h1>РЎРµСЂРІРёСЃС‹ РЅР° СЃРµСЂРІРµСЂРµ</h1>
    <div class="muted" id="servicesUpdatedAt">-</div>
    <table style="width:100%;margin-top:8px;border-collapse:collapse">
      <thead><tr><th style="text-align:left">РЎРµСЂРІРёСЃ</th><th style="text-align:left">РЎС‚Р°С‚СѓСЃ</th></tr></thead>
      <tbody id="servicesBody"></tbody>
    </table>
  </div>
  <div class="panel" id="viewState" style="display:none;margin:12px;">
    <div class="section-tag">State</div>
    <h1>РЎРѕСЃС‚РѕСЏРЅРёРµ СЃР»СѓР¶Р±</h1>
    <div class="muted" id="stateUpdatedAt">-</div>
    <table style="width:100%;margin-top:8px;border-collapse:collapse">
      <thead><tr><th style="text-align:left">РџР°СЂР°РјРµС‚СЂ</th><th style="text-align:left">Р—РЅР°С‡РµРЅРёРµ</th></tr></thead>
      <tbody id="stateBody"></tbody>
    </table>
  </div>
  <script>
    const users = {users_json};
    const list = document.getElementById("list");
    const search = document.getElementById("search");
    const userKpis = document.getElementById("userKpis");
    const riskPreview = document.getElementById("riskPreview");
    const meta = document.getElementById("meta");
    const message = document.getElementById("message");
    const statusBox = document.getElementById("status");
    const selectionHint = document.getElementById("selectionHint");
    const scanNotice = document.getElementById("scanNotice");
    const miniKpis = document.getElementById("miniKpis");
    const eventLog = document.getElementById("eventLog");
    const actionsLog = document.getElementById("actionsLog");
    const errorsLog = document.getElementById("errorsLog");
    let selected = null;
    let activeJobId = "";
    let pollTimer = null;
    const actionApiBase = "root-api";
    let consoleCommand = null;
    let consoleRun = null;
    let consoleOutput = null;

    function setupConsoleTab() {{
      const tabsWrap = document.getElementById("tabUsers")?.parentElement;
      if (!tabsWrap || document.getElementById("tabConsole")) return;
      const tabBtn = document.createElement("button");
      tabBtn.id = "tabConsole";
      tabBtn.type = "button";
      tabBtn.textContent = "Console";
      tabsWrap.appendChild(tabBtn);

      const consolePanel = document.createElement("div");
      consolePanel.className = "panel";
      consolePanel.id = "viewConsole";
      consolePanel.style.display = "none";
      consolePanel.style.margin = "12px";
      consolePanel.innerHTML = `
        <h1>Server Console</h1>
        <div class="muted">Working directory: {html.escape(str(APP_ROOT))}</div>
        <div style="display:grid;grid-template-columns:1fr auto;gap:8px;margin-top:10px">
          <input id="consoleCommand" type="text" placeholder="РќР°РїСЂРёРјРµСЂ: systemctl status vol29app --no-pager">
          <button id="consoleRun" class="btn-primary" type="button" style="min-width:120px">Run</button>
        </div>
        <pre id="consoleOutput" style="margin-top:10px;border:1px solid var(--border);border-radius:10px;padding:10px;background:#fafafa;min-height:220px;max-height:60vh;overflow:auto;white-space:pre-wrap">Ready.</pre>
      `;
      const anchor = document.getElementById("viewState");
      if (anchor?.parentElement) {{
        anchor.parentElement.insertBefore(consolePanel, anchor.nextSibling);
      }} else {{
        document.body.appendChild(consolePanel);
      }}

      consoleCommand = document.getElementById("consoleCommand");
      consoleRun = document.getElementById("consoleRun");
      consoleOutput = document.getElementById("consoleOutput");
      tabBtn.addEventListener("click", () => switchTab("console"));
      if (consoleRun) consoleRun.addEventListener("click", runConsoleCommand);
      if (consoleCommand) {{
        consoleCommand.addEventListener("keydown", (e) => {{
          if (e.key === "Enter" && !e.shiftKey) {{
            e.preventDefault();
            runConsoleCommand();
          }}
        }});
      }}
    }}

    function esc(v) {{
      return String(v ?? "").replace(/[&<>"']/g, m => ({{"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","'":"&#39;"}}[m]));
    }}
    function userLabel(u) {{
      return `ID ${{u.user_id}} ${{u.username ? "@" + u.username : ""}}`;
    }}
    function filteredUsers() {{
      const q = String(search.value || "").trim().toLowerCase();
      const qDigits = q.replace(/\D+/g, "");

      // Exact ID/username lookup should not be blocked by status filter.
      if (q) {{
        const exact = users.find((u) => {{
          const id = String(u.user_id || "").trim();
          const un = String(u.username || "").trim().toLowerCase();
          return (
            id === q ||
            (qDigits && id === qDigits) ||
            un === q.replace(/^@/, "") ||
            ("@" + un) === q
          );
        }});
        if (exact) return [exact];
      }}

      let rows = users.filter(u => {{
        const id = String(u.user_id || "");
        const un = String(u.username || "").toLowerCase();
        return (!q || id.includes(q) || un.includes(q) || ("@" + un).includes(q));
      }});
      const toNum = (v, d=0) => Number.isFinite(Number(v)) ? Number(v) : d;
      rows.sort((a,b) => toNum(a.user_id, 10**9) - toNum(b.user_id, 10**9));
      return rows;
    }}

    function renderKpis(rows) {{
      const totalFiltered = rows.length;
      const totalAll = users.length;
      const paidAll = users.filter(u => Number(u.subscriptions || 0) > 0).length;
      const riskAll = users.filter(u => ["expired", "expiring_7", "expiring_30"].includes(String(u.status || ""))).length;
      const noSubsAll = users.filter(u => String(u.status || "") === "no_subs").length;
      userKpis.innerHTML = `
        <div class="card"><div class="muted">Найдено (фильтр)</div><b>${{esc(totalFiltered)}}</b></div>
        <div class="card"><div class="muted">Всего в базе</div><b>${{esc(totalAll)}}</b></div>
        <div class="card"><div class="muted">С подписками</div><b>${{esc(paidAll)}}</b></div>
        <div class="card"><div class="muted">Без подписки</div><b>${{esc(noSubsAll)}}</b></div>
      `;
      if (riskPreview) {{
        const risky = users
          .filter(u => ["expired", "expiring_7", "expiring_30"].includes(String(u.status || "")))
          .sort((a,b) => (Number(a.days_left ?? 9999) - Number(b.days_left ?? 9999)))
          .slice(0, 3);
        if (!risky.length) {{
          riskPreview.innerHTML = "<b>ТОП риск:</b> сейчас критичных пользователей нет";
        }} else {{
          riskPreview.innerHTML = "<b>ТОП риск:</b> " + risky.map(
            u => "ID " + esc(u.user_id) + " (" + esc((u.days_left ?? "-")) + " дн.)"
          ).join(" | ");
        }}
      }}
      if (miniKpis) {{
        const active = users.filter(u => ["active", "expiring_7", "expiring_30"].includes(String(u.status || ""))).length;
        const expiring = users.filter(u => ["expiring_7", "expiring_30"].includes(String(u.status || ""))).length;
        const expired = users.filter(u => String(u.status || "") === "expired").length;
        miniKpis.innerHTML = `
          <div class="mini-kpi">Всего<b>${{esc(users.length)}}</b></div>
          <div class="mini-kpi">Активные<b>${{esc(active)}}</b></div>
          <div class="mini-kpi">На грани<b>${{esc(expiring)}}</b></div>
          <div class="mini-kpi">Истекли<b>${{esc(expired)}}</b></div>
        `;
      }}
    }}

    function copySelected(kind) {{
      if (!selected) {{
        statusBox.textContent = "РЎРЅР°С‡Р°Р»Р° РІС‹Р±РµСЂРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.";
        return;
      }}
      let value = "";
      if (kind === "id") value = String(selected.user_id || "");
      if (kind === "username") value = selected.username ? `@${{selected.username}}` : "";
      if (!value) {{
        statusBox.textContent = "Р”Р°РЅРЅС‹Рµ РґР»СЏ РєРѕРїРёСЂРѕРІР°РЅРёСЏ РѕС‚СЃСѓС‚СЃС‚РІСѓСЋС‚.";
        return;
      }}
      navigator.clipboard.writeText(value).then(() => {{
        statusBox.textContent = `РЎРєРѕРїРёСЂРѕРІР°РЅРѕ: ${{value}}`;
      }}).catch(() => {{
        statusBox.textContent = "РќРµ СѓРґР°Р»РѕСЃСЊ СЃРєРѕРїРёСЂРѕРІР°С‚СЊ РІ Р±СѓС„РµСЂ.";
      }});
    }}

    function fillSupportTemplate() {{
      if (!selected) {{
        statusBox.textContent = "РЎРЅР°С‡Р°Р»Р° РІС‹Р±РµСЂРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.";
        return;
      }}
      const uname = selected.username ? `@${{selected.username}}` : "-";
      message.value = `Р—РґСЂР°РІСЃС‚РІСѓР№С‚Рµ!\\nРџСЂРѕРІРµСЂСЏСЋ РѕР±СЂР°С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ ID: ${{selected.user_id}} (${{uname}}).\\nРћРїРёС€РёС‚Рµ, РїРѕР¶Р°Р»СѓР№СЃС‚Р°, С‡С‚Рѕ РёРјРµРЅРЅРѕ РЅРµ СЂР°Р±РѕС‚Р°РµС‚ Рё РїСЂРёР»РѕР¶РёС‚Рµ СЃРєСЂРёРЅ РѕС€РёР±РєРё.`;
      statusBox.textContent = "РЁР°Р±Р»РѕРЅ Р·Р°РїРѕР»РЅРµРЅ.";
      pushEvent("РЁР°Р±Р»РѕРЅ РѕС‚РІРµС‚Р° Р·Р°РїРѕР»РЅРµРЅ");
    }}

    function pushEvent(text) {{
      if (!eventLog) return;
      const ts = new Date().toLocaleTimeString();
      const line = document.createElement("div");
      line.textContent = `[${{ts}}] ${{text}}`;
      eventLog.prepend(line);
      while (eventLog.childElementCount > 12) {{
        eventLog.removeChild(eventLog.lastChild);
      }}
    }}

    function renderList() {{
      const rows = filteredUsers();
      renderKpis(rows);
      list.innerHTML = rows.map(u => `
        <div class="item ${{selected && String(selected.user_id) === String(u.user_id) ? "active" : ""}}" data-id="${{esc(u.user_id)}}">
          <div>${{esc(userLabel(u))}}</div>
          <div class="muted">РџРѕРґРїРёСЃРѕРє: ${{esc(u.subscriptions)}} В· РЎС‚Р°С‚СѓСЃ: ${{esc(u.status_label)}}</div>
        </div>
      `).join("") || `<div class="item"><div class="muted">РќРёС‡РµРіРѕ РЅРµ РЅР°Р№РґРµРЅРѕ</div></div>`;
    }}
    function renderMeta() {{
      if (!selected) {{
        meta.innerHTML = '<div class="card"><div class="muted">РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РІС‹Р±СЂР°РЅ</div></div>';
        return;
      }}
      const cards = [];
      const addCard = (label, value, full = false) => {{
        const text = String(value ?? "").trim();
        if (!text || text === "-" || text === "0" || text === "0.0") return;
        cards.push(`<div class="card"${{full ? ' style="grid-column: 1 / -1;"' : ""}}><div class="muted">${{esc(label)}}</div><b>${{esc(text)}}</b></div>`);
      }};
      const addCardAllowZero = (label, value, full = false) => {{
        const text = String(value ?? "").trim();
        if (!text || text === "-") return;
        cards.push(`<div class="card"${{full ? ' style="grid-column: 1 / -1;"' : ""}}><div class="muted">${{esc(label)}}</div><b>${{esc(text)}}</b></div>`);
      }};

      const requestsTotal = Number(selected.requests_count ?? 0);
      const incomingCount = Number(selected.incoming_count ?? 0);
      const wizardCount = Number(selected.wizard_count ?? 0);
      const mailCount = Number(selected.mail_count ?? 0);
      const hasRequestInfo = requestsTotal > 0 || incomingCount > 0 || wizardCount > 0 || mailCount > 0 || Boolean(selected.last_request_at) || Boolean(selected.last_request_text);
      addCardAllowZero("ID", selected.user_id);
      addCard("Username", selected.username ? "@" + selected.username : "");
      addCard("Регистрация", selected.registration_date || "");
      addCardAllowZero("Подписок", selected.subscriptions);
      addCard("Баланс", selected.balance_rub_text || "");
      addCard("Всего пополнено", selected.total_topped_up_rub_text || "");
      addCard("Локации", selected.locations || "");
      addCard("Ближайшее истечение", selected.nearest_expiration || "");
      addCard("Дней до окончания", selected.days_left !== "" ? selected.days_left : "");

      if (hasRequestInfo) {{
        addCardAllowZero("Всего обращений", requestsTotal);
        if (incomingCount > 0) addCardAllowZero("Входящие обращения", incomingCount);
        if (wizardCount > 0) addCardAllowZero("Отправок в Wizard", wizardCount);
        if (mailCount > 0) addCardAllowZero("Отправок сообщений", mailCount);
        addCard("Последнее обращение", selected.last_request_at || "");
        addCard("Текст последнего обращения", selected.last_request_text || "", true);
      }}

      meta.innerHTML = cards.join("") || '<div class="card"><div class="muted">Нет данных</div></div>';
    }}
    async function pollJob(jobId) {{
      if (pollTimer) clearTimeout(pollTimer);
      try {{
        const r = await fetch(`${{actionApiBase}}/job/${{encodeURIComponent(jobId)}}`, {{ cache: "no-store" }});
        const p = await r.json();
        if (!r.ok || !p.ok || !p.job) {{
          statusBox.textContent = "РћС€РёР±РєР° РїРѕР»СѓС‡РµРЅРёСЏ СЃС‚Р°С‚СѓСЃР° Р·Р°РґР°С‡Рё.";
          return;
        }}
        const j = p.job;
        const lines = [
          `РЎС‚Р°С‚СѓСЃ: ${{j.status || "-"}}`,
          j.id ? `Р—Р°РґР°С‡Р°: ${{j.id}}` : "",
          j.result_text ? `Р РµР·СѓР»СЊС‚Р°С‚: ${{String(j.result_text).slice(0, 400)}}` : "",
          j.error_text ? `РћС€РёР±РєР°: ${{j.error_text}}` : "",
        ].filter(Boolean);
        statusBox.textContent = lines.join("\\n");
        if (j.status === "queued" || j.status === "running") {{
          pollTimer = setTimeout(() => pollJob(jobId), 1200);
        }}
      }} catch (e) {{
        statusBox.textContent = `РћС€РёР±РєР° РѕРїСЂРѕСЃР°: ${{e}}`;
      }}
    }}
    async function submit(action, needMessage) {{
      if (!selected) {{
        statusBox.textContent = "РЎРЅР°С‡Р°Р»Р° РІС‹Р±РµСЂРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.";
        return;
      }}
      const text = String(message.value || "").trim();
      if (needMessage && !text) {{
        statusBox.textContent = "Р”РѕР±Р°РІСЊ С‚РµРєСЃС‚ СЃРѕРѕР±С‰РµРЅРёСЏ.";
        return;
      }}
      statusBox.textContent = "Р—Р°РґР°С‡Р° РѕС‚РїСЂР°РІР»РµРЅР°...";
      try {{
        const r = await fetch(`${{actionApiBase}}/action`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            action,
            user: String(selected.user_id || ""),
            message: text,
          }}),
        }});
        const p = await r.json();
        if (!r.ok || !p.ok || !p.job || !p.job.id) {{
          statusBox.textContent = `РћС€РёР±РєР° API: ${{(p && p.error) || "unknown_error"}}`;
          return;
        }}
        activeJobId = String(p.job.id || "");
        await pollJob(activeJobId);
      }} catch (e) {{
        statusBox.textContent = `РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё: ${{e}}`;
      }}
    }}

    const scenarioTemplates = {{
      key: "Р—РґСЂР°РІСЃС‚РІСѓР№С‚Рµ. РџСЂРѕРІРµСЂРёРј РєР»СЋС‡ РїРѕ С€Р°РіР°Рј:\\n1) РЎРєРѕРїРёСЂСѓР№С‚Рµ РєР»СЋС‡ Р·Р°РЅРѕРІРѕ РёР· РїРѕРґРїРёСЃРєРё РїРѕР»РЅРѕСЃС‚СЊСЋ.\\n2) РЈРґР°Р»РёС‚Рµ СЃС‚Р°СЂС‹Р№ РїСЂРѕС„РёР»СЊ Рё РёРјРїРѕСЂС‚РёСЂСѓР№С‚Рµ РЅРѕРІС‹Р№.\\n3) РџСЂРѕРІРµСЂСЊС‚Рµ Р°РІС‚Рѕ-РґР°С‚Сѓ Рё Р°РІС‚Рѕ-РІСЂРµРјСЏ.\\n4) РџРµСЂРµРєР»СЋС‡РёС‚Рµ СЃРµС‚СЊ Wi-Fi/РјРѕР±РёР»СЊРЅР°СЏ.\\nР•СЃР»Рё РЅРµ РїРѕРјРѕР¶РµС‚, РѕС‚РІРµС‚СЊС‚Рµ: РЅРµ РїРѕРјРѕРіР»Рѕ.",
      pay: "Р—РґСЂР°РІСЃС‚РІСѓР№С‚Рµ. Р”Р»СЏ РїСЂРѕРІРµСЂРєРё РїР»Р°С‚РµР¶Р° РїСЂРёС€Р»РёС‚Рµ:\\n1) ID РёР· СЂР°Р·РґРµР»Р° РџСЂРѕС„РёР»СЊ\\n2) Р’СЂРµРјСЏ Рё СЃСѓРјРјСѓ РїР»Р°С‚РµР¶Р°\\n3) РџРѕСЃР»РµРґРЅРёРµ С†РёС„СЂС‹ РѕРїРµСЂР°С†РёРё РёР»Рё С‡РµРє\\nРџРѕСЃР»Рµ СЌС‚РѕРіРѕ СЃСЂР°Р·Сѓ РїРµСЂРµРґР°Рј РІ РїРѕРґРґРµСЂР¶РєСѓ.",
      ios: "РРЅСЃС‚СЂСѓРєС†РёСЏ iOS:\\n1) РЈСЃС‚Р°РЅРѕРІРёС‚Рµ РїСЂРёР»РѕР¶РµРЅРёРµ РґР»СЏ VPN.\\n2) РЎРєРѕРїРёСЂСѓР№С‚Рµ РєР»СЋС‡ РёР· РїРѕРґРїРёСЃРєРё.\\n3) РРјРїРѕСЂС‚РёСЂСѓР№С‚Рµ РєР»СЋС‡ РІ РїСЂРёР»РѕР¶РµРЅРёРµ.\\n4) Р’РєР»СЋС‡РёС‚Рµ VPN-РїСЂРѕС„РёР»СЊ.\\nР•СЃР»Рё РѕС€РёР±РєР° РѕСЃС‚Р°РЅРµС‚СЃСЏ вЂ” РїСЂРёС€Р»РёС‚Рµ СЃРєСЂРёРЅ Рё С‚РµРєСЃС‚ РѕС€РёР±РєРё.",
      android: "РРЅСЃС‚СЂСѓРєС†РёСЏ Android:\\n1) РЈСЃС‚Р°РЅРѕРІРёС‚Рµ v2rayNG.\\n2) РЎРєРѕРїРёСЂСѓР№С‚Рµ РєР»СЋС‡ РёР· РїРѕРґРїРёСЃРєРё.\\n3) РРјРїРѕСЂС‚РёСЂСѓР№С‚Рµ РєР»СЋС‡ РІ РїСЂРёР»РѕР¶РµРЅРёРµ.\\n4) РќР°Р¶РјРёС‚Рµ РџРѕРґРєР»СЋС‡РёС‚СЊ.\\nР•СЃР»Рё РЅРµ РїРѕРґРєР»СЋС‡Р°РµС‚СЃСЏ вЂ” РїСЂРёС€Р»РёС‚Рµ С‚РµРєСЃС‚ РѕС€РёР±РєРё Рё РјРѕРґРµР»СЊ С‚РµР»РµС„РѕРЅР°.",
    }};
    function applyScenario(key) {{
      const text = scenarioTemplates[key];
      if (!text) return;
      message.value = text;
      statusBox.textContent = "РЎС†РµРЅР°СЂРёР№ РїРѕРґСЃС‚Р°РІР»РµРЅ. РќР°Р¶РјРёС‚Рµ Mail РёР»Рё Wizard.";
    }}

    async function loadServices() {{
      const body = document.getElementById("servicesBody");
      const updated = document.getElementById("servicesUpdatedAt");
      try {{
        const r = await fetch(`${{actionApiBase}}/services`, {{ cache: "no-store" }});
        const p = await r.json();
        if (!r.ok || !p.ok || !p.services) throw new Error("bad_response");
        const data = p.services;
        updated.textContent = `РћР±РЅРѕРІР»РµРЅРѕ: ${{data.generated_at || "-"}}`;
        body.innerHTML = (data.services || []).map(s => `<tr><td>${{esc(s.service)}}</td><td>${{esc(s.status)}}</td></tr>`).join("") || "<tr><td colspan='2'>РќРµС‚ РґР°РЅРЅС‹С…</td></tr>";
      }} catch (e) {{
        body.innerHTML = `<tr><td colspan='2'>РћС€РёР±РєР°: ${{esc(e)}}</td></tr>`;
      }}
    }}

    async function loadState() {{
      const body = document.getElementById("stateBody");
      const updated = document.getElementById("stateUpdatedAt");
      try {{
        const r = await fetch(`${{actionApiBase}}/overview`, {{ cache: "no-store" }});
        const p = await r.json();
        if (!r.ok || !p.ok || !p.overview) throw new Error("bad_response");
        const ov = p.overview;
        const proc = ov.processes || {{}};
        updated.textContent = `РћР±РЅРѕРІР»РµРЅРѕ: ${{ov.generated_at || "-"}}`;
        const rows = [
          ["Admin flow", proc.admin_flow || "-"],
          ["Scan active", proc.scan_active ? "РґР°" : "РЅРµС‚"],
          ["Mail2 active", proc.mail2_active ? "РґР°" : "РЅРµС‚"],
          ["Wizard pending", proc.wizard_pending ?? "-"],
          ["GPT active/pending", `${{proc.gpt_active ?? 0}} / ${{proc.gpt_pending ?? 0}}`],
          ["Unresolved open", ov.unresolved_open_count ?? 0],
        ];
        body.innerHTML = rows.map(([k,v]) => `<tr><td>${{esc(k)}}</td><td>${{esc(v)}}</td></tr>`).join("");
      }} catch (e) {{
        body.innerHTML = `<tr><td colspan='2'>РћС€РёР±РєР°: ${{esc(e)}}</td></tr>`;
      }}
    }}

    async function refreshScanNotice() {{
      if (!scanNotice) return;
      try {{
        const r = await fetch(`${{actionApiBase}}/overview`, {{ cache: "no-store" }});
        const p = await r.json();
        if (!r.ok || !p.ok || !p.overview) return;
        const proc = p.overview.processes || {{}};
        if (!proc.scan_active) {{
          scanNotice.style.display = "none";
          scanNotice.textContent = "";
          return;
        }}
        const nextId = Number(proc.scan_next_user_id || 0);
        const total = Number(proc.scan_total_users_hint || 0);
        const checked = Math.max(0, nextId > 0 ? nextId - 1 : 0);
        const progress = total > 0 ? `${{checked}}/${{total}}` : `${{checked}}`;
        scanNotice.style.display = "block";
        scanNotice.textContent = `Скан запущен: проверено ${{progress}}`;
      }} catch (e) {{
        // ignore transient polling errors
      }}
    }}

    async function loadActionsLog() {{
      if (!actionsLog) return;
      try {{
        const r = await fetch(`${{actionApiBase}}/actions`, {{ cache: "no-store" }});
        const p = await r.json();
        const rows = (r.ok && p.ok && p.payload && Array.isArray(p.payload.rows)) ? p.payload.rows : [];
        actionsLog.innerHTML = rows.slice(0, 12).map((row) => {{
          const at = esc(row.created_at || "-");
          const act = esc(row.action || "-");
          const usr = esc(row.resolved_user_id || row.user_lookup || "-");
          const st = esc(row.status || "-");
          const err = String(row.error_text || "").trim();
          const txt = err ? esc(err) : esc(String(row.result_text || "").slice(0, 120));
          return `<div>[${{at}}] ${{act}} | user: ${{usr}} | ${{st}}${{txt ? " | " + txt : ""}}</div>`;
        }}).join("") || "<div>Нет данных</div>";
      }} catch (e) {{
        actionsLog.innerHTML = `<div>Ошибка загрузки: ${{esc(e)}}</div>`;
      }}
    }}

    async function loadErrorsLog() {{
      if (!errorsLog) return;
      try {{
        const r = await fetch(`${{actionApiBase}}/errors`, {{ cache: "no-store" }});
        const p = await r.json();
        const rows = (r.ok && p.ok && p.payload && Array.isArray(p.payload.rows)) ? p.payload.rows : [];
        errorsLog.innerHTML = rows.slice(-12).map((line) => `<div>${{esc(line)}}</div>`).join("") || "<div>Ошибок нет</div>";
      }} catch (e) {{
        errorsLog.innerHTML = `<div>Ошибка загрузки: ${{esc(e)}}</div>`;
      }}
    }}

    async function runConsoleCommand() {{
      const cmd = String(consoleCommand?.value || "").trim();
      if (!cmd) {{
        if (consoleOutput) consoleOutput.textContent = "Р’РІРµРґРёС‚Рµ РєРѕРјР°РЅРґСѓ.";
        return;
      }}
      if (consoleRun) consoleRun.disabled = true;
      if (consoleOutput) consoleOutput.textContent = `Running: ${{cmd}} ...`;
      try {{
        const r = await fetch(`${{actionApiBase}}/terminal`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ command: cmd }}),
        }});
        const p = await r.json();
        if (!r.ok || !p.ok) {{
          const err = (p && (p.error || p.detail)) || "unknown_error";
          if (consoleOutput) consoleOutput.textContent = `Error: ${{err}}`;
          return;
        }}
        const lines = [
          `Command: ${{p.command || cmd}}`,
          `Exit code: ${{p.code}}`,
          `Time: ${{p.elapsed_ms}} ms`,
          `At: ${{p.generated_at || "-"}}`,
          "",
          String(p.output || "(no output)"),
        ];
        if (consoleOutput) consoleOutput.textContent = lines.join("\\n");
      }} catch (e) {{
        if (consoleOutput) consoleOutput.textContent = `Request failed: ${{e}}`;
      }} finally {{
        if (consoleRun) consoleRun.disabled = false;
      }}
    }}

    let activeTab = "users";
    function switchTab(name) {{
      activeTab = name;
      document.getElementById("viewUsers").style.display = name === "users" ? "grid" : "none";
      document.getElementById("viewServices").style.display = name === "services" ? "block" : "none";
      document.getElementById("viewState").style.display = name === "state" ? "block" : "none";
      ["tabUsers","tabServices","tabState","tabConsole"].forEach((id) => {{
        const el = document.getElementById(id);
        if (!el) return;
        const isActive =
          (name === "users" && id === "tabUsers") ||
          (name === "services" && id === "tabServices") ||
          (name === "state" && id === "tabState") ||
          (name === "console" && id === "tabConsole");
        el.classList.toggle("active", !!isActive);
      }});
      const consolePanel = document.getElementById("viewConsole");
      if (consolePanel) {{
        consolePanel.style.display = name === "console" ? "block" : "none";
      }}
      if (name === "services") loadServices();
      if (name === "state") loadState();
    }}

    // Mobile-friendly renderer with clear chips for subscription/status
    renderList = function() {{
      const rows = filteredUsers();
      renderKpis(rows);
      const statusChipClass = (s) => {{
        const v = String(s || "");
        if (v === "active") return "good";
        if (v === "expired") return "bad";
        if (v === "expiring_7" || v === "expiring_30") return "warn";
        return "";
      }};
      list.innerHTML = rows.map(u => `
        <div class="item ${{selected && String(selected.user_id) === String(u.user_id) ? "active" : ""}}" data-id="${{esc(u.user_id)}}">
          <div>${{esc(userLabel(u))}}</div>
          <div class="chips">
            <span class="chip">РџРѕРґРїРёСЃРѕРє: ${{esc(u.subscriptions)}}</span>
            <span class="chip ${{statusChipClass(u.status)}}">${{esc(u.status_label)}}</span>
          </div>
        </div>
      `).join("") || `<div class="item"><div class="muted">РќРёС‡РµРіРѕ РЅРµ РЅР°Р№РґРµРЅРѕ</div></div>`;
    }}

    list.addEventListener("click", (e) => {{
      const row = e.target.closest(".item[data-id]");
      if (!row) return;
      const id = String(row.dataset.id || "");
      selected = filteredUsers().find(u => String(u.user_id) === id) || users.find(u => String(u.user_id) === id) || null;
      renderList();
      renderMeta();
      statusBox.textContent = selected ? `Р’С‹Р±СЂР°РЅ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ ${{selected.user_id}}.` : "РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РІС‹Р±СЂР°РЅ.";
    }});
    list.addEventListener("click", () => {{
      if (!selectionHint) return;
      if (selected) {{
        selectionHint.textContent = `Р’С‹Р±СЂР°РЅ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ: ID ${{selected.user_id}}${{selected.username ? " | @" + selected.username : ""}}`;
      }} else {{
        selectionHint.textContent = "Р’С‹Р±РµСЂРёС‚Рµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ СЃР»РµРІР°, С‡С‚РѕР±С‹ РѕС‚РєСЂС‹С‚СЊ РґРµС‚Р°Р»СЊРЅСѓСЋ РєР°СЂС‚РѕС‡РєСѓ Рё Р±С‹СЃС‚СЂС‹Рµ РґРµР№СЃС‚РІРёСЏ.";
      }}
    }});
    search.addEventListener("input", renderList);
    document.getElementById("btnWizard").addEventListener("click", () => submit("wizard_card", false));
    document.getElementById("btnMail").addEventListener("click", () => submit("mail", true));
    document.getElementById("btnPromo").addEventListener("click", () => submit("promo", false));
    document.getElementById("scKey").addEventListener("click", () => applyScenario("key"));
    document.getElementById("scPay").addEventListener("click", () => applyScenario("pay"));
    document.getElementById("scIos").addEventListener("click", () => applyScenario("ios"));
    document.getElementById("scAndroid").addEventListener("click", () => applyScenario("android"));
    document.getElementById("btnCopyId").addEventListener("click", () => copySelected("id"));
    document.getElementById("btnCopyUsername").addEventListener("click", () => copySelected("username"));
    document.getElementById("btnFillTemplate").addEventListener("click", fillSupportTemplate);
    document.getElementById("btnClearText").addEventListener("click", () => {{
      message.value = "";
      pushEvent("РўРµРєСЃС‚ РѕС‡РёС‰РµРЅ");
    }});
    document.getElementById("btnToTop").addEventListener("click", () => {{
      window.scrollTo({{ top: 0, behavior: "smooth" }});
      pushEvent("РџСЂРѕРєСЂСѓС‚РєР° РІРІРµСЂС…");
    }});
    document.getElementById("btnWizardFromTemplate").addEventListener("click", async () => {{
      if (!String(message.value || "").trim()) fillSupportTemplate();
      await submit("wizard_card", false);
      pushEvent("РћС‚РїСЂР°РІРєР° РІ Wizard РёР· С€Р°Р±Р»РѕРЅР°");
    }});
    document.getElementById("tabUsers").addEventListener("click", () => switchTab("users"));
    document.getElementById("tabServices").addEventListener("click", () => switchTab("services"));
    document.getElementById("tabState").addEventListener("click", () => switchTab("state"));
    if (selectionHint) {{
      selectionHint.addEventListener("click", () => {{
        search.focus();
        renderList();
        pushEvent("Фокус на поиск");
      }});
    }}
    setupConsoleTab();
    renderList();
    renderMeta();
    refreshScanNotice();
    loadActionsLog();
    loadErrorsLog();
    setInterval(() => {{
      if (activeTab === "services") loadServices();
      if (activeTab === "state") loadState();
      refreshScanNotice();
      loadActionsLog();
      loadErrorsLog();
    }}, 15000);
  </script>
</body>
</html>"""


def admin_user_rows_json(records: list[dict]) -> str:
    now = datetime.now()
    rows: list[dict[str, object]] = []

    def normalize_profile_text(value: str) -> str:
        return sanitize_outgoing_text(str(value or "")).replace("\xa0", " ")

    def parse_money(text: str, patterns: list[str]) -> float | None:
        normalized = normalize_profile_text(text)
        for pattern in patterns:
            match = re.search(pattern, normalized, flags=re.IGNORECASE)
            if not match:
                continue
            token = str(match.group(1) or "").replace(" ", "").replace(",", ".")
            try:
                return float(token)
            except ValueError:
                continue
        return None

    def money_text(value: float | None) -> str:
        if value is None:
            return "-"
        if float(value).is_integer():
            return f"{int(value)} в‚Ѕ"
        return f"{value:.2f} в‚Ѕ"

    def derive_location(sub: dict) -> str:
        def is_location_id_like(value: str) -> bool:
            token = normalize_profile_text(value).strip()
            return bool(token) and bool(re.fullmatch(r"\d{1,8}", token))

        raw_location = normalize_profile_text(str(sub.get("location") or "")).strip()
        if raw_location and raw_location.casefold() not in {"-", "без локации", "unknown"} and not is_location_id_like(raw_location):
            return raw_location

        button_text = normalize_profile_text(str(sub.get("button_text") or "")).strip()
        button_loc = extract_location_from_subscription_button(button_text).strip()
        if button_loc and button_loc.casefold() not in {"-", "без локации", "unknown"} and not is_location_id_like(button_loc):
            return button_loc

        detail_text = normalize_profile_text(str(sub.get("detail_text") or ""))
        for pattern in (
            r"(?:локац(?:ия|ии)|страна|сервер|гео)\s*[:\-]\s*([^\n\r,;|]{2,40})",
            r"(?:location|country|server)\s*[:\-]\s*([^\n\r,;|]{2,40})",
        ):
            match = re.search(pattern, detail_text, flags=re.IGNORECASE)
            if match:
                candidate = normalize_profile_text(match.group(1)).strip(" .")
                if candidate and candidate.casefold() not in {"-", "без локации", "unknown"} and not is_location_id_like(candidate):
                    return candidate
        return ""

    def split_locations(value: str) -> list[str]:
        text = normalize_profile_text(value).strip()
        if not text:
            return []
        parts = re.split(r"[,;|/]+", text)
        cleaned: list[str] = []
        for item in parts:
            candidate = re.sub(r"\s+", " ", item).strip(" .")
            if not candidate:
                continue
            if candidate.casefold() in {"локация", "локации", "location", "country", "server"}:
                continue
            if re.fullmatch(r"\d{1,8}", candidate):
                continue
            if len(candidate) > 36:
                continue
            cleaned.append(candidate)
        if cleaned:
            return cleaned
        single = re.sub(r"\s+", " ", text).strip(" .")
        return [single] if single else []

    for record in records:
        user_id = str(record.get("user_id") or "").strip()
        if not user_id:
            continue
        username = normalize_username(str(record.get("username") or ""))
        profile_text = normalize_profile_text(str(record.get("user_text") or ""))
        parsed_profile = record.get("parsed_profile") if isinstance(record.get("parsed_profile"), dict) else {}
        balance_rub = parsed_profile.get("balance_rub")
        if not isinstance(balance_rub, (int, float)):
            balance_rub = parse_money(
                profile_text,
                [
                    r"(?:баланс|balance|wallet)\D{0,24}([0-9][0-9\s.,]*)",
                    r"(?:на\s+счете|на\s+счёте|на\s+балансе)\D{0,24}([0-9][0-9\s.,]*)",
                ],
            )
        total_topped_up_rub = parsed_profile.get("total_topped_up_rub")
        if not isinstance(total_topped_up_rub, (int, float)):
            total_topped_up_rub = parse_money(
                profile_text,
                [
                    r"(?:всего\s+пополнено|пополнено\s+всего|сумма\s+пополнений)\D{0,28}([0-9][0-9\s.,]*)",
                    r"(?:total\s+topped\s*up|total\s+recharge|total\s+deposits?)\D{0,28}([0-9][0-9\s.,]*)",
                ],
            )
        raw_subscriptions = list(record.get("subscriptions") or [])
        deduped_subscriptions: list[dict] = []
        seen_sub_keys: set[str] = set()
        for sub in raw_subscriptions:
            sub_id = normalize_profile_text(str(sub.get("subscription_id") or "")).strip()
            btn = normalize_profile_text(str(sub.get("button_text") or "")).strip()
            loc = derive_location(sub)
            key = sub_id or f"{btn}|{loc}"
            if not key or key in seen_sub_keys:
                continue
            seen_sub_keys.add(key)
            sub_copy = dict(sub)
            sub_copy["subscription_id"] = sub_id
            sub_copy["button_text"] = btn
            sub_copy["location"] = loc
            sub_copy["detail_text"] = normalize_profile_text(str(sub.get("detail_text") or ""))
            deduped_subscriptions.append(sub_copy)
        subscriptions = deduped_subscriptions
        locations_set: set[str] = set()
        for sub in subscriptions:
            loc_text = str(sub.get("location") or "").strip()
            for loc_item in split_locations(loc_text):
                locations_set.add(loc_item)

            detail_text = normalize_profile_text(str(sub.get("detail_text") or ""))
            for pattern in (
                r"(?:локации|локация|страны|страна)\s*[:\-]\s*([^\n\r]{2,100})",
                r"(?:locations?|countries?)\s*[:\-]\s*([^\n\r]{2,100})",
            ):
                match = re.search(pattern, detail_text, flags=re.IGNORECASE)
                if not match:
                    continue
                for loc_item in split_locations(match.group(1)):
                    locations_set.add(loc_item)

        locations = sorted(locations_set)
        nearest_expiration: datetime | None = None
        for sub in subscriptions:
            expires_at = extract_expiration_date(str(sub.get("detail_text") or ""))
            if not expires_at:
                continue
            if nearest_expiration is None or expires_at < nearest_expiration:
                nearest_expiration = expires_at

        days_left = ""
        status = "no_subs"
        status_label = "Р‘РµР· РїРѕРґРїРёСЃРєРё"
        nearest_expiration_text = "-"
        if subscriptions:
            if nearest_expiration is None:
                status = "unknown_date"
                status_label = "Р”Р°С‚Р° РЅРµРёР·РІРµСЃС‚РЅР°"
            else:
                days_left_int = (nearest_expiration.date() - now.date()).days
                days_left = days_left_int
                nearest_expiration_text = nearest_expiration.strftime("%Y-%m-%d")
                if days_left_int < 0:
                    status = "expired"
                    status_label = "РСЃС‚РµРєР»Рё"
                elif days_left_int <= 7:
                    status = "expiring_7"
                    status_label = "РСЃС‚РµРєР°СЋС‚ РґРѕ 7 РґРЅРµР№"
                elif days_left_int <= 30:
                    status = "expiring_30"
                    status_label = "РСЃС‚РµРєР°СЋС‚ РґРѕ 30 РґРЅРµР№"
                else:
                    status = "active"
                    status_label = "РђРєС‚РёРІРЅС‹Рµ"

        rows.append(
            {
                "user_id": user_id,
                "username": username,
                "registration_date": str(record.get("registration_date") or ""),
                "subscriptions": len(subscriptions),
                "locations": " • ".join(locations),
                "nearest_expiration": nearest_expiration_text,
                "days_left": days_left,
                "status": status,
                "status_label": status_label,
                "balance_rub": balance_rub,
                "balance_rub_text": money_text(balance_rub),
                "total_topped_up_rub": total_topped_up_rub,
                "total_topped_up_rub_text": money_text(total_topped_up_rub),
            }
        )
    rows.sort(
        key=lambda item: (
            0 if str(item.get("user_id") or "").isdigit() else 1,
            int(str(item.get("user_id") or "0")) if str(item.get("user_id") or "").isdigit() else str(item.get("user_id") or ""),
        )
    )
    return json.dumps(rows, ensure_ascii=False).replace("</", "<\\/")


def live_admin_dashboard_url() -> str:
    if settings.dashboard_intro_enabled:
        return publish_dashboard_loader_file("admin.html")
    return build_dashboard_public_url("admin.html")


def live_root_panel_url() -> str:
    return build_dashboard_public_url("root.html")


def resolve_dashboard_user_id(user_lookup: str) -> str | None:
    cleaned = (user_lookup or "").strip()
    if not cleaned:
        return None
    if re.fullmatch(r"\d{1,20}", cleaned):
        return cleaned
    record = load_latest_record_by_lookup_from_database(cleaned)
    if record:
        resolved = str(record.get("user_id") or "").strip()
        if re.fullmatch(r"\d{1,20}", resolved):
            return resolved
    extracted = extract_user_id(cleaned)
    if extracted and re.fullmatch(r"\d{1,20}", extracted):
        return extracted
    return None


def dashboard_job_snapshot(job: dict[str, object] | None) -> dict[str, object] | None:
    if not job:
        return None
    return {key: value for key, value in job.items() if key != "updated_ts"}


def dashboard_trim_jobs_locked() -> None:
    if len(dashboard_action_jobs) <= DASHBOARD_ACTION_JOBS_LIMIT:
        return
    ordered = sorted(
        dashboard_action_jobs.items(),
        key=lambda item: float(item[1].get("updated_ts") or 0.0),
        reverse=True,
    )
    keep_ids = {job_id for job_id, _ in ordered[:DASHBOARD_ACTION_JOBS_LIMIT]}
    for job_id in list(dashboard_action_jobs.keys()):
        if job_id not in keep_ids:
            dashboard_action_jobs.pop(job_id, None)


def dashboard_create_job(action: str, user_lookup: str, message_text: str) -> dict[str, object]:
    now_iso = datetime.now().isoformat(timespec="seconds")
    now_ts = now_timestamp()
    job_id = uuid.uuid4().hex[:12]
    job = {
        "id": job_id,
        "action": action,
        "status": "queued",
        "created_at": now_iso,
        "updated_at": now_iso,
        "updated_ts": now_ts,
        "user_lookup": user_lookup,
        "resolved_user_id": "",
        "message_text": message_text,
        "result_text": "",
        "error_text": "",
    }
    with dashboard_action_jobs_lock:
        dashboard_action_jobs[job_id] = job
        dashboard_trim_jobs_locked()
    dashboard_log_action_event(
        action=action,
        user_lookup=user_lookup,
        resolved_user_id="",
        status="queued",
    )
    return dashboard_job_snapshot(job) or {"id": job_id}


def dashboard_update_job(job_id: str, **fields: object) -> dict[str, object] | None:
    status_changed_to = ""
    with dashboard_action_jobs_lock:
        job = dashboard_action_jobs.get(job_id)
        if not job:
            return None
        prev_status = str(job.get("status") or "")
        job.update(fields)
        new_status = str(job.get("status") or "")
        if new_status != prev_status:
            status_changed_to = new_status
        job["updated_at"] = datetime.now().isoformat(timespec="seconds")
        job["updated_ts"] = now_timestamp()
        snapshot = dashboard_job_snapshot(job)
    if status_changed_to in {"queued", "running", "done", "failed"} and snapshot:
        dashboard_log_action_event(
            action=str(snapshot.get("action") or ""),
            user_lookup=str(snapshot.get("user_lookup") or ""),
            resolved_user_id=str(snapshot.get("resolved_user_id") or ""),
            status=status_changed_to,
            result_text=str(snapshot.get("result_text") or ""),
            error_text=str(snapshot.get("error_text") or ""),
        )
    return snapshot


def dashboard_get_job(job_id: str) -> dict[str, object] | None:
    with dashboard_action_jobs_lock:
        return dashboard_job_snapshot(dashboard_action_jobs.get(job_id))


def dashboard_log_action_event(
    *,
    action: str,
    user_lookup: str,
    resolved_user_id: str = "",
    status: str = "queued",
    result_text: str = "",
    error_text: str = "",
) -> None:
    try:
        with connect_database() as conn:
            initialize_database(conn)
            conn.execute(
                """
                INSERT INTO action_logs (
                    created_at, action, user_lookup, resolved_user_id, status, result_text, error_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now().isoformat(timespec="seconds"),
                    str(action or "")[:64],
                    str(user_lookup or "")[:64],
                    str(resolved_user_id or "")[:64],
                    str(status or "")[:24],
                    str(result_text or "")[:1200],
                    str(error_text or "")[:1200],
                ),
            )
            conn.commit()
    except Exception:
        logging.exception("Failed to write action_logs")


def dashboard_recent_actions_payload(limit: int = 30) -> dict[str, object]:
    rows_out: list[dict[str, object]] = []
    try:
        with connect_database() as conn:
            initialize_database(conn)
            rows = conn.execute(
                """
                SELECT created_at, action, user_lookup, resolved_user_id, status, result_text, error_text
                FROM action_logs
                ORDER BY id DESC
                LIMIT ?
                """,
                (max(1, min(int(limit), 200)),),
            ).fetchall()
        for row in rows:
            rows_out.append(
                {
                    "created_at": str(row["created_at"] or ""),
                    "action": str(row["action"] or ""),
                    "user_lookup": str(row["user_lookup"] or ""),
                    "resolved_user_id": str(row["resolved_user_id"] or ""),
                    "status": str(row["status"] or ""),
                    "result_text": str(row["result_text"] or ""),
                    "error_text": str(row["error_text"] or ""),
                }
            )
    except Exception:
        logging.exception("Failed to read action_logs")
    return {"generated_at": datetime.now().isoformat(timespec="seconds"), "rows": rows_out}


def dashboard_recent_errors_payload(limit: int = 20) -> dict[str, object]:
    out: list[str] = []
    try:
        log_path = Path(settings.log_file or "userbot.log")
        if not log_path.is_absolute():
            log_path = APP_ROOT / log_path
        if log_path.exists():
            lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            for line in reversed(lines):
                if "ERROR" in line or "Traceback" in line:
                    out.append(line.strip())
                if len(out) >= max(1, min(int(limit), 200)):
                    break
            out.reverse()
    except Exception:
        logging.exception("Failed to read recent errors")
    return {"generated_at": datetime.now().isoformat(timespec="seconds"), "rows": out}


def build_dashboard_operator_request(
    *,
    action_label: str,
    user_lookup: str,
    resolved_user_id: str,
    message_text: str,
) -> str:
    record = load_latest_record_by_lookup_from_database(resolved_user_id or user_lookup)
    card_text = format_user_summary_from_record(record) if record else ""
    lines = [
        f"Р—Р°РґР°С‡Р° РёР· live admin: {action_label}",
        f"Р’СЂРµРјСЏ: {datetime.now().isoformat(timespec='seconds')}",
        f"РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ: {resolved_user_id or user_lookup or '-'}",
        f"Lookup: {user_lookup or '-'}",
    ]
    if message_text.strip():
        lines.extend(("", "РљРѕРјРјРµРЅС‚Р°СЂРёР№:", message_text.strip()))
    if card_text:
        lines.extend(("", "РљР°СЂС‚РѕС‡РєР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ:", card_text))
    return "\n".join(lines)


async def dashboard_execute_job(job_id: str) -> None:
    global active_mail2_cancel_event, active_scan_cancel_event, active_scan_owner_id
    global active_scan_reset_requested, active_scan_action_delay_seconds, active_scan_base_delay_seconds

    job = dashboard_get_job(job_id)
    if not job:
        return

    action = str(job.get("action") or "").strip().casefold()
    user_lookup = str(job.get("user_lookup") or "").strip()
    message_text = str(job.get("message_text") or "").strip()
    dashboard_update_job(job_id, status="running")

    try:
        if action == "user_status":
            resolved_user_id = resolve_dashboard_user_id(user_lookup)
            if not resolved_user_id:
                raise ValueError("РќРµ СѓРґР°Р»РѕСЃСЊ РѕРїСЂРµРґРµР»РёС‚СЊ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.")
            record = load_latest_record_from_database(resolved_user_id)
            if not record:
                raise ValueError("РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РЅР°Р№РґРµРЅ РІ SQL Р±Р°Р·Рµ.")
            result_text = format_user_summary_from_record(record)
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id=resolved_user_id,
                result_text=result_text[:1200],
                error_text="",
            )
            return

        if action == "mail":
            if not message_text:
                raise ValueError("РўРµРєСЃС‚ СЃРѕРѕР±С‰РµРЅРёСЏ РїСѓСЃС‚РѕР№.")
            resolved_user_id = resolve_dashboard_user_id(user_lookup)
            if not resolved_user_id:
                raise ValueError("РќРµ СѓРґР°Р»РѕСЃСЊ РѕРїСЂРµРґРµР»РёС‚СЊ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.")
            result_text = await send_mail_to_user_in_admin_bot(resolved_user_id, message_text)
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id=resolved_user_id,
                result_text=result_text,
                error_text="",
            )
            return

        if action == "broadcast":
            if not message_text:
                raise ValueError("РўРµРєСЃС‚ СЂР°СЃСЃС‹Р»РєРё РїСѓСЃС‚РѕР№.")
            cancel_event = asyncio.Event()
            active_mail2_cancel_event = cancel_event
            try:
                result_text = await send_mail2_to_users_without_subscriptions(
                    message_text,
                    progress_callback=None,
                    cancel_event=cancel_event,
                )
            finally:
                if active_mail2_cancel_event is cancel_event:
                    active_mail2_cancel_event = None
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id="",
                result_text=result_text[:1200],
                error_text="",
            )
            return

        if action == "promo":
            resolved_user_id = resolve_dashboard_user_id(user_lookup)
            if not resolved_user_id:
                raise ValueError("РќРµ СѓРґР°Р»РѕСЃСЊ РѕРїСЂРµРґРµР»РёС‚СЊ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.")
            promo_code = f"{resolved_user_id}nPromo"
            promo_result = await create_promo_code_in_admin_bot(
                resolved_user_id,
                promo_code,
                progress_callback=None,
            )
            mail_text = message_text.strip() or f"Р”Р»СЏ РІР°СЃ СЃРѕР·РґР°РЅ РїСЂРѕРјРѕРєРѕРґ: {promo_code}"
            mail_result = await send_mail_to_user_in_admin_bot(
                resolved_user_id,
                mail_text,
                progress_callback=None,
            )
            result_text = f"{promo_result}\n\nMail:\n{mail_result}"
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id=resolved_user_id,
                result_text=result_text[:1200],
                error_text="",
            )
            return

        if action == "replace_key":
            resolved_user_id = resolve_dashboard_user_id(user_lookup)
            if not resolved_user_id:
                raise ValueError("РќРµ СѓРґР°Р»РѕСЃСЊ РѕРїСЂРµРґРµР»РёС‚СЊ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.")
            final_text = build_dashboard_operator_request(
                action_label="Р—Р°РјРµРЅР° РєР»СЋС‡Р°",
                user_lookup=user_lookup,
                resolved_user_id=resolved_user_id,
                message_text=message_text,
            )
            await send_to_wizard_target(final_text)
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id=resolved_user_id,
                result_text=final_text[:1200],
                error_text="",
            )
            return

        if action == "delete_access":
            resolved_user_id = resolve_dashboard_user_id(user_lookup)
            if not resolved_user_id:
                raise ValueError("РќРµ СѓРґР°Р»РѕСЃСЊ РѕРїСЂРµРґРµР»РёС‚СЊ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.")
            final_text = build_dashboard_operator_request(
                action_label="РЈРґР°Р»РµРЅРёРµ РґРѕСЃС‚СѓРїР°",
                user_lookup=user_lookup,
                resolved_user_id=resolved_user_id,
                message_text=message_text,
            )
            await send_to_wizard_target(final_text)
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id=resolved_user_id,
                result_text=final_text[:1200],
                error_text="",
            )
            return

        if action == "wizard_card":
            resolved_user_id = resolve_dashboard_user_id(user_lookup)
            if not resolved_user_id:
                raise ValueError("РќРµ СѓРґР°Р»РѕСЃСЊ РѕРїСЂРµРґРµР»РёС‚СЊ ID РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.")
            record = load_latest_record_from_database(resolved_user_id)
            if record:
                card_text = format_user_summary_from_record(record)
            else:
                card_text = await find_user_in_admin_bot(
                    resolved_user_id,
                    progress_callback=None,
                    progress_title="Dashboard wizard",
                    progress_steps=WIZARD_STEPS,
                )
            final_text = card_text if not message_text else f"{card_text}\n\nР”РѕРїРѕР»РЅРµРЅРёРµ:\n{message_text}"
            await send_to_wizard_target(final_text)
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id=resolved_user_id,
                result_text=final_text[:1200],
                error_text="",
            )
            return

        if action in {"scan_new", "scan_continue"}:
            if active_scan_cancel_event and not active_scan_cancel_event.is_set():
                raise ValueError("Scan already running. Stop it first or wait for completion.")
            if action == "scan_new":
                clear_scan_checkpoint()
                reset_scan_database()
                clear_scan_outputs()

            active_scan_cancel_event = asyncio.Event()
            active_scan_owner_id = 0
            active_scan_reset_requested = False
            active_scan_base_delay_seconds = max(
                0.05,
                min(settings.scan_action_delay_seconds, settings.scan_turbo_delay_seconds),
            )
            active_scan_action_delay_seconds = active_scan_base_delay_seconds
            try:
                result_text = await scan_all_users_in_admin_bot(
                    progress_callback=None,
                    progress_interval_seconds=max(0.25, env_float("SCAN_PROGRESS_INTERVAL_SECONDS", 0.5)),
                    cancel_event=active_scan_cancel_event,
                )
            finally:
                active_scan_cancel_event = None
                active_scan_owner_id = None
                active_scan_reset_requested = False
                active_scan_action_delay_seconds = settings.scan_action_delay_seconds
                active_scan_base_delay_seconds = settings.scan_action_delay_seconds
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id="",
                result_text=result_text[:1200],
                error_text="",
            )
            return

        if action == "scan_results":
            result_text = build_scan_results_text()
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id="",
                result_text=result_text[:1200],
                error_text="",
            )
            return

        if action == "scan_reset":
            if active_scan_cancel_event and not active_scan_cancel_event.is_set():
                active_scan_reset_requested = True
                active_scan_cancel_event.set()
                clear_scan_checkpoint()
                reset_scan_database()
                result_text = "Scan reset requested. Active scan is stopping, checkpoint and SQL data are cleared."
            else:
                clear_scan_checkpoint()
                reset_scan_database()
                result_text = "Scan checkpoint and SQL data are cleared."
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id="",
                result_text=result_text,
                error_text="",
            )
            return

        if action == "scan_pause":
            if not active_scan_cancel_event or active_scan_cancel_event.is_set():
                result_text = "Scan is not active."
            else:
                active_scan_cancel_event.set()
                result_text = "Scan pause requested."
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id="",
                result_text=result_text,
                error_text="",
            )
            return

        if action == "pause_scan":
            if not active_scan_cancel_event or active_scan_cancel_event.is_set():
                result_text = "Scan СЃРµР№С‡Р°СЃ РЅРµ Р°РєС‚РёРІРµРЅ."
            else:
                active_scan_cancel_event.set()
                result_text = "РџР°СѓР·Р° scan РїРѕСЃС‚Р°РІР»РµРЅР° РёР· admin panel."
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id="",
                result_text=result_text,
                error_text="",
            )
            return

        if action == "stop_mail2":
            if not active_mail2_cancel_event or active_mail2_cancel_event.is_set():
                result_text = "Mail2 СЃРµР№С‡Р°СЃ РЅРµ Р°РєС‚РёРІРµРЅ."
            else:
                active_mail2_cancel_event.set()
                result_text = "Mail2 РѕСЃС‚Р°РЅРѕРІРєР° Р·Р°РїСЂРѕС€РµРЅР° РёР· admin panel."
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id="",
                result_text=result_text,
                error_text="",
            )
            return

        if action == "wizard_text":
            if not message_text:
                raise ValueError("РўРµРєСЃС‚ РґР»СЏ wizard РїСѓСЃС‚РѕР№.")
            await send_to_wizard_target(message_text)
            dashboard_update_job(
                job_id,
                status="done",
                resolved_user_id="",
                result_text=message_text[:1200],
                error_text="",
            )
            return

        raise ValueError(f"РќРµРёР·РІРµСЃС‚РЅРѕРµ РґРµР№СЃС‚РІРёРµ: {action}")
    except Exception as error:
        logging.exception("Dashboard action failed job_id=%s action=%s", job_id, action)
        dashboard_update_job(
            job_id,
            status="failed",
            error_text=str(error)[:600],
        )

def dashboard_start_job(action: str, user_lookup: str, message_text: str) -> dict[str, object]:
    job = dashboard_create_job(action, user_lookup, message_text)
    job_id = str(job.get("id") or "")
    future = asyncio.run_coroutine_threadsafe(dashboard_execute_job(job_id), loop)

    def _on_done(done_future) -> None:
        try:
            done_future.result()
        except Exception:
            logging.exception("Unhandled dashboard action exception job_id=%s", job_id)

    future.add_done_callback(_on_done)
    return dashboard_get_job(job_id) or job


class DashboardRequestHandler(BaseHTTPRequestHandler):
    server_version = "VPNKBRDashboard/1.0"

    def log_message(self, format: str, *args) -> None:
        logging.info("Dashboard HTTP: " + format, *args)

    def do_GET(self) -> None:
        parts = self.resolve_public_parts()
        if parts is None:
            return
        if self.try_serve_api(parts, send_body=True):
            return
        self.serve_dashboard(parts=parts, send_body=True)

    def do_HEAD(self) -> None:
        parts = self.resolve_public_parts()
        if parts is None:
            return
        if self.try_serve_api(parts, send_body=False):
            return
        self.serve_dashboard(parts=parts, send_body=False)

    def do_POST(self) -> None:
        parts = self.resolve_public_parts()
        if parts is None:
            return
        if not self.try_serve_api(parts, send_body=True):
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def resolve_public_parts(self) -> list[str] | None:
        path = unquote(urlsplit(self.path).path)
        parts = [part for part in path.split("/") if part]
        prefix = settings.dashboard_public_path_prefix.strip("/")
        if not parts or parts[0] != prefix:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return None

        parts = parts[1:]
        if settings.dashboard_public_token:
            if not parts or parts[0] != settings.dashboard_public_token:
                self.send_error(HTTPStatus.NOT_FOUND, "Not found")
                return None
            parts = parts[1:]
        return parts

    def send_json(self, payload: dict[str, object], status: HTTPStatus = HTTPStatus.OK, *, send_body: bool = True) -> None:
        safe_payload = sanitize_outgoing_payload(payload)
        raw = json.dumps(safe_payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if send_body:
            self.wfile.write(raw)

    def read_json_body(self) -> dict[str, object]:
        raw_length = self.headers.get("Content-Length", "0")
        try:
            body_length = max(0, min(int(raw_length), 200_000))
        except ValueError:
            body_length = 0
        raw = self.rfile.read(body_length) if body_length else b""
        if not raw:
            return {}
        payload = json.loads(raw.decode("utf-8", errors="replace"))
        if isinstance(payload, dict):
            return payload
        raise ValueError("JSON body must be object")

    def try_serve_api(self, parts: list[str], *, send_body: bool) -> bool:
        if not parts or parts[0] not in {"admin-api", "root-api"}:
            return False

        api_name = parts[0]
        api_parts = parts[1:]
        if self.command in {"GET", "HEAD"}:
            if len(api_parts) == 2 and api_parts[0] == "job":
                job_id = str(api_parts[1]).strip()
                if not re.fullmatch(r"[a-f0-9]{6,32}", job_id):
                    self.send_json({"ok": False, "error": "bad_job_id"}, HTTPStatus.BAD_REQUEST, send_body=send_body)
                    return True
                job = dashboard_get_job(job_id)
                if not job:
                    self.send_json({"ok": False, "error": "job_not_found"}, HTTPStatus.NOT_FOUND, send_body=send_body)
                    return True
                self.send_json({"ok": True, "job": job}, HTTPStatus.OK, send_body=send_body)
                return True
            if len(api_parts) == 1 and api_parts[0] == "ping":
                self.send_json(
                    {"ok": True, "status": "alive", "time": datetime.now().isoformat(timespec="seconds")},
                    HTTPStatus.OK,
                    send_body=send_body,
                )
                return True
            if len(api_parts) == 1 and api_parts[0] == "overview":
                self.send_json(
                    {"ok": True, "overview": dashboard_live_overview_payload()},
                    HTTPStatus.OK,
                    send_body=send_body,
                )
                return True
            if len(api_parts) == 1 and api_parts[0] == "services":
                self.send_json(
                    {"ok": True, "services": dashboard_server_services_payload()},
                    HTTPStatus.OK,
                    send_body=send_body,
                )
                return True
            if api_name == "root-api" and len(api_parts) == 1 and api_parts[0] == "users":
                query_text = str(urlsplit(self.path).query or "")
                query_match = re.search(r"(?:^|&)q=([^&]*)", query_text)
                query_value = unquote(query_match.group(1)) if query_match else ""
                self.send_json(
                    {"ok": True, "payload": dashboard_root_users_payload(query_value)},
                    HTTPStatus.OK,
                    send_body=send_body,
                )
                return True
            if api_name == "root-api" and len(api_parts) == 2 and api_parts[0] == "user":
                lookup = str(api_parts[1] or "").strip()
                detail = dashboard_root_user_detail_payload(lookup)
                if not detail:
                    self.send_json({"ok": False, "error": "user_not_found"}, HTTPStatus.NOT_FOUND, send_body=send_body)
                    return True
                self.send_json({"ok": True, "user": detail}, HTTPStatus.OK, send_body=send_body)
                return True
            if api_name == "root-api" and len(api_parts) == 1 and api_parts[0] == "actions":
                self.send_json(
                    {"ok": True, "payload": dashboard_recent_actions_payload(30)},
                    HTTPStatus.OK,
                    send_body=send_body,
                )
                return True
            if api_name == "root-api" and len(api_parts) == 1 and api_parts[0] == "errors":
                self.send_json(
                    {"ok": True, "payload": dashboard_recent_errors_payload(20)},
                    HTTPStatus.OK,
                    send_body=send_body,
                )
                return True
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return True

        if self.command != "POST":
            self.send_error(HTTPStatus.METHOD_NOT_ALLOWED, "Method not allowed")
            return True
        if api_name == "root-api" and len(api_parts) == 1 and api_parts[0] == "terminal":
            try:
                payload = self.read_json_body()
                command_text = str(payload.get("command") or "").strip()
                result = dashboard_terminal_execute(command_text)
                status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
                self.send_json(result, status, send_body=send_body)
                return True
            except json.JSONDecodeError:
                self.send_json({"ok": False, "error": "bad_json"}, HTTPStatus.BAD_REQUEST, send_body=send_body)
                return True
            except Exception:
                logging.exception("Root terminal API failed")
                self.send_json({"ok": False, "error": "server_error"}, HTTPStatus.INTERNAL_SERVER_ERROR, send_body=send_body)
                return True
        if len(api_parts) != 1 or api_parts[0] != "action":
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return True

        try:
            payload = self.read_json_body()
            action = str(payload.get("action") or "").strip().casefold()
            user_lookup = str(payload.get("user") or "").strip()
            message_text = str(payload.get("message") or "").strip()
            if action not in {
                "user_status",
                "mail",
                "broadcast",
                "promo",
                "replace_key",
                "delete_access",
                "wizard_card",
                "wizard_text",
                "scan_new",
                "scan_continue",
                "scan_results",
                "scan_reset",
                "scan_pause",
                "pause_scan",
                "stop_mail2",
            }:
                self.send_json({"ok": False, "error": "bad_action"}, HTTPStatus.BAD_REQUEST, send_body=send_body)
                return True
            if action in {"user_status", "mail", "promo", "replace_key", "delete_access", "wizard_card"} and not user_lookup:
                self.send_json({"ok": False, "error": "missing_user"}, HTTPStatus.BAD_REQUEST, send_body=send_body)
                return True
            if action in {"mail", "broadcast", "wizard_text"} and not message_text:
                self.send_json({"ok": False, "error": "missing_message"}, HTTPStatus.BAD_REQUEST, send_body=send_body)
                return True
            job = dashboard_start_job(action, user_lookup, message_text)
            self.send_json({"ok": True, "job": job}, HTTPStatus.ACCEPTED, send_body=send_body)
            return True
        except json.JSONDecodeError:
            self.send_json({"ok": False, "error": "bad_json"}, HTTPStatus.BAD_REQUEST, send_body=send_body)
            return True
        except Exception:
            logging.exception("Dashboard action API failed")
            self.send_json({"ok": False, "error": "server_error"}, HTTPStatus.INTERNAL_SERVER_ERROR, send_body=send_body)
            return True

    def serve_dashboard(self, *, parts: list[str], send_body: bool) -> None:
        if len(parts) != 1:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        file_name = parts[0]
        if file_name == "admin.html":
            self.serve_live_admin_dashboard(send_body=send_body)
            return
        if file_name == "root.html":
            self.serve_live_root_panel(send_body=send_body)
            return

        allowed_suffixes = {".html", ".jpg", ".jpeg", ".png", ".webp", ".gif", ".svg", ".css", ".js"}
        suffix = Path(file_name).suffix.casefold()
        if "/" in file_name or "\\" in file_name or suffix not in allowed_suffixes:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        public_dir = dashboard_public_dir().resolve()
        file_path = (public_dir / file_name).resolve()
        if public_dir not in file_path.parents or not file_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        content = file_path.read_bytes()
        if suffix == ".html":
            try:
                content = sanitize_outgoing_text(content.decode("utf-8", errors="replace")).encode("utf-8")
            except Exception:
                logging.exception("Failed to sanitize html file %s", file_path)
        self.send_response(HTTPStatus.OK)
        content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        if suffix == ".html":
            content_type = "text/html; charset=utf-8"
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if send_body:
            self.wfile.write(content)

    def serve_live_admin_dashboard(self, *, send_body: bool) -> None:
        try:
            content = sanitize_outgoing_text(build_live_admin_dashboard_html()).encode("utf-8")
        except Exception:
            logging.exception("Failed to build live admin dashboard")
            content = sanitize_outgoing_text(build_dashboard_empty_admin_html("Не удалось собрать живую админ-панель. Подробности записаны в лог.")).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if send_body:
            self.wfile.write(content)

    def serve_live_root_panel(self, *, send_body: bool) -> None:
        try:
            content = sanitize_outgoing_text(build_live_root_panel_html()).encode("utf-8")
        except Exception:
            logging.exception("Failed to build live root panel")
            content = sanitize_outgoing_text(build_dashboard_empty_admin_html("Не удалось собрать root-панель. Подробности записаны в лог.")).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if send_body:
            self.wfile.write(content)


def start_dashboard_http_server() -> None:
    global dashboard_http_server, dashboard_http_thread
    if not settings.dashboard_http_enabled:
        logging.info("Dashboard HTTP server disabled")
        return
    if dashboard_http_server is not None:
        return

    dashboard_public_dir()
    try:
        server = ThreadingHTTPServer(
            (settings.dashboard_http_host, settings.dashboard_http_port),
            DashboardRequestHandler,
        )
    except OSError:
        logging.exception(
            "Failed to start Dashboard HTTP server on %s:%s",
            settings.dashboard_http_host,
            settings.dashboard_http_port,
        )
        return
    thread = threading.Thread(target=server.serve_forever, name="dashboard-http", daemon=True)
    thread.start()
    dashboard_http_server = server
    dashboard_http_thread = thread
    logging.info(
        "Dashboard HTTP server started on %s:%s public_url=%s/%s",
        settings.dashboard_http_host,
        settings.dashboard_http_port,
        settings.dashboard_public_base_url.rstrip("/"),
        settings.dashboard_public_path_prefix.strip("/"),
    )


def connect_database() -> sqlite3.Connection:
    conn = sqlite3.connect(database_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_database_column(conn: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
    columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name not in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def initialize_database(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS scan_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            generated_at TEXT NOT NULL,
            pages_total INTEGER NOT NULL DEFAULT 0,
            users_total INTEGER NOT NULL DEFAULT 0,
            subscriptions_total INTEGER NOT NULL DEFAULT 0,
            stats_json TEXT NOT NULL,
            admin_statistics_json TEXT NOT NULL DEFAULT '{}',
            summary_text TEXT NOT NULL DEFAULT '',
            detailed_text TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            user_id TEXT NOT NULL,
            username TEXT NOT NULL DEFAULT '',
            user_button_text TEXT NOT NULL DEFAULT '',
            user_text TEXT NOT NULL DEFAULT '',
            registration_date TEXT,
            subscriptions_count INTEGER NOT NULL DEFAULT 0,
            parsed_profile_json TEXT NOT NULL DEFAULT '{}',
            raw_json TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES scan_runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            user_db_id INTEGER NOT NULL,
            user_id TEXT NOT NULL,
            subscription_id TEXT NOT NULL,
            button_text TEXT NOT NULL DEFAULT '',
            location TEXT NOT NULL DEFAULT '',
            detail_text TEXT NOT NULL DEFAULT '',
            expires_at TEXT,
            parsed_subscription_json TEXT NOT NULL DEFAULT '{}',
            raw_json TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES scan_runs(id) ON DELETE CASCADE,
            FOREIGN KEY (user_db_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS scan_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            user_id TEXT NOT NULL DEFAULT '',
            happened_at TEXT NOT NULL,
            stage TEXT NOT NULL DEFAULT '',
            error_type TEXT NOT NULL DEFAULT '',
            error_message TEXT NOT NULL DEFAULT '',
            raw_json TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES scan_runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS requesters (
            lookup_key TEXT PRIMARY KEY,
            user_id TEXT NOT NULL DEFAULT '',
            username TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            added_at TEXT NOT NULL,
            added_by TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS latest_users (
            user_id TEXT PRIMARY KEY,
            username TEXT NOT NULL DEFAULT '',
            user_button_text TEXT NOT NULL DEFAULT '',
            user_text TEXT NOT NULL DEFAULT '',
            registration_date TEXT,
            subscriptions_count INTEGER NOT NULL DEFAULT 0,
            parsed_profile_json TEXT NOT NULL DEFAULT '{}',
            raw_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS latest_subscriptions (
            user_id TEXT NOT NULL,
            subscription_id TEXT NOT NULL,
            button_text TEXT NOT NULL DEFAULT '',
            location TEXT NOT NULL DEFAULT '',
            detail_text TEXT NOT NULL DEFAULT '',
            expires_at TEXT,
            parsed_subscription_json TEXT NOT NULL DEFAULT '{}',
            raw_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (user_id, subscription_id),
            FOREIGN KEY (user_id) REFERENCES latest_users(user_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS unresolved_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '',
            reason TEXT NOT NULL DEFAULT '',
            sender_id TEXT NOT NULL DEFAULT '',
            sender_username TEXT NOT NULL DEFAULT '',
            sender_name TEXT NOT NULL DEFAULT '',
            chat_id TEXT NOT NULL DEFAULT '',
            message_id TEXT NOT NULL DEFAULT '',
            question_text TEXT NOT NULL DEFAULT '',
            transcript_text TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'open',
            resolved_at TEXT NOT NULL DEFAULT '',
            resolution_note TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS action_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            action TEXT NOT NULL DEFAULT '',
            user_lookup TEXT NOT NULL DEFAULT '',
            resolved_user_id TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            result_text TEXT NOT NULL DEFAULT '',
            error_text TEXT NOT NULL DEFAULT ''
        );

        CREATE INDEX IF NOT EXISTS idx_users_run_user_id ON users(run_id, user_id);
        CREATE INDEX IF NOT EXISTS idx_subscriptions_run_user_id ON subscriptions(run_id, user_id);
        CREATE INDEX IF NOT EXISTS idx_subscriptions_expires_at ON subscriptions(expires_at);
        CREATE INDEX IF NOT EXISTS idx_scan_errors_run_user_id ON scan_errors(run_id, user_id);
        CREATE INDEX IF NOT EXISTS idx_requesters_user_id ON requesters(user_id);
        CREATE INDEX IF NOT EXISTS idx_requesters_username ON requesters(username);
        CREATE INDEX IF NOT EXISTS idx_latest_users_updated_at ON latest_users(updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_latest_users_registration_date ON latest_users(registration_date);
        CREATE INDEX IF NOT EXISTS idx_latest_subscriptions_user_id ON latest_subscriptions(user_id);
        CREATE INDEX IF NOT EXISTS idx_latest_subscriptions_expires_at ON latest_subscriptions(expires_at);
        CREATE INDEX IF NOT EXISTS idx_unresolved_requests_status_created_at ON unresolved_requests(status, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_unresolved_requests_sender_id ON unresolved_requests(sender_id);
        CREATE INDEX IF NOT EXISTS idx_action_logs_created_at ON action_logs(created_at DESC);
        """
    )
    ensure_database_column(conn, "users", "username", "TEXT NOT NULL DEFAULT ''")
    ensure_database_column(conn, "latest_users", "username", "TEXT NOT NULL DEFAULT ''")
    ensure_database_column(conn, "users", "parsed_profile_json", "TEXT NOT NULL DEFAULT '{}'")
    ensure_database_column(conn, "subscriptions", "parsed_subscription_json", "TEXT NOT NULL DEFAULT '{}'")
    ensure_database_column(conn, "latest_users", "parsed_profile_json", "TEXT NOT NULL DEFAULT '{}'")
    ensure_database_column(conn, "latest_subscriptions", "parsed_subscription_json", "TEXT NOT NULL DEFAULT '{}'")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_latest_users_username ON latest_users(username)")
    conn.commit()


def reset_database(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS latest_subscriptions;
        DROP TABLE IF EXISTS latest_users;
        DROP TABLE IF EXISTS subscriptions;
        DROP TABLE IF EXISTS scan_errors;
        DROP TABLE IF EXISTS action_logs;
        DROP TABLE IF EXISTS users;
        DROP TABLE IF EXISTS scan_runs;
        """
    )
    conn.commit()
    initialize_database(conn)


def reset_scan_database() -> None:
    with connect_database() as conn:
        reset_database(conn)


def clear_scan_outputs() -> None:
    report_dir = reports_dir()
    patterns = ("scan-*.txt", "scan-*.json", "scan-*-dashboard.html", "latest-scan-dashboard.html")
    for pattern in patterns:
        for path in report_dir.glob(pattern):
            try:
                path.unlink()
            except OSError:
                logging.exception("Failed to remove scan output: %s", path)
    public_dir = dashboard_public_dir()
    for pattern in ("scan-*.html", "latest-scan-dashboard.html", "latest-scan-dashboard-loader.html"):
        for path in public_dir.glob(pattern):
            try:
                path.unlink()
            except OSError:
                logging.exception("Failed to remove public dashboard output: %s", path)


def ensure_database_file() -> None:
    with connect_database() as conn:
        initialize_database(conn)
        seed_latest_records_from_scan_runs(conn)


def requester_key_for_id(user_id: str | int) -> str:
    return f"id:{str(user_id).strip()}"


def requester_key_for_username(username: str) -> str:
    return f"username:{normalize_username(username)}"


def requester_count() -> int:
    with connect_database() as conn:
        initialize_database(conn)
        return int(conn.execute("SELECT COUNT(*) FROM requesters").fetchone()[0])


def sender_username(sender) -> str:
    return normalize_username(str(getattr(sender, "username", "") or ""))


def sender_full_name(sender) -> str:
    return " ".join(
        part
        for part in (
            str(getattr(sender, "first_name", "") or "").strip(),
            str(getattr(sender, "last_name", "") or "").strip(),
        )
        if part
    ).strip()


def save_unresolved_request(
    *,
    sender_id: str = "",
    sender_username_value: str = "",
    sender_name: str = "",
    chat_id: str = "",
    message_id: str = "",
    source: str = "",
    reason: str = "",
    question_text: str = "",
    transcript_text: str = "",
    payload: dict | None = None,
) -> int:
    created_at = datetime.now().isoformat(timespec="seconds")
    with connect_database() as conn:
        initialize_database(conn)
        cursor = conn.execute(
            """
            INSERT INTO unresolved_requests (
                created_at,
                source,
                reason,
                sender_id,
                sender_username,
                sender_name,
                chat_id,
                message_id,
                question_text,
                transcript_text,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                str(source or "").strip(),
                str(reason or "").strip(),
                str(sender_id or "").strip(),
                str(sender_username_value or "").strip(),
                str(sender_name or "").strip(),
                str(chat_id or "").strip(),
                str(message_id or "").strip(),
                str(question_text or "").strip(),
                str(transcript_text or "").strip(),
                json.dumps(payload or {}, ensure_ascii=False),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid or 0)


def save_unresolved_from_event(
    event,
    sender,
    *,
    source: str,
    reason: str,
    question_text: str = "",
    transcript_text: str = "",
    payload: dict | None = None,
) -> int:
    return save_unresolved_request(
        sender_id=str(getattr(sender, "id", "") or "").strip(),
        sender_username_value=sender_username(sender),
        sender_name=sender_full_name(sender),
        chat_id=str(getattr(event, "chat_id", "") or "").strip(),
        message_id=str(getattr(getattr(event, "message", None), "id", "") or "").strip(),
        source=source,
        reason=reason,
        question_text=question_text,
        transcript_text=transcript_text,
        payload=payload,
    )


def unresolved_requests_count(*, status: str = "open") -> int:
    with connect_database() as conn:
        initialize_database(conn)
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM unresolved_requests WHERE status = ?",
                (str(status or "open").strip(),),
            ).fetchone()[0]
        )


def list_unresolved_requests(*, status: str = "open", limit: int = 15) -> list[sqlite3.Row]:
    with connect_database() as conn:
        initialize_database(conn)
        return conn.execute(
            """
            SELECT id, created_at, source, reason, sender_id, sender_username, question_text, status
            FROM unresolved_requests
            WHERE status = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (str(status or "open").strip(), max(1, min(int(limit), 100))),
        ).fetchall()


def get_unresolved_request(request_id: int) -> sqlite3.Row | None:
    with connect_database() as conn:
        initialize_database(conn)
        return conn.execute(
            """
            SELECT *
            FROM unresolved_requests
            WHERE id = ?
            LIMIT 1
            """,
            (int(request_id),),
        ).fetchone()


def resolve_unresolved_request(request_id: int, note: str = "") -> bool:
    resolved_at = datetime.now().isoformat(timespec="seconds")
    with connect_database() as conn:
        initialize_database(conn)
        cursor = conn.execute(
            """
            UPDATE unresolved_requests
            SET status = 'resolved',
                resolved_at = ?,
                resolution_note = ?
            WHERE id = ?
              AND status <> 'resolved'
            """,
            (resolved_at, str(note or "").strip(), int(request_id)),
        )
        conn.commit()
        return cursor.rowcount > 0


def unresolved_reason_label(reason: str) -> str:
    mapping = {
        "support_escalation": "РїРµСЂРµРґР°РЅРѕ РІ РїРѕРґРґРµСЂР¶РєСѓ",
        "gpt_not_configured": "GPT РЅРµ РЅР°СЃС‚СЂРѕРµРЅ",
        "gpt_rate_limit_timeout": "Р»РёРјРёС‚ GPT Р±РѕР»РµРµ 2 РјРёРЅСѓС‚",
        "gpt_error": "РѕС€РёР±РєР° GPT",
        "voice_transcription_failed": "РЅРµ СѓРґР°Р»РѕСЃСЊ СЂР°СЃРїРѕР·РЅР°С‚СЊ РіРѕР»РѕСЃ",
    }
    cleaned = str(reason or "").strip()
    return mapping.get(cleaned, cleaned or "-")


def build_unresolved_list_text(*, status: str = "open", limit: int = 15) -> str:
    rows = list_unresolved_requests(status=status, limit=limit)
    title = "РќРµСЂР°Р·РѕР±СЂР°РЅРЅС‹Рµ РѕР±СЂР°С‰РµРЅРёСЏ" if status == "open" else "Р Р°Р·РѕР±СЂР°РЅРЅС‹Рµ РѕР±СЂР°С‰РµРЅРёСЏ"
    if not rows:
        return f"{title}\n\nРЎРїРёСЃРѕРє РїСѓСЃС‚."
    lines = [title, ""]
    for row in rows:
        sender_part = str(row["sender_id"] or "-")
        username_value = str(row["sender_username"] or "").strip()
        if username_value:
            sender_part += f" (@{username_value})"
        question_preview = " ".join(str(row["question_text"] or "").split()).strip()
        if len(question_preview) > 80:
            question_preview = question_preview[:77].rstrip() + "..."
        lines.append(
            f"#{int(row['id'])} | {str(row['created_at'] or '-')[:19].replace('T', ' ')} | "
            f"{unresolved_reason_label(str(row['reason'] or ''))} | {sender_part}"
        )
        if question_preview:
            lines.append(f"  {question_preview}")
    lines.append("")
    lines.append("РљРѕРјР°РЅРґС‹: /unresolved <id>, /unresolved done <id> [Р·Р°РјРµС‚РєР°], /unresolved all")
    return "\n".join(lines)


def build_unresolved_detail_text(request_id: int) -> str:
    row = get_unresolved_request(request_id)
    if not row:
        return f"РћР±СЂР°С‰РµРЅРёРµ #{request_id} РЅРµ РЅР°Р№РґРµРЅРѕ."
    lines = [
        f"РћР±СЂР°С‰РµРЅРёРµ #{int(row['id'])}",
        "",
        f"РЎС‚Р°С‚СѓСЃ: {str(row['status'] or '-')}",
        f"РџСЂРёС‡РёРЅР°: {unresolved_reason_label(str(row['reason'] or ''))}",
        f"СЃС‚РѕС‡РЅРёРє: {str(row['source'] or '-')}",
        f"РЎРѕР·РґР°РЅРѕ: {str(row['created_at'] or '-')[:19].replace('T', ' ')}",
        f"Sender ID: {str(row['sender_id'] or '-')}",
        (
            f"Username: @{str(row['sender_username'] or '').strip()}"
            if str(row["sender_username"] or "").strip()
            else "Username: -"
        ),
        f"РјСЏ: {str(row['sender_name'] or '-')}",
        f"Chat: {str(row['chat_id'] or '-')}",
        f"Message: {str(row['message_id'] or '-')}",
        "",
        "РўРµРєСЃС‚ Р·Р°РїСЂРѕСЃР°:",
        str(row["question_text"] or "[РїСѓСЃС‚Рѕ]"),
    ]
    transcript_text = str(row["transcript_text"] or "").strip()
    if transcript_text:
        lines.extend(("", "РўСЂР°РЅСЃРєСЂРёРїС‚:", transcript_text))
    resolved_at = str(row["resolved_at"] or "").strip()
    resolution_note = str(row["resolution_note"] or "").strip()
    if resolved_at or resolution_note:
        lines.extend(
            (
                "",
                f"Р—Р°РєСЂС‹С‚Рѕ: {resolved_at[:19].replace('T', ' ') if resolved_at else '-'}",
                f"Р—Р°РјРµС‚РєР°: {resolution_note or '-'}",
            )
        )
    return "\n".join(lines)


def record_voice_failure(event, sender, question_text: str, *, sender_id: int) -> None:
    try:
        save_unresolved_from_event(
            event,
            sender,
            source="voice",
            reason="voice_transcription_failed",
            question_text=question_text,
        )
    except Exception:
        logging.exception("Failed to save unresolved voice request sender_id=%s", sender_id)


def dashboard_process_snapshot() -> dict[str, object]:
    prune_expired_pending_requests()
    checkpoint = load_scan_checkpoint()
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "admin_flow": active_admin_flow_text(),
        "admin_bot": format_admin_bot_health(),
        "scan_active": bool(active_scan_cancel_event and not active_scan_cancel_event.is_set()),
        "scan_owner_id": active_scan_owner_id or "",
        "scan_checkpoint": format_scan_checkpoint_text(),
        "scan_delay_seconds": round(float(active_scan_action_delay_seconds), 2),
        "scan_auto_resume": bool(active_scan_auto_resume_task and not active_scan_auto_resume_task.done()),
        "scan_next_user_id": int(checkpoint.get("next_user_id") or 0) if checkpoint else 0,
        "scan_total_users_hint": int(checkpoint.get("total_users_hint") or 0) if checkpoint else 0,
        "mail2_active": bool(active_mail2_cancel_event and not active_mail2_cancel_event.is_set()),
        "wizard_pending": len(pending_wizard_requests),
        "mail2_pending": len(pending_mail2_requests),
        "gpt_active": len(active_gpt_requests),
        "gpt_pending": len(pending_gpt_requests),
        "smart_pending": len(pending_smart_actions),
        "pending_ttl_seconds": int(PENDING_REQUEST_TTL_SECONDS),
    }


def dashboard_unresolved_rows(limit: int = 25) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in list_unresolved_requests(status="open", limit=limit):
        preview = " ".join(str(row["question_text"] or "").split()).strip()
        if len(preview) > 160:
            preview = preview[:157].rstrip() + "..."
        rows.append(
            {
                "id": int(row["id"]),
                "created_at": str(row["created_at"] or "")[:19].replace("T", " "),
                "source": str(row["source"] or "").strip(),
                "reason": str(row["reason"] or "").strip(),
                "reason_label": unresolved_reason_label(str(row["reason"] or "")),
                "sender_id": str(row["sender_id"] or "").strip(),
                "sender_username": str(row["sender_username"] or "").strip(),
                "question_preview": preview,
                "status": str(row["status"] or "").strip() or "open",
            }
        )
    return rows


def dashboard_live_overview_payload() -> dict[str, object]:
    version = collect_runtime_version_info()
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "version": {
            "version": str(version.get("version") or ""),
            "commit_short": str(version.get("commit_short") or ""),
            "started_at": str(version.get("started_at") or ""),
        },
        "processes": dashboard_process_snapshot(),
        "unresolved_open_count": unresolved_requests_count(status="open"),
        "unresolved_rows": dashboard_unresolved_rows(limit=25),
    }


async def handle_unresolved_command_event(event, unresolved_command: tuple[str, int | None, str]) -> bool:
    action, request_id, note = unresolved_command
    if action == "list":
        await safe_event_reply(event, build_unresolved_list_text(status="open", limit=15))
        return True
    if action == "all":
        await safe_event_reply(event, build_unresolved_list_text(status="resolved", limit=15))
        return True
    if action == "view" and request_id is not None:
        await safe_event_reply(event, build_unresolved_detail_text(request_id))
        return True
    if action == "resolve" and request_id is not None:
        resolved = resolve_unresolved_request(request_id, note)
        if resolved:
            await safe_event_reply(
                event,
                f"РћР±СЂР°С‰РµРЅРёРµ #{request_id} РѕС‚РјРµС‡РµРЅРѕ РєР°Рє СЂР°Р·РѕР±СЂР°РЅРЅРѕРµ." + (f"\nР—Р°РјРµС‚РєР°: {note}" if note else ""),
            )
        else:
            await safe_event_reply(
                event,
                f"РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РєСЂС‹С‚СЊ РѕР±СЂР°С‰РµРЅРёРµ #{request_id}. Р’РѕР·РјРѕР¶РЅРѕ, РѕРЅРѕ СѓР¶Рµ Р·Р°РєСЂС‹С‚Рѕ РёР»Рё РЅРµ РЅР°Р№РґРµРЅРѕ.",
            )
        return True
    return False


def upsert_requester(
    lookup: str,
    *,
    username: str = "",
    note: str = "",
    added_by: str = "",
) -> str:
    cleaned = (lookup or "").strip()
    normalized_username = normalize_username(username)
    if cleaned.casefold() == "me":
        raise ValueError("me must be resolved before upsert_requester")

    user_id = ""
    lookup_key = ""
    if re.fullmatch(r"\d{1,20}", cleaned):
        user_id = cleaned
        lookup_key = requester_key_for_id(user_id)
    else:
        normalized_username = normalize_username(cleaned)
        if not normalized_username:
            raise ValueError("Use numeric user_id, @username, or me")
        lookup_key = requester_key_for_username(normalized_username)

    with connect_database() as conn:
        initialize_database(conn)
        conn.execute(
            """
            INSERT INTO requesters (lookup_key, user_id, username, note, added_at, added_by)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(lookup_key) DO UPDATE SET
                user_id=excluded.user_id,
                username=excluded.username,
                note=excluded.note,
                added_at=excluded.added_at,
                added_by=excluded.added_by
            """,
            (
                lookup_key,
                user_id,
                normalized_username,
                note.strip(),
                datetime.now().isoformat(timespec="seconds"),
                added_by,
            ),
        )
        conn.commit()
    return lookup_key


def delete_requester(lookup: str) -> bool:
    cleaned = (lookup or "").strip()
    if re.fullmatch(r"\d{1,20}", cleaned):
        keys = [requester_key_for_id(cleaned)]
    else:
        username = normalize_username(cleaned)
        if not username:
            return False
        keys = [requester_key_for_username(username)]

    with connect_database() as conn:
        initialize_database(conn)
        cursor = conn.execute(
            "DELETE FROM requesters WHERE lookup_key IN ({})".format(",".join("?" for _ in keys)),
            keys,
        )
        conn.commit()
        return cursor.rowcount > 0


def load_requesters() -> list[sqlite3.Row]:
    with connect_database() as conn:
        initialize_database(conn)
        return list(
            conn.execute(
                """
                SELECT lookup_key, user_id, username, note, added_at, added_by
                FROM requesters
                ORDER BY added_at, lookup_key
                """
            ).fetchall()
        )


def is_requester_allowed(sender_id: int, sender) -> bool:
    username = sender_username(sender)
    keys = [requester_key_for_id(sender_id)]
    if username:
        keys.append(requester_key_for_username(username))

    with connect_database() as conn:
        initialize_database(conn)
        row = conn.execute(
            "SELECT 1 FROM requesters WHERE lookup_key IN ({}) LIMIT 1".format(",".join("?" for _ in keys)),
            keys,
        ).fetchone()
    if row:
        return True

    env_items = tuple(str(item or "").strip() for item in settings.root_requester_ids if str(item or "").strip())
    env_id_match = any(str(sender_id) == item for item in env_items)
    env_username_match = bool(username) and any(
        username == normalize_username(item.lstrip("@"))
        for item in env_items
        if not re.fullmatch(r"\d{1,20}", item)
    )
    return env_id_match or env_username_match


def seed_requesters_from_settings() -> None:
    for lookup in settings.root_requester_ids:
        try:
            upsert_requester(lookup, note="seed from ROOT_REQUESTER_IDS", added_by="env")
        except ValueError:
            logging.warning("Invalid ROOT_REQUESTER_IDS item ignored: %r", lookup)


def build_roots_text() -> str:
    rows = load_requesters()
    lines = [
        "РЎРїРёСЃРѕРє Р·Р°РїСЂРѕСЃРЅРёРєРѕРІ",
        "",
        "РўРѕР»СЊРєРѕ СЌС‚Рё Р°РєРєР°СѓРЅС‚С‹ РјРѕРіСѓС‚ РѕС‚РїСЂР°РІР»СЏС‚СЊ РєРѕРјР°РЅРґС‹ СЌС‚РѕРјСѓ Р°РєРєР°СѓРЅС‚Сѓ.",
        "",
    ]
    if not rows:
        lines.extend(
            [
                "РЎРїРёСЃРѕРє РїСѓСЃС‚.",
                "Р§С‚РѕР±С‹ РґРѕР±Р°РІРёС‚СЊ СЃРµР±СЏ: /roots add me",
                "Р§С‚РѕР±С‹ РґРѕР±Р°РІРёС‚СЊ РґСЂСѓРіРѕРіРѕ: /roots add 123456789 РєРѕРјРјРµРЅС‚Р°СЂРёР№",
                "РњРѕР¶РЅРѕ РґРѕР±Р°РІРёС‚СЊ username: /roots add @username РєРѕРјРјРµРЅС‚Р°СЂРёР№",
            ]
        )
        return "\n".join(lines)

    for index, row in enumerate(rows, start=1):
        identity = row["user_id"] or (f"@{row['username']}" if row["username"] else row["lookup_key"])
        note = f" - {row['note']}" if row["note"] else ""
        lines.append(f"{index}. {identity}{note}")
    lines.extend(
        [
            "",
            "РљРѕРјР°РЅРґС‹:",
            "/roots add me",
            "/roots add <user_id|@username> [РєРѕРјРјРµРЅС‚Р°СЂРёР№]",
            "/roots del <user_id|@username>",
            "/roots clear",
        ]
    )
    return "\n".join(lines)


def build_roots_buttons():
    return [
        [Button.text("/roots"), Button.text("/roots add me")],
        [Button.text("/roots del 123456789"), Button.text("menu")],
    ]


def seed_latest_records_from_scan_runs(conn: sqlite3.Connection) -> None:
    latest_count = int(conn.execute("SELECT COUNT(*) FROM latest_users").fetchone()[0])
    records: list[dict] = []
    source_name = ""

    def accept_records(candidate_records: list[dict], candidate_source: str) -> None:
        nonlocal records, source_name
        if len(candidate_records) > len(records):
            records = candidate_records
            source_name = candidate_source

    rows = conn.execute(
        "SELECT generated_at, stats_json FROM scan_runs ORDER BY id DESC LIMIT 10"
    ).fetchall()
    for row in rows:
        try:
            stats = json.loads(str(row["stats_json"]))
        except json.JSONDecodeError:
            logging.exception("Failed to read scan_runs stats for SQL seed")
            continue
        accept_records(
            list((stats or {}).get("records") or []),
            f"scan_runs:{row['generated_at']}",
        )

    report_dir = Path(settings.report_dir)
    if report_dir.exists():
        for json_path in report_dir.glob("scan-*.json"):
            try:
                file_stats = json.loads(json_path.read_text(encoding="utf-8"))
            except Exception:
                logging.exception("Failed to read scan JSON for SQL seed: %s", json_path)
                continue
            accept_records(list((file_stats or {}).get("records") or []), str(json_path))

    if not records:
        return
    if latest_count >= len(records):
        return

    conn.execute("DELETE FROM latest_subscriptions")
    conn.execute("DELETE FROM latest_users")
    observed_at = datetime.now().isoformat(timespec="seconds")
    for record in records:
        upsert_latest_record_with_conn(conn, record, observed_at=observed_at)
    conn.commit()
    logging.info(
        "Seeded latest SQL records from %s: users=%s previous_users=%s",
        source_name or "unknown",
        len(records),
        latest_count,
    )


def save_scan_data_to_database(summary_text: str, detailed_text: str, stats: dict) -> int:
    records = list(stats.get("records") or [])
    with connect_database() as conn:
        initialize_database(conn)
        conn.execute("DELETE FROM scan_runs")
        cursor = conn.execute(
            """
            INSERT INTO scan_runs (
                generated_at,
                pages_total,
                users_total,
                subscriptions_total,
                stats_json,
                admin_statistics_json,
                summary_text,
                detailed_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(stats.get("generated_at") or datetime.now().isoformat(timespec="seconds")),
                int(stats.get("pages_total") or 0),
                int(stats.get("users_total") or len(records)),
                int(stats.get("subscriptions_total") or 0),
                json.dumps(stats, ensure_ascii=False),
                json.dumps(stats.get("admin_statistics") or {}, ensure_ascii=False),
                summary_text,
                detailed_text,
            ),
        )
        run_id = int(cursor.lastrowid)

        for record in records:
            subscriptions = list(record.get("subscriptions") or [])
            username = extract_username_from_record(record)
            parsed_profile = parse_profile_text_features(str(record.get("user_text") or ""))
            user_cursor = conn.execute(
                """
                INSERT INTO users (
                    run_id,
                    user_id,
                    username,
                    user_button_text,
                    user_text,
                    registration_date,
                    subscriptions_count,
                    parsed_profile_json,
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    str(record.get("user_id") or ""),
                    username,
                    str(record.get("user_button_text") or ""),
                    str(record.get("user_text") or ""),
                    record.get("registration_date"),
                    len(subscriptions),
                    json.dumps(parsed_profile, ensure_ascii=False),
                    json.dumps(record, ensure_ascii=False),
                ),
            )
            user_db_id = int(user_cursor.lastrowid)

            for subscription in subscriptions:
                expires_at = extract_expiration_date(str(subscription.get("detail_text") or ""))
                parsed_subscription = parse_subscription_text_features(
                    str(subscription.get("detail_text") or ""),
                    expires_at=expires_at,
                )
                conn.execute(
                    """
                    INSERT INTO subscriptions (
                        run_id,
                        user_db_id,
                        user_id,
                        subscription_id,
                        button_text,
                        location,
                        detail_text,
                        expires_at,
                        parsed_subscription_json,
                        raw_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        user_db_id,
                        str(record.get("user_id") or ""),
                        str(subscription.get("subscription_id") or ""),
                        str(subscription.get("button_text") or ""),
                        str(subscription.get("location") or ""),
                        str(subscription.get("detail_text") or ""),
                        expires_at.strftime("%Y-%m-%d") if expires_at else None,
                        json.dumps(parsed_subscription, ensure_ascii=False),
                        json.dumps(subscription, ensure_ascii=False),
                    ),
                )

        conn.execute("DELETE FROM latest_subscriptions")
        conn.execute("DELETE FROM latest_users")
        observed_at = datetime.now().isoformat(timespec="seconds")
        for record in records:
            upsert_latest_record_with_conn(conn, record, observed_at=observed_at)

        for scan_error in list(stats.get("scan_errors") or []):
            conn.execute(
                """
                INSERT INTO scan_errors (
                    run_id,
                    user_id,
                    happened_at,
                    stage,
                    error_type,
                    error_message,
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    str(scan_error.get("user_id") or ""),
                    str(scan_error.get("happened_at") or datetime.now().isoformat(timespec="seconds")),
                    str(scan_error.get("stage") or ""),
                    str(scan_error.get("error_type") or ""),
                    str(scan_error.get("error_message") or ""),
                    json.dumps(scan_error, ensure_ascii=False),
                ),
            )

        conn.commit()
        return run_id


def load_latest_scan_stats_from_database() -> dict | None:
    with connect_database() as conn:
        initialize_database(conn)
        row = conn.execute(
            "SELECT stats_json FROM scan_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row:
            try:
                data = json.loads(str(row["stats_json"]))
                if isinstance(data, dict):
                    return data
            except json.JSONDecodeError:
                logging.exception("Failed to parse latest scan stats from database")

        records = load_latest_records_from_database()
        if not records:
            return None
        _, fallback_stats = build_scan_report(records, pages_total=0, admin_statistics={})
        fallback_stats["generated_at"] = datetime.now().isoformat(timespec="seconds")
        fallback_stats["database"] = {
            "path": str(database_path()),
            "source": "latest_tables_fallback",
        }
        return fallback_stats


def upsert_latest_record_with_conn(conn: sqlite3.Connection, record: dict, *, observed_at: str | None = None) -> None:
    observed_at = observed_at or datetime.now().isoformat(timespec="seconds")
    user_id = str(record.get("user_id") or "").strip()
    if not user_id:
        return

    subscriptions = list(record.get("subscriptions") or [])
    username = extract_username_from_record(record)
    parsed_profile = parse_profile_text_features(str(record.get("user_text") or ""))
    conn.execute(
        """
        INSERT INTO latest_users (
            user_id,
            username,
            user_button_text,
            user_text,
            registration_date,
            subscriptions_count,
            parsed_profile_json,
            raw_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            user_button_text=excluded.user_button_text,
            user_text=excluded.user_text,
            registration_date=excluded.registration_date,
            subscriptions_count=excluded.subscriptions_count,
            parsed_profile_json=excluded.parsed_profile_json,
            raw_json=excluded.raw_json,
            updated_at=excluded.updated_at
        """,
        (
            user_id,
            username,
            str(record.get("user_button_text") or ""),
            str(record.get("user_text") or ""),
            record.get("registration_date"),
            len(subscriptions),
            json.dumps(parsed_profile, ensure_ascii=False),
            json.dumps(record, ensure_ascii=False),
            observed_at,
        ),
    )

    conn.execute("DELETE FROM latest_subscriptions WHERE user_id = ?", (user_id,))
    for subscription in subscriptions:
        expires_at = extract_expiration_date(str(subscription.get("detail_text") or ""))
        parsed_subscription = parse_subscription_text_features(
            str(subscription.get("detail_text") or ""),
            expires_at=expires_at,
        )
        conn.execute(
            """
            INSERT INTO latest_subscriptions (
                user_id,
                subscription_id,
                button_text,
                location,
                detail_text,
                expires_at,
                parsed_subscription_json,
                raw_json,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                str(subscription.get("subscription_id") or ""),
                str(subscription.get("button_text") or ""),
                str(subscription.get("location") or ""),
                str(subscription.get("detail_text") or ""),
                expires_at.strftime("%Y-%m-%d") if expires_at else None,
                json.dumps(parsed_subscription, ensure_ascii=False),
                json.dumps(subscription, ensure_ascii=False),
                observed_at,
            ),
        )


def upsert_latest_record(record: dict, *, observed_at: str | None = None) -> None:
    with connect_database() as conn:
        initialize_database(conn)
        upsert_latest_record_with_conn(conn, record, observed_at=observed_at)
        conn.commit()


def load_latest_records_from_database() -> list[dict]:
    with connect_database() as conn:
        initialize_database(conn)
        seed_latest_records_from_scan_runs(conn)
        user_rows = conn.execute(
            """
            SELECT user_id, username, user_button_text, user_text, registration_date, parsed_profile_json
            FROM latest_users
            ORDER BY CAST(user_id AS INTEGER)
            """
        ).fetchall()
        if not user_rows:
            return []

        sub_rows = conn.execute(
            """
            SELECT user_id, subscription_id, button_text, location, detail_text, parsed_subscription_json
            FROM latest_subscriptions
            ORDER BY CAST(user_id AS INTEGER), subscription_id
            """
        ).fetchall()

    subs_by_user: dict[str, list[dict]] = {}
    for row in sub_rows:
        user_id = str(row["user_id"])
        subs_by_user.setdefault(user_id, []).append(
            {
                "subscription_id": str(row["subscription_id"] or ""),
                "button_text": str(row["button_text"] or ""),
                "location": str(row["location"] or ""),
                "detail_text": str(row["detail_text"] or ""),
                "parsed": json.loads(str(row["parsed_subscription_json"] or "{}")),
            }
        )

    records: list[dict] = []
    for row in user_rows:
        user_id = str(row["user_id"] or "")
        records.append(
            {
                "user_id": user_id,
                "username": str(row["username"] or ""),
                "user_button_text": str(row["user_button_text"] or ""),
                "user_text": str(row["user_text"] or ""),
                "registration_date": row["registration_date"],
                "parsed_profile": json.loads(str(row["parsed_profile_json"] or "{}")),
                "subscriptions": subs_by_user.get(user_id, []),
            }
        )
    return records


def load_users_without_subscriptions_from_database() -> list[str]:
    with connect_database() as conn:
        initialize_database(conn)
        seed_latest_records_from_scan_runs(conn)
        rows = conn.execute(
            """
            SELECT u.user_id
            FROM latest_users AS u
            LEFT JOIN latest_subscriptions AS s
              ON s.user_id = u.user_id
            WHERE s.user_id IS NULL
            ORDER BY
              CASE WHEN u.user_id GLOB '[0-9]*' THEN 0 ELSE 1 END,
              CASE WHEN u.user_id GLOB '[0-9]*' THEN CAST(u.user_id AS INTEGER) END,
              u.user_id
            """
        ).fetchall()
    return [str(row["user_id"] or "").strip() for row in rows if str(row["user_id"] or "").strip()]


def load_latest_record_from_database_with_conn(conn: sqlite3.Connection, user_id: str) -> dict | None:
    lookup_user_id = str(user_id).strip()
    if not lookup_user_id:
        return None

    row = conn.execute(
        """
        SELECT user_id, username, user_button_text, user_text, registration_date, parsed_profile_json
        FROM latest_users
        WHERE user_id = ?
        """,
        (lookup_user_id,),
    ).fetchone()
    if not row:
        return None
    sub_rows = conn.execute(
        """
        SELECT subscription_id, button_text, location, detail_text, parsed_subscription_json
        FROM latest_subscriptions
        WHERE user_id = ?
        ORDER BY subscription_id
        """,
        (lookup_user_id,),
    ).fetchall()

    return {
        "user_id": str(row["user_id"] or ""),
        "username": str(row["username"] or ""),
        "user_button_text": str(row["user_button_text"] or ""),
        "user_text": str(row["user_text"] or ""),
        "registration_date": row["registration_date"],
        "parsed_profile": json.loads(str(row["parsed_profile_json"] or "{}")),
        "subscriptions": [
            {
                "subscription_id": str(sub_row["subscription_id"] or ""),
                "button_text": str(sub_row["button_text"] or ""),
                "location": str(sub_row["location"] or ""),
                "detail_text": str(sub_row["detail_text"] or ""),
                "parsed": json.loads(str(sub_row["parsed_subscription_json"] or "{}")),
            }
            for sub_row in sub_rows
        ],
    }


def dashboard_server_services_payload() -> dict[str, object]:
    services = ["vol29app", "xray", "ssh", "cron", "nginx"]
    rows: list[dict[str, str]] = []
    for name in services:
        try:
            result = subprocess.run(
                ["systemctl", "is-active", f"{name}.service"],
                capture_output=True,
                text=True,
                timeout=3,
                check=False,
            )
            status = (result.stdout or result.stderr or "unknown").strip().splitlines()[0]
        except Exception:
            status = "error"
        rows.append({"service": name, "status": status})
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "services": rows,
    }


def dashboard_terminal_execute(command: str) -> dict[str, object]:
    raw = str(command or "").strip()
    if not raw:
        return {"ok": False, "error": "empty_command"}
    if len(raw) > 400:
        return {"ok": False, "error": "command_too_long"}
    lowered = raw.casefold()
    banned_tokens = (
        " rm -rf",
        "mkfs",
        "shutdown",
        "reboot",
        "poweroff",
        ":(){",
        "dd if=",
        "> /dev/sd",
        "chmod -R 777 /",
    )
    if any(token in f" {lowered}" for token in banned_tokens):
        return {"ok": False, "error": "command_blocked"}
    started_at = time.monotonic()
    try:
        completed = subprocess.run(
            raw,
            shell=True,
            capture_output=True,
            text=True,
            timeout=20,
            cwd=str(APP_ROOT),
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        elapsed_ms = int((time.monotonic() - started_at) * 1000)
        output = (completed.stdout or "") + ((("\n" + completed.stderr) if completed.stderr else ""))
        output = output.strip()
        if len(output) > 12000:
            output = output[:12000] + "\n... output truncated ..."
        return {
            "ok": True,
            "command": raw,
            "code": int(completed.returncode),
            "elapsed_ms": elapsed_ms,
            "output": output or "(no output)",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }
    except subprocess.TimeoutExpired as exc:
        elapsed_ms = int((time.monotonic() - started_at) * 1000)
        out = ((exc.stdout or "") + ("\n" + (exc.stderr or "") if exc.stderr else "")).strip()
        if len(out) > 4000:
            out = out[:4000] + "\n... output truncated ..."
        return {
            "ok": False,
            "error": "timeout",
            "command": raw,
            "elapsed_ms": elapsed_ms,
            "output": out or "Command timed out after 20 seconds.",
        }
    except Exception as exc:
        return {"ok": False, "error": "exec_failed", "command": raw, "detail": str(exc)}


def dashboard_root_users_payload(query: str = "") -> dict[str, object]:
    records = load_latest_records_from_database()
    try:
        rows = json.loads(admin_user_rows_json(records))
    except Exception:
        rows = []
    q = str(query or "").strip().casefold()
    if q:
        filtered = []
        for row in rows:
            user_id = str(row.get("user_id") or "").casefold()
            username = str(row.get("username") or "").casefold()
            if q in user_id or q in username or q in f"@{username}":
                filtered.append(row)
        rows = filtered
    user_ids = [str(row.get("user_id") or "").strip() for row in rows if str(row.get("user_id") or "").strip()]
    support_map = dashboard_support_summary_by_user_ids(user_ids)
    for row in rows:
        uid = str(row.get("user_id") or "").strip()
        info = support_map.get(uid, {})
        row["requests_count"] = int(info.get("count") or 0)
        row["incoming_count"] = int(info.get("incoming_count") or 0)
        row["wizard_count"] = int(info.get("wizard_count") or 0)
        row["mail_count"] = int(info.get("mail_count") or 0)
        row["last_request_text"] = str(info.get("last_text") or "")
        row["last_request_at"] = str(info.get("last_at") or "")
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "count": len(rows),
        "users": rows[:5000],
    }


def dashboard_root_user_detail_payload(user_lookup: str) -> dict[str, object] | None:
    record = load_latest_record_by_lookup_from_database(user_lookup)
    if not record:
        return None
    try:
        row = json.loads(admin_user_rows_json([record]))[0]
    except Exception:
        row = {"user_id": str(record.get("user_id") or "")}
    uid = str(row.get("user_id") or "").strip()
    info = dashboard_support_summary_by_user_ids([uid]).get(uid, {})
    row["requests_count"] = int(info.get("count") or 0)
    row["incoming_count"] = int(info.get("incoming_count") or 0)
    row["wizard_count"] = int(info.get("wizard_count") or 0)
    row["mail_count"] = int(info.get("mail_count") or 0)
    row["last_request_text"] = str(info.get("last_text") or "")
    row["last_request_at"] = str(info.get("last_at") or "")
    row["raw_record"] = record
    return row


def dashboard_support_summary_by_user_ids(user_ids: list[str]) -> dict[str, dict[str, object]]:
    cleaned_ids = [str(uid or "").strip() for uid in user_ids if str(uid or "").strip()]
    if not cleaned_ids:
        return {}
    placeholders = ",".join("?" for _ in cleaned_ids)
    out: dict[str, dict[str, object]] = {}
    try:
        with connect_database() as conn:
            initialize_database(conn)
            rows = conn.execute(
                f"""
                SELECT sender_id, created_at, question_text, transcript_text
                FROM unresolved_requests
                WHERE sender_id IN ({placeholders})
                ORDER BY datetime(created_at) DESC, id DESC
                """,
                tuple(cleaned_ids),
            ).fetchall()
        for row in rows:
            uid = str(row["sender_id"] or "").strip()
            if not uid:
                continue
            item = out.setdefault(
                uid,
                {
                    "count": 0,
                    "incoming_count": 0,
                    "wizard_count": 0,
                    "mail_count": 0,
                    "last_text": "",
                    "last_at": "",
                },
            )
            item["count"] = int(item.get("count") or 0) + 1
            item["incoming_count"] = int(item.get("incoming_count") or 0) + 1
            if not item["last_at"]:
                item["last_at"] = str(row["created_at"] or "")
                text = str(row["question_text"] or "").strip() or str(row["transcript_text"] or "").strip()
                item["last_text"] = text[:180]

        action_rows = conn.execute(
            f"""
            SELECT resolved_user_id, action, status
            FROM action_logs
            WHERE resolved_user_id IN ({placeholders})
            """,
            tuple(cleaned_ids),
        ).fetchall()
        for row in action_rows:
            uid = str(row["resolved_user_id"] or "").strip()
            if not uid:
                continue
            status = str(row["status"] or "").strip().casefold()
            if status != "done":
                continue
            action = str(row["action"] or "").strip().casefold()
            item = out.setdefault(
                uid,
                {
                    "count": 0,
                    "incoming_count": 0,
                    "wizard_count": 0,
                    "mail_count": 0,
                    "last_text": "",
                    "last_at": "",
                },
            )
            if action in {"wizard_card", "wizard_text", "replace_key", "delete_access"}:
                item["wizard_count"] = int(item.get("wizard_count") or 0) + 1
                item["count"] = int(item.get("count") or 0) + 1
            elif action in {"mail", "broadcast", "promo"}:
                item["mail_count"] = int(item.get("mail_count") or 0) + 1
                item["count"] = int(item.get("count") or 0) + 1
    except Exception:
        logging.exception("Failed to load support summary for users")
    return out


def load_latest_record_from_database(user_id: str) -> dict | None:
    with connect_database() as conn:
        initialize_database(conn)
        seed_latest_records_from_scan_runs(conn)
        return load_latest_record_from_database_with_conn(conn, user_id)


def load_latest_record_by_lookup_from_database(query: str) -> dict | None:
    cleaned = (query or "").strip()
    if not cleaned:
        return None

    if re.fullmatch(r"\d{1,20}", cleaned):
        return load_latest_record_from_database(cleaned)

    username = normalize_username(cleaned)
    if not username:
        return None

    with connect_database() as conn:
        initialize_database(conn)
        seed_latest_records_from_scan_runs(conn)
        row = conn.execute(
            """
            SELECT user_id
            FROM latest_users
            WHERE username = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (username,),
        ).fetchone()

        if row:
            return load_latest_record_from_database_with_conn(conn, str(row["user_id"] or ""))

        row = conn.execute(
            """
            SELECT user_id
            FROM latest_users
            WHERE lower(user_text) LIKE ?
               OR lower(raw_json) LIKE ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (f"%@{username}%", f"%@{username}%"),
        ).fetchone()
        if row:
            return load_latest_record_from_database_with_conn(conn, str(row["user_id"] or ""))

    return None


def analyze_business_status(stats: dict) -> dict:
    records = list(stats.get("records") or [])
    now = datetime.now()
    price = max(0.0, FORECAST_PRICE_PER_SUBSCRIPTION_RUB)
    total_users = len(records) or int(stats.get("users_total") or 0)
    total_subscriptions = int(stats.get("subscriptions_total") or 0)
    paid_users = int(stats.get("users_with_subscriptions_total") or 0)
    forecast = dict(stats.get("forecast") or {})
    existing_financial = dict(forecast.get("financial_projection") or {})
    stats_month_profit = float(existing_financial.get("stats_month_profit_rub") or 0.0)
    estimated_mrr = float(forecast.get("estimated_mrr_rub") or total_subscriptions * price)
    baseline_month_revenue = stats_month_profit if stats_month_profit > 0 else estimated_mrr

    monthly: dict[str, dict[str, int]] = {}
    registration_dates: list[date] = []
    for record in records:
        raw_reg = record.get("registration_date")
        reg_date = extract_expiration_date(str(raw_reg)) if raw_reg else None
        if not reg_date:
            reg_date = extract_registration_date(str(record.get("user_text") or ""))
        if not reg_date:
            continue
        month_key = reg_date.strftime("%Y-%m")
        registration_dates.append(reg_date.date())
        item = monthly.setdefault(month_key, {"users": 0, "paid_users": 0, "subscriptions": 0})
        subscriptions_count = len(record.get("subscriptions") or [])
        item["users"] += 1
        if subscriptions_count:
            item["paid_users"] += 1
        item["subscriptions"] += subscriptions_count

    if registration_dates:
        observation_start = min(registration_dates)
        observation_days = max((now.date() - observation_start).days + 1, 1)
    else:
        observation_start = None
        observation_days = 0

    users_per_day = (total_users / observation_days) if observation_days else 0.0
    paid_users_per_day = (paid_users / observation_days) if observation_days else 0.0
    subscriptions_per_day = (total_subscriptions / observation_days) if observation_days else 0.0
    monthly_growth_rate = min(0.35, max(-0.20, (subscriptions_per_day * 30 / max(total_subscriptions, 1))))
    if monthly_growth_rate == 0 and total_subscriptions:
        monthly_growth_rate = 0.03

    horizons = [1, 3, 6, 9, 12]
    projections = []
    for months in horizons:
        days = months * 30
        projected_users = total_users + users_per_day * days
        projected_paid_users = paid_users + paid_users_per_day * days
        projected_subscriptions = total_subscriptions + subscriptions_per_day * days
        projected_revenue = baseline_month_revenue * ((1 + monthly_growth_rate) ** months)
        projections.append(
            {
                "months": months,
                "users": round(projected_users, 2),
                "paid_users": round(projected_paid_users, 2),
                "subscriptions": round(projected_subscriptions, 2),
                "revenue_rub": round(max(0.0, projected_revenue), 2),
            }
        )

    monthly_rows = [
        {
            "month": month,
            "users": item["users"],
            "paid_users": item["paid_users"],
            "subscriptions": item["subscriptions"],
            "estimated_revenue_rub": round(item["subscriptions"] * price, 2),
        }
        for month, item in sorted(monthly.items())
    ]
    recent_monthly_rows = monthly_rows[-12:]
    best_month = max(monthly_rows, key=lambda item: item["subscriptions"], default=None)

    return {
        "generated_at": now.isoformat(timespec="seconds"),
        "source_scan_generated_at": stats.get("generated_at"),
        "observation_start": observation_start.isoformat() if observation_start else None,
        "observation_days": observation_days,
        "total_users": total_users,
        "paid_users": paid_users,
        "total_subscriptions": total_subscriptions,
        "estimated_mrr_rub": round(estimated_mrr, 2),
        "baseline_month_revenue_rub": round(baseline_month_revenue, 2),
        "price_per_subscription_rub": round(price, 2),
        "users_per_day": round(users_per_day, 4),
        "paid_users_per_day": round(paid_users_per_day, 4),
        "subscriptions_per_day": round(subscriptions_per_day, 4),
        "monthly_growth_rate": round(monthly_growth_rate, 5),
        "registration_months_found": len(monthly_rows),
        "recent_monthly_rows": recent_monthly_rows,
        "best_month": best_month,
        "projections": projections,
        "scan_errors_total": len(stats.get("scan_errors") or []),
    }


def build_status_dashboard_from_database() -> tuple[Path, dict] | None:
    stats = load_latest_scan_stats_from_database()
    records_fallback: list[dict] = []
    try:
        records_fallback = load_latest_records_from_database()
    except Exception:
        logging.exception("Failed to load fallback latest records for status dashboard")
        records_fallback = []

    # Fallback path: no scan_runs row, but latest_* tables already contain data.
    if not stats:
        if not records_fallback:
            return None
        subscriptions_total = sum(len(record.get("subscriptions") or []) for record in records_fallback)
        users_total = len(records_fallback)
        users_with_subscriptions_total = sum(1 for record in records_fallback if record.get("subscriptions"))
        estimated_mrr = round(float(subscriptions_total) * FORECAST_PRICE_PER_SUBSCRIPTION_RUB, 2)
        stats = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "records": records_fallback,
            "users_total": users_total,
            "users_with_subscriptions_total": users_with_subscriptions_total,
            "subscriptions_total": subscriptions_total,
            "scan_errors": [],
            "forecast": {
                "estimated_mrr_rub": estimated_mrr,
                "financial_projection": {
                    "stats_month_profit_rub": estimated_mrr,
                },
            },
        }

    # Heal incomplete stats (for older scan rows without embedded records).
    if not stats.get("records") and records_fallback:
        stats["records"] = records_fallback
    if not int(stats.get("users_total") or 0) and stats.get("records"):
        stats["users_total"] = len(list(stats.get("records") or []))
    if not int(stats.get("users_with_subscriptions_total") or 0) and stats.get("records"):
        stats["users_with_subscriptions_total"] = sum(
            1 for record in list(stats.get("records") or []) if record.get("subscriptions")
        )
    if not int(stats.get("subscriptions_total") or 0) and stats.get("records"):
        stats["subscriptions_total"] = sum(
            len(record.get("subscriptions") or []) for record in list(stats.get("records") or [])
        )

    stats["database"] = {
        "path": str(database_path()),
        "source": "sqlite",
    }
    report_dir = Path(settings.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    dashboard_path = report_dir / f"status-dashboard-{stamp}.html"
    analysis = analyze_business_status(stats)
    stats["business_analysis"] = analysis
    atomic_write_text(dashboard_path, build_scan_dashboard_html(stats))
    public_path, public_url = publish_dashboard_file(dashboard_path, latest_name="latest-status-dashboard.html")
    stats["dashboard_public_path"] = str(public_path)
    stats["dashboard_public_url"] = public_url
    return dashboard_path, stats


def build_status_summary_from_stats(stats: dict, dashboard_path: Path) -> str:
    analysis = dict(stats.get("business_analysis") or analyze_business_status(stats))
    projections = list(analysis.get("projections") or [])
    dashboard_url = str(stats.get("dashboard_public_url") or ensure_dashboard_public_url(dashboard_path, "latest-status-dashboard.html"))
    admin_url = live_admin_dashboard_url()
    lines = [
        "РЎС‚Р°С‚СѓСЃ Р±Р°Р·С‹ Рё Р°РґРјРёРЅ-СЃРёСЃС‚РµРјС‹",
        f"РђРґРјРёРЅ-Р±РѕС‚: {format_admin_bot_health()}",
        f"SQLite: {database_path()}",
        f"Admin system: {admin_url}" if admin_url else f"Admin system: {dashboard_url or dashboard_path}",
        f"Backup dashboard: {dashboard_url or dashboard_path}",
        "",
        f"РџРѕСЃР»РµРґРЅРёР№ scan: {str(stats.get('generated_at') or '-').replace('T', ' ')}",
        f"РџРѕР»СЊР·РѕРІР°С‚РµР»РµР№: {int(analysis.get('total_users') or 0)}",
        f"РџР»Р°С‚СЏС‰РёС…: {int(analysis.get('paid_users') or 0)}",
        f"РџРѕРґРїРёСЃРѕРє: {int(analysis.get('total_subscriptions') or 0)}",
        f"РћС†РµРЅРєР° MRR: {float(analysis.get('estimated_mrr_rub') or 0):.0f} RUB",
        f"Р РѕСЃС‚ РїРѕРґРїРёСЃРѕРє / РјРµСЃСЏС†: {float(analysis.get('monthly_growth_rate') or 0) * 100:.1f}%",
        f"РћС€РёР±РѕРє scan: {int(analysis.get('scan_errors_total') or 0)}",
    ]
    if projections:
        lines.append("")
        lines.append("РџСЂРѕРіРЅРѕР· РґРѕС…РѕРґР°:")
        for item in projections:
            lines.append(
                f"- {int(item['months'])} РјРµСЃ: {float(item['revenue_rub']):.0f} RUB, "
                f"users ~{int(round(float(item['users'])))} / subs ~{int(round(float(item['subscriptions'])))}"
            )
    return "\n".join(lines)


def has_button_text(message, expected_text: str) -> bool:
    expected = expected_text.casefold()
    if not message.buttons:
        return False

    for row in message.buttons:
        for button in row:
            if expected in button.text.casefold():
                return True
    return False


def find_button_by_keywords(
    message,
    required_groups: tuple[tuple[str, ...], ...],
    *,
    optional_keywords: tuple[str, ...] = (),
    exclude_keywords: tuple[str, ...] = (),
) -> dict[str, int | str] | None:
    weighted: list[tuple[int, dict[str, int | str]]] = []
    for button in extract_all_buttons(message):
        text = str(button["text"])
        lowered = text.casefold()
        if any(keyword and keyword.casefold() in lowered for keyword in exclude_keywords):
            continue

        matched_required = True
        score = 0
        for group in required_groups:
            clean_group = tuple(keyword for keyword in group if keyword)
            if not clean_group:
                continue
            if not any(keyword.casefold() in lowered for keyword in clean_group):
                matched_required = False
                break
            score += 20

        if not matched_required:
            continue

        for keyword in optional_keywords:
            if keyword and keyword.casefold() in lowered:
                score += 5
        if is_navigation_button_text(text):
            score -= 50
        weighted.append((score, button))

    if not weighted:
        return None
    weighted.sort(key=lambda item: (item[0], -int(item[1]["row"]), -int(item[1]["column"])), reverse=True)
    return weighted[0][1]


async def click_keyword_button_and_read(
    bot,
    message,
    required_groups: tuple[tuple[str, ...], ...],
    *,
    label: str,
    optional_keywords: tuple[str, ...] = (),
    exclude_keywords: tuple[str, ...] = (),
):
    button = find_button_by_keywords(
        message,
        required_groups,
        optional_keywords=optional_keywords,
        exclude_keywords=exclude_keywords,
    )
    if not button:
        available = [str(item["text"]) for item in extract_all_buttons(message)]
        raise RuntimeError(f"Button for {label!r} not found. Available buttons: {available}")
    return await click_button_position_and_read(
        bot,
        message,
        int(button["row"]),
        int(button["column"]),
        str(button["text"]),
    )


async def click_keyword_button_and_wait_ready(
    bot,
    message,
    required_groups: tuple[tuple[str, ...], ...],
    *,
    label: str,
    ready,
    timeout_seconds: float | None = None,
    optional_keywords: tuple[str, ...] = (),
    exclude_keywords: tuple[str, ...] = (),
):
    button = find_button_by_keywords(
        message,
        required_groups,
        optional_keywords=optional_keywords,
        exclude_keywords=exclude_keywords,
    )
    if not button:
        available = [str(item["text"]) for item in extract_all_buttons(message)]
        raise RuntimeError(f"Button for {label!r} not found. Available buttons: {available}")

    ready_task = asyncio.create_task(
        wait_bot_update(
            bot,
            message_snapshot(message),
            ready=ready,
            timeout_seconds=timeout_seconds,
        )
    )

    async def click_with_retry():
        logging.info(
            "Clicking ready-wait button %r at row=%s column=%s for %s",
            button["text"],
            button["row"],
            button["column"],
            label,
        )
        try:
            result = await message.click(int(button["row"]), int(button["column"]))
            note_success_action()
            return result
        except FloodWaitError as error:
            wait_seconds = int(getattr(error, "seconds", 1) or 1)
            note_floodwait(wait_seconds)
            logging.warning(
                "FloodWait on click_keyword_button_and_wait_ready: waiting %ss and retrying button %r",
                wait_seconds,
                button["text"],
            )
            await asyncio.sleep(wait_seconds + 1)
            result = await message.click(int(button["row"]), int(button["column"]))
            note_success_action()
            return result

    click_task = asyncio.create_task(click_with_retry())
    next_message = await wait_for_click_or_update(click_task, ready_task)
    if POST_ACTION_SETTLE_SECONDS > 0:
        await asyncio.sleep(POST_ACTION_SETTLE_SECONDS)
    log_message(f"After clicking {label!r} and waiting ready", next_message)
    return next_message


async def click_keyword_button_and_settle(
    bot,
    message,
    required_groups: tuple[tuple[str, ...], ...],
    *,
    label: str,
    settle_seconds: float,
    optional_keywords: tuple[str, ...] = (),
    exclude_keywords: tuple[str, ...] = (),
):
    button = find_button_by_keywords(
        message,
        required_groups,
        optional_keywords=optional_keywords,
        exclude_keywords=exclude_keywords,
    )
    if not button:
        available = [str(item["text"]) for item in extract_all_buttons(message)]
        raise RuntimeError(f"Button for {label!r} not found. Available buttons: {available}")

    logging.info(
        "Clicking settle button %r at row=%s column=%s for %s",
        button["text"],
        button["row"],
        button["column"],
        label,
    )
    try:
        await message.click(int(button["row"]), int(button["column"]))
        note_success_action()
    except FloodWaitError as error:
        wait_seconds = int(getattr(error, "seconds", 1) or 1)
        note_floodwait(wait_seconds)
        logging.warning(
            "FloodWait on click_keyword_button_and_settle: waiting %ss and retrying button %r",
            wait_seconds,
            button["text"],
        )
        await asyncio.sleep(wait_seconds + 1)
        await message.click(int(button["row"]), int(button["column"]))
        note_success_action()

    await asyncio.sleep(settle_seconds)
    try:
        next_message = await latest_bot_message(bot)
    except Exception:
        logging.exception("Failed to read latest message after clicking %s; using previous message", label)
        return message
    log_message(f"After clicking {label!r} and settling", next_message)
    return next_message


async def ensure_message_with_keyword_button(
    conv,
    bot,
    message,
    required_groups: tuple[tuple[str, ...], ...],
    *,
    label: str,
    optional_keywords: tuple[str, ...] = (),
    exclude_keywords: tuple[str, ...] = (),
):
    if find_button_by_keywords(
        message,
        required_groups,
        optional_keywords=optional_keywords,
        exclude_keywords=exclude_keywords,
    ):
        return message

    if has_button_text(message, settings.cancel_button_text):
        logging.info("Button for %s not found; clicking cancel before reopening admin menu", label)
        try:
            message = await click_and_read(bot, message, settings.cancel_button_text)
        except Exception:
            logging.exception("Failed to click cancel while recovering admin menu for %s", label)

    message = await send_admin_and_get_menu(conv, bot)
    if find_button_by_keywords(
        message,
        required_groups,
        optional_keywords=optional_keywords,
        exclude_keywords=exclude_keywords,
    ):
        return message
    available = [str(item["text"]) for item in extract_all_buttons(message)]
    raise RuntimeError(f"Admin menu does not contain button for {label!r}. Available buttons: {available}")


async def click_and_read(bot, message, button_text: str, expected_button_text: str | None = None):
    ready = None
    if expected_button_text:
        ready = lambda updated_message: has_button_text(updated_message, expected_button_text)
    update_task = asyncio.create_task(wait_bot_update(bot, message_snapshot(message), ready=ready))
    click_task = asyncio.create_task(click_button_by_text(message, button_text))
    next_message = await wait_for_click_or_update(click_task, update_task)
    if POST_ACTION_SETTLE_SECONDS > 0:
        await asyncio.sleep(POST_ACTION_SETTLE_SECONDS)
    log_message(f"After clicking {button_text!r}", next_message)
    return next_message


async def click_button_position_and_read(
    bot,
    message,
    row_index: int,
    column_index: int,
    label: str,
    expected_button_text: str | None = None,
):
    ready = None
    if expected_button_text:
        ready = lambda updated_message: has_button_text(updated_message, expected_button_text)
    update_task = asyncio.create_task(wait_bot_update(bot, message_snapshot(message), ready=ready))
    logging.info("Clicking button %r at row=%s column=%s", label, row_index, column_index)
    async def click_with_retry():
        try:
            result = await message.click(row_index, column_index)
            note_success_action()
            return result
        except FloodWaitError as error:
            wait_seconds = int(getattr(error, "seconds", 1) or 1)
            note_floodwait(wait_seconds)
            logging.warning(
                "FloodWait on click_button_position_and_read: waiting %ss and retrying button %r",
                wait_seconds,
                label,
            )
            await asyncio.sleep(wait_seconds + 1)
            result = await message.click(row_index, column_index)
            note_success_action()
            return result

    click_task = asyncio.create_task(click_with_retry())
    next_message = await wait_for_click_or_update(click_task, update_task)
    if POST_ACTION_SETTLE_SECONDS > 0:
        await asyncio.sleep(POST_ACTION_SETTLE_SECONDS)
    log_message(f"After clicking {label!r}", next_message)
    return next_message


async def send_admin_and_get_menu(conv, bot):
    logging.info("Sending admin command: %s", settings.admin_command)
    try:
        previous_snapshot = message_snapshot(await latest_bot_message(bot))
    except Exception:
        previous_snapshot = None
    await send_conv_message_with_retry(bot, settings.admin_command)
    try:
        admin_message = await wait_bot_update(bot, previous_snapshot)
    except ValueError as error:
        if "too many incoming messages" in str(error).casefold():
            logging.warning(
                "Conversation overflow after %s; refreshing state from latest admin message.",
                settings.admin_command,
            )
            admin_message = await latest_bot_message(bot, limit=40)
        else:
            raise
    except TimeoutError:
        logging.warning("Admin command produced no visible update; using latest admin bot message")
        admin_message = await latest_bot_message(bot)
        if is_intermediate_message(admin_message):
            raise
    log_message("Admin response", admin_message)
    return admin_message


async def send_conv_message_with_retry(bot, payload):
    try:
        await client.send_message(bot, payload)
        note_success_action()
    except ValueError as error:
        if "too many incoming messages" in str(error).casefold():
            logging.warning("Ignored stale conversation overflow while sending admin payload directly")
            await client.send_message(bot, payload)
            note_success_action()
            return
        raise
    except FloodWaitError as error:
        wait_seconds = int(getattr(error, "seconds", 1) or 1)
        note_floodwait(wait_seconds)
        logging.warning("FloodWait on client.send_message: waiting %ss before retry", wait_seconds)
        await asyncio.sleep(wait_seconds + 1)
        await client.send_message(bot, payload)
        note_success_action()


async def reset_admin_state_if_needed(conv, bot, message):
    if is_users_page_message(message):
        return message

    if has_button_text(message, settings.cancel_button_text):
        update_task = asyncio.create_task(wait_bot_update(bot, message_snapshot(message)))
        click_task = asyncio.create_task(click_button_by_text(message, settings.cancel_button_text))
        reset_message = await wait_for_click_or_update(click_task, update_task)
        log_message("After cancel", reset_message)
        return reset_message
    else:
        logging.info("Cancel button %r not found; resending admin command anyway.", settings.cancel_button_text)

    return await send_admin_and_get_menu(conv, bot)


async def open_user_in_admin_bot(
    conv,
    bot,
    user_id: str,
    progress_callback: ProgressCallback | None = None,
    progress_title: str = "РџРѕРёСЃРє РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
    progress_steps: list[str] | None = None,
):
    steps = progress_steps or SEARCH_STEPS
    await emit_process_progress(
        progress_callback,
        progress_title,
        steps,
        1,
        user_id=user_id,
        extra_lines=[f"РђРґРјРёРЅ-Р±РѕС‚: @{settings.admin_bot_username}", f"РљРѕРјР°РЅРґР°: {settings.admin_command}"],
    )
    admin_message = await send_admin_and_get_menu(conv, bot)
    admin_message = await reset_admin_state_if_needed(conv, bot, admin_message)

    await emit_process_progress(
        progress_callback,
        progress_title,
        steps,
        2,
        user_id=user_id,
        extra_lines=[f"РљРЅРѕРїРєР° СЂР°Р·РґРµР»Р°: {settings.users_button_text}"],
    )
    users_message = await click_and_read(
        bot,
        admin_message,
        settings.users_button_text,
        expected_button_text=settings.find_user_button_text,
    )

    await emit_process_progress(
        progress_callback,
        progress_title,
        steps,
        3,
        user_id=user_id,
        extra_lines=[f"РљРЅРѕРїРєР° РїРѕРёСЃРєР°: {settings.find_user_button_text}", f"РћС‚РїСЂР°РІР»СЏСЋ ID: {user_id}"],
    )
    find_message = await click_and_read(bot, users_message, settings.find_user_button_text)

    logging.info("Sending searched user_id=%s", user_id)
    previous_snapshot = message_snapshot(find_message)
    await send_conv_message_with_retry(bot, user_id)
    result_message = await wait_bot_update(bot, previous_snapshot)
    log_message("Search result", result_message)
    return result_message


async def find_user_in_admin_bot(
    user_id: str,
    progress_callback: ProgressCallback | None = None,
    progress_title: str = "РџРѕРёСЃРє РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
    progress_steps: list[str] | None = None,
) -> str:
    steps = progress_steps or SEARCH_STEPS
    await emit_process_progress(
        progress_callback,
        progress_title,
        steps,
        1,
        user_id=user_id,
        extra_lines=["РћР¶РёРґР°СЋ СЃРІРѕР±РѕРґРЅС‹Р№ Р°РґРјРёРЅ-РїСЂРѕС†РµСЃСЃ"],
    )
    async with admin_flow_context(
        progress_title,
        user_id=user_id,
        progress_callback=progress_callback,
        progress_title=progress_title,
        progress_steps=steps,
        progress_step=1,
    ):
        await emit_process_progress(
            progress_callback,
            progress_title,
            steps,
            1,
            user_id=user_id,
            extra_lines=[f"РџРѕР»СѓС‡Р°СЋ Telegram entity @{settings.admin_bot_username}"],
        )
        bot = await get_admin_bot_entity()
        logging.info("Starting admin search for user_id=%s in @%s", user_id, settings.admin_bot_username)

        async with admin_conversation(bot) as conv:
            result_message = await open_user_in_admin_bot(
                conv,
                bot,
                user_id,
                progress_callback=progress_callback,
                progress_title=progress_title,
                progress_steps=steps,
            )
            await emit_process_progress(
                progress_callback,
                progress_title,
                steps,
                4,
                user_id=user_id,
                extra_lines=[f"РљРЅРѕРїРєР° РїРѕРґРїРёСЃРѕРє: {settings.subscriptions_button_text}"],
            )
            subscriptions_message = await click_and_read(
                bot,
                result_message,
                settings.subscriptions_button_text,
            )

        subscription_numbers = extract_subscription_numbers(subscriptions_message)
        await emit_process_progress(
            progress_callback,
            progress_title,
            steps,
            5,
            user_id=user_id,
            extra_lines=[
                f"РќР°Р№РґРµРЅРѕ РїРѕРґРїРёСЃРѕРє: {len(subscription_numbers)}",
                "Р“РѕС‚РѕРІР»СЋ РєРѕСЂРѕС‚РєСѓСЋ РєР°СЂС‚РѕС‡РєСѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
            ],
        )
        result_text = format_user_summary(
            user_id,
            result_message.raw_text or "",
            subscriptions_message,
        )
        print("\n===== USER SEARCH RESULT =====")
        print(result_text)
        print("==============================\n")
        logging.info("Admin search finished for user_id=%s", user_id)
        return result_text


def make_keys_copyable_html(text: str) -> str:
    pattern = re.compile(
        r"(\u0412\u0430\u0448\s+\u043a\u043b\u044e\u0447:\s*\n)(\S+)",
        flags=re.IGNORECASE,
    )
    result: list[str] = []
    position = 0

    for match in pattern.finditer(text):
        result.append(html.escape(text[position:match.start()]))
        result.append(html.escape(match.group(1)))
        result.append(f"<code>{html.escape(match.group(2))}</code>")
        position = match.end()

    result.append(html.escape(text[position:]))
    return "".join(result)


def format_subscription_info_html(user_id: str, user_text: str, subscriptions_message, details: list[tuple[str, str, str]]) -> str:
    subscriptions_text = subscriptions_message.raw_text or ""
    user_number = extract_user_number(user_text, subscriptions_text)

    lines = [
        f"1. Username \u0431\u043e\u0442\u0430: @{html.escape(settings.admin_bot_username)}",
        f"2. ID \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439: {html.escape(user_number or user_id)}",
    ]

    if not details:
        lines.append("3. \u0418\u043d\u0444\u043e \u043f\u043e\u0434\u043f\u0438\u0441\u043e\u043a: \u043f\u043e\u0434\u043f\u0438\u0441\u043e\u043a \u043d\u0435\u0442")
        return "\n".join(lines)

    lines.append("3. \u0418\u043d\u0444\u043e \u043f\u043e\u0434\u043f\u0438\u0441\u043e\u043a:")
    for subscription_id, button_text, detail_text in details:
        lines.append("")
        lines.append(f"[{html.escape(subscription_id)}] {html.escape(button_text)}")
        lines.append(make_keys_copyable_html(detail_text.strip() or "[empty subscription response]"))

    return "\n".join(lines)


def extract_location_from_subscription_button(text: str) -> str:
    raw = sanitize_outgoing_text(str(text or "")).strip()
    if not raw:
        return "\u0431\u0435\u0437 \u043b\u043e\u043a\u0430\u0446\u0438\u0438"

    # Remove obvious technical fragments.
    cleaned = re.sub(r"\[\d+\]", "", raw)
    cleaned = re.sub(r"\b(?:id|srv|server|loc|location)\s*[:#-]?\s*\d+\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" -|:;,.")

    # Keep friendly location part before service separators.
    parts = [p.strip(" -|:;,.") for p in re.split(r"\s+[|/]\s+|\s+-\s+|\s+—\s+", cleaned) if p.strip()]
    best = parts[0] if parts else cleaned
    # Remove numeric tails and technical fragments.
    best = re.sub(r"\b\d{1,8}\b", "", best)
    best = re.sub(r"\b(?:id|srv|server|loc|location|node|uuid)\b", "", best, flags=re.IGNORECASE)
    best = re.sub(r"\s{2,}", " ", best).strip(" -|:;,.")

    # If a flag exists in the source, keep "flag + name".
    flag_match = re.search(r"[\U0001F1E6-\U0001F1FF]{2}", raw)
    if flag_match:
        flag = flag_match.group(0)
        best_no_flag = best.replace(flag, "").strip()
        if best_no_flag:
            return f"{flag} {best_no_flag}".strip()
        return flag

    return best or "\u0431\u0435\u0437 \u043b\u043e\u043a\u0430\u0446\u0438\u0438"


def extract_expiration_date(text: str) -> datetime | None:
    patterns = (
        r"(\d{1,2})[.](\d{1,2})[.](\d{2,4})",
        r"(\d{4})-(\d{1,2})-(\d{1,2})",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            try:
                if pattern.startswith("(\\d{4})"):
                    year, month, day = map(int, match.groups())
                else:
                    day, month, year = map(int, match.groups())
                    if year < 100:
                        year += 2000
                return datetime(year, month, day)
            except ValueError:
                continue
    return None


def extract_registration_date(text: str) -> datetime | None:
    labels = (
        "РґР°С‚Р° СЂРµРіРёСЃС‚СЂР°С†РёРё",
        "Р·Р°СЂРµРіРёСЃС‚СЂРёСЂРѕРІР°РЅ",
        "СЂРµРіРёСЃС‚СЂР°С†РёСЏ",
        "created at",
        "registered at",
        "registration date",
    )
    lowered = text.casefold()
    for label in labels:
        idx = lowered.find(label)
        if idx == -1:
            continue
        snippet = text[idx: idx + 90]
        found = extract_expiration_date(snippet)
        if found:
            return found

    # fallback: date only from lines that look like registration metadata
    for raw_line in text.splitlines():
        line = raw_line.strip()
        line_lower = line.casefold()
        if not line:
            continue
        if not any(token in line_lower for token in ("СЂРµРі", "register", "created")):
            continue
        found = extract_expiration_date(line)
        if found and 2000 <= found.year <= datetime.now().year + 1:
            return found
    return None


def user_card_has_zero_subscriptions(text: str) -> bool:
    normalized = sanitize_outgoing_text(str(text or "")).casefold()
    patterns = (
        r"(?:подпис|vpn)\D{0,20}0\s*шт",
        r"0\s*шт\D{0,20}(?:подпис|vpn)",
        r"(?:subscriptions?)\D{0,20}0\b",
    )
    return any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in patterns)


def parse_profile_text_features(text: str) -> dict[str, object]:
    raw = str(text or "")
    lowered = raw.casefold()
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    key_values: dict[str, str] = {}
    for line in lines:
        if ":" not in line:
            continue
        left, right = line.split(":", 1)
        key = re.sub(r"\s+", " ", left.strip()).lower()
        value = right.strip()
        if key and value and key not in key_values:
            key_values[key] = value

    def find_number(*patterns: str) -> float | int | None:
        for pattern in patterns:
            match = re.search(pattern, raw, flags=re.IGNORECASE)
            if match:
                token = str(match.group(1)).replace(" ", "").replace(",", ".")
                try:
                    number = float(token)
                    if number.is_integer():
                        return int(number)
                    return number
                except ValueError:
                    continue
        return None

    return {
        "has_username": bool(re.search(r"@\w{3,}", raw)),
        "telegram_id": find_number(r"(?:telegram\s*id|tg\s*id|id)\D{0,8}(\d{5,20})"),
        "balance_rub": find_number(r"(?:Р±Р°Р»Р°РЅСЃ|balance)\D{0,12}([0-9][0-9\s.,]*)"),
        "referrals": find_number(r"(?:СЂРµС„РµСЂР°Р»|referral)\D{0,12}([0-9]{1,9})"),
        "key_values": key_values,
        "text_size": len(raw),
        "contains_payment_words": any(token in lowered for token in ("РѕРїР»Р°С‚", "payment", "РїР»Р°С‚РµР¶", "С‡РµРє")),
    }


def parse_subscription_text_features(detail_text: str, *, expires_at: datetime | None = None) -> dict[str, object]:
    raw = str(detail_text or "")
    lowered = raw.casefold()

    def find_number(pattern: str) -> float | int | None:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if not match:
            return None
        token = str(match.group(1)).replace(" ", "").replace(",", ".")
        try:
            number = float(token)
            if number.is_integer():
                return int(number)
            return number
        except ValueError:
            return None

    days_left = None
    if expires_at is not None:
        days_left = (expires_at.date() - datetime.now().date()).days

    return {
        "expires_at": expires_at.strftime("%Y-%m-%d") if expires_at else None,
        "days_left": days_left,
        "has_key": ("vless://" in lowered) or ("vmess://" in lowered) or ("trojan://" in lowered),
        "traffic_total_gb": find_number(r"(?:Р»РёРјРёС‚|total|РІСЃРµРіРѕ)\D{0,12}([0-9][0-9\s.,]*)\s*(?:gb|РіР±)"),
        "traffic_used_gb": find_number(r"(?:РёСЃРїРѕР»СЊР·|used)\D{0,12}([0-9][0-9\s.,]*)\s*(?:gb|РіР±)"),
        "traffic_left_gb": find_number(r"(?:РѕСЃС‚Р°С‚|left|remain)\D{0,12}([0-9][0-9\s.,]*)\s*(?:gb|РіР±)"),
        "contains_block_words": any(token in lowered for token in ("Р±Р»РѕРє", "ban", "РѕРіСЂР°РЅРёС‡", "suspend")),
        "contains_payment_words": any(token in lowered for token in ("РѕРїР»Р°С‚", "payment", "РїР»Р°С‚РµР¶", "С‡РµРє")),
        "text_size": len(raw),
    }


def build_scan_report(records: list[dict], pages_total: int = 0, admin_statistics: dict | None = None) -> tuple[str, dict]:
    def fmt_money(value: float) -> str:
        return f"{value:,.0f}".replace(",", " ")

    locations = Counter()
    expiring_soon: list[dict] = []  # 0..3 days
    expiring_within_7_days: list[dict] = []  # 0..7 days
    expiring_within_30_days: list[dict] = []  # 0..30 days
    expired_subscriptions: list[dict] = []
    subscriptions_per_user: dict[str, int] = {}
    users_without_subscriptions: list[str] = []
    expiring_soon_by_location = Counter()
    due_next_month_by_location = Counter()
    renewal_income_next_month_by_location: dict[str, float] = {}
    now = datetime.now()
    soon_limit = now + timedelta(days=3)
    week_limit = now + timedelta(days=7)
    month_limit = now + timedelta(days=30)

    total_subscriptions = 0
    dated_subscriptions = 0
    undated_subscriptions = 0
    active_subscriptions_with_date = 0
    earliest_expiration: datetime | None = None
    latest_expiration: datetime | None = None
    timing_buckets = {
        "expired": 0,
        "0_3_days": 0,
        "4_7_days": 0,
        "8_14_days": 0,
        "15_30_days": 0,
        "31_60_days": 0,
        "61_plus_days": 0,
        "without_date": 0,
    }

    for record in records:
        user_id = str(record["user_id"])
        raw_user_subscriptions = list(record.get("subscriptions") or [])
        user_subscriptions: list[dict] = []
        seen_sub_keys: set[str] = set()
        for sub in raw_user_subscriptions:
            sub_id = str(sub.get("subscription_id") or "").strip()
            btn = str(sub.get("button_text") or "").strip()
            loc = str(sub.get("location") or "").strip()
            key = sub_id or f"{btn}|{loc}"
            if not key or key in seen_sub_keys:
                continue
            seen_sub_keys.add(key)
            user_subscriptions.append(sub)

        subscriptions_per_user[user_id] = len(user_subscriptions)
        if not user_subscriptions:
            users_without_subscriptions.append(user_id)

        for subscription in user_subscriptions:
            total_subscriptions += 1
            locations[subscription["location"]] += 1
            expires_at = extract_expiration_date(subscription["detail_text"])
            item = {
                "user_id": user_id,
                "subscription_id": subscription["subscription_id"],
                "location": subscription["location"],
                "expires_at": expires_at.strftime("%Y-%m-%d") if expires_at else None,
                "days_left": None,
            }

            if not expires_at:
                undated_subscriptions += 1
                timing_buckets["without_date"] += 1
                continue

            dated_subscriptions += 1
            if earliest_expiration is None or expires_at < earliest_expiration:
                earliest_expiration = expires_at
            if latest_expiration is None or expires_at > latest_expiration:
                latest_expiration = expires_at

            days_left = (expires_at.date() - now.date()).days
            item["days_left"] = days_left
            if expires_at < now:
                expired_subscriptions.append(item)
                timing_buckets["expired"] += 1
                continue

            active_subscriptions_with_date += 1
            if expires_at <= soon_limit:
                expiring_soon.append(item)
                expiring_soon_by_location[subscription["location"]] += 1
            if expires_at <= week_limit:
                expiring_within_7_days.append(item)
            if expires_at <= month_limit:
                expiring_within_30_days.append(item)
                due_next_month_by_location[subscription["location"]] += 1

            if days_left <= 3:
                timing_buckets["0_3_days"] += 1
            elif days_left <= 7:
                timing_buckets["4_7_days"] += 1
            elif days_left <= 14:
                timing_buckets["8_14_days"] += 1
            elif days_left <= 30:
                timing_buckets["15_30_days"] += 1
            elif days_left <= 60:
                timing_buckets["31_60_days"] += 1
            else:
                timing_buckets["61_plus_days"] += 1

    users_with_subscriptions = len(records) - len(users_without_subscriptions)
    avg_subscriptions_per_user = (total_subscriptions / len(records)) if records else 0
    avg_subscriptions_per_active_user = (
        total_subscriptions / users_with_subscriptions if users_with_subscriptions else 0
    )
    top_users_by_subscriptions = sorted(
        subscriptions_per_user.items(),
        key=lambda item: item[1],
        reverse=True,
    )

    registrations_all: list[datetime] = []
    registrations_paid: list[datetime] = []
    subs_linked_to_registration = 0
    registration_subscription_points: list[tuple[date, int]] = []
    for record in records:
        reg_raw = record.get("registration_date")
        reg_date = None
        if reg_raw:
            reg_date = extract_expiration_date(str(reg_raw))
        if not reg_date:
            user_text = str(record.get("user_text") or "")
            reg_date = extract_registration_date(user_text)
        if not reg_date:
            continue
        registrations_all.append(reg_date)
        # Keep registration-linked stats in sync with deduped subscriptions logic.
        reg_subs = list(record.get("subscriptions") or [])
        reg_seen: set[str] = set()
        reg_count = 0
        for sub in reg_subs:
            sub_id = str(sub.get("subscription_id") or "").strip()
            btn = str(sub.get("button_text") or "").strip()
            loc = str(sub.get("location") or "").strip()
            key = sub_id or f"{btn}|{loc}"
            if not key or key in reg_seen:
                continue
            reg_seen.add(key)
            reg_count += 1
        sub_count = reg_count
        registration_subscription_points.append((reg_date.date(), sub_count))
        if sub_count > 0:
            registrations_paid.append(reg_date)
            subs_linked_to_registration += sub_count

    projection_days = 182
    if registrations_all:
        observation_start = min(registrations_all).date()
        observation_days = max((now.date() - observation_start).days + 1, 1)
    else:
        observation_start = None
        observation_days = 0

    if observation_days > 0:
        users_growth_per_day = len(records) / observation_days
        paid_users_growth_per_day = users_with_subscriptions / observation_days
        subscriptions_growth_per_day = total_subscriptions / observation_days
    else:
        users_growth_per_day = 0.0
        paid_users_growth_per_day = 0.0
        subscriptions_growth_per_day = 0.0

    projected_users_6m = len(records) + users_growth_per_day * projection_days
    projected_paid_users_6m = users_with_subscriptions + paid_users_growth_per_day * projection_days
    projected_subscriptions_6m = total_subscriptions + subscriptions_growth_per_day * projection_days
    projected_mrr_6m = projected_subscriptions_6m * max(0.0, FORECAST_PRICE_PER_SUBSCRIPTION_RUB)

    registration_coverage_users = (len(registrations_all) / len(records)) if records else 0.0
    registration_coverage_paid = (
        len(registrations_paid) / users_with_subscriptions
        if users_with_subscriptions
        else 0.0
    )

    def month_start(value: date) -> date:
        return date(value.year, value.month, 1)

    def add_months(value: date, delta: int) -> date:
        month_index = value.month - 1 + delta
        year = value.year + month_index // 12
        month = month_index % 12 + 1
        return date(year, month, 1)

    current_month_start = month_start(now.date())
    timeline_months = [add_months(current_month_start, delta) for delta in range(-6, 7)]
    timeline_labels = [item.strftime("%Y-%m") for item in timeline_months]
    users_actual_series: list[float | None] = []
    users_forecast_series: list[float | None] = []
    subscriptions_actual_series: list[float | None] = []
    subscriptions_forecast_series: list[float | None] = []
    current_users_total = len(records)
    current_subscriptions_total = total_subscriptions

    for month_point in timeline_months:
        next_month = add_months(month_point, 1)
        if month_point <= current_month_start:
            users_actual = sum(1 for reg_date in registrations_all if reg_date.date() < next_month)
            subs_actual = sum(
                subs_count
                for reg_day, subs_count in registration_subscription_points
                if reg_day < next_month
            )
            users_actual_series.append(float(users_actual))
            subscriptions_actual_series.append(float(subs_actual))
            users_forecast_series.append(float(users_actual) if month_point == current_month_start else None)
            subscriptions_forecast_series.append(float(subs_actual) if month_point == current_month_start else None)
            continue

        users_actual_series.append(None)
        subscriptions_actual_series.append(None)
        days_ahead = max((month_point - current_month_start).days, 0)
        users_forecast_series.append(current_users_total + users_growth_per_day * days_ahead)
        subscriptions_forecast_series.append(current_subscriptions_total + subscriptions_growth_per_day * days_ahead)

    estimated_active_total = active_subscriptions_with_date + int(round(undated_subscriptions * 0.5))
    price = max(0.0, FORECAST_PRICE_PER_SUBSCRIPTION_RUB)
    renew_7 = max(0.0, min(1.0, FORECAST_RENEWAL_RATE_7_DAYS))
    renew_30 = max(0.0, min(1.0, FORECAST_RENEWAL_RATE_30_DAYS))
    winback = max(0.0, min(1.0, FORECAST_WINBACK_RATE_EXPIRED))
    expiring_7_total = len(expiring_within_7_days)
    expiring_30_total = len(expiring_within_30_days)
    expired_total = len(expired_subscriptions)

    expected_renewal_revenue_7d = expiring_7_total * price * renew_7
    expected_renewal_revenue_30d = expiring_30_total * price * renew_30
    expected_winback_revenue_30d = expired_total * price * winback
    churn_risk_revenue_30d = expiring_30_total * price * (1 - renew_30)
    estimated_mrr_base = estimated_active_total * price

    stats_profit_by_period = dict((admin_statistics or {}).get("profit_by_period") or {})
    stats_users_by_period = dict((admin_statistics or {}).get("users_by_period") or {})
    stats_month_profit = float(stats_profit_by_period.get("month") or 0.0)
    stats_half_year_profit = float(stats_profit_by_period.get("half_year") or 0.0)
    stats_year_profit = float(stats_profit_by_period.get("year") or 0.0)
    stats_monthly_from_half_year = (stats_half_year_profit / 6.0) if stats_half_year_profit > 0 else 0.0
    stats_monthly_from_year = (stats_year_profit / 12.0) if stats_year_profit > 0 else 0.0

    renewal_rate_low = max(0.0, renew_30 - 0.1)
    renewal_rate_high = min(1.0, renew_30 + 0.1)
    scenario_low = expiring_30_total * price * renewal_rate_low
    scenario_base = expiring_30_total * price * renew_30
    scenario_high = expiring_30_total * price * renewal_rate_high
    expected_renewals_next_month_base = expiring_30_total * renew_30
    expected_renewals_next_month_low = expiring_30_total * renewal_rate_low
    expected_renewals_next_month_high = expiring_30_total * renewal_rate_high

    for location, due_count in due_next_month_by_location.items():
        renewal_income_next_month_by_location[location] = round(due_count * price * renew_30, 2)

    baseline_revenue = stats_month_profit if stats_month_profit > 0 else estimated_mrr_base
    if baseline_revenue <= 0:
        baseline_revenue = scenario_base
    ratio_candidates = []
    if stats_month_profit > 0 and stats_monthly_from_half_year > 0:
        ratio_candidates.append(stats_month_profit / stats_monthly_from_half_year)
    if stats_month_profit > 0 and stats_monthly_from_year > 0:
        ratio_candidates.append(stats_month_profit / stats_monthly_from_year)
    history_momentum = sum(ratio_candidates) / len(ratio_candidates) if ratio_candidates else 1.0
    history_momentum = max(0.75, min(1.25, history_momentum))

    if total_subscriptions > 0:
        raw_monthly_growth = (max(projected_subscriptions_6m, 1.0) / max(total_subscriptions, 1.0)) ** (1 / 6) - 1
    else:
        raw_monthly_growth = subscriptions_growth_per_day * 30
    blended_growth = raw_monthly_growth * 0.6 + (history_momentum - 1.0) * 0.4
    blended_growth = max(-0.15, min(0.2, blended_growth))

    renewal_based_month = scenario_base + expected_winback_revenue_30d
    financial_month_1 = max(0.0, renewal_based_month * 0.6 + baseline_revenue * (1 + blended_growth) * 0.4)
    financial_month_6 = max(0.0, financial_month_1 * ((1 + blended_growth) ** 5))
    financial_month_12 = max(0.0, financial_month_1 * ((1 + blended_growth) ** 11))

    forecast = {
        "assumptions": {
            "price_per_subscription_rub": price,
            "renewal_rate_7_days": renew_7,
            "renewal_rate_30_days": renew_30,
            "winback_rate_expired": winback,
            "undated_active_share": 0.5,
        },
        "active_subscriptions_with_date": active_subscriptions_with_date,
        "estimated_active_subscriptions_total": estimated_active_total,
        "estimated_mrr_rub": round(estimated_mrr_base, 2),
        "next_month_due_subscriptions_total": expiring_30_total,
        "next_month_expected_renewals_count_base": round(expected_renewals_next_month_base, 2),
        "next_month_expected_renewals_count_low": round(expected_renewals_next_month_low, 2),
        "next_month_expected_renewals_count_high": round(expected_renewals_next_month_high, 2),
        "next_month_projected_revenue_low_rub": round(scenario_low, 2),
        "next_month_projected_revenue_base_rub": round(scenario_base, 2),
        "next_month_projected_revenue_high_rub": round(scenario_high, 2),
        "expiring_within_7_days_total": expiring_7_total,
        "expiring_within_30_days_total": expiring_30_total,
        "expired_total": expired_total,
        "expected_renewal_revenue_7_days_rub": round(expected_renewal_revenue_7d, 2),
        "expected_renewal_revenue_30_days_rub": round(expected_renewal_revenue_30d, 2),
        "expected_winback_revenue_30_days_rub": round(expected_winback_revenue_30d, 2),
        "churn_risk_revenue_30_days_rub": round(churn_risk_revenue_30d, 2),
        "due_next_month_by_location": dict(due_next_month_by_location.most_common()),
        "expected_renewal_income_next_month_by_location_rub": {
            key: round(value, 2)
            for key, value in sorted(renewal_income_next_month_by_location.items(), key=lambda item: item[1], reverse=True)
        },
        "timing_buckets": timing_buckets,
        "financial_projection": {
            "baseline_revenue_source": "statistics_month_profit" if stats_month_profit > 0 else "subscriptions_mrr",
            "baseline_revenue_month_rub": round(baseline_revenue, 2),
            "history_momentum_factor": round(history_momentum, 4),
            "monthly_growth_rate_blended": round(blended_growth, 5),
            "profit_projection_month_1_rub": round(financial_month_1, 2),
            "profit_projection_month_6_rub": round(financial_month_6, 2),
            "profit_projection_month_12_rub": round(financial_month_12, 2),
            "stats_month_profit_rub": round(stats_month_profit, 2),
            "stats_half_year_profit_rub": round(stats_half_year_profit, 2),
            "stats_year_profit_rub": round(stats_year_profit, 2),
            "stats_users_by_period": stats_users_by_period,
            "stats_profit_by_period": stats_profit_by_period,
        },
        "six_month_projection": {
            "projection_days": projection_days,
            "observation_start": observation_start.isoformat() if observation_start else None,
            "observation_days": observation_days,
            "users_growth_per_day": round(users_growth_per_day, 4),
            "paid_users_growth_per_day": round(paid_users_growth_per_day, 4),
            "subscriptions_growth_per_day": round(subscriptions_growth_per_day, 4),
            "users_total_current": len(records),
            "users_with_subscriptions_current": users_with_subscriptions,
            "subscriptions_total_current": total_subscriptions,
            "users_total_projected_6m": round(projected_users_6m, 2),
            "users_with_subscriptions_projected_6m": round(projected_paid_users_6m, 2),
            "subscriptions_total_projected_6m": round(projected_subscriptions_6m, 2),
            "projected_mrr_6m_rub": round(projected_mrr_6m, 2),
            "registration_coverage_users": round(registration_coverage_users, 4),
            "registration_coverage_paid_users": round(registration_coverage_paid, 4),
            "registrations_found_total": len(registrations_all),
            "registrations_found_paid_users": len(registrations_paid),
            "subscriptions_linked_to_registration": subs_linked_to_registration,
            "timeline_labels": timeline_labels,
            "users_actual_series": users_actual_series,
            "users_forecast_series": users_forecast_series,
            "subscriptions_actual_series": subscriptions_actual_series,
            "subscriptions_forecast_series": subscriptions_forecast_series,
        },
    }

    stats = {
        "generated_at": now.isoformat(timespec="seconds"),
        "pages_total": pages_total,
        "users_total": len(records),
        "users_with_subscriptions_total": users_with_subscriptions,
        "users_without_subscriptions_total": len(users_without_subscriptions),
        "users_without_subscriptions": sorted(users_without_subscriptions),
        "subscriptions_total": total_subscriptions,
        "average_subscriptions_per_user": round(avg_subscriptions_per_user, 2),
        "average_subscriptions_per_user_with_subscriptions": round(avg_subscriptions_per_active_user, 2),
        "subscriptions_per_user": subscriptions_per_user,
        "top_users_by_subscriptions": [
            {"user_id": user_id, "subscriptions": count}
            for user_id, count in top_users_by_subscriptions
        ],
        "locations": dict(locations.most_common()),
        "locations_with_expiring_within_3_days": dict(expiring_soon_by_location.most_common()),
        "subscriptions_with_date_total": dated_subscriptions,
        "subscriptions_without_date_total": undated_subscriptions,
        "earliest_expiration": earliest_expiration.strftime("%Y-%m-%d") if earliest_expiration else None,
        "latest_expiration": latest_expiration.strftime("%Y-%m-%d") if latest_expiration else None,
        "expired_subscriptions": sorted(
            expired_subscriptions,
            key=lambda item: item["expires_at"] or "",
        ),
        "expiring_within_3_days": sorted(
            expiring_soon,
            key=lambda item: item["expires_at"] or "",
        ),
        "expiring_within_7_days": sorted(
            expiring_within_7_days,
            key=lambda item: item["expires_at"] or "",
        ),
        "expiring_within_30_days": sorted(
            expiring_within_30_days,
            key=lambda item: item["expires_at"] or "",
        ),
        "forecast": forecast,
        "admin_statistics": admin_statistics or {},
        "records": records,
        "registration_dates_total_found": len(registrations_all),
        "registration_dates_paid_users_found": len(registrations_paid),
    }

    lines = [
        "РћС‚С‡РµС‚ scan",
        f"РЎС„РѕСЂРјРёСЂРѕРІР°РЅ: {now.strftime('%Y-%m-%d %H:%M:%S')}",
        f"РџСЂРѕРІРµСЂРµРЅРѕ ID: {pages_total}",
        f"РџРѕР»СЊР·РѕРІР°С‚РµР»РµР№: {len(records)}",
        f"РџРѕР»СЊР·РѕРІР°С‚РµР»РµР№ СЃ РїРѕРґРїРёСЃРєР°РјРё: {users_with_subscriptions}",
        f"РџРѕР»СЊР·РѕРІР°С‚РµР»РµР№ Р±РµР· РїРѕРґРїРёСЃРѕРє: {len(users_without_subscriptions)}",
        f"РџРѕРґРїРёСЃРѕРє: {total_subscriptions}",
        f"РЎСЂРµРґРЅРµРµ РїРѕРґРїРёСЃРѕРє РЅР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ: {avg_subscriptions_per_user:.2f}",
        f"РЎСЂРµРґРЅРµРµ РїРѕРґРїРёСЃРѕРє РЅР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ СЃ РїРѕРґРїРёСЃРєР°РјРё: {avg_subscriptions_per_active_user:.2f}",
        f"РџРѕРґРїРёСЃРѕРє СЃ РґР°С‚РѕР№ РѕРєРѕРЅС‡Р°РЅРёСЏ: {dated_subscriptions}",
        f"РџРѕРґРїРёСЃРѕРє Р±РµР· РґР°С‚С‹ РѕРєРѕРЅС‡Р°РЅРёСЏ: {undated_subscriptions}",
        f"РЎР°РјР°СЏ СЂР°РЅРЅСЏСЏ РґР°С‚Р° РѕРєРѕРЅС‡Р°РЅРёСЏ: {stats['earliest_expiration'] or 'РЅРµС‚'}",
        f"РЎР°РјР°СЏ РїРѕР·РґРЅСЏСЏ РґР°С‚Р° РѕРєРѕРЅС‡Р°РЅРёСЏ: {stats['latest_expiration'] or 'РЅРµС‚'}",
        "",
        "Р”РѕС…РѕРґРЅРѕСЃС‚СЊ РЅР° СЃР»РµРґСѓСЋС‰РёР№ РјРµСЃСЏС† (РїРѕ СЂРµР°Р»СЊРЅС‹Рј СЃСЂРѕРєР°Рј РёСЃС‚РµС‡РµРЅРёСЏ):",
        f"- РџРѕРґРїРёСЃРѕРє СЃ РёСЃС‚РµС‡РµРЅРёРµРј РІ 30 РґРЅРµР№: {expiring_30_total}",
        f"- Р‘Р°Р·РѕРІС‹Р№ СЃС†РµРЅР°СЂРёР№ (70% РїСЂРѕРґР»СЏС‚): ~{fmt_money(scenario_base)} RUB",
        f"- РљРѕРЅСЃРµСЂРІР°С‚РёРІРЅС‹Р№ (60%): ~{fmt_money(scenario_low)} RUB",
        f"- РћРїС‚РёРјРёСЃС‚РёС‡РЅС‹Р№ (80%): ~{fmt_money(scenario_high)} RUB",
        f"- Р РёСЃРє РїРѕС‚РµСЂРё РІС‹СЂСѓС‡РєРё РїСЂРё РЅРµРїСЂРѕРґР»РµРЅРёРё: ~{fmt_money(churn_risk_revenue_30d)} RUB",
        f"- РџРѕС‚РµРЅС†РёР°Р» РІРѕР·РІСЂР°С‚Р° СѓР¶Рµ РёСЃС‚РµРєС€РёС… (winback): ~{fmt_money(expected_winback_revenue_30d)} RUB",
        "",
        "Р¤РёРЅР°РЅСЃРѕРІС‹Р№ РїСЂРѕРіРЅРѕР· (РѕР±СЉРµРґРёРЅРµРЅРёРµ СЃС‚Р°С‚РёСЃС‚РёРєРё Рё РїРѕРґРїРёСЃРѕРє):",
        f"- Р§РµСЂРµР· 1 РјРµСЃСЏС†: ~{fmt_money(financial_month_1)} RUB",
        f"- Р§РµСЂРµР· 6 РјРµСЃСЏС†РµРІ: ~{fmt_money(financial_month_6)} RUB",
        f"- Р§РµСЂРµР· 12 РјРµСЃСЏС†РµРІ: ~{fmt_money(financial_month_12)} RUB",
        f"- СЃС‚РѕС‡РЅРёРє Р±Р°Р·С‹: {'РїСЂРёР±С‹Р»СЊ РёР· СЃС‚Р°С‚РёСЃС‚РёРєРё' if stats_month_profit > 0 else 'РѕС†РµРЅРєР° MRR РїРѕ РїРѕРґРїРёСЃРєР°Рј'}",
        f"- СЃС‚РѕСЂРёСЏ РїСЂРёР±С‹Р»Рё РёР· СЃС‚Р°С‚РёСЃС‚РёРєРё: РјРµСЃСЏС† {fmt_money(stats_month_profit)} / РїРѕР»РіРѕРґР° {fmt_money(stats_half_year_profit)} / РіРѕРґ {fmt_money(stats_year_profit)} RUB",
        "",
        "РџСЂРѕРіРЅРѕР· С‡РµСЂРµР· 6 РјРµСЃСЏС†РµРІ (РїРѕ СЃРєРѕСЂРѕСЃС‚Рё РїСЂРёСЂРѕСЃС‚Р° РѕС‚ РґР°С‚С‹ СЂРµРіРёСЃС‚СЂР°С†РёРё):",
        f"- РџРµСЂРёРѕРґ РЅР°Р±Р»СЋРґРµРЅРёСЏ: {observation_days} РґРЅРµР№ (СЃ {observation_start.isoformat() if observation_start else 'РЅРµС‚ РґР°РЅРЅС‹С…'})",
        f"- РџРѕРєСЂС‹С‚РёРµ РґР°С‚ СЂРµРіРёСЃС‚СЂР°С†РёРё (РІСЃРµ РїРѕР»СЊР·РѕРІР°С‚РµР»Рё): {registration_coverage_users:.0%}",
        f"- РџРѕРєСЂС‹С‚РёРµ РґР°С‚ СЂРµРіРёСЃС‚СЂР°С†РёРё (РїР»Р°С‚СЏС‰РёРµ): {registration_coverage_paid:.0%}",
        f"- РЎРєРѕСЂРѕСЃС‚СЊ РїСЂРёСЂРѕСЃС‚Р° РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№: {users_growth_per_day:.2f}/РґРµРЅСЊ",
        f"- РЎРєРѕСЂРѕСЃС‚СЊ РїСЂРёСЂРѕСЃС‚Р° РїР»Р°С‚СЏС‰РёС…: {paid_users_growth_per_day:.2f}/РґРµРЅСЊ",
        f"- РЎРєРѕСЂРѕСЃС‚СЊ РїСЂРёСЂРѕСЃС‚Р° РїРѕРґРїРёСЃРѕРє: {subscriptions_growth_per_day:.2f}/РґРµРЅСЊ",
        f"- РџСЂРѕРіРЅРѕР· РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ С‡РµСЂРµР· 6Рј: ~{int(round(projected_users_6m))}",
        f"- РџСЂРѕРіРЅРѕР· РїР»Р°С‚СЏС‰РёС… С‡РµСЂРµР· 6Рј: ~{int(round(projected_paid_users_6m))}",
        f"- РџСЂРѕРіРЅРѕР· РїРѕРґРїРёСЃРѕРє С‡РµСЂРµР· 6Рј: ~{int(round(projected_subscriptions_6m))}",
        f"- РџСЂРѕРіРЅРѕР· MRR С‡РµСЂРµР· 6Рј: ~{fmt_money(projected_mrr_6m)} RUB",
        "",
        "Р Р°СЃРїСЂРµРґРµР»РµРЅРёРµ РїРѕ СЃСЂРѕРєР°Рј (Р°РєС‚РёРІРЅС‹Рµ Рё РёСЃС‚РµРєС€РёРµ):",
        f"- СЃС‚РµРєР»Рё: {timing_buckets['expired']}",
        f"- 0..3 РґРЅСЏ: {timing_buckets['0_3_days']}",
        f"- 4..7 РґРЅРµР№: {timing_buckets['4_7_days']}",
        f"- 8..14 РґРЅРµР№: {timing_buckets['8_14_days']}",
        f"- 15..30 РґРЅРµР№: {timing_buckets['15_30_days']}",
        f"- 31..60 РґРЅРµР№: {timing_buckets['31_60_days']}",
        f"- 61+ РґРЅРµР№: {timing_buckets['61_plus_days']}",
        f"- Р‘РµР· РґР°С‚С‹: {timing_buckets['without_date']}",
        "",
        "Р›РѕРєР°С†РёРё:",
    ]
    if locations:
        lines.extend(f"- {location}: {count}" for location, count in locations.most_common())
    else:
        lines.append("- РЅРµС‚ РґР°РЅРЅС‹С…")

    lines.append("")
    lines.append("Р”РѕС…РѕРґ СЃР»РµРґСѓСЋС‰РµРіРѕ РјРµСЃСЏС†Р° РїРѕ Р»РѕРєР°С†РёСЏРј (СЃС†РµРЅР°СЂРёР№ 70%):")
    if renewal_income_next_month_by_location:
        for location, amount in sorted(renewal_income_next_month_by_location.items(), key=lambda item: item[1], reverse=True):
            due_count = due_next_month_by_location[location]
            lines.append(f"- {location}: {fmt_money(amount)} RUB (РёСЃС‚РµРєР°РµС‚ {due_count})")
    else:
        lines.append("- РЅРµС‚ РґР°РЅРЅС‹С…")

    lines.append("")
    lines.append("РўРѕРї РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ РїРѕ С‡РёСЃР»Сѓ РїРѕРґРїРёСЃРѕРє:")
    if top_users_by_subscriptions:
        for user_id, count in top_users_by_subscriptions[:20]:
            lines.append(f"- user {user_id}: {count}")
    else:
        lines.append("- РЅРµС‚ РґР°РЅРЅС‹С…")

    lines.append("")
    lines.append("Р—Р°РєР°РЅС‡РёРІР°СЋС‚СЃСЏ РІ С‚РµС‡РµРЅРёРµ 3 РґРЅРµР№:")
    if expiring_soon:
        for item in expiring_soon:
            lines.append(
                f"- user {item['user_id']}, sub {item['subscription_id']}, {item['location']}, {item['expires_at']}"
            )
    else:
        lines.append("- РЅРµС‚")

    lines.append("")
    lines.append("Р—Р°РєР°РЅС‡РёРІР°СЋС‚СЃСЏ РІ С‚РµС‡РµРЅРёРµ 7 РґРЅРµР№:")
    if expiring_within_7_days:
        for item in expiring_within_7_days:
            lines.append(
                f"- user {item['user_id']}, sub {item['subscription_id']}, {item['location']}, {item['expires_at']}"
            )
    else:
        lines.append("- РЅРµС‚")

    lines.append("")
    lines.append("Р—Р°РєР°РЅС‡РёРІР°СЋС‚СЃСЏ РІ С‚РµС‡РµРЅРёРµ 30 РґРЅРµР№:")
    if expiring_within_30_days:
        for item in expiring_within_30_days[:50]:
            lines.append(
                f"- user {item['user_id']}, sub {item['subscription_id']}, {item['location']}, {item['expires_at']}"
            )
    else:
        lines.append("- РЅРµС‚")

    lines.append("")
    lines.append("РЈР¶Рµ РёСЃС‚РµРєР»Рё:")
    if expired_subscriptions:
        for item in expired_subscriptions:
            lines.append(
                f"- user {item['user_id']}, sub {item['subscription_id']}, {item['location']}, {item['expires_at']}"
            )
    else:
        lines.append("- РЅРµС‚")

    lines.append("")
    lines.append("РџРѕР»СЊР·РѕРІР°С‚РµР»Рё Р±РµР· РїРѕРґРїРёСЃРѕРє:")
    if users_without_subscriptions:
        lines.extend(f"- user {user_id}" for user_id in sorted(users_without_subscriptions))
    else:
        lines.append("- РЅРµС‚")

    lines.append("")
    lines.append("Р”РѕРїСѓС‰РµРЅРёСЏ РїСЂРѕРіРЅРѕР·Р°:")
    lines.append(f"- Р¦РµРЅР° РїРѕРґРїРёСЃРєРё: {fmt_money(price)} RUB")
    lines.append(f"- РџСЂРѕРґР»РµРЅРёРµ РІ 7 РґРЅРµР№: {renew_7:.0%}")
    lines.append(f"- РџСЂРѕРґР»РµРЅРёРµ РІ 30 РґРЅРµР№: {renew_30:.0%}")
    lines.append(f"- Р’РѕР·РІСЂР°С‚ РёСЃС‚РµРєС€РёС…: {winback:.0%}")
    lines.append("- Р”Р»СЏ РїРѕРґРїРёСЃРѕРє Р±РµР· РґР°С‚С‹ Р±РµСЂРµС‚СЃСЏ 50% РєР°Рє Р°РєС‚РёРІРЅС‹Рµ.")

    return "\n".join(lines), stats


def build_detailed_scan_report(records: list[dict]) -> str:
    lines = ["РџРѕРґСЂРѕР±РЅС‹Р№ РѕС‚С‡РµС‚ scan", ""]
    for index, record in enumerate(records, start=1):
        user_id = str(record["user_id"])
        user_button_text = str(record["user_button_text"])
        subscriptions = record["subscriptions"]
        lines.append(f"{index}. user_id={user_id} subscriptions={len(subscriptions)}")
        lines.append(f"   button: {user_button_text}")

        user_text = (record.get("user_text") or "").strip()
        registration_date = extract_registration_date(user_text)
        lines.append(
            f"   registration_date: {registration_date.strftime('%Y-%m-%d') if registration_date else 'unknown'}"
        )
        if user_text:
            lines.append("   user_text:")
            for raw_line in user_text.splitlines():
                lines.append(f"     {raw_line}")

        if not subscriptions:
            lines.append("   subscriptions: none")
            lines.append("")
            continue

        lines.append("   subscriptions:")
        for subscription in subscriptions:
            expires_at = extract_expiration_date(subscription["detail_text"])
            expires_text = expires_at.strftime("%Y-%m-%d") if expires_at else "unknown"
            lines.append(
                "   - id={id} location={location} expires_at={expires_at} button={button}".format(
                    id=subscription["subscription_id"],
                    location=subscription["location"],
                    expires_at=expires_text,
                    button=subscription["button_text"],
                )
            )
            detail_text = (subscription.get("detail_text") or "").strip()
            if detail_text:
                lines.append("     detail_text:")
                for raw_line in detail_text.splitlines():
                    lines.append(f"       {raw_line}")
        lines.append("")

    return "\n".join(lines)


def build_scan_dashboard_html(stats: dict) -> str:
    def esc(value) -> str:
        return html.escape(str(value))

    def fmt_int(value: int) -> str:
        return f"{int(value):,}".replace(",", " ")

    def fmt_money(value: float) -> str:
        return f"{value:,.0f}".replace(",", " ")

    forecast = dict(stats.get("forecast") or {})
    financial = dict(forecast.get("financial_projection") or {})
    six_month = dict(forecast.get("six_month_projection") or {})
    assumptions = dict(forecast.get("assumptions") or {})
    locations = dict(stats.get("locations") or {})
    timing_buckets = dict(forecast.get("timing_buckets") or {})
    due_by_location = dict(forecast.get("due_next_month_by_location") or {})
    due_income_by_location = dict(forecast.get("expected_renewal_income_next_month_by_location_rub") or {})
    top_users = list(stats.get("top_users_by_subscriptions") or [])
    expiring_3 = list(stats.get("expiring_within_3_days") or [])
    expiring_7 = list(stats.get("expiring_within_7_days") or [])
    expiring_30 = list(stats.get("expiring_within_30_days") or [])
    expired = list(stats.get("expired_subscriptions") or [])
    records = list(stats.get("records") or [])

    location_rows = "".join(
        f"<tr><td>{esc(location)}</td><td>{fmt_int(count)}</td></tr>"
        for location, count in sorted(locations.items(), key=lambda item: item[1], reverse=True)[:5]
    ) or "<tr><td colspan='2'>РќРµС‚ РґР°РЅРЅС‹С…</td></tr>"

    top_user_rows = "".join(
        f"<tr><td>{esc(item.get('user_id', '-'))}</td><td>{fmt_int(item.get('subscriptions', 0))}</td></tr>"
        for item in top_users[:5]
    ) or "<tr><td colspan='2'>РќРµС‚ РґР°РЅРЅС‹С…</td></tr>"

    def build_expiration_rows(items: list[dict], limit: int = 30, with_days: bool = True) -> str:
        rows = "".join(
            (
                f"<tr><td>{esc(item.get('user_id', '-'))}</td>"
                f"<td>{esc(item.get('subscription_id', '-'))}</td>"
                f"<td>{esc(item.get('location', '-'))}</td>"
                f"<td>{esc(item.get('expires_at', '-'))}</td>"
                + (
                    f"<td>{esc(item.get('days_left', '-'))}</td></tr>"
                    if with_days
                    else "</tr>"
                )
            )
            for item in items[:limit]
        )
        colspan = "5" if with_days else "4"
        return rows or f"<tr><td colspan='{colspan}'>РќРµС‚ РґР°РЅРЅС‹С…</td></tr>"

    def admin_user_rows_json(records: list[dict]) -> str:
        today = datetime.now().date()
        rows: list[dict[str, object]] = []
        for record in records:
            user_id = str(record.get("user_id") or "").strip()
            username = extract_username_from_record(record)
            user_text = str(record.get("user_text") or "")
            registration_date = str(record.get("registration_date") or "").strip()
            if not registration_date:
                parsed_registration_date = extract_registration_date(user_text)
                registration_date = parsed_registration_date.strftime("%Y-%m-%d") if parsed_registration_date else ""
            subscriptions = list(record.get("subscriptions") or [])
            locations_for_user = sorted(
                {
                    str(subscription.get("location") or "").strip()
                    for subscription in subscriptions
                    if str(subscription.get("location") or "").strip()
                }
            )
            expiration_dates: list[date] = []
            for subscription in subscriptions:
                expires_at = extract_expiration_date(str(subscription.get("detail_text") or ""))
                if expires_at:
                    expiration_dates.append(expires_at.date() if isinstance(expires_at, datetime) else expires_at)
            nearest_expiration = min(expiration_dates).strftime("%Y-%m-%d") if expiration_dates else ""
            nearest_days = min((expires_at - today).days for expires_at in expiration_dates) if expiration_dates else None
            if not subscriptions:
                status = "no_subs"
                status_label = "Р‘РµР· РїРѕРґРїРёСЃРєРё"
            elif nearest_days is None:
                status = "unknown_date"
                status_label = "Р•СЃС‚СЊ РїРѕРґРїРёСЃРєР°, РґР°С‚Р° РЅРµРёР·РІРµСЃС‚РЅР°"
            elif nearest_days < 0:
                status = "expired"
                status_label = "СЃС‚РµРєР»Р°"
            elif nearest_days <= 7:
                status = "expiring_7"
                status_label = "СЃС‚РµРєР°РµС‚ Р·Р° 7 РґРЅРµР№"
            elif nearest_days <= 30:
                status = "expiring_30"
                status_label = "СЃС‚РµРєР°РµС‚ Р·Р° 30 РґРЅРµР№"
            else:
                status = "active"
                status_label = "РђРєС‚РёРІРЅР°"
            rows.append(
                {
                    "user_id": user_id,
                    "username": username,
                    "registration_date": registration_date,
                    "subscriptions": len(subscriptions),
                    "locations": ", ".join(locations_for_user),
                    "nearest_expiration": nearest_expiration,
                    "days_left": nearest_days if nearest_days is not None else "",
                    "status": status,
                    "status_label": status_label,
                    "search": " ".join(
                        (
                            user_id,
                            username,
                            registration_date,
                            " ".join(locations_for_user),
                            user_text,
                        )
                    ).casefold(),
                }
            )
        return json.dumps(rows, ensure_ascii=False).replace("</", "<\\/")

    def build_history_forecast_chart_svg(
        labels: list[str],
        actual_series: list[float | None],
        forecast_series: list[float | None],
        *,
        actual_color: str,
        forecast_color: str,
    ) -> str:
        width = 920
        height = 270
        left = 52
        right = 18
        top = 16
        bottom = 40
        plot_w = width - left - right
        plot_h = height - top - bottom
        points_count = max(len(labels), 2)

        merged_values = [
            float(value)
            for value in (list(actual_series) + list(forecast_series))
            if value is not None
        ]
        max_value = max(merged_values) if merged_values else 1.0
        max_value = max(max_value, 1.0)

        def xy(index: int, value: float) -> tuple[float, float]:
            x = left + (plot_w * index / (points_count - 1))
            y = top + plot_h - (value / max_value) * plot_h
            return x, y

        def polyline(series: list[float | None]) -> str:
            points = []
            for idx, raw in enumerate(series):
                if raw is None:
                    continue
                x, y = xy(idx, float(raw))
                points.append(f"{x:.1f},{y:.1f}")
            return " ".join(points)

        grid_lines = []
        for step in range(0, 6):
            y = top + plot_h * step / 5
            value = max_value * (1 - step / 5)
            grid_lines.append(
                f"<line x1='{left}' y1='{y:.1f}' x2='{left + plot_w}' y2='{y:.1f}' stroke='rgba(174,185,214,.22)' stroke-width='1'/>"
            )
            grid_lines.append(
                f"<text x='{left - 8}' y='{y + 4:.1f}' text-anchor='end' fill='#aeb9d6' font-size='11'>{int(round(value))}</text>"
            )

        ticks = []
        tick_step = max(1, (points_count - 1) // 6)
        for idx, label in enumerate(labels):
            if idx % tick_step != 0 and idx != points_count - 1:
                continue
            x, _ = xy(idx, 0.0)
            ticks.append(
                f"<text x='{x:.1f}' y='{height - 12}' text-anchor='middle' fill='#aeb9d6' font-size='11'>{html.escape(label[2:])}</text>"
            )

        actual_points = polyline(actual_series)
        forecast_points = polyline(forecast_series)

        return (
            f"<svg viewBox='0 0 {width} {height}' width='100%' height='260' aria-hidden='true'>"
            + "".join(grid_lines)
            + f"<line x1='{left}' y1='{top + plot_h:.1f}' x2='{left + plot_w}' y2='{top + plot_h:.1f}' stroke='rgba(174,185,214,.45)' stroke-width='1.2'/>"
            + f"<line x1='{left}' y1='{top:.1f}' x2='{left}' y2='{top + plot_h:.1f}' stroke='rgba(174,185,214,.45)' stroke-width='1.2'/>"
            + (f"<polyline fill='none' stroke='{actual_color}' stroke-width='3' points='{actual_points}'/>" if actual_points else "")
            + (f"<polyline fill='none' stroke='{forecast_color}' stroke-width='3' stroke-dasharray='7 6' points='{forecast_points}'/>" if forecast_points else "")
            + "".join(ticks)
            + "</svg>"
        )

    timing_rows = "".join(
        (
            f"<tr><td>СЃС‚РµРєР»Рё</td><td>{fmt_int(timing_buckets.get('expired', 0))}</td></tr>"
            f"<tr><td>0..3 РґРЅСЏ</td><td>{fmt_int(timing_buckets.get('0_3_days', 0))}</td></tr>"
            f"<tr><td>4..7 РґРЅРµР№</td><td>{fmt_int(timing_buckets.get('4_7_days', 0))}</td></tr>"
            f"<tr><td>8..14 РґРЅРµР№</td><td>{fmt_int(timing_buckets.get('8_14_days', 0))}</td></tr>"
            f"<tr><td>15..30 РґРЅРµР№</td><td>{fmt_int(timing_buckets.get('15_30_days', 0))}</td></tr>"
            f"<tr><td>31..60 РґРЅРµР№</td><td>{fmt_int(timing_buckets.get('31_60_days', 0))}</td></tr>"
            f"<tr><td>61+ РґРЅРµР№</td><td>{fmt_int(timing_buckets.get('61_plus_days', 0))}</td></tr>"
            f"<tr><td>Р‘РµР· РґР°С‚С‹</td><td>{fmt_int(timing_buckets.get('without_date', 0))}</td></tr>"
        )
    )

    due_location_rows = "".join(
        f"<tr><td>{esc(location)}</td><td>{fmt_int(due_count)}</td><td>{fmt_money(float(due_income_by_location.get(location, 0.0)))} в‚Ѕ</td></tr>"
        for location, due_count in sorted(due_by_location.items(), key=lambda item: item[1], reverse=True)[:5]
    ) or "<tr><td colspan='3'>РќРµС‚ РґР°РЅРЅС‹С…</td></tr>"

    generated_at = esc(stats.get("generated_at", "-")).replace("T", " ")
    pages_total = fmt_int(stats.get("pages_total", 0))
    users_total = fmt_int(stats.get("users_total", 0))
    subscriptions_total = fmt_int(stats.get("subscriptions_total", 0))
    mrr_estimate = fmt_money(float(forecast.get("estimated_mrr_rub", 0.0)))
    renew_7 = fmt_money(float(forecast.get("expected_renewal_revenue_7_days_rub", 0.0)))
    renew_30 = fmt_money(float(forecast.get("expected_renewal_revenue_30_days_rub", 0.0)))
    winback_30 = fmt_money(float(forecast.get("expected_winback_revenue_30_days_rub", 0.0)))
    churn_risk = fmt_money(float(forecast.get("churn_risk_revenue_30_days_rub", 0.0)))
    due_30_count = fmt_int(int(forecast.get("next_month_due_subscriptions_total", 0)))
    revenue_next_low = fmt_money(float(forecast.get("next_month_projected_revenue_low_rub", 0.0)))
    revenue_next_base = fmt_money(float(forecast.get("next_month_projected_revenue_base_rub", 0.0)))
    revenue_next_high = fmt_money(float(forecast.get("next_month_projected_revenue_high_rub", 0.0)))
    renewals_next_base = fmt_int(round(float(forecast.get("next_month_expected_renewals_count_base", 0.0))))
    profit_m1 = fmt_money(float(financial.get("profit_projection_month_1_rub", 0.0)))
    profit_m6 = fmt_money(float(financial.get("profit_projection_month_6_rub", 0.0)))
    profit_y1 = fmt_money(float(financial.get("profit_projection_month_12_rub", 0.0)))
    stats_profit_month = fmt_money(float(financial.get("stats_month_profit_rub", 0.0)))
    stats_profit_half = fmt_money(float(financial.get("stats_half_year_profit_rub", 0.0)))
    stats_profit_year = fmt_money(float(financial.get("stats_year_profit_rub", 0.0)))
    stats_users_period = dict(financial.get("stats_users_by_period") or {})
    obs_days = fmt_int(int(six_month.get("observation_days", 0)))
    obs_start = esc(six_month.get("observation_start", "-"))
    reg_cov_all = f"{float(six_month.get('registration_coverage_users', 0.0)) * 100:.0f}%"
    reg_cov_paid = f"{float(six_month.get('registration_coverage_paid_users', 0.0)) * 100:.0f}%"
    growth_users_day = float(six_month.get("users_growth_per_day", 0.0))
    growth_paid_day = float(six_month.get("paid_users_growth_per_day", 0.0))
    growth_subs_day = float(six_month.get("subscriptions_growth_per_day", 0.0))
    proj_users_6m = fmt_int(round(float(six_month.get("users_total_projected_6m", 0.0))))
    proj_paid_6m = fmt_int(round(float(six_month.get("users_with_subscriptions_projected_6m", 0.0))))
    proj_subs_6m = fmt_int(round(float(six_month.get("subscriptions_total_projected_6m", 0.0))))
    proj_mrr_6m = fmt_money(float(six_month.get("projected_mrr_6m_rub", 0.0)))
    timeline_labels = [str(item) for item in list(six_month.get("timeline_labels") or [])]
    users_actual_chart = list(six_month.get("users_actual_series") or [])
    users_forecast_chart = list(six_month.get("users_forecast_series") or [])
    subs_actual_chart = list(six_month.get("subscriptions_actual_series") or [])
    subs_forecast_chart = list(six_month.get("subscriptions_forecast_series") or [])
    # Apple-like light theme for dashboard readability on desktop/mobile.
    theme_bg = "#f5f5f7"
    theme_panel = "#ffffff"
    theme_panel_soft = "#ffffff"
    theme_text = "#1d1d1f"
    theme_muted = "#6e6e73"
    theme_primary = "#0071e3"
    theme_good = "#1d9d62"
    theme_warn = "#b26a00"
    theme_bad = "#c93434"
    theme_border = "#e5e5ea"
    dashboard_brand = esc(settings.dashboard_brand_name or settings.app_name or "Vpn_Bot_assist")
    dashboard_title = esc(settings.dashboard_title or "VPN Dashboard")
    dashboard_subtitle = esc(settings.dashboard_subtitle or "")

    logo_src = ""
    raw_logo_path = (settings.dashboard_logo_path or "").strip()
    if raw_logo_path:
        if re.match(r"^https?://", raw_logo_path, flags=re.IGNORECASE):
            logo_src = raw_logo_path
        else:
            logo_path = Path(raw_logo_path)
            if not logo_path.is_absolute():
                logo_path = Path.cwd() / logo_path
            if logo_path.exists():
                logo_src = logo_path.resolve().as_uri()

    logo_html = (
        f"<img class='logo' src='{esc(logo_src)}' alt='logo'>"
        if logo_src
        else f"<div class='logo-badge'>{dashboard_brand}</div>"
    )
    users_chart_svg = build_history_forecast_chart_svg(
        timeline_labels,
        users_actual_chart,
        users_forecast_chart,
        actual_color=theme_primary,
        forecast_color=theme_good,
    ) if timeline_labels else ""
    subs_chart_svg = build_history_forecast_chart_svg(
        timeline_labels,
        subs_actual_chart,
        subs_forecast_chart,
        actual_color=theme_warn,
        forecast_color=theme_bad,
    ) if timeline_labels else ""
    price = fmt_money(float(assumptions.get("price_per_subscription_rub", 0.0)))
    renew_7_rate = f"{float(assumptions.get('renewal_rate_7_days', 0.0)) * 100:.0f}%"
    renew_30_rate = f"{float(assumptions.get('renewal_rate_30_days', 0.0)) * 100:.0f}%"
    winback_rate = f"{float(assumptions.get('winback_rate_expired', 0.0)) * 100:.0f}%"
    admin_users_json = admin_user_rows_json(records)
    admin_overview_json = json.dumps(dashboard_live_overview_payload(), ensure_ascii=False).replace("</", "<\\/")

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{dashboard_brand} - Dashboard</title>
  <style>
    :root {{
      --bg: {theme_bg};
      --panel: {theme_panel};
      --panel-soft: {theme_panel_soft};
      --text: {theme_text};
      --muted: {theme_muted};
      --accent: {theme_primary};
      --good: {theme_good};
      --warn: {theme_warn};
      --bad: {theme_bad};
      --border: {theme_border};
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Helvetica Neue", Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    .wrap {{ max-width: 1280px; margin: 0 auto; padding: 28px 20px 40px; }}
    .header {{ display: flex; align-items: center; gap: 14px; margin-bottom: 8px; }}
    .logo {{ width: 58px; height: 58px; object-fit: contain; border-radius: 10px; border: 1px solid var(--border); background: rgba(255,255,255,.03); }}
    .logo-badge {{ min-width: 58px; height: 58px; padding: 0 10px; display: inline-flex; align-items: center; justify-content: center; border-radius: 10px; border: 1px solid var(--border); background: rgba(255,255,255,.03); color: var(--accent); font-weight: 700; font-size: 11px; letter-spacing: .2px; text-transform: uppercase; }}
    .brand {{ color: var(--muted); font-size: 13px; }}
    h1 {{ margin: 0 0 8px; font-size: 32px; line-height: 1.15; }}
    .sub {{ color: var(--muted); margin-bottom: 18px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px;
      min-height: 96px;
    }}
    .k {{ color: var(--muted); font-size: 13px; margin-bottom: 8px; }}
    .v {{ font-size: 26px; font-weight: 700; letter-spacing: .2px; }}
    .v.good {{ color: var(--good); }}
    .v.warn {{ color: var(--warn); }}
    .v.bad {{ color: var(--bad); }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px;
      margin-top: 12px;
    }}
    h2 {{ margin: 4px 0 10px; font-size: 19px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid var(--border); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 600; }}
    .cols {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
    }}
    @media (max-width: 980px) {{
      .cols {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 28px; }}
    }}
    .assumptions {{ color: var(--muted); font-size: 13px; line-height: 1.6; }}
    .chart-wrap {{
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 8px;
      margin-top: 8px;
      background: rgba(11,16,32,.35);
    }}
    .legend {{ color: var(--muted); font-size: 12px; margin-top: 4px; }}
    .admin-shell {{
      display: grid;
      grid-template-columns: 220px minmax(0, 1fr);
      gap: 14px;
      align-items: start;
    }}
    .side-nav {{
      position: sticky;
      top: 12px;
      display: grid;
      gap: 8px;
      max-height: calc(100vh - 40px);
      overflow: auto;
      padding-right: 2px;
    }}
    .nav-btn, .filter-btn {{
      border: 1px solid var(--border);
      background: #fff;
      color: var(--text);
      border-radius: 8px;
      padding: 10px 12px;
      text-align: left;
      cursor: pointer;
      font: inherit;
    }}
    .nav-btn.active, .filter-btn.active {{
      border-color: var(--accent);
      color: var(--accent);
      background: rgba(0,113,227,.08);
    }}
    .tab-panel {{ display: none; }}
    .tab-panel.active {{ display: block; }}
    .toolbar {{
      display: grid;
      grid-template-columns: minmax(180px, 1fr) repeat(5, minmax(130px, 170px));
      gap: 10px;
      margin: 10px 0 12px;
      align-items: stretch;
    }}
    .toolbar input, .toolbar select {{
      width: 100%;
      border: 1px solid var(--border);
      background: #fff;
      color: var(--text);
      border-radius: 8px;
      padding: 10px 12px;
      font: inherit;
    }}
    .filter-row {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 8px 0 12px; }}
    .status-pill {{ display: inline-flex; border-radius: 999px; padding: 3px 8px; border: 1px solid var(--border); font-size: 12px; color: var(--muted); }}
    .status-pill.expired {{ color: var(--bad); border-color: rgba(248,113,113,.45); }}
    .status-pill.expiring_7, .status-pill.expiring_30 {{ color: var(--warn); border-color: rgba(245,158,11,.45); }}
    .status-pill.active {{ color: var(--good); border-color: rgba(52,211,153,.45); }}
    .table-scroll {{
      overflow-x: auto;
      overflow-y: hidden;
      -webkit-overflow-scrolling: touch;
      border: 1px solid var(--border);
      border-radius: 8px;
    }}
    .table-scroll table {{ min-width: 860px; }}
    .admin-kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 8px; margin-bottom: 10px; }}
    .admin-kpi {{ border: 1px solid var(--border); border-radius: 10px; padding: 10px; background: #fff; }}
    .admin-kpi b {{ display: block; font-size: 22px; line-height: 1.1; margin-top: 4px; }}
    .pager {{ display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-top: 10px; flex-wrap: wrap; }}
    .pager-btn {{ border: 1px solid var(--border); background: #fff; color: var(--text); border-radius: 8px; padding: 8px 12px; cursor: pointer; font: inherit; }}
    .pager-btn[disabled] {{ opacity: .45; cursor: default; }}
    .action-panel {{
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px;
      background: #fff;
      margin-bottom: 12px;
    }}
    .action-grid {{
      display: grid;
      grid-template-columns: minmax(180px, 240px) minmax(0, 1fr);
      gap: 10px;
      margin-bottom: 10px;
    }}
    .action-grid input, .action-grid textarea {{
      width: 100%;
      border: 1px solid var(--border);
      background: #fff;
      color: var(--text);
      border-radius: 8px;
      padding: 10px 12px;
      font: inherit;
    }}
    .action-grid textarea {{ min-height: 110px; resize: vertical; }}
    .action-buttons {{ display: flex; flex-wrap: wrap; gap: 8px; }}
    .action-btn {{
      border: 1px solid var(--border);
      background: #fff;
      color: var(--text);
      border-radius: 8px;
      padding: 9px 12px;
      cursor: pointer;
      font: inherit;
    }}
    .action-btn.primary {{ border-color: rgba(0,113,227,.45); background: rgba(0,113,227,.08); color: var(--accent); }}
    .action-btn.good {{ border-color: rgba(29,157,98,.45); background: rgba(29,157,98,.08); color: var(--good); }}
    .action-btn.warn {{ border-color: rgba(178,106,0,.45); background: rgba(178,106,0,.08); color: var(--warn); }}
    .action-btn[disabled] {{ opacity: .5; cursor: default; }}
    .action-status {{
      margin-top: 10px;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 10px;
      color: var(--muted);
      white-space: pre-wrap;
      line-height: 1.5;
      font-size: 13px;
    }}
    .muted {{ color: var(--muted); }}
    @media (max-width: 980px) {{
      .admin-shell {{ grid-template-columns: 1fr; }}
      .side-nav {{
        position: static;
        display: flex;
        gap: 8px;
        overflow-x: auto;
        max-height: none;
      }}
      .side-nav .nav-btn {{
        white-space: nowrap;
        flex: 0 0 auto;
      }}
      .toolbar {{ grid-template-columns: 1fr; }}
      .action-grid {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 768px) {{
      .wrap {{ padding: 16px 10px 24px; }}
      .header {{ gap: 10px; }}
      .logo, .logo-badge {{ width: 46px; height: 46px; min-width: 46px; }}
      h1 {{ font-size: 24px; margin-bottom: 4px; }}
      .sub {{ font-size: 12px; margin-bottom: 12px; }}
      .grid {{ grid-template-columns: 1fr; gap: 10px; }}
      .card {{ padding: 12px; min-height: 0; }}
      .v {{ font-size: 24px; }}
      .panel {{ padding: 12px; margin-top: 10px; }}
      .cols {{ grid-template-columns: 1fr; gap: 10px; }}
      th, td {{ padding: 7px 8px; font-size: 13px; }}
      .side-nav {{
        display: flex;
        gap: 8px;
        overflow-x: auto;
        padding-bottom: 2px;
      }}
      .nav-btn {{
        white-space: nowrap;
        flex: 0 0 auto;
      }}
      .filter-row {{
        display: grid;
        grid-template-columns: 1fr 1fr;
      }}
      .filter-btn {{
        text-align: center;
      }}
      .nav-btn, .filter-btn, .action-btn, .pager-btn {{ min-height: 40px; }}
      .admin-kpis {{ grid-template-columns: 1fr 1fr; }}
      .admin-kpi b {{ font-size: 20px; }}
      .table-scroll table {{ min-width: 540px; }}
      .toolbar input, .toolbar select, .action-grid input, .action-grid textarea {{
        font-size: 16px;
      }}
      .pager {{
        justify-content: flex-start;
      }}
      .action-buttons {{
        display: grid;
        grid-template-columns: 1fr 1fr;
      }}
    }}
    @media (max-width: 480px) {{
      .admin-kpis {{ grid-template-columns: 1fr; }}
      .filter-row {{ grid-template-columns: 1fr; }}
      .action-buttons {{ grid-template-columns: 1fr; }}
      .toolbar {{ gap: 8px; }}
      .table-scroll table {{ min-width: 480px; }}
      th, td {{ font-size: 12px; }}
    }}
    code {{
      background: #f2f2f7;
      border: 1px solid var(--border);
      padding: 1px 6px;
      border-radius: 6px;
      color: #1d1d1f;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      {logo_html}
      <div>
        <div class="brand">{dashboard_brand}</div>
        <h1>{dashboard_title}</h1>
      </div>
    </div>
    <div class="sub">{dashboard_subtitle} | Generated: {generated_at}</div>
    <div class="panel">
      <h2>Quick guide</h2>
      <div class="assumptions">
        {esc(settings.dashboard_hint_primary)}<br>
        {esc(settings.dashboard_hint_secondary)}<br>
        {esc(settings.dashboard_hint_tertiary)}
      </div>
    </div>

    <div class="grid">
      <div class="card"><div class="k">РџСЂРѕРІРµСЂРµРЅРѕ ID</div><div class="v">{pages_total}</div></div>
      <div class="card"><div class="k">РџРѕР»СЊР·РѕРІР°С‚РµР»РµР№</div><div class="v">{users_total}</div></div>
      <div class="card"><div class="k">РџРѕРґРїРёСЃРѕРє</div><div class="v">{subscriptions_total}</div></div>
      <div class="card"><div class="k">СЃС‚РµРєР°РµС‚ РІ 30 РґРЅРµР№</div><div class="v warn">{due_30_count}</div></div>
      <div class="card"><div class="k">Р”РѕС…РѕРґ next month (70%)</div><div class="v good">{revenue_next_base} в‚Ѕ</div></div>
      <div class="card"><div class="k">РџСЂРёР±С‹Р»СЊ 1 РјРµСЃСЏС† (РёС‚РѕРі)</div><div class="v good">{profit_m1} в‚Ѕ</div></div>
      <div class="card"><div class="k">РџСЂРёР±С‹Р»СЊ 6 РјРµСЃСЏС†РµРІ (РёС‚РѕРі)</div><div class="v good">{profit_m6} в‚Ѕ</div></div>
      <div class="card"><div class="k">РџСЂРёР±С‹Р»СЊ 12 РјРµСЃСЏС†РµРІ (РёС‚РѕРі)</div><div class="v good">{profit_y1} в‚Ѕ</div></div>
      <div class="card"><div class="k">РћР¶РёРґР°РµРјС‹Рµ РїСЂРѕРґР»РµРЅРёСЏ</div><div class="v good">{renewals_next_base}</div></div>
      <div class="card"><div class="k">Р‘Р°Р·РѕРІС‹Р№ MRR</div><div class="v">{mrr_estimate} в‚Ѕ</div></div>
      <div class="card"><div class="k">Р’РѕР·РІСЂР°С‚С‹ РёСЃС‚РµРєС€РёС… 30 РґРЅРµР№</div><div class="v warn">{winback_30} в‚Ѕ</div></div>
      <div class="card"><div class="k">Р РёСЃРє РїРѕС‚РµСЂРё 30 РґРЅРµР№</div><div class="v bad">{churn_risk} в‚Ѕ</div></div>
    </div>

    <div class="panel">
      <h2>Р”РѕС…РѕРґРЅРѕСЃС‚СЊ РЅР° СЃР»РµРґСѓСЋС‰РёР№ РјРµСЃСЏС†</h2>
      <table>
        <thead><tr><th>РЎС†РµРЅР°СЂРёР№</th><th>РЎС‚Р°РІРєР° РїСЂРѕРґР»РµРЅРёСЏ</th><th>РџСЂРѕРіРЅРѕР· РІС‹СЂСѓС‡РєРё</th></tr></thead>
        <tbody>
          <tr><td>РљРѕРЅСЃРµСЂРІР°С‚РёРІРЅС‹Р№</td><td>60%</td><td>{revenue_next_low} в‚Ѕ</td></tr>
          <tr><td>Р‘Р°Р·РѕРІС‹Р№</td><td>70%</td><td>{revenue_next_base} в‚Ѕ</td></tr>
          <tr><td>РћРїС‚РёРјРёСЃС‚РёС‡РЅС‹Р№</td><td>80%</td><td>{revenue_next_high} в‚Ѕ</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h2>СЃС‚РѕСЂРёС‡РµСЃРєРёРµ РїРѕРєР°Р·Р°С‚РµР»Рё РёР· РєРЅРѕРїРєРё РЎС‚Р°С‚РёСЃС‚РёРєР°</h2>
      <table>
        <thead><tr><th>РџРµСЂРёРѕРґ</th><th>РџРѕР»СЊР·РѕРІР°С‚РµР»Рё</th><th>РџСЂРёР±С‹Р»СЊ</th></tr></thead>
        <tbody>
          <tr><td>РњРµСЃСЏС†</td><td>{fmt_int(int(stats_users_period.get("month", 0)))}</td><td>{stats_profit_month} в‚Ѕ</td></tr>
          <tr><td>РџРѕР»РіРѕРґР°</td><td>{fmt_int(int(stats_users_period.get("half_year", 0)))}</td><td>{stats_profit_half} в‚Ѕ</td></tr>
          <tr><td>Р“РѕРґ</td><td>{fmt_int(int(stats_users_period.get("year", 0)))}</td><td>{stats_profit_year} в‚Ѕ</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h2>РџСЂРѕРіРЅРѕР· РЅР° 6 РјРµСЃСЏС†РµРІ (СЃРєРѕСЂРѕСЃС‚СЊ РїСЂРёСЂРѕСЃС‚Р°)</h2>
      <table>
        <thead><tr><th>РњРµС‚СЂРёРєР°</th><th>Р—РЅР°С‡РµРЅРёРµ</th></tr></thead>
        <tbody>
          <tr><td>РџРµСЂРёРѕРґ РЅР°Р±Р»СЋРґРµРЅРёСЏ</td><td>{obs_days} РґРЅРµР№ (СЃ {obs_start})</td></tr>
          <tr><td>РџРѕРєСЂС‹С‚РёРµ РґР°С‚ СЂРµРіРёСЃС‚СЂР°С†РёРё (РІСЃРµ)</td><td>{reg_cov_all}</td></tr>
          <tr><td>РџРѕРєСЂС‹С‚РёРµ РґР°С‚ СЂРµРіРёСЃС‚СЂР°С†РёРё (РїР»Р°С‚СЏС‰РёРµ)</td><td>{reg_cov_paid}</td></tr>
          <tr><td>РџСЂРёСЂРѕСЃС‚ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№</td><td>{growth_users_day:.2f}/РґРµРЅСЊ</td></tr>
          <tr><td>РџСЂРёСЂРѕСЃС‚ РїР»Р°С‚СЏС‰РёС…</td><td>{growth_paid_day:.2f}/РґРµРЅСЊ</td></tr>
          <tr><td>РџСЂРёСЂРѕСЃС‚ РїРѕРґРїРёСЃРѕРє</td><td>{growth_subs_day:.2f}/РґРµРЅСЊ</td></tr>
          <tr><td>РџРѕР»СЊР·РѕРІР°С‚РµР»Рё С‡РµСЂРµР· 6Рј</td><td>{proj_users_6m}</td></tr>
          <tr><td>РџР»Р°С‚СЏС‰РёРµ С‡РµСЂРµР· 6Рј</td><td>{proj_paid_6m}</td></tr>
          <tr><td>РџРѕРґРїРёСЃРєРё С‡РµСЂРµР· 6Рј</td><td>{proj_subs_6m}</td></tr>
          <tr><td>РџСЂРѕРіРЅРѕР· MRR С‡РµСЂРµР· 6Рј</td><td>{proj_mrr_6m} в‚Ѕ</td></tr>
        </tbody>
      </table>
      <div class="chart-wrap">
        <div class="legend">РџРѕР»СЊР·РѕРІР°С‚РµР»Рё: СЃРїР»РѕС€РЅР°СЏ Р»РёРЅРёСЏ вЂ” РёСЃС‚РѕСЂРёСЏ, РїСѓРЅРєС‚РёСЂ вЂ” РїСЂРѕРіРЅРѕР·</div>
        {users_chart_svg}
      </div>
      <div class="chart-wrap">
        <div class="legend">РџРѕРґРїРёСЃРєРё: СЃРїР»РѕС€РЅР°СЏ Р»РёРЅРёСЏ вЂ” РёСЃС‚РѕСЂРёСЏ, РїСѓРЅРєС‚РёСЂ вЂ” РїСЂРѕРіРЅРѕР·</div>
        {subs_chart_svg}
      </div>
    </div>

    <div class="cols">
      <div class="panel">
        <h2>Р›РѕРєР°С†РёРё (С‚РѕРї 5)</h2>
        <table>
          <thead><tr><th>Р›РѕРєР°С†РёСЏ</th><th>РџРѕРґРїРёСЃРѕРє</th></tr></thead>
          <tbody>{location_rows}</tbody>
        </table>
      </div>
      <div class="panel">
        <h2>РўРѕРї РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ (5)</h2>
        <table>
          <thead><tr><th>User ID</th><th>РџРѕРґРїРёСЃРѕРє</th></tr></thead>
          <tbody>{top_user_rows}</tbody>
        </table>
      </div>
    </div>

    <div class="panel">
      <h2>Р Р°СЃРїСЂРµРґРµР»РµРЅРёРµ СЃСЂРѕРєРѕРІ РїРѕРґРїРёСЃРѕРє</h2>
      <table>
        <thead><tr><th>Р”РёР°РїР°Р·РѕРЅ</th><th>РљРѕР»-РІРѕ</th></tr></thead>
        <tbody>{timing_rows}</tbody>
      </table>
    </div>

    <div class="panel">
      <h2>Р’С‹СЂСѓС‡РєР° next month РїРѕ Р»РѕРєР°С†РёСЏРј (С‚РѕРї 5, 70%)</h2>
      <table>
        <thead><tr><th>Р›РѕРєР°С†РёСЏ</th><th>СЃС‚РµРєР°РµС‚ РІ 30 РґРЅРµР№</th><th>РџСЂРѕРіРЅРѕР· РІС‹СЂСѓС‡РєРё</th></tr></thead>
        <tbody>{due_location_rows}</tbody>
      </table>
    </div>

    <div class="panel">
      <h2>РљСЂРёС‚РёС‡РЅС‹Рµ РёСЃС‚РµС‡РµРЅРёСЏ (0..3 РґРЅСЏ, С‚РѕРї 5)</h2>
      <table>
        <thead><tr><th>User</th><th>Sub</th><th>Р›РѕРєР°С†РёСЏ</th><th>Р”Р°С‚Р°</th><th>Р”РЅРµР№ РґРѕ РєРѕРЅС†Р°</th></tr></thead>
        <tbody>{build_expiration_rows(expiring_3, limit=5)}</tbody>
      </table>
    </div>

    <div class="panel">
      <h2>СЃС‚РµС‡РµРЅРёСЏ 0..30 РґРЅРµР№ (С‚РѕРї 5)</h2>
      <table>
        <thead><tr><th>User</th><th>Sub</th><th>Р›РѕРєР°С†РёСЏ</th><th>Р”Р°С‚Р°</th><th>Р”РЅРµР№ РґРѕ РєРѕРЅС†Р°</th></tr></thead>
        <tbody>{build_expiration_rows(expiring_30, limit=5)}</tbody>
      </table>
    </div>

    <div class="panel">
      <h2>РЈР¶Рµ РёСЃС‚РµРєР»Рё (С‚РѕРї 5)</h2>
      <table>
        <thead><tr><th>User</th><th>Sub</th><th>Р›РѕРєР°С†РёСЏ</th><th>Р”Р°С‚Р°</th><th>Р”РЅРµР№ РґРѕ РєРѕРЅС†Р°</th></tr></thead>
        <tbody>{build_expiration_rows(expired, limit=5)}</tbody>
      </table>
    </div>

    <div class="panel" id="admin">
      <h2>РђРґРјРёРЅ-СЃР°Р№С‚: РїРѕР»СЊР·РѕРІР°С‚РµР»Рё, С„РёР»СЊС‚СЂС‹ Рё Р±С‹СЃС‚СЂС‹Р№ СЂР°Р·Р±РѕСЂ Р±Р°Р·С‹</h2>
      <div class="admin-shell">
        <div class="side-nav">
          <button class="nav-btn active" data-tab="users">РџРѕР»СЊР·РѕРІР°С‚РµР»Рё</button>
          <button class="nav-btn" data-tab="attention">РќСѓР¶РЅРѕ РІРЅРёРјР°РЅРёРµ</button>
          <button class="nav-btn" data-tab="segments">РЎРµРіРјРµРЅС‚С‹</button>
          <button class="nav-btn" data-tab="forecast">РџСЂРѕРіРЅРѕР·</button>
          <button class="nav-btn" data-tab="processes">РџСЂРѕС†РµСЃСЃС‹</button>
          <button class="nav-btn" data-tab="unresolved">РќРµСЂР°Р·РѕР±СЂР°РЅРЅРѕРµ</button>
        </div>
        <div>
          <section class="tab-panel active" data-panel="users">
            <div class="toolbar">
              <input id="adminSearch" placeholder="РџРѕРёСЃРє: ID, username, Р»РѕРєР°С†РёСЏ, С‚РµРєСЃС‚ РєР°СЂС‚РѕС‡РєРё">
              <select id="adminStatus">
                <option value="all">Р’СЃРµ СЃС‚Р°С‚СѓСЃС‹</option>
                <option value="active">РђРєС‚РёРІРЅС‹Рµ</option>
                <option value="expiring_7">СЃС‚РµРєР°СЋС‚ Р·Р° 7 РґРЅРµР№</option>
                <option value="expiring_30">СЃС‚РµРєР°СЋС‚ Р·Р° 30 РґРЅРµР№</option>
                <option value="expired">СЃС‚РµРєС€РёРµ</option>
                <option value="no_subs">Р‘РµР· РїРѕРґРїРёСЃРєРё</option>
                <option value="unknown_date">Р”Р°С‚Р° РЅРµРёР·РІРµСЃС‚РЅР°</option>
              </select>
              <select id="adminSort">
                <option value="risk">РЎРЅР°С‡Р°Р»Р° СЂРёСЃРє</option>
                <option value="subs">Р‘РѕР»СЊС€Рµ РїРѕРґРїРёСЃРѕРє</option>
                <option value="new">РќРѕРІС‹Рµ СЂРµРіРёСЃС‚СЂР°С†РёРё</option>
                <option value="id">ID РїРѕ РІРѕР·СЂР°СЃС‚Р°РЅРёСЋ</option>
              </select>
              <select id="adminLocation"><option value="all">Р›РѕРєР°С†РёРё: РІСЃРµ</option></select>
              <select id="adminRegMonth"><option value="all">: </option></select>
              <select id="adminPageSize">
                <option value="25">25 / .</option>
                <option value="50">50 / .</option>
                <option value="100">100 / .</option>
              </select>
            </div>
            <div class="filter-row">
              <button class="filter-btn active" data-status="all">Р’СЃРµ</button>
              <button class="filter-btn" data-status="no_subs">Р‘РµР· РїРѕРґРїРёСЃРєРё</button>
              <button class="filter-btn" data-status="expired">СЃС‚РµРєС€РёРµ</button>
              <button class="filter-btn" data-status="expiring_7">7 РґРЅРµР№</button>
              <button class="filter-btn" data-status="expiring_30">30 РґРЅРµР№</button>
              <button class="filter-btn" data-status="active">РђРєС‚РёРІРЅС‹Рµ</button>
            </div>
            <div class="muted" id="adminCount"></div>
            <div class="admin-kpis" id="adminKpis"></div>
            <div class="action-panel">
              <h2>Р‘С‹СЃС‚СЂРѕРµ СѓРїСЂР°РІР»РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»РµРј</h2>
              <div class="action-grid">
                <input id="actionUser" placeholder="ID РёР»Рё @username">
                <textarea id="actionMessage" placeholder="РўРµРєСЃС‚ СЃРѕРѕР±С‰РµРЅРёСЏ (РґР»СЏ Mail Рё РґРѕРїРёСЃРєРё РІ Wizard)"></textarea>
              </div>
              <div class="action-buttons">
                <button class="action-btn" id="actionUserStatus">РЎС‚Р°С‚СѓСЃ РёР· Р±Р°Р·С‹</button>
                <button class="action-btn primary" id="actionMail">РћС‚РїСЂР°РІРёС‚СЊ Mail</button>
                <button class="action-btn" id="actionBroadcast">Р Р°СЃСЃС‹Р»РєР° Р±РµР· РїРѕРґРїРёСЃРєРё</button>
                <button class="action-btn good" id="actionPromo">РџСЂРѕРјРѕРєРѕРґ + Mail</button>
                <button class="action-btn" id="actionReplaceKey">Р—Р°РјРµРЅРёС‚СЊ РєР»СЋС‡</button>
                <button class="action-btn warn" id="actionDeleteAccess">РЎРЅСЏС‚СЊ РґРѕСЃС‚СѓРї</button>
                <button class="action-btn good" id="actionWizardCard">РљР°СЂС‚РѕС‡РєР° РІ Wizard</button>
                <button class="action-btn warn" id="actionWizardText">РўРµРєСЃС‚ РІ Wizard</button>
              </div>
              <div class="action-status" id="actionStatus">Р“РѕС‚РѕРІРѕ. Р’С‹Р±РµСЂРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ, РїСЂРё РЅРµРѕР±С…РѕРґРёРјРѕСЃС‚Рё РґРѕР±Р°РІСЊ С‚РµРєСЃС‚ Рё Р·Р°РїСѓСЃС‚Рё РЅСѓР¶РЅРѕРµ РґРµР№СЃС‚РІРёРµ.</div>
            </div>
            <div class="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>ID</th><th>Username</th><th>Р РµРіРёСЃС‚СЂР°С†РёСЏ</th><th>РџРѕРґРїРёСЃРѕРє</th><th>Р›РѕРєР°С†РёРё</th><th>Р‘Р»РёР¶Р°Р№С€РµРµ РѕРєРѕРЅС‡Р°РЅРёРµ</th><th>РЎС‚Р°С‚СѓСЃ</th>
                  </tr>
                </thead>
                <tbody id="adminUsersBody"></tbody>
              </table>
            </div>
            <div class="pager">
              <button class="pager-btn" id="adminPrev"></button>
              <div class="muted" id="adminPageInfo"></div>
              <button class="pager-btn" id="adminNext">Р”Р°Р»РµРµ</button>
            </div>
          </section>
          <section class="tab-panel" data-panel="attention">
            <div class="cols">
              <div class="panel">
                <h2>РџРµСЂРІС‹Рµ РЅР° СЃРІСЏР·СЊ</h2>
                <p class="muted">РџРѕР»СЊР·РѕРІР°С‚РµР»Рё СЃ РёСЃС‚РµРєС€РёРјРё РёР»Рё РїРѕС‡С‚Рё РёСЃС‚РµРєС€РёРјРё РїРѕРґРїРёСЃРєР°РјРё. С… РІС‹РіРѕРґРЅРµРµ РѕР±СЂР°Р±РѕС‚Р°С‚СЊ РїРµСЂРІС‹РјРё.</p>
                <div class="table-scroll"><table><thead><tr><th>ID</th><th>Username</th><th>Р”Р°С‚Р°</th><th>РЎС‚Р°С‚СѓСЃ</th></tr></thead><tbody id="attentionBody"></tbody></table></div>
              </div>
              <div class="panel">
                <h2>Р‘РµР· РїРѕРґРїРёСЃРєРё</h2>
                <p class="muted">Р“РѕС‚РѕРІР°СЏ Р°СѓРґРёС‚РѕСЂРёСЏ РґР»СЏ Р°РєРєСѓСЂР°С‚РЅРѕР№ СЂР°СЃСЃС‹Р»РєРё `/broadcast`.</p>
                <div class="table-scroll"><table><thead><tr><th>ID</th><th>Username</th><th>Р РµРіРёСЃС‚СЂР°С†РёСЏ</th></tr></thead><tbody id="noSubsBody"></tbody></table></div>
              </div>
            </div>
          </section>
          <section class="tab-panel" data-panel="segments">
            <div class="cols">
              <div class="panel">
                <h2>РЎС‚Р°С‚СѓСЃС‹ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№</h2>
                <table><tbody id="statusSegmentsBody"></tbody></table>
              </div>
              <div class="panel">
                <h2>Р§С‚Рѕ РґРµР»Р°С‚СЊ РґР°Р»СЊС€Рµ</h2>
                <table>
                  <tbody>
                    <tr><td>СЃС‚РµРєС€РёРµ</td><td>РџСЂРµРґР»РѕР¶РёС‚СЊ РїСЂРѕРјРѕРєРѕРґ РёР»Рё Р·Р°РјРµРЅСѓ РєР»СЋС‡Р° С‡РµСЂРµР· wizard.</td></tr>
                    <tr><td>0..7 РґРЅРµР№</td><td>РќР°РїРѕРјРЅРёС‚СЊ Рѕ РїСЂРѕРґР»РµРЅРёРё РґРѕ РѕРєРѕРЅС‡Р°РЅРёСЏ СЃСЂРѕРєР°.</td></tr>
                    <tr><td>Р‘РµР· РїРѕРґРїРёСЃРєРё</td><td>Р—Р°РїСѓСЃС‚РёС‚СЊ РјСЏРіРєСѓСЋ СЂР°СЃСЃС‹Р»РєСѓ С‡РµСЂРµР· `/broadcast`.</td></tr>
                    <tr><td>Р”Р°С‚Р° РЅРµРёР·РІРµСЃС‚РЅР°</td><td>РџРµСЂРµСЃРєР°РЅРёСЂРѕРІР°С‚СЊ РёР»Рё РїСЂРѕРІРµСЂРёС‚СЊ С‚РѕС‡РµС‡РЅРѕ С‡РµСЂРµР· `/subs &lt;id&gt;`.</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
          </section>
          <section class="tab-panel" data-panel="forecast">
            <div class="grid">
              <div class="card"><div class="k">Р§РµСЂРµР· РјРµСЃСЏС†</div><div class="v good">{profit_m1} в‚Ѕ</div></div>
              <div class="card"><div class="k">Р§РµСЂРµР· РїРѕР»РіРѕРґР°</div><div class="v good">{profit_m6} в‚Ѕ</div></div>
              <div class="card"><div class="k">Р§РµСЂРµР· РіРѕРґ</div><div class="v good">{profit_y1} в‚Ѕ</div></div>
              <div class="card"><div class="k">РџРѕРґРїРёСЃРѕРє С‡РµСЂРµР· 6Рј</div><div class="v">{proj_subs_6m}</div></div>
            </div>
            <p class="muted">РџСЂРѕРіРЅРѕР· СЃС‚СЂРѕРёС‚СЃСЏ РёР· РёСЃС‚РѕСЂРёРё СЂРµРіРёСЃС‚СЂР°С†РёРё, С‚РµРєСѓС‰РёС… РїРѕРґРїРёСЃРѕРє, СЃСЂРѕРєРѕРІ РѕРєРѕРЅС‡Р°РЅРёСЏ Рё СЃС‚Р°С‚РёСЃС‚РёРєРё РїСЂРёР±С‹Р»Рё РёР· Р°РґРјРёРЅ-Р±РѕС‚Р°.</p>
          </section>
          <section class="tab-panel" data-panel="processes">
            <div class="grid" id="processCards"></div>
            <div class="cols">
              <div class="panel">
                <h2>Р–РёРІРѕРµ СЃРѕСЃС‚РѕСЏРЅРёРµ</h2>
                <table><tbody id="processStateBody"></tbody></table>
              </div>
              <div class="panel">
                <h2>Р§С‚Рѕ СЃРµР№С‡Р°СЃ Р·Р°РЅСЏС‚Рѕ</h2>
                <table><tbody id="processMetaBody"></tbody></table>
              </div>
            </div>
            <div class="panel">
              <h2>РћР±РЅРѕРІР»РµРЅРёРµ</h2>
              <div class="muted" id="processRefreshInfo">РџР°РЅРµР»СЊ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё РїРѕРґС‚СЏРіРёРІР°РµС‚ Р¶РёРІРѕРµ СЃРѕСЃС‚РѕСЏРЅРёРµ admin flow, scan, GPT Рё pending-РѕС‡РµСЂРµРґРµР№.</div>
              <div class="action-buttons" style="margin-top:12px;">
                <button class="action-btn" id="actionPauseScan">РџР°СѓР·Р° scan</button>
                <button class="action-btn warn" id="actionStopMail2">РћСЃС‚Р°РЅРѕРІРёС‚СЊ mail2</button>
              </div>
            </div>
          </section>
          <section class="tab-panel" data-panel="unresolved">
            <div class="grid">
              <div class="card"><div class="k">РћС‚РєСЂС‹С‚Рѕ СЃР»СѓС‡Р°РµРІ</div><div class="v warn" id="unresolvedOpenCount">0</div></div>
              <div class="card"><div class="k">РџРѕСЃР»РµРґРЅРµРµ РѕР±РЅРѕРІР»РµРЅРёРµ</div><div class="v" id="overviewGeneratedAt">-</div></div>
            </div>
            <div class="panel">
              <h2>РќРµСЂР°Р·РѕР±СЂР°РЅРЅС‹Рµ РѕР±СЂР°С‰РµРЅРёСЏ</h2>
              <p class="muted">Р—РґРµСЃСЊ РІРёРґРЅС‹ СЃР»СѓС‡Р°Рё, РіРґРµ Р±РѕС‚ РЅРµ СЃРјРѕРі СѓРІРµСЂРµРЅРЅРѕ Р·Р°РєСЂС‹С‚СЊ РІРѕРїСЂРѕСЃ СЃР°Рј.</p>
              <div class="table-scroll">
                <table>
                  <thead><tr><th>ID</th><th>Р’СЂРµРјСЏ</th><th>РџСЂРёС‡РёРЅР°</th><th>РћС‚РєСѓРґР°</th><th>РћС‚РїСЂР°РІРёС‚РµР»СЊ</th><th>РџСЂРµРІСЊСЋ</th></tr></thead>
                  <tbody id="unresolvedBody"></tbody>
                </table>
              </div>
            </div>
          </section>
        </div>
      </div>
    </div>
    <script>
      const adminUsers = {admin_users_json};
      let adminOverview = {admin_overview_json};
      const riskRank = {{expired: 0, expiring_7: 1, expiring_30: 2, unknown_date: 3, no_subs: 4, active: 5}};
      const body = document.getElementById("adminUsersBody");
      const count = document.getElementById("adminCount");
      const search = document.getElementById("adminSearch");
      const status = document.getElementById("adminStatus");
      const sort = document.getElementById("adminSort");
      const locationFilter = document.getElementById("adminLocation");
      const regMonthFilter = document.getElementById("adminRegMonth");
      const pageSizeSelect = document.getElementById("adminPageSize");
      const kpis = document.getElementById("adminKpis");
      const pageInfo = document.getElementById("adminPageInfo");
      const prevButton = document.getElementById("adminPrev");
      const nextButton = document.getElementById("adminNext");
      const actionUser = document.getElementById("actionUser");
      const actionMessage = document.getElementById("actionMessage");
      const actionStatus = document.getElementById("actionStatus");
      const actionUserStatusButton = document.getElementById("actionUserStatus");
      const actionMailButton = document.getElementById("actionMail");
      const actionBroadcastButton = document.getElementById("actionBroadcast");
      const actionPromoButton = document.getElementById("actionPromo");
      const actionReplaceKeyButton = document.getElementById("actionReplaceKey");
      const actionDeleteAccessButton = document.getElementById("actionDeleteAccess");
      const actionWizardCardButton = document.getElementById("actionWizardCard");
      const actionWizardTextButton = document.getElementById("actionWizardText");
      const actionPauseScanButton = document.getElementById("actionPauseScan");
      const actionStopMail2Button = document.getElementById("actionStopMail2");
      const processCards = document.getElementById("processCards");
      const processStateBody = document.getElementById("processStateBody");
      const processMetaBody = document.getElementById("processMetaBody");
      const processRefreshInfo = document.getElementById("processRefreshInfo");
      const unresolvedOpenCount = document.getElementById("unresolvedOpenCount");
      const overviewGeneratedAt = document.getElementById("overviewGeneratedAt");
      const unresolvedBody = document.getElementById("unresolvedBody");
      const actionApiBase = "admin-api";
      let operatorUserInput = null;
      let operatorMessageInput = null;
      let operatorStatusBox = null;
      let currentPage = 1;
      let activeJobId = "";
      let activeJobPollTimer = null;
      let overviewRefreshTimer = null;
      const statusLabels = {{
        active: "РђРєС‚РёРІРЅС‹Рµ",
        expiring_7: "СЃС‚РµРєР°СЋС‚ Р·Р° 7 РґРЅРµР№",
        expiring_30: "СЃС‚РµРєР°СЋС‚ Р·Р° 30 РґРЅРµР№",
        expired: "СЃС‚РµРєС€РёРµ",
        no_subs: "Р‘РµР· РїРѕРґРїРёСЃРєРё",
        unknown_date: "Р”Р°С‚Р° РЅРµРёР·РІРµСЃС‚РЅР°"
      }};

      function escapeText(value) {{
        return String(value ?? "").replace(/[&<>"']/g, ch => ({{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}}[ch]));
      }}

      function initOperatorMode() {{
        const sideNav = document.querySelector(".side-nav");
        const usersPanel = document.querySelector('[data-panel="users"]');
        if (!sideNav || !usersPanel) return;

        const opButton = document.createElement("button");
        opButton.className = "nav-btn active";
        opButton.dataset.tab = "operator";
        opButton.textContent = "РћРїРµСЂР°С‚РѕСЂ";
        sideNav.insertBefore(opButton, sideNav.firstChild);

        const usersTabButton = sideNav.querySelector('[data-tab="users"]');
        if (usersTabButton) usersTabButton.classList.remove("active");
        usersPanel.classList.remove("active");

        const operatorPanel = document.createElement("section");
        operatorPanel.className = "tab-panel active";
        operatorPanel.dataset.panel = "operator";
        operatorPanel.innerHTML = `
          <div class="grid">
            <div class="card"><div class="k">РћС‚РєСЂС‹С‚С‹С… РѕР±СЂР°С‰РµРЅРёР№</div><div class="v warn" id="opUnresolvedCount">0</div></div>
            <div class="card"><div class="k">РЎС‚РµРєР°СЋС‚ Р·Р° 7 РґРЅРµР№</div><div class="v warn" id="opExpiring7">0</div></div>
            <div class="card"><div class="k">Р‘РµР· РїРѕРґРїРёСЃРєРё</div><div class="v" id="opNoSubs">0</div></div>
            <div class="card"><div class="k">РџРѕРґРїРёСЃРѕРє РІСЃРµРіРѕ</div><div class="v" id="opSubsTotal">0</div></div>
          </div>
          <div class="action-panel">
            <h2>Р‘С‹СЃС‚СЂС‹Рµ РґРµР№СЃС‚РІРёСЏ РѕРїРµСЂР°С‚РѕСЂР°</h2>
            <div class="action-grid">
              <input id="opUser" placeholder="ID РёР»Рё @username">
              <textarea id="opMessage" placeholder="РўРµРєСЃС‚ РґР»СЏ СЃРѕРѕР±С‰РµРЅРёСЏ РёР»Рё Wizard"></textarea>
            </div>
            <div class="action-buttons">
              <button class="action-btn" id="opUserStatus">РЎС‚Р°С‚СѓСЃ</button>
              <button class="action-btn primary" id="opMail">Mail</button>
              <button class="action-btn good" id="opWizardCard">Wizard РєР°СЂС‚РѕС‡РєР°</button>
              <button class="action-btn warn" id="opWizardText">Wizard С‚РµРєСЃС‚</button>
              <button class="action-btn" id="opPromo">РџСЂРѕРјРѕ</button>
              <button class="action-btn warn" id="opDeleteAccess">РЎРЅСЏС‚СЊ РґРѕСЃС‚СѓРї</button>
            </div>
            <div class="action-status" id="opStatus">Р—Р°РїРѕР»РЅРё ID Рё РЅСѓР¶РЅС‹Р№ С‚РµРєСЃС‚, Р·Р°С‚РµРј РІС‹Р±РµСЂРё РґРµР№СЃС‚РІРёРµ.</div>
          </div>
          <div class="panel">
            <h2>Р“РѕСЂСЏС‡РёРµ РїРµСЂРµС…РѕРґС‹</h2>
            <div class="action-buttons">
              <button class="action-btn" id="gotoUsers">РџРѕР»РЅР°СЏ Р±Р°Р·Р° РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№</button>
              <button class="action-btn" id="gotoAttention">Р—РѕРЅР° СЂРёСЃРєР°</button>
              <button class="action-btn" id="gotoProcesses">РџСЂРѕС†РµСЃСЃС‹ Р±РѕС‚Р°</button>
              <button class="action-btn" id="gotoUnresolved">РќРµСЂР°Р·РѕР±СЂР°РЅРЅРѕРµ</button>
            </div>
          </div>
        `;
        usersPanel.parentElement.insertBefore(operatorPanel, usersPanel);
        operatorUserInput = operatorPanel.querySelector("#opUser");
        operatorMessageInput = operatorPanel.querySelector("#opMessage");
        operatorStatusBox = operatorPanel.querySelector("#opStatus");

        const copyToMain = () => {{
          actionUser.value = String(operatorUserInput.value || "").trim();
          actionMessage.value = String(operatorMessageInput.value || "").trim();
        }};
        const run = (actionName, needUser, needMessage) => {{
          copyToMain();
          submitDashboardAction(actionName, needUser, needMessage);
          if (operatorStatusBox) operatorStatusBox.textContent = actionStatus.textContent;
        }};

        operatorPanel.querySelector("#opUserStatus").addEventListener("click", () => run("user_status", true, false));
        operatorPanel.querySelector("#opMail").addEventListener("click", () => run("mail", true, true));
        operatorPanel.querySelector("#opWizardCard").addEventListener("click", () => run("wizard_card", true, false));
        operatorPanel.querySelector("#opWizardText").addEventListener("click", () => run("wizard_text", false, true));
        operatorPanel.querySelector("#opPromo").addEventListener("click", () => run("promo", true, false));
        operatorPanel.querySelector("#opDeleteAccess").addEventListener("click", () => run("delete_access", true, false));

        operatorPanel.querySelector("#gotoUsers").addEventListener("click", () => document.querySelector('[data-tab="users"]')?.click());
        operatorPanel.querySelector("#gotoAttention").addEventListener("click", () => document.querySelector('[data-tab="attention"]')?.click());
        operatorPanel.querySelector("#gotoProcesses").addEventListener("click", () => document.querySelector('[data-tab="processes"]')?.click());
        operatorPanel.querySelector("#gotoUnresolved").addEventListener("click", () => document.querySelector('[data-tab="unresolved"]')?.click());
      }}

      function numericId(value) {{
        const parsed = Number.parseInt(String(value || "0"), 10);
        return Number.isFinite(parsed) ? parsed : 0;
      }}

      function registrationMonth(value) {{
        const text = String(value || "").trim();
        return /^\\d{{4}}-\\d{{2}}/.test(text) ? text.slice(0, 7) : "";
      }}

      function fillDynamicFilters() {{
        const locations = new Set();
        const months = new Set();
        adminUsers.forEach(row => {{
          String(row.locations || "").split(",").map(item => item.trim()).filter(Boolean).forEach(item => locations.add(item));
          const month = registrationMonth(row.registration_date);
          if (month) months.add(month);
        }});
        [...locations].sort((a, b) => a.localeCompare(b)).forEach(location => {{
          locationFilter.insertAdjacentHTML("beforeend", `<option value="${{escapeText(location)}}">${{escapeText(location)}}</option>`);
        }});
        [...months].sort().reverse().forEach(month => {{
          regMonthFilter.insertAdjacentHTML("beforeend", `<option value="${{month}}">${{month}}</option>`);
        }});
      }}

      function sortedRows(rows) {{
        const mode = sort.value;
        return [...rows].sort((a, b) => {{
          if (mode === "subs") return Number(b.subscriptions || 0) - Number(a.subscriptions || 0);
          if (mode === "new") return String(b.registration_date || "").localeCompare(String(a.registration_date || ""));
          if (mode === "id") return numericId(a.user_id) - numericId(b.user_id);
          return (riskRank[a.status] ?? 99) - (riskRank[b.status] ?? 99)
            || Number(a.days_left || 999999) - Number(b.days_left || 999999);
        }});
      }}

      function filteredRows() {{
        const q = search.value.trim().toLowerCase();
        const selectedStatus = status.value;
        const selectedLocation = locationFilter.value;
        const selectedMonth = regMonthFilter.value;
        return sortedRows(adminUsers.filter(row => {{
          const statusOk = selectedStatus === "all" || row.status === selectedStatus;
          const searchOk = !q || String(row.search || "").toLowerCase().includes(q);
          const locationOk = selectedLocation === "all" || String(row.locations || "").split(",").map(item => item.trim()).includes(selectedLocation);
          const monthOk = selectedMonth === "all" || registrationMonth(row.registration_date) === selectedMonth;
          return statusOk && searchOk && locationOk && monthOk;
        }}));
      }}

      function renderUsers() {{
        const rows = filteredRows();
        count.textContent = `РџРѕРєР°Р·Р°РЅРѕ ${{rows.length}} РёР· ${{adminUsers.length}} РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№`;
        body.innerHTML = rows.slice(0, 300).map(row => `
          <tr>
            <td>${{escapeText(row.user_id)}}</td>
            <td>${{row.username ? "@" + escapeText(row.username) : "<span class='muted'>РЅРµС‚</span>"}}</td>
            <td>${{escapeText(row.registration_date || "-")}}</td>
            <td>${{escapeText(row.subscriptions)}}</td>
            <td>${{escapeText(row.locations || "-")}}</td>
            <td>${{escapeText(row.nearest_expiration || "-")}} ${{row.days_left !== "" ? "(" + escapeText(row.days_left) + " РґРЅ.)" : ""}}</td>
            <td><span class="status-pill ${{escapeText(row.status)}}">${{escapeText(row.status_label)}}</span></td>
          </tr>
        `).join("") || "<tr><td colspan='7'>РќРµС‚ РґР°РЅРЅС‹С…</td></tr>";
      }}

      function renderKpis(rows) {{
        const total = rows.length;
        const paid = rows.filter(row => Number(row.subscriptions || 0) > 0).length;
        const urgent = rows.filter(row => row.status === "expired" || row.status === "expiring_7").length;
        const noSubs = rows.filter(row => row.status === "no_subs").length;
        kpis.innerHTML = `
          <div class="admin-kpi"><span class="muted"></span><b>${{total}}</b></div>
          <div class="admin-kpi"><span class="muted"> </span><b>${{paid}}</b></div>
          <div class="admin-kpi"><span class="muted"></span><b>${{urgent}}</b></div>
          <div class="admin-kpi"><span class="muted">РµР· РїРѕРґРїРёСЃРєРё</span><b>${{noSubs}}</b></div>
        `;
      }}

      function renderUsersEnhanced() {{
        const rows = filteredRows();
        const pageSize = Math.max(1, Number.parseInt(pageSizeSelect.value || "25", 10) || 25);
        const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
        currentPage = Math.max(1, Math.min(currentPage, totalPages));
        const start = (currentPage - 1) * pageSize;
        const pageRows = rows.slice(start, start + pageSize);
        count.textContent = ` ${{rows.length}}  ${{adminUsers.length}} `;
        pageInfo.textContent = ` ${{currentPage}}  ${{totalPages}}`;
        prevButton.disabled = currentPage <= 1;
        nextButton.disabled = currentPage >= totalPages;
        renderKpis(rows);
        const opExpiring7 = document.getElementById("opExpiring7");
        const opNoSubs = document.getElementById("opNoSubs");
        const opSubsTotal = document.getElementById("opSubsTotal");
        if (opExpiring7) opExpiring7.textContent = String(adminUsers.filter(row => row.status === "expiring_7").length);
        if (opNoSubs) opNoSubs.textContent = String(adminUsers.filter(row => row.status === "no_subs").length);
        if (opSubsTotal) opSubsTotal.textContent = String(adminUsers.reduce((acc, row) => acc + (Number(row.subscriptions || 0) || 0), 0));
        body.innerHTML = pageRows.map(row => `
          <tr data-user-id="${{escapeText(row.user_id)}}">
            <td>${{escapeText(row.user_id)}}</td>
            <td>${{row.username ? "@" + escapeText(row.username) : "<span class='muted'></span>"}}</td>
            <td>${{escapeText(row.registration_date || "-")}}</td>
            <td>${{escapeText(row.subscriptions)}}</td>
            <td>${{escapeText(row.locations || "-")}}</td>
            <td>${{escapeText(row.nearest_expiration || "-")}} ${{row.days_left !== "" ? "(" + escapeText(row.days_left) + " .)" : ""}}</td>
            <td><span class="status-pill ${{escapeText(row.status)}}">${{escapeText(row.status_label)}}</span></td>
          </tr>
        `).join("") || "<tr><td colspan='7'> </td></tr>";
      }}

      function renderAttention() {{
        const attention = sortedRows(adminUsers.filter(row => ["expired", "expiring_7", "expiring_30"].includes(row.status))).slice(0, 25);
        document.getElementById("attentionBody").innerHTML = attention.map(row => `
          <tr><td>${{escapeText(row.user_id)}}</td><td>${{row.username ? "@" + escapeText(row.username) : "-"}}</td><td>${{escapeText(row.nearest_expiration || "-")}}</td><td>${{escapeText(row.status_label)}}</td></tr>
        `).join("") || "<tr><td colspan='4'>РќРµС‚ СЃСЂРѕС‡РЅС‹С… РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№</td></tr>";
        const noSubs = adminUsers.filter(row => row.status === "no_subs").slice(0, 25);
        document.getElementById("noSubsBody").innerHTML = noSubs.map(row => `
          <tr><td>${{escapeText(row.user_id)}}</td><td>${{row.username ? "@" + escapeText(row.username) : "-"}}</td><td>${{escapeText(row.registration_date || "-")}}</td></tr>
        `).join("") || "<tr><td colspan='3'>РќРµС‚ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ Р±РµР· РїРѕРґРїРёСЃРєРё</td></tr>";
      }}

      function renderSegments() {{
        const counts = adminUsers.reduce((acc, row) => {{
          acc[row.status] = (acc[row.status] || 0) + 1;
          return acc;
        }}, {{}});
        document.getElementById("statusSegmentsBody").innerHTML = Object.keys(statusLabels).map(key => `
          <tr><td>${{escapeText(statusLabels[key])}}</td><td>${{escapeText(counts[key] || 0)}}</td></tr>
        `).join("");
      }}

      function renderProcesses() {{
        const processes = adminOverview.processes || {{}};
        processCards.innerHTML = `
          <div class="card"><div class="k">Admin flow</div><div class="v">${{escapeText(processes.admin_flow || "-")}}</div></div>
          <div class="card"><div class="k">Scan</div><div class="v">${{processes.scan_active ? "РђРєС‚РёРІРµРЅ" : "РЎРІРѕР±РѕРґРµРЅ"}}</div></div>
          <div class="card"><div class="k">Mail2</div><div class="v">${{processes.mail2_active ? "РђРєС‚РёРІРЅР°" : "РЎРІРѕР±РѕРґРЅР°"}}</div></div>
          <div class="card"><div class="k">GPT</div><div class="v">${{escapeText(processes.gpt_active || 0)}} active / ${{escapeText(processes.gpt_pending || 0)}} pending</div></div>
        `;
        processStateBody.innerHTML = `
          <tr><td>Admin bot</td><td>${{escapeText(processes.admin_bot || "-")}}</td></tr>
          <tr><td>Scan checkpoint</td><td>${{escapeText(processes.scan_checkpoint || "-")}}</td></tr>
          <tr><td>Scan owner</td><td>${{escapeText(processes.scan_owner_id || "-")}}</td></tr>
          <tr><td>Scan delay</td><td>${{escapeText(processes.scan_delay_seconds || 0)}}s</td></tr>
          <tr><td>Auto-resume</td><td>${{processes.scan_auto_resume ? "Р”Р°" : "РќРµС‚"}}</td></tr>
        `;
        processMetaBody.innerHTML = `
          <tr><td>Wizard pending</td><td>${{escapeText(processes.wizard_pending || 0)}}</td></tr>
          <tr><td>Mail2 pending</td><td>${{escapeText(processes.mail2_pending || 0)}}</td></tr>
          <tr><td>Smart pending</td><td>${{escapeText(processes.smart_pending || 0)}}</td></tr>
          <tr><td>Pending TTL</td><td>${{escapeText(processes.pending_ttl_seconds || 0)}}s</td></tr>
          <tr><td>РЎР»РµРґСѓСЋС‰РёР№ user_id</td><td>${{escapeText(processes.scan_next_user_id || "-")}}</td></tr>
        `;
        processRefreshInfo.textContent = `РџРѕСЃР»РµРґРЅРµРµ РѕР±РЅРѕРІР»РµРЅРёРµ: ${{escapeText(adminOverview.generated_at || "-")}}`;
      }}

      function renderUnresolved() {{
        unresolvedOpenCount.textContent = escapeText(adminOverview.unresolved_open_count || 0);
        const opUnresolvedCount = document.getElementById("opUnresolvedCount");
        if (opUnresolvedCount) opUnresolvedCount.textContent = String(adminOverview.unresolved_open_count || 0);
        overviewGeneratedAt.textContent = escapeText(adminOverview.generated_at || "-");
        const rows = Array.isArray(adminOverview.unresolved_rows) ? adminOverview.unresolved_rows : [];
        unresolvedBody.innerHTML = rows.map(row => `
          <tr>
            <td>#${{escapeText(row.id)}}</td>
            <td>${{escapeText(row.created_at || "-")}}</td>
            <td>${{escapeText(row.reason_label || row.reason || "-")}}</td>
            <td>${{escapeText(row.source || "-")}}</td>
            <td>${{escapeText(row.sender_id || "-")}}${{row.sender_username ? " (@" + escapeText(row.sender_username) + ")" : ""}}</td>
            <td>${{escapeText(row.question_preview || "-")}}</td>
          </tr>
        `).join("") || "<tr><td colspan='6'>РќРµС‚ РѕС‚РєСЂС‹С‚С‹С… РѕР±СЂР°С‰РµРЅРёР№</td></tr>";
      }}

      async function refreshOverview() {{
        try {{
          const response = await fetch(`${{actionApiBase}}/overview`, {{ cache: "no-store" }});
          const payload = await response.json();
          if (!response.ok || !payload.ok || !payload.overview) return;
          adminOverview = payload.overview;
          renderProcesses();
          renderUnresolved();
        }} catch (error) {{
          processRefreshInfo.textContent = `РќРµ СѓРґР°Р»РѕСЃСЊ РѕР±РЅРѕРІРёС‚СЊ live-СЃРѕСЃС‚РѕСЏРЅРёРµ: ${{error}}`;
        }} finally {{
          clearTimeout(overviewRefreshTimer);
          overviewRefreshTimer = setTimeout(refreshOverview, 15000);
        }}
      }}

      function setActionBusy(isBusy) {{
        [
          actionUserStatusButton,
          actionMailButton,
          actionBroadcastButton,
          actionPromoButton,
          actionReplaceKeyButton,
          actionDeleteAccessButton,
          actionWizardCardButton,
          actionWizardTextButton,
          actionPauseScanButton,
          actionStopMail2Button,
        ].forEach(button => {{
          if (button) button.disabled = Boolean(isBusy);
        }});
      }}

      function updateActionStatusFromJob(job) {{
        if (!job) {{
          actionStatus.textContent = "РћС€РёР±РєР°: РїСѓСЃС‚РѕР№ РѕС‚РІРµС‚ РѕС‚ API.";
          return;
        }}
        const lines = [
          `РЎС‚Р°С‚СѓСЃ Р·Р°РґР°С‡Рё: ${{job.status || "-"}}`,
          job.id ? `ID Р·Р°РґР°С‡Рё: ${{job.id}}` : "",
          job.resolved_user_id ? `РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ: ${{job.resolved_user_id}}` : "",
          job.error_text ? `РћС€РёР±РєР°: ${{job.error_text}}` : "",
          job.result_text ? `Р РµР·СѓР»СЊС‚Р°С‚: ${{String(job.result_text).slice(0, 500)}}` : "",
        ].filter(Boolean);
        actionStatus.textContent = lines.join("\\n");
        if (operatorStatusBox) operatorStatusBox.textContent = actionStatus.textContent;
      }}

      function stopJobPolling() {{
        if (activeJobPollTimer) {{
          clearTimeout(activeJobPollTimer);
          activeJobPollTimer = null;
        }}
      }}

      async function pollJob(jobId) {{
        stopJobPolling();
        try {{
          const response = await fetch(`${{actionApiBase}}/job/${{encodeURIComponent(jobId)}}`, {{ cache: "no-store" }});
          const payload = await response.json();
          if (!response.ok || !payload.ok) {{
            actionStatus.textContent = "РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕР»СѓС‡РёС‚СЊ СЃС‚Р°С‚СѓСЃ Р·Р°РґР°С‡Рё.";
            setActionBusy(false);
            return;
          }}
          const job = payload.job || {{}};
          updateActionStatusFromJob(job);
          if (job.status === "queued" || job.status === "running") {{
            activeJobPollTimer = setTimeout(() => pollJob(jobId), 1200);
            return;
          }}
          setActionBusy(false);
        }} catch (error) {{
          actionStatus.textContent = `РћС€РёР±РєР° РѕРїСЂРѕСЃР°: ${{error}}`;
          setActionBusy(false);
        }}
      }}

      async function submitDashboardAction(actionName, requireUser, requireMessage) {{
        const user = String(actionUser.value || "").trim();
        const message = String(actionMessage.value || "").trim();
        if (requireUser && !user) {{
          actionStatus.textContent = "РЈРєР°Р¶Рё ID РёР»Рё @username.";
          return;
        }}
        if (requireMessage && !message) {{
          actionStatus.textContent = "Р”РѕР±Р°РІСЊ С‚РµРєСЃС‚ СЃРѕРѕР±С‰РµРЅРёСЏ.";
          return;
        }}
        setActionBusy(true);
        if (operatorStatusBox) operatorStatusBox.textContent = actionStatus.textContent;
        actionStatus.textContent = "Р—Р°РґР°С‡Р° РѕС‚РїСЂР°РІР»РµРЅР°. РћР¶РёРґР°СЋ РѕС‚РІРµС‚...";
        try {{
          const response = await fetch(`${{actionApiBase}}/action`, {{
            method: "POST",
            headers: {{ "Content-Type": "application/json" }},
            body: JSON.stringify({{
              action: actionName,
              user,
              message,
            }}),
          }});
          const payload = await response.json();
          if (!response.ok || !payload.ok || !payload.job || !payload.job.id) {{
            const errorText = payload && payload.error ? payload.error : "unknown_error";
            actionStatus.textContent = `РћС€РёР±РєР° API: ${{errorText}}`;
            setActionBusy(false);
            return;
          }}
          activeJobId = String(payload.job.id || "");
          updateActionStatusFromJob(payload.job);
          pollJob(activeJobId);
        }} catch (error) {{
          actionStatus.textContent = `РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё: ${{error}}`;
          setActionBusy(false);
        }}
      }}

      body.addEventListener("click", event => {{
        const row = event.target.closest("tr[data-user-id]");
        if (!row) return;
        const selectedUserId = String(row.dataset.userId || "").trim();
        if (!selectedUserId) return;
        actionUser.value = selectedUserId;
        actionStatus.textContent = `Р’С‹Р±СЂР°РЅ РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ: ${{selectedUserId}}`;
      }});

      document.querySelectorAll(".nav-btn").forEach(button => {{
        button.addEventListener("click", () => {{
          document.querySelectorAll(".nav-btn").forEach(item => item.classList.remove("active"));
          document.querySelectorAll(".tab-panel").forEach(item => item.classList.remove("active"));
          button.classList.add("active");
          document.querySelector(`[data-panel="${{button.dataset.tab}}"]`).classList.add("active");
        }});
      }});
      document.querySelectorAll(".filter-btn").forEach(button => {{
        button.addEventListener("click", () => {{
          document.querySelectorAll(".filter-btn").forEach(item => item.classList.remove("active"));
          button.classList.add("active");
          status.value = button.dataset.status;
          resetToFirstPageAndRender();
        }});
      }});
      function resetToFirstPageAndRender() {{
        currentPage = 1;
        renderUsersEnhanced();
      }}

      search.addEventListener("input", resetToFirstPageAndRender);
      status.addEventListener("change", resetToFirstPageAndRender);
      sort.addEventListener("change", resetToFirstPageAndRender);
      locationFilter.addEventListener("change", resetToFirstPageAndRender);
      regMonthFilter.addEventListener("change", resetToFirstPageAndRender);
      pageSizeSelect.addEventListener("change", resetToFirstPageAndRender);
      prevButton.addEventListener("click", () => {{ currentPage -= 1; renderUsersEnhanced(); }});
      nextButton.addEventListener("click", () => {{ currentPage += 1; renderUsersEnhanced(); }});
      actionUserStatusButton.addEventListener("click", () => submitDashboardAction("user_status", true, false));
      actionMailButton.addEventListener("click", () => submitDashboardAction("mail", true, true));
      actionBroadcastButton.addEventListener("click", () => submitDashboardAction("broadcast", false, true));
      actionPromoButton.addEventListener("click", () => submitDashboardAction("promo", true, false));
      actionReplaceKeyButton.addEventListener("click", () => submitDashboardAction("replace_key", true, false));
      actionDeleteAccessButton.addEventListener("click", () => submitDashboardAction("delete_access", true, false));
      actionWizardCardButton.addEventListener("click", () => submitDashboardAction("wizard_card", true, false));
      actionWizardTextButton.addEventListener("click", () => submitDashboardAction("wizard_text", false, true));
      actionPauseScanButton.addEventListener("click", () => submitDashboardAction("pause_scan", false, false));
      actionStopMail2Button.addEventListener("click", () => submitDashboardAction("stop_mail2", false, false));
      fillDynamicFilters();
      initOperatorMode();
      renderUsersEnhanced();
      renderAttention();
      renderSegments();
      renderProcesses();
      renderUnresolved();
      refreshOverview();
    </script>

    <div class="panel">
      <h2>Р”РѕРїСѓС‰РµРЅРёСЏ РїСЂРѕРіРЅРѕР·Р°</h2>
      <div class="assumptions">
        Р¦РµРЅР° РїРѕРґРїРёСЃРєРё: <code>{price} в‚Ѕ</code><br>
        Р”РѕС…РѕРґ next month СЃС‡РёС‚Р°РµС‚СЃСЏ С‚РѕР»СЊРєРѕ РїРѕ РїРѕРґРїРёСЃРєР°Рј, С‡РµР№ СЃСЂРѕРє РёСЃС‚РµС‡РµС‚ РІ Р±Р»РёР¶Р°Р№С€РёРµ 30 РґРЅРµР№.<br>
        РџСЂРѕРґР»РµРЅРёСЏ РІ 30 РґРЅРµР№ (Р±Р°Р·Р°): <code>{renew_30_rate}</code><br>
        РџСЂРѕРґР»РµРЅРёРµ РІ 7 РґРЅРµР№: <code>{renew_7_rate}</code><br>
        Р’РѕР·РІСЂР°С‚ РёСЃС‚РµРєС€РёС…: <code>{winback_rate}</code><br>
        Р”Р»СЏ РїРѕРґРїРёСЃРѕРє Р±РµР· РґР°С‚С‹ РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ РґРѕР»СЏ Р°РєС‚РёРІРЅС‹С…: <code>50%</code>
      </div>
    </div>
  </div>
</body>
</html>"""


def save_scan_report(summary_text: str, detailed_text: str, stats: dict) -> tuple[Path, Path, Path, Path]:
    report_dir = Path(settings.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    json_path = report_dir / f"scan-{stamp}.json"
    txt_path = report_dir / f"scan-{stamp}.txt"
    detailed_txt_path = report_dir / f"scan-{stamp}-detailed.txt"
    dashboard_path = report_dir / f"scan-{stamp}-dashboard.html"
    run_id = save_scan_data_to_database(summary_text, detailed_text, stats)
    dashboard_stats = load_latest_scan_stats_from_database() or stats
    dashboard_stats["database"] = {
        "path": str(database_path()),
        "run_id": run_id,
    }
    dashboard_html = build_scan_dashboard_html(dashboard_stats)
    atomic_write_text(json_path, json.dumps(stats, ensure_ascii=False, indent=2))
    atomic_write_text(txt_path, summary_text)
    atomic_write_text(detailed_txt_path, detailed_text)
    atomic_write_text(dashboard_path, dashboard_html)
    public_path, public_url = publish_dashboard_file(dashboard_path, latest_name="latest-scan-dashboard.html")
    stats["dashboard_public_path"] = str(public_path)
    stats["dashboard_public_url"] = public_url
    dashboard_stats["dashboard_public_path"] = str(public_path)
    dashboard_stats["dashboard_public_url"] = public_url
    atomic_write_text(json_path, json.dumps(stats, ensure_ascii=False, indent=2))
    return txt_path, json_path, detailed_txt_path, dashboard_path


def scan_checkpoint_path() -> Path:
    report_dir = Path(settings.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir / "scan-checkpoint.json"


def save_scan_checkpoint(
    page_number: int,
    pages_scanned: int,
    records: list[dict],
    seen_users: set[str],
    status: str = "running",
    next_user_id: int | None = None,
    total_users_hint: int | None = None,
    admin_statistics: dict | None = None,
    scan_errors: list[dict] | None = None,
) -> None:
    checkpoint = {
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "page_number": page_number,
        "pages_scanned": pages_scanned,
        "records": records,
        "seen_users": sorted(seen_users),
        "scan_errors": scan_errors or [],
    }
    if next_user_id is not None:
        checkpoint["next_user_id"] = int(next_user_id)
    if total_users_hint is not None:
        checkpoint["total_users_hint"] = int(total_users_hint)
    if admin_statistics:
        checkpoint["admin_statistics"] = admin_statistics
    atomic_write_text(scan_checkpoint_path(), json.dumps(checkpoint, ensure_ascii=False, indent=2))


def save_scan_checkpoint_best_effort(*args, **kwargs) -> None:
    try:
        save_scan_checkpoint(*args, **kwargs)
    except Exception:
        logging.exception("Failed to save scan checkpoint")


def load_scan_checkpoint() -> dict | None:
    path = scan_checkpoint_path()
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None
        return data
    except Exception:
        logging.exception("Failed to read scan checkpoint")
        return None


def clear_scan_checkpoint() -> None:
    path = scan_checkpoint_path()
    try:
        if path.exists():
            path.unlink()
    except OSError:
        logging.exception("Failed to clear scan checkpoint")


def build_scan_menu_text() -> str:
    checkpoint = load_scan_checkpoint()
    checkpoint_text = "РЅРµС‚"
    if checkpoint:
        next_user_id = int(checkpoint.get("next_user_id") or checkpoint.get("page_number") or 1)
        total_users_hint = int(checkpoint.get("total_users_hint") or 0)
        range_text = f"{next_user_id}" if total_users_hint <= 0 else f"{next_user_id}/{total_users_hint}"
        checkpoint_text = (
            f"ID РїРѕР·РёС†РёСЏ {range_text}, "
            f"РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ {len(checkpoint.get('records') or [])}, "
            f"СЃРѕС…СЂР°РЅРµРЅ {checkpoint.get('saved_at', '-')}"
        )

    report_dir = Path(settings.report_dir)
    recent_reports: list[str] = []
    if report_dir.exists():
        report_files = sorted(
            report_dir.glob("scan-*.txt"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in report_files:
            if path.name.endswith("-detailed.txt"):
                continue
            recent_reports.append(path.name)
            if len(recent_reports) >= 3:
                break

    running_text = "РґР°" if active_scan_cancel_event and not active_scan_cancel_event.is_set() else "РЅРµС‚"
    lines = [
        "РњРµРЅСЋ scan",
        f"РђРєС‚РёРІРЅС‹Р№ scan: {running_text}",
        f"РђРґРјРёРЅ-Р±РѕС‚: {format_admin_bot_health()}",
        f"Checkpoint: {checkpoint_text}",
        "",
        "Р’С‹Р±РµСЂРё РґРµР№СЃС‚РІРёРµ РєРЅРѕРїРєРѕР№ РёР»Рё С†РёС„СЂРѕР№:",
        "1 - РќРѕРІС‹Р№ scan СЃ РїРµСЂРІРѕР№ СЃС‚СЂР°РЅРёС†С‹",
        "2 - РџСЂРѕРґРѕР»Р¶РёС‚СЊ СЃРѕС…СЂР°РЅРµРЅРЅС‹Р№ scan",
        "3 - Stop scan: РїР°СѓР·Р° Рё С‚РµРєСѓС‰РёРµ СЂРµР·СѓР»СЊС‚Р°С‚С‹",
        "4 - Р РµР·СѓР»СЊС‚Р°С‚С‹ scan",
        "5 - РЎР±СЂРѕСЃ СЃРѕС…СЂР°РЅРµРЅРЅРѕРіРѕ scan",
        "6 - РћР±РЅРѕРІРёС‚СЊ СЃС‚Р°С‚СѓСЃ",
        "",
        "РљРѕРјР°РЅРґС‹: scan new, scan continue, stop СЃРєР°РЅ, scan results, scan reset.",
    ]
    if recent_reports:
        lines.append("")
        lines.append("РџРѕСЃР»РµРґРЅРёРµ РѕС‚С‡РµС‚С‹:")
        lines.extend(f"- {name}" for name in recent_reports)
    return "\n".join(lines)


def build_scan_menu_buttons():
    return [
        [Button.text("scan new"), Button.text("scan continue")],
        [Button.text("stop СЃРєР°РЅ"), Button.text("scan results")],
        [Button.text("scan reset"), Button.text("menu")],
    ]


def build_scan_menu_text_fast() -> str:
    return build_scan_menu_text()


def format_scan_checkpoint_text() -> str:
    checkpoint = load_scan_checkpoint()
    if not checkpoint:
        return "РЅРµС‚"
    next_user_id = int(checkpoint.get("next_user_id") or checkpoint.get("page_number") or 1)
    total_users_hint = int(checkpoint.get("total_users_hint") or 0)
    range_text = f"{next_user_id}" if total_users_hint <= 0 else f"{next_user_id}/{total_users_hint}"
    return (
        f"{checkpoint.get('status', 'saved')}, "
        f"РїРѕР·РёС†РёСЏ ID {range_text}, "
        f"РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ {len(checkpoint.get('records') or [])}, "
        f"ID РїСЂРѕРІРµСЂРµРЅРѕ {int(checkpoint.get('pages_scanned') or 0)}, "
        f"СЃРѕС…СЂР°РЅРµРЅ {checkpoint.get('saved_at', '-')}"
    )


def latest_scan_report_paths() -> tuple[Path | None, Path | None, Path | None, Path | None]:
    report_dir = Path(settings.report_dir)
    if not report_dir.exists():
        return None, None, None, None
    summary_files = sorted(
        (
            path
            for path in report_dir.glob("scan-*.txt")
            if not path.name.endswith("-detailed.txt")
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not summary_files:
        return None, None, None, None
    txt_path = summary_files[0]
    stem = txt_path.stem
    detailed_path = report_dir / f"{stem}-detailed.txt"
    json_path = report_dir / f"{stem}.json"
    dashboard_path = report_dir / f"{stem}-dashboard.html"
    return (
        txt_path,
        detailed_path if detailed_path.exists() else None,
        json_path if json_path.exists() else None,
        dashboard_path if dashboard_path.exists() else None,
    )


def build_scan_results_text() -> str:
    checkpoint = load_scan_checkpoint()
    checkpoint_records = list((checkpoint or {}).get("records") or [])
    txt_path, detailed_path, json_path, dashboard_path = latest_scan_report_paths()

    lines = [
        "Р РµР·СѓР»СЊС‚Р°С‚С‹ scan",
        f"РђРєС‚РёРІРЅС‹Р№ scan: {'РґР°' if active_scan_cancel_event and not active_scan_cancel_event.is_set() else 'РЅРµС‚'}",
        f"РђРґРјРёРЅ-Р±РѕС‚: {format_admin_bot_health()}",
        f"РЎРѕС…СЂР°РЅРµРЅРЅС‹Р№ РїСЂРѕРіСЂРµСЃСЃ: {format_scan_checkpoint_text()}",
    ]

    if checkpoint_records:
        summary_text, _ = build_scan_report(
            checkpoint_records,
            int((checkpoint or {}).get("pages_scanned") or 0),
            admin_statistics=dict((checkpoint or {}).get("admin_statistics") or {}),
        )
        lines.extend(("", "Р§Р°СЃС‚РёС‡РЅС‹Р№ РїСЂРѕРіСЂРµСЃСЃ:", summary_text))

    if txt_path:
        lines.extend(("", "РџРѕСЃР»РµРґРЅРёР№ РіРѕС‚РѕРІС‹Р№ РѕС‚С‡РµС‚:", f"TXT: {txt_path}"))
        if detailed_path:
            lines.append(f"DETAILS: {detailed_path}")
        if json_path:
            lines.append(f"JSON: {json_path}")
        if dashboard_path:
            dashboard_url = ensure_dashboard_public_url(dashboard_path, "latest-scan-dashboard.html")
            lines.append(f"DASHBOARD: {dashboard_url or dashboard_path}")
        if json_path:
            try:
                stats_data = json.loads(json_path.read_text(encoding="utf-8"))
                forecast = dict((stats_data or {}).get("forecast") or {})
                if forecast:
                    six_month = dict(forecast.get("six_month_projection") or {})
                    financial = dict(forecast.get("financial_projection") or {})
                    lines.extend(
                        (
                            "",
                            "РљР»СЋС‡РµРІРѕР№ РїСЂРѕРіРЅРѕР· РЅР° СЃР»РµРґСѓСЋС‰РёР№ РјРµСЃСЏС†:",
                            f"- РџРѕРґРїРёСЃРѕРє СЃ РёСЃС‚РµС‡РµРЅРёРµРј РІ 30 РґРЅРµР№: {int(forecast.get('next_month_due_subscriptions_total', 0))}",
                            f"- Р”РѕС…РѕРґ (Р±Р°Р·Р° 70%): {float(forecast.get('next_month_projected_revenue_base_rub', 0.0)):.0f} RUB",
                            f"- Р”РѕС…РѕРґ (60%): {float(forecast.get('next_month_projected_revenue_low_rub', 0.0)):.0f} RUB",
                            f"- Р”РѕС…РѕРґ (80%): {float(forecast.get('next_month_projected_revenue_high_rub', 0.0)):.0f} RUB",
                        )
                    )
                    if financial:
                        lines.extend(
                            (
                                "",
                                "С‚РѕРіРѕРІС‹Р№ РїСЂРѕРіРЅРѕР· РїСЂРёР±С‹Р»Рё:",
                                f"- Р§РµСЂРµР· 1 РјРµСЃСЏС†: ~{float(financial.get('profit_projection_month_1_rub', 0.0)):.0f} RUB",
                                f"- Р§РµСЂРµР· 6 РјРµСЃСЏС†РµРІ: ~{float(financial.get('profit_projection_month_6_rub', 0.0)):.0f} RUB",
                                f"- Р§РµСЂРµР· 12 РјРµСЃСЏС†РµРІ: ~{float(financial.get('profit_projection_month_12_rub', 0.0)):.0f} RUB",
                            )
                        )
                    if six_month:
                        lines.extend(
                            (
                                "",
                                "РљР»СЋС‡РµРІРѕР№ РїСЂРѕРіРЅРѕР· РЅР° 6 РјРµСЃСЏС†РµРІ:",
                                f"- РџРѕР»СЊР·РѕРІР°С‚РµР»Рё: ~{int(round(float(six_month.get('users_total_projected_6m', 0.0))))}",
                                f"- РџР»Р°С‚СЏС‰РёРµ: ~{int(round(float(six_month.get('users_with_subscriptions_projected_6m', 0.0))))}",
                                f"- РџРѕРґРїРёСЃРєРё: ~{int(round(float(six_month.get('subscriptions_total_projected_6m', 0.0))))}",
                                f"- MRR: ~{float(six_month.get('projected_mrr_6m_rub', 0.0)):.0f} RUB",
                            )
                        )
            except Exception:
                logging.exception("Failed to parse latest scan JSON for forecast preview")
        try:
            preview = txt_path.read_text(encoding="utf-8").strip()
        except Exception:
            logging.exception("Failed to read latest scan report")
            preview = ""
        if preview:
            if len(preview) > 2500:
                preview = preview[:2500].rstrip() + "\n..."
            lines.extend(("", "РљСЂР°С‚РєРёР№ РїСЂРѕСЃРјРѕС‚СЂ:", preview))
    elif not checkpoint_records:
        lines.extend(("", "Р“РѕС‚РѕРІС‹С… РѕС‚С‡РµС‚РѕРІ РїРѕРєР° РЅРµС‚."))

    return "\n".join(lines)


def dashboard_target_url(url: str, fallback_path: Path | None = None, *, admin_url: str | None = None) -> str:
    admin_url = admin_url if admin_url is not None else live_admin_dashboard_url()
    if admin_url and re.match(r"^https?://", admin_url, flags=re.IGNORECASE):
        return admin_url
    if url and re.match(r"^https?://", url, flags=re.IGNORECASE):
        return url
    return str(fallback_path or "")


def dashboard_link_buttons(url: str, fallback_path: Path | None = None, *, admin_url: str | None = None):
    target = dashboard_target_url(url, fallback_path, admin_url=admin_url)
    if not target or not re.match(r"^https?://", target, flags=re.IGNORECASE):
        return None
    return [[Button.url("РћС‚РєСЂС‹С‚СЊ admin system", target)]]


def dashboard_message_text(title: str, url: str, fallback_path: Path | None = None, *, admin_url: str | None = None) -> str:
    target = dashboard_target_url(url, fallback_path, admin_url=admin_url)
    resolved_admin_url = admin_url if admin_url is not None else live_admin_dashboard_url()
    if target and settings.dashboard_intro_enabled and target == resolved_admin_url:
        return f"{title}\n{target}\n\nРЎРЅР°С‡Р°Р»Р° РѕС‚РєСЂРѕРµС‚СЃСЏ РєРѕСЂРѕС‚РєР°СЏ Р°РЅРёРјР°С†РёСЏ VPN_KBR, РїРѕС‚РѕРј admin system."
    return f"{title}\n{target}"


async def send_live_admin_dashboard_link(event) -> bool:
    admin_url = live_admin_dashboard_url()
    if not admin_url or not re.match(r"^https?://", admin_url, flags=re.IGNORECASE):
        await safe_event_reply(event, "Admin system СЃРµР№С‡Р°СЃ РЅРµРґРѕСЃС‚СѓРїРЅР°. РџСЂРѕРІРµСЂСЊ DASHBOARD_HTTP_* Рё DASHBOARD_PUBLIC_*.")
        return False
    sent = await safe_event_reply(
        event,
        dashboard_message_text("Admin system:", admin_url, admin_url=admin_url),
        buttons=dashboard_link_buttons(admin_url, admin_url=admin_url),
    )
    return sent is not None


async def send_live_root_panel_link(event) -> bool:
    root_url = live_root_panel_url()
    if not root_url or not re.match(r"^https?://", root_url, flags=re.IGNORECASE):
        await safe_event_reply(event, "Root panel СЃРµР№С‡Р°СЃ РЅРµРґРѕСЃС‚СѓРїРЅР°. РџСЂРѕРІРµСЂСЊ DASHBOARD_HTTP_* Рё DASHBOARD_PUBLIC_*.")
        return False
    sent = await safe_event_reply(
        event,
        f"Root panel:\n{root_url}",
        buttons=[[Button.url("РћС‚РєСЂС‹С‚СЊ Root panel", root_url)]],
    )
    return sent is not None


async def send_system_panel_link(event) -> bool:
    target = system_panel_url()
    if not target or not re.match(r"^https?://", target, flags=re.IGNORECASE):
        await safe_event_reply(event, "System panel URL РЅРµ РЅР°СЃС‚СЂРѕРµРЅ.")
        return False
    sent = await safe_event_reply(
        event,
        f"System panel:\n{target}",
        buttons=[[Button.url("РћС‚РєСЂС‹С‚СЊ System", target)]],
    )
    return sent is not None


async def send_latest_dashboard_to_chat(event) -> bool:
    _, _, _, dashboard_path = latest_scan_report_paths()
    if not dashboard_path:
        return False
    dashboard_url = ensure_dashboard_public_url(dashboard_path, "latest-scan-dashboard.html")
    if not dashboard_url:
        _, dashboard_url = publish_dashboard_file(dashboard_path, latest_name="latest-scan-dashboard.html")
    admin_url = live_admin_dashboard_url()
    sent = await safe_event_reply(
        event,
        dashboard_message_text("Admin system:", dashboard_url, dashboard_path, admin_url=admin_url),
        buttons=dashboard_link_buttons(dashboard_url, dashboard_path, admin_url=admin_url),
    )
    return sent is not None


async def send_latest_dashboard_to_chat_id(chat_id: int) -> bool:
    _, _, _, dashboard_path = latest_scan_report_paths()
    if not dashboard_path:
        return False
    try:
        dashboard_url = ensure_dashboard_public_url(dashboard_path, "latest-scan-dashboard.html")
        if not dashboard_url:
            _, dashboard_url = publish_dashboard_file(dashboard_path, latest_name="latest-scan-dashboard.html")
        admin_url = live_admin_dashboard_url()
        await client.send_message(
            chat_id,
            dashboard_message_text("Admin system:", dashboard_url, dashboard_path, admin_url=admin_url),
            buttons=dashboard_link_buttons(dashboard_url, dashboard_path, admin_url=admin_url),
        )
        note_success_action()
        return True
    except FloodWaitError as error:
        wait_seconds = int(getattr(error, "seconds", 1) or 1)
        note_floodwait(wait_seconds)
        logging.warning("FloodWait on dashboard send: message suppressed for %ss", wait_seconds)
        return False
    except Exception:
        logging.exception("Failed to send latest dashboard to chat_id=%s", chat_id)
        return False


async def send_status_dashboard_from_database(event) -> bool:
    built = build_status_dashboard_from_database()
    if not built:
        await safe_event_reply(
            event,
            "SQL Р±Р°Р·Р° РїСѓСЃС‚Р°. РЎРЅР°С‡Р°Р»Р° Р·Р°РїСѓСЃС‚Рё `scan new`, С‡С‚РѕР±С‹ СЃРѕР±СЂР°С‚СЊ РґР°РЅРЅС‹Рµ.",
        )
        return False
    dashboard_path, stats = built
    summary_text = build_status_summary_from_stats(stats, dashboard_path)
    dashboard_url = str(stats.get("dashboard_public_url") or "")
    admin_url = live_admin_dashboard_url()
    sent = await safe_event_reply(
        event,
        summary_text,
        buttons=dashboard_link_buttons(dashboard_url, dashboard_path, admin_url=admin_url),
    )
    return sent is not None


async def get_user_subscriptions_info_in_admin_bot(
    user_id: str,
    progress_callback: ProgressCallback | None = None,
) -> str:
    await emit_process_progress(
        progress_callback,
        "Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
        INFO_STEPS,
        1,
        user_id=user_id,
        extra_lines=["РћР¶РёРґР°СЋ СЃРІРѕР±РѕРґРЅС‹Р№ Р°РґРјРёРЅ-РїСЂРѕС†РµСЃСЃ"],
    )
    async with admin_flow_context(
        "Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
        user_id=user_id,
        progress_callback=progress_callback,
        progress_title="Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
        progress_steps=INFO_STEPS,
        progress_step=1,
    ):
        await emit_process_progress(
            progress_callback,
            "Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
            INFO_STEPS,
            1,
            user_id=user_id,
            extra_lines=[f"РџРѕР»СѓС‡Р°СЋ Telegram entity @{settings.admin_bot_username}"],
        )
        bot = await get_admin_bot_entity()
        logging.info("Starting admin info for user_id=%s in @%s", user_id, settings.admin_bot_username)

        async with admin_conversation(bot) as conv:
            result_message = await open_user_in_admin_bot(
                conv,
                bot,
                user_id,
                progress_callback=progress_callback,
                progress_title="Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
                progress_steps=INFO_STEPS,
            )
            await emit_process_progress(
                progress_callback,
                "Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
                INFO_STEPS,
                4,
                user_id=user_id,
                extra_lines=[f"РљРЅРѕРїРєР° РїРѕРґРїРёСЃРѕРє: {settings.subscriptions_button_text}"],
            )
            subscriptions_message = await click_and_read(
                bot,
                result_message,
                settings.subscriptions_button_text,
            )

            details: list[tuple[str, str, str]] = []
            subscription_buttons = extract_subscription_buttons(subscriptions_message)
            logging.info("Found %s subscription buttons for user_id=%s", len(subscription_buttons), user_id)
            await emit_process_progress(
                progress_callback,
                "Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
                INFO_STEPS,
                5,
                user_id=user_id,
                extra_lines=[f"РќР°Р№РґРµРЅРѕ РїРѕРґРїРёСЃРѕРє РґР»СЏ С‡С‚РµРЅРёСЏ: {len(subscription_buttons)}"],
            )

            current_menu = subscriptions_message
            for index, subscription in enumerate(subscription_buttons, start=1):
                await emit_process_progress(
                    progress_callback,
                    "Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
                    INFO_STEPS,
                    5,
                    user_id=user_id,
                    extra_lines=[
                        f"РџРѕРґРїРёСЃРєР° {index}/{len(subscription_buttons)}",
                        f"РљРЅРѕРїРєР°: {subscription['text']}",
                    ],
                )
                detail_message = await click_button_position_and_read(
                    bot,
                    current_menu,
                    int(subscription["row"]),
                    int(subscription["column"]),
                    str(subscription["text"]),
                    expected_button_text=settings.back_button_text,
                )
                details.append(
                    (
                        str(subscription["id"]),
                        str(subscription["text"]),
                        detail_message.raw_text or "",
                    )
                )

                current_menu = await click_and_read(
                    bot,
                    detail_message,
                    settings.back_button_text,
                    expected_button_text=str(subscription["text"]),
                )

        await emit_process_progress(
            progress_callback,
            "Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
            INFO_STEPS,
            6,
            user_id=user_id,
            extra_lines=[f"РџСЂРѕС‡РёС‚Р°РЅРѕ РїРѕРґРїРёСЃРѕРє: {len(details)}", "РЎРѕР±РёСЂР°СЋ HTML-РѕС‚РІРµС‚"],
        )
        result_text = format_subscription_info_html(
            user_id,
            result_message.raw_text or "",
            subscriptions_message,
            details,
        )
        resolved_user_id = (
            extract_user_number(result_message.raw_text or "", subscriptions_message.raw_text or "")
            or (user_id if re.fullmatch(r"\d{1,20}", str(user_id)) else "")
            or extract_user_id(result_message.raw_text or "")
            or user_id
        )
        record = {
            "user_id": resolved_user_id,
            "username": extract_username_from_text(result_message.raw_text or ""),
            "user_button_text": f"ID {user_id}",
            "user_text": result_message.raw_text or "",
            "registration_date": (
                extract_registration_date(result_message.raw_text or "").strftime("%Y-%m-%d")
                if extract_registration_date(result_message.raw_text or "")
                else None
            ),
            "subscriptions": [
                {
                    "subscription_id": subscription_id,
                    "button_text": button_text,
                    "location": extract_location_from_subscription_button(button_text),
                    "detail_text": detail_text,
                }
                for subscription_id, button_text, detail_text in details
            ],
        }
        try:
            upsert_latest_record(record)
        except Exception:
            logging.exception("Failed to upsert latest SQL record after info lookup=%s", user_id)
        print("\n===== USER INFO RESULT =====")
        print(result_text)
        print("============================\n")
        logging.info("Admin info finished for user_id=%s", user_id)
        return result_text


async def open_users_page(conv, bot):
    admin_message = await send_admin_and_get_menu(conv, bot)
    admin_message = await reset_admin_state_if_needed(conv, bot, admin_message)
    if is_users_page_message(admin_message):
        return admin_message

    # Prefer explicit configured button, but fall back to inferred candidates.
    if has_button_text(admin_message, settings.users_button_text):
        next_message = await click_and_read(
            bot,
            admin_message,
            settings.users_button_text,
            expected_button_text=settings.find_user_button_text,
        )
        if is_users_page_message(next_message):
            return next_message
        admin_message = next_message

    for candidate in get_users_menu_candidates(admin_message):
        candidate_text = str(candidate["text"])
        try:
            next_message = await click_button_position_and_read(
                bot,
                admin_message,
                int(candidate["row"]),
                int(candidate["column"]),
                candidate_text,
            )
        except Exception:
            continue
        if is_users_page_message(next_message):
            return next_message

    raise RuntimeError("Could not open users list page from current admin menu buttons.")


async def get_admin_statistics_snapshot(conv, bot) -> tuple[int, dict]:
    admin_message = await send_admin_and_get_menu(conv, bot)
    admin_message = await reset_admin_state_if_needed(conv, bot, admin_message)
    if has_button_text(admin_message, "стат"):
        stats_message = await click_and_read(bot, admin_message, "стат")
    else:
        stats_button = get_statistics_menu_button(admin_message)
        if not stats_button:
            raise RuntimeError("Statistics button not found in admin menu.")
        stats_message = await click_button_position_and_read(
            bot,
            admin_message,
            int(stats_button["row"]),
            int(stats_button["column"]),
            str(stats_button["text"]),
        )

    stats_text = stats_message.raw_text or ""
    snapshot = extract_admin_statistics_snapshot(stats_text)
    total_users = int(snapshot.get("users_total") or 0)
    if not total_users:
        raise RuntimeError("Could not parse total users from statistics text.")
    return total_users, snapshot


async def return_to_users_page_from_user_card(conv, bot, message):
    back_button = get_back_page_button(message)
    if back_button:
        users_page_message = await click_button_position_and_read(
            bot,
            message,
            int(back_button["row"]),
            int(back_button["column"]),
            str(back_button["text"]),
        )
    elif has_button_text(message, settings.cancel_button_text):
        users_page_message = await click_and_read(bot, message, settings.cancel_button_text)
    else:
        users_page_message = await open_users_page(conv, bot)
    if not has_button_text(users_page_message, settings.find_user_button_text):
        users_page_message = await open_users_page(conv, bot)
    return users_page_message


async def collect_user_record_via_search(
    conv,
    bot,
    users_page_message,
    user_id: str,
    progress_callback: ProgressCallback | None = None,
    progress_context: str = "",
) -> tuple[dict | None, object]:
    async def emit_collect_progress(text: str) -> None:
        if not progress_callback:
            return
        prefix = f"{progress_context}. " if progress_context else ""
        await progress_callback(f"{prefix}{text}")

    await emit_collect_progress(f"РћС‚РєСЂС‹РІР°СЋ РїРѕРёСЃРє Рё Р·Р°РїСЂР°С€РёРІР°СЋ ID {user_id}.")
    find_message = await click_and_read(bot, users_page_message, settings.find_user_button_text)
    previous_snapshot = message_snapshot(find_message)
    await send_conv_message_with_retry(bot, user_id)
    result_message = await wait_bot_update(bot, previous_snapshot)
    log_message(f"Search result for user_id={user_id}", result_message)

    if not has_button_text(result_message, settings.subscriptions_button_text):
        await emit_collect_progress(f"ID {user_id}: РєР°СЂС‚РѕС‡РєР° РЅРµ РЅР°Р№РґРµРЅР° РёР»Рё Р±РµР· РґРѕСЃС‚СѓРїР°.")
        back_button = get_back_page_button(result_message)
        if back_button:
            users_page_message = await click_button_position_and_read(
                bot,
                result_message,
                int(back_button["row"]),
                int(back_button["column"]),
                str(back_button["text"]),
            )
        elif has_button_text(result_message, settings.cancel_button_text):
            users_page_message = await click_and_read(bot, result_message, settings.cancel_button_text)
        else:
            users_page_message = await open_users_page(conv, bot)
        if not has_button_text(users_page_message, settings.find_user_button_text):
            users_page_message = await open_users_page(conv, bot)
        return None, users_page_message

    if user_card_has_zero_subscriptions(result_message.raw_text or ""):
        await emit_collect_progress(f"ID {user_id}: на карточке 0 шт подписок, пропускаю вход в раздел подписок.")
        back_button = get_back_page_button(result_message)
        if back_button:
            users_page_message = await click_button_position_and_read(
                bot,
                result_message,
                int(back_button["row"]),
                int(back_button["column"]),
                str(back_button["text"]),
            )
        elif has_button_text(result_message, settings.cancel_button_text):
            users_page_message = await click_and_read(bot, result_message, settings.cancel_button_text)
        else:
            users_page_message = await open_users_page(conv, bot)
        if not has_button_text(users_page_message, settings.find_user_button_text):
            users_page_message = await open_users_page(conv, bot)
        return {
            "user_id": str(user_id),
            "username": extract_username_from_text(result_message.raw_text or ""),
            "user_button_text": f"ID {user_id}",
            "user_text": result_message.raw_text or "",
            "registration_date": (
                extract_registration_date(result_message.raw_text or "").strftime("%Y-%m-%d")
                if extract_registration_date(result_message.raw_text or "")
                else None
            ),
            "subscriptions": [],
        }, users_page_message

    await emit_collect_progress("РљР°СЂС‚РѕС‡РєР° РЅР°Р№РґРµРЅР°. Р§РёС‚Р°СЋ РїРѕРґРїРёСЃРєРё.")
    subscriptions_message = await click_and_read(
        bot,
        result_message,
        settings.subscriptions_button_text,
    )
    subscriptions = []
    current_subscription_menu = subscriptions_message
    subscription_buttons = extract_subscription_buttons(subscriptions_message)
    await emit_collect_progress(f"РќР°Р№РґРµРЅРѕ РїРѕРґРїРёСЃРѕРє: {len(subscription_buttons)}.")
    for subscription_index, subscription_button in enumerate(subscription_buttons, start=1):
        await emit_collect_progress(
            f"РџРѕРґРїРёСЃРєР° {subscription_index}/{len(subscription_buttons)}: {subscription_button['text']}."
        )
        detail_message = await click_button_position_and_read(
            bot,
            current_subscription_menu,
            int(subscription_button["row"]),
            int(subscription_button["column"]),
            str(subscription_button["text"]),
            expected_button_text=settings.back_button_text,
        )
        subscriptions.append(
            {
                "subscription_id": str(subscription_button["id"]),
                "button_text": str(subscription_button["text"]),
                "location": extract_location_from_subscription_button(str(subscription_button["text"])),
                "detail_text": detail_message.raw_text or "",
            }
        )
        back_button = get_back_page_button(detail_message)
        if not back_button:
            raise RuntimeError("Back button not found on subscription details page.")
        current_subscription_menu = await click_button_position_and_read(
            bot,
            detail_message,
            int(back_button["row"]),
            int(back_button["column"]),
            str(back_button["text"]),
        )

    back_to_user_button = get_back_page_button(current_subscription_menu)
    if not back_to_user_button:
        raise RuntimeError("Back button not found on subscriptions list page.")
    user_page_again = await click_button_position_and_read(
        bot,
        current_subscription_menu,
        int(back_to_user_button["row"]),
        int(back_to_user_button["column"]),
        str(back_to_user_button["text"]),
    )
    back_to_users_button = get_back_page_button(user_page_again)
    if not back_to_users_button:
        raise RuntimeError("Back button not found on user card page.")
    users_page_again = await click_button_position_and_read(
        bot,
        user_page_again,
        int(back_to_users_button["row"]),
        int(back_to_users_button["column"]),
        str(back_to_users_button["text"]),
    )
    if not has_button_text(users_page_again, settings.find_user_button_text):
        users_page_again = await open_users_page(conv, bot)

    registration_date = extract_registration_date(result_message.raw_text or "")
    record = {
        "user_id": user_id,
        "username": extract_username_from_text(result_message.raw_text or ""),
        "user_button_text": f"ID {user_id}",
        "user_text": result_message.raw_text or "",
        "registration_date": registration_date.strftime("%Y-%m-%d") if registration_date else None,
        "subscriptions": subscriptions,
    }
    return record, users_page_again


async def collect_current_user_record(
    bot,
    users_page_message,
    user_button: dict[str, int | str],
    progress_callback: ProgressCallback | None = None,
    progress_context: str = "",
) -> tuple[dict, object]:
    user_id = str(user_button["id"])
    logging.info("Scanning user_id=%s button=%r", user_id, user_button["text"])

    async def emit_collect_progress(text: str) -> None:
        if not progress_callback:
            return
        prefix = f"{progress_context}. " if progress_context else ""
        await progress_callback(f"{prefix}{text}")

    await emit_collect_progress(f"РћС‚РєСЂС‹РІР°СЋ РєР°СЂС‚РѕС‡РєСѓ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ ID {user_id}.")
    user_message = await click_button_position_and_read(
        bot,
        users_page_message,
        int(user_button["row"]),
        int(user_button["column"]),
        str(user_button["text"]),
        expected_button_text=settings.subscriptions_button_text,
    )
    subscriptions_message = await click_and_read(
        bot,
        user_message,
        settings.subscriptions_button_text,
    )

    subscriptions = []
    current_subscription_menu = subscriptions_message
    subscription_buttons = extract_subscription_buttons(subscriptions_message)
    await emit_collect_progress(f"РќР°Р№РґРµРЅРѕ РїРѕРґРїРёСЃРѕРє: {len(subscription_buttons)}. Р§РёС‚Р°СЋ РґРµС‚Р°Р»Рё.")
    for subscription_index, subscription_button in enumerate(subscription_buttons, start=1):
        await emit_collect_progress(
            f"РџРѕРґРїРёСЃРєР° {subscription_index}/{len(subscription_buttons)}: {subscription_button['text']}."
        )
        detail_message = await click_button_position_and_read(
            bot,
            current_subscription_menu,
            int(subscription_button["row"]),
            int(subscription_button["column"]),
            str(subscription_button["text"]),
            expected_button_text=settings.back_button_text,
        )
        subscriptions.append(
            {
                "subscription_id": str(subscription_button["id"]),
                "button_text": str(subscription_button["text"]),
                "location": extract_location_from_subscription_button(str(subscription_button["text"])),
                "detail_text": detail_message.raw_text or "",
            }
        )
        back_button = get_back_page_button(detail_message)
        if not back_button:
            raise RuntimeError("Back button not found on subscription details page.")
        current_subscription_menu = await click_button_position_and_read(
            bot,
            detail_message,
            int(back_button["row"]),
            int(back_button["column"]),
            str(back_button["text"]),
        )

    back_to_user_button = get_back_page_button(current_subscription_menu)
    if not back_to_user_button:
        raise RuntimeError("Back button not found on subscriptions list page.")
    user_page_again = await click_button_position_and_read(
        bot,
        current_subscription_menu,
        int(back_to_user_button["row"]),
        int(back_to_user_button["column"]),
        str(back_to_user_button["text"]),
    )
    await emit_collect_progress("Р’РѕР·РІСЂР°С‰Р°СЋСЃСЊ Рє СЃРїРёСЃРєСѓ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№.")
    back_to_users_button = get_back_page_button(user_page_again)
    if not back_to_users_button:
        raise RuntimeError("Back button not found on user card page.")
    users_page_again = await click_button_position_and_read(
        bot,
        user_page_again,
        int(back_to_users_button["row"]),
        int(back_to_users_button["column"]),
        str(back_to_users_button["text"]),
    )

    registration_date = extract_registration_date(user_message.raw_text or "")
    record = {
        "user_id": user_id,
        "username": extract_username_from_text(user_message.raw_text or ""),
        "user_button_text": str(user_button["text"]),
        "user_text": user_message.raw_text or "",
        "registration_date": registration_date.strftime("%Y-%m-%d") if registration_date else None,
        "subscriptions": subscriptions,
    }
    logging.info("Scanned user_id=%s subscriptions=%s", user_id, len(subscriptions))
    return record, users_page_again


async def scan_all_users_in_admin_bot(
    progress_callback=None,
    progress_interval_seconds: float = 1.2,
    cancel_event: asyncio.Event | None = None,
) -> str:
    global active_scan_reset_requested

    if progress_callback:
        await progress_callback("РћР¶РёРґР°СЋ СЃРІРѕР±РѕРґРЅС‹Р№ Р°РґРјРёРЅ-РїСЂРѕС†РµСЃСЃ РґР»СЏ scan.")
    async with admin_flow_context(
        "Scan РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№",
        progress_callback=progress_callback,
        progress_title="Scan РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№",
        progress_steps=["РћР¶РёРґР°СЋ", "РЎРєР°РЅРёСЂСѓСЋ"],
        progress_step=1,
    ):
        if cancel_event and cancel_event.is_set():
            if active_scan_reset_requested:
                clear_scan_checkpoint()
                active_scan_reset_requested = False
                return "Scan СЃР±СЂРѕС€РµРЅ. РЎРѕС…СЂР°РЅРµРЅРЅС‹Р№ РїСЂРѕРіСЂРµСЃСЃ РѕС‡РёС‰РµРЅ."
            return "Scan РЅР° РїР°СѓР·Рµ. РќРѕРІС‹С… РґРµР№СЃС‚РІРёР№ РЅРµ РІС‹РїРѕР»РЅРµРЅРѕ."

        bot = await get_admin_bot_entity()
        logging.info("Starting full admin scan in @%s", settings.admin_bot_username)

        checkpoint = load_scan_checkpoint()
        records: list[dict] = list((checkpoint or {}).get("records") or [])
        scan_errors: list[dict] = list((checkpoint or {}).get("scan_errors") or [])
        seen_users: set[str] = {
            str(record.get("user_id"))
            for record in records
            if record.get("user_id") is not None
        }
        seen_users.update(str(item) for item in ((checkpoint or {}).get("seen_users") or []))
        checked_ids_total = int((checkpoint or {}).get("pages_scanned") or 0)
        start_user_id = max(1, int((checkpoint or {}).get("next_user_id") or (checkpoint or {}).get("page_number") or 1))
        admin_statistics_snapshot = dict((checkpoint or {}).get("admin_statistics") or {})
        paused = False
        reset_requested = False
        last_progress_text = ""
        last_progress_at = 0.0
        last_checkpoint_at = 0.0
        last_checkpoint_checked_ids = checked_ids_total
        total_users = 0
        consecutive_failures = 0
        session_restarts = 0
        current_user_id = start_user_id
        user_failures: dict[str, int] = {}
        skipped_users: set[str] = set()

        def remember_scan_error(user_id: str, stage: str, error: Exception) -> None:
            scan_errors.append(
                {
                    "user_id": user_id,
                    "happened_at": datetime.now().isoformat(timespec="seconds"),
                    "stage": stage,
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                }
            )

        async def emit_progress(text: str, force: bool = False) -> None:
            nonlocal last_progress_text, last_progress_at
            if not progress_callback:
                return
            now_monotonic = loop.time()
            if not force and text == last_progress_text:
                return
            if not force and (now_monotonic - last_progress_at) < progress_interval_seconds:
                return
            last_progress_text = text
            last_progress_at = now_monotonic
            await progress_callback(text)

        if checkpoint:
            await emit_progress(
                (
                    "РќР°Р№РґРµРЅ СЃРѕС…СЂР°РЅРµРЅРЅС‹Р№ РїСЂРѕРіСЂРµСЃСЃ scan РїРѕ ID. "
                    f"РџСЂРѕРґРѕР»Р¶Р°СЋ СЃ ID {start_user_id}, СѓР¶Рµ СЃРѕР±СЂР°РЅРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№: {len(records)}."
                ),
                force=True,
            )
        else:
            await emit_progress("РЎРєР°РЅРёСЂРѕРІР°РЅРёРµ РїРѕ ID Р·Р°РїСѓС‰РµРЅРѕ СЃ С‡РёСЃС‚РѕРіРѕ СЃРѕСЃС‚РѕСЏРЅРёСЏ.", force=True)

        while current_user_id <= (total_users or current_user_id):
            if cancel_event and cancel_event.is_set():
                reset_requested = active_scan_reset_requested
                paused = not reset_requested
                break

            try:
                async with admin_conversation(bot) as conv:
                    if not total_users:
                        await emit_progress("РћС‚РєСЂС‹РІР°СЋ /admin СЃС‚Р°С‚РёСЃС‚РёРєСѓ Рё СЃС‡РёС‚С‹РІР°СЋ РѕР±С‰РµРµ С‡РёСЃР»Рѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№.", force=True)
                        try:
                            total_users, admin_statistics_snapshot = await retry_async(
                                "get admin statistics",
                                lambda: get_admin_statistics_snapshot(conv, bot),
                            )
                        except Exception:
                            total_users = int((checkpoint or {}).get("total_users_hint") or 0)
                            if not total_users:
                                raise
                            logging.exception(
                                "Failed to refresh admin statistics; using checkpoint total_users_hint=%s",
                                total_users,
                            )
                            await emit_progress(
                                f"РќРµ СѓРґР°Р»РѕСЃСЊ РѕР±РЅРѕРІРёС‚СЊ СЃС‚Р°С‚РёСЃС‚РёРєСѓ, РїСЂРѕРґРѕР»Р¶Р°СЋ РїРѕ checkpoint total={total_users}.",
                                force=True,
                            )
                        await emit_progress(f"Р’СЃРµРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ РїРѕ СЃС‚Р°С‚РёСЃС‚РёРєРµ: {total_users}.", force=True)
                        if current_user_id > total_users:
                            current_user_id = total_users + 1

                    users_page_message = await retry_async("open users page", lambda: open_users_page(conv, bot))

                    while current_user_id <= total_users:
                        if cancel_event and cancel_event.is_set():
                            reset_requested = active_scan_reset_requested
                            paused = not reset_requested
                            break

                        user_id = str(current_user_id)
                        checked_ids_total += 1
                        await emit_progress(
                            (
                                f"РЎРєР°РЅРёСЂРѕРІР°РЅРёРµ РїРѕ ID: {current_user_id}/{total_users}. "
                                f"РЎРѕР±СЂР°РЅРѕ Р·Р°РїРёСЃРµР№: {len(records)}."
                            ),
                        )

                        if user_id in seen_users:
                            current_user_id += 1
                            continue

                        async def emit_user_progress(text: str) -> None:
                            await emit_progress(text)

                        try:
                            last_user_error: Exception | None = None
                            record = None
                            for attempt in range(1, 3):
                                try:
                                    record, users_page_message = await collect_user_record_via_search(
                                        conv,
                                        bot,
                                        users_page_message,
                                        user_id,
                                        progress_callback=emit_user_progress,
                                        progress_context=f"ID {user_id}",
                                    )
                                    last_user_error = None
                                    break
                                except Exception as user_error:
                                    last_user_error = user_error
                                    remember_scan_error(user_id, f"collect_user_record_attempt_{attempt}", user_error)
                                    if attempt < 2:
                                        await emit_progress(
                                            f"ID {user_id}: локальная ошибка, повтор попытки {attempt + 1}/2.",
                                            force=True,
                                        )
                                        users_page_message = await retry_async(
                                            "recover users page before user retry",
                                            lambda: open_users_page(conv, bot),
                                        )
                                    else:
                                        raise user_error
                        except Exception as error:
                            logging.exception("Failed to collect user_id=%s via search; resetting users page", user_id)
                            consecutive_failures += 1
                            user_failures[user_id] = int(user_failures.get(user_id) or 0) + 1
                            remember_scan_error(user_id, "collect_user_record_via_search", error)
                            save_scan_checkpoint_best_effort(
                                current_user_id,
                                checked_ids_total,
                                records,
                                seen_users,
                                status="running",
                                next_user_id=current_user_id,
                                total_users_hint=total_users,
                                admin_statistics=admin_statistics_snapshot,
                                scan_errors=scan_errors,
                            )
                            await emit_progress(
                                (
                                    f"ID {user_id}: РѕС€РёР±РєР°, РїСЂРѕР±СѓСЋ РІРѕСЃСЃС‚Р°РЅРѕРІРёС‚СЊСЃСЏ. "
                                    f"РџРѕРґСЂСЏРґ РѕС€РёР±РѕРє: {consecutive_failures}/{SCAN_MAX_CONSECUTIVE_FAILURES}."
                                ),
                                force=True,
                            )
                            if user_failures.get(user_id, 0) >= 3:
                                skipped_users.add(user_id)
                                seen_users.add(user_id)
                                await emit_progress(
                                    f"ID {user_id}: пропускаю после {user_failures[user_id]} неудачных попыток.",
                                    force=True,
                                )
                                current_user_id += 1
                                continue
                            if consecutive_failures >= SCAN_MAX_CONSECUTIVE_FAILURES:
                                logging.warning(
                                    "Restarting admin conversation after %s consecutive failures at user_id=%s",
                                    consecutive_failures,
                                    user_id,
                                )
                                set_admin_bot_health("[WAIT]", "РїРµСЂРµР·Р°РїСѓСЃРє", "РјРЅРѕРіРѕ РѕС€РёР±РѕРє РїРѕРґСЂСЏРґ")
                                consecutive_failures = 0
                                await asyncio.sleep(SCAN_SESSION_RESTART_DELAY_SECONDS)
                                break
                            try:
                                users_page_message = await retry_async(
                                    "recover users page after user collection failure",
                                    lambda: open_users_page(conv, bot),
                                )
                            except Exception as recover_error:
                                logging.exception(
                                    "Failed to recover users page after user_id=%s; restarting conversation",
                                    user_id,
                                )
                                remember_scan_error(user_id, "recover_users_page", recover_error)
                                set_admin_bot_health("[WAIT]", "РїРµСЂРµР·Р°РїСѓСЃРє", "СЃС‚СЂР°РЅРёС†Р° РЅРµ РІРѕСЃСЃС‚Р°РЅРѕРІРёР»Р°СЃСЊ")
                                await asyncio.sleep(SCAN_SESSION_RESTART_DELAY_SECONDS)
                                break
                            current_user_id += 1
                            continue

                        if record:
                            records.append(record)
                            seen_users.add(user_id)
                            user_failures.pop(user_id, None)
                            try:
                                upsert_latest_record(record)
                            except Exception:
                                logging.exception("Failed to upsert latest SQL record for user_id=%s", user_id)
                        consecutive_failures = 0
                        session_restarts = 0

                        current_user_id += 1
                        now_monotonic = loop.time()
                        should_save_checkpoint = (
                            (checked_ids_total - last_checkpoint_checked_ids) >= SCAN_CHECKPOINT_USER_INTERVAL
                            or (now_monotonic - last_checkpoint_at) >= SCAN_CHECKPOINT_MIN_INTERVAL_SECONDS
                            or current_user_id > total_users
                        )
                        if should_save_checkpoint:
                            save_scan_checkpoint_best_effort(
                                current_user_id,
                                checked_ids_total,
                                records,
                                seen_users,
                                status="running",
                                next_user_id=current_user_id,
                                total_users_hint=total_users,
                                admin_statistics=admin_statistics_snapshot,
                                scan_errors=scan_errors,
                            )
                            last_checkpoint_at = now_monotonic
                            last_checkpoint_checked_ids = checked_ids_total

                    if reset_requested or paused or current_user_id > total_users:
                        break

            except Exception as session_error:
                session_restarts += 1
                remember_scan_error(str(current_user_id), "scan_session", session_error)
                logging.exception(
                    "Scan session failed at user_id=%s; restart %s/%s",
                    current_user_id,
                    session_restarts,
                    SCAN_MAX_SESSION_RESTARTS,
                )
                save_scan_checkpoint_best_effort(
                    current_user_id,
                    checked_ids_total,
                    records,
                    seen_users,
                    status="running",
                    next_user_id=current_user_id,
                    total_users_hint=total_users or None,
                    admin_statistics=admin_statistics_snapshot,
                    scan_errors=scan_errors,
                )
                await emit_progress(
                    (
                        f"РЎРµСЃСЃРёСЏ scan Р·Р°РІРёСЃР»Р°/СЃР»РѕРјР°Р»Р°СЃСЊ РЅР° ID {current_user_id}. "
                        f"РџРµСЂРµР·Р°РїСѓСЃРє {session_restarts}/{SCAN_MAX_SESSION_RESTARTS}."
                    ),
                    force=True,
                )
                set_admin_bot_health("[WAIT]", "РїРµСЂРµР·Р°РїСѓСЃРє", f"scan session {session_restarts}")
                if session_restarts >= SCAN_MAX_SESSION_RESTARTS:
                    paused = True
                    break
                await asyncio.sleep(SCAN_SESSION_RESTART_DELAY_SECONDS)
                continue

            if reset_requested or paused or current_user_id > total_users:
                break

            session_restarts += 1
            save_scan_checkpoint_best_effort(
                current_user_id,
                checked_ids_total,
                records,
                seen_users,
                status="running",
                next_user_id=current_user_id,
                total_users_hint=total_users,
                admin_statistics=admin_statistics_snapshot,
                scan_errors=scan_errors,
            )
            await emit_progress(
                f"РџРµСЂРµР·Р°РїСѓСЃРєР°СЋ scan-СЃРµСЃСЃРёСЋ Рё РїСЂРѕРґРѕР»Р¶Р°СЋ СЃ ID {current_user_id}.",
                force=True,
            )
            if session_restarts >= SCAN_MAX_SESSION_RESTARTS:
                paused = True
                break
            await asyncio.sleep(SCAN_SESSION_RESTART_DELAY_SECONDS)

        if reset_requested:
            clear_scan_checkpoint()
            reset_scan_database()
            active_scan_reset_requested = False
            await emit_progress("Scan СЃР±СЂРѕС€РµРЅ. РЎРѕС…СЂР°РЅРµРЅРЅС‹Р№ РїСЂРѕРіСЂРµСЃСЃ РѕС‡РёС‰РµРЅ.", force=True)
            return "Scan СЃР±СЂРѕС€РµРЅ. РЎРѕС…СЂР°РЅРµРЅРЅС‹Р№ РїСЂРѕРіСЂРµСЃСЃ РѕС‡РёС‰РµРЅ."

        next_user_id = current_user_id if "current_user_id" in locals() else start_user_id
        next_user_id = min(total_users + 1, max(1, int(next_user_id)))
        if paused:
            save_scan_checkpoint(
                next_user_id,
                checked_ids_total,
                records,
                seen_users,
                status="paused",
                next_user_id=next_user_id,
                total_users_hint=total_users,
                admin_statistics=admin_statistics_snapshot,
                scan_errors=scan_errors,
            )

        records_for_reports = load_latest_records_from_database() or records
        summary_text, stats = build_scan_report(
            records_for_reports,
            checked_ids_total,
            admin_statistics=admin_statistics_snapshot,
        )
        stats["scan_errors"] = scan_errors
        stats["scan_skipped_users"] = sorted(skipped_users)
        detailed_text = build_detailed_scan_report(records_for_reports)
        txt_path, json_path, detailed_txt_path, dashboard_path = save_scan_report(summary_text, detailed_text, stats)
        logging.info(
            "Full scan finished users=%s report=%s detailed=%s json=%s dashboard=%s checked_ids=%s total_users=%s",
            len(records),
            txt_path,
            detailed_txt_path,
            json_path,
            dashboard_path,
            checked_ids_total,
            total_users,
        )
        dashboard_url = ensure_dashboard_public_url(dashboard_path, "latest-scan-dashboard.html")
        if paused:
            await emit_progress(
                (
                    f"Scan РЅР° РїР°СѓР·Рµ: РїСЂРѕРІРµСЂРµРЅРѕ ID {checked_ids_total}, "
                    f"РѕР±СЂР°Р±РѕС‚Р°РЅРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ {len(records)}, СЃР»РµРґСѓСЋС‰Р°СЏ РїРѕР·РёС†РёСЏ ID {next_user_id}."
                ),
                force=True,
            )
        else:
            clear_scan_checkpoint()
            await emit_progress(
                f"Scan Р·Р°РІРµСЂС€РµРЅ: РїСЂРѕРІРµСЂРµРЅРѕ ID {checked_ids_total}, РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ СЃРѕР±СЂР°РЅРѕ {len(records)}.",
                force=True,
            )
        return "\n".join(
            (
                "Scan РЅР° РїР°СѓР·Рµ. Р§Р°СЃС‚РёС‡РЅС‹Р№ РѕС‚С‡РµС‚ СЃРѕС…СЂР°РЅРµРЅ:" if paused else "Scan Р·Р°РІРµСЂС€РµРЅ.",
                "",
                summary_text,
                "",
                f"TXT: {txt_path}",
                f"DETAILS: {detailed_txt_path}",
                f"JSON: {json_path}",
                f"DASHBOARD: {dashboard_url or dashboard_path}",
                f"SQLITE: {database_path()}",
            )
        )


async def request_scan_pause_for_priority_command(event, command_name: str) -> dict | None:
    if not active_scan_cancel_event or active_scan_cancel_event.is_set():
        return None

    active_scan_cancel_event.set()
    interruption = {
        "chat_id": int(event.chat_id),
        "owner_id": int(active_scan_owner_id or event.sender_id or 0),
        "requested_by": int(event.sender_id or 0),
        "command": command_name,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    logging.info(
        "Auto-pausing active scan for priority command=%s chat_id=%s owner_id=%s",
        command_name,
        interruption["chat_id"],
        interruption["owner_id"],
    )
    await safe_event_reply(
        event,
        (
            f"[SCAN] РђРєС‚РёРІРЅС‹Р№ scan РІСЂРµРјРµРЅРЅРѕ СЃС‚Р°РІР»СЋ РЅР° РїР°СѓР·Сѓ РґР»СЏ РєРѕРјР°РЅРґС‹ `{command_name}`.\n"
            "Р—Р°РІРµСЂС€Сѓ С‚РµРєСѓС‰РµРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ, РІС‹РїРѕР»РЅСЋ РєРѕРјР°РЅРґСѓ Рё РїСЂРѕРґРѕР»Р¶Сѓ scan Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё."
        ),
    )
    return interruption


async def request_mail2_stop_for_priority_command(event, command_name: str) -> bool:
    if not active_mail2_cancel_event or active_mail2_cancel_event.is_set():
        return False
    active_mail2_cancel_event.set()
    logging.info("Mail2 stop requested for priority command=%s", command_name)
    await safe_event_reply(
        event,
        (
            f"[MAIL2] РђРєС‚РёРІРЅСѓСЋ СЂР°СЃСЃС‹Р»РєСѓ РѕСЃС‚Р°РЅР°РІР»РёРІР°СЋ РґР»СЏ РєРѕРјР°РЅРґС‹ `{command_name}`.\n"
            "Р”РѕР¶РґСѓСЃСЊ Р·Р°РІРµСЂС€РµРЅРёСЏ С‚РµРєСѓС‰РµРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ Рё РѕСЃРІРѕР±РѕР¶Сѓ Р°РґРјРёРЅ-РїСЂРѕС†РµСЃСЃ."
        ),
    )
    return True


def schedule_scan_auto_resume(interruption: dict | None) -> None:
    global active_scan_auto_resume_task
    if not interruption:
        return
    active_scan_auto_resume_task = asyncio.create_task(auto_resume_scan_after_priority_command(interruption))


async def auto_resume_scan_after_priority_command(interruption: dict) -> None:
    async with scan_auto_resume_lock:
        await run_scan_auto_resume_after_priority_command(interruption)


async def run_scan_auto_resume_after_priority_command(interruption: dict) -> None:
    global active_scan_cancel_event, active_scan_owner_id, active_scan_reset_requested
    global active_scan_action_delay_seconds, active_scan_base_delay_seconds

    chat_id = int(interruption.get("chat_id") or 0)
    owner_id = int(interruption.get("owner_id") or 0)
    command_name = str(interruption.get("command") or "admin command")
    if not chat_id:
        return

    try:
        for _ in range(120):
            if active_scan_cancel_event is None:
                break
            await asyncio.sleep(0.5)

        if active_scan_reset_requested:
            logging.info("Skip scan auto-resume after %s: reset requested", command_name)
            return
        if active_scan_cancel_event is not None:
            logging.warning("Skip scan auto-resume after %s: previous scan state is still active", command_name)
            return
        if not load_scan_checkpoint():
            logging.info("Skip scan auto-resume after %s: checkpoint is empty", command_name)
            return

        active_scan_cancel_event = asyncio.Event()
        active_scan_owner_id = owner_id or None
        active_scan_reset_requested = False
        active_scan_base_delay_seconds = max(
            0.05,
            min(settings.scan_action_delay_seconds, settings.scan_turbo_delay_seconds),
        )
        active_scan_action_delay_seconds = active_scan_base_delay_seconds

        progress_interval_seconds = max(0.25, env_float("SCAN_PROGRESS_INTERVAL_SECONDS", 0.5))
        progress_message = await client.send_message(
            chat_id,
            build_scan_status(
                f"РџСЂРѕРґРѕР»Р¶Р°СЋ scan РїРѕСЃР»Рµ РєРѕРјР°РЅРґС‹ `{command_name}`.",
                checkpoint_text=format_scan_checkpoint_text(),
            ),
            buttons=[[Button.inline("РџР°СѓР·Р° scan", data=SCAN_CANCEL_CALLBACK_DATA)]],
        )

        async def update_auto_scan_progress(
            text: str,
            *,
            done: bool = False,
            failed: bool = False,
            paused: bool = False,
        ) -> None:
            buttons = None if done or failed or paused else [[Button.inline("РџР°СѓР·Р° scan", data=SCAN_CANCEL_CALLBACK_DATA)]]
            await edit_status_message(
                progress_message,
                build_scan_status(
                    text,
                    checkpoint_text=format_scan_checkpoint_text(),
                    done=done,
                    failed=failed,
                    paused=paused,
                ),
                buttons=buttons,
                force=done or failed or paused,
            )

        result = await scan_all_users_in_admin_bot(
            progress_callback=update_auto_scan_progress,
            progress_interval_seconds=progress_interval_seconds,
            cancel_event=active_scan_cancel_event,
        )
        if "РЅР° РїР°СѓР·Рµ" in result.casefold():
            await update_auto_scan_progress("Scan СЃРЅРѕРІР° РЅР° РїР°СѓР·Рµ. РџСЂРѕРіСЂРµСЃСЃ СЃРѕС…СЂР°РЅРµРЅ.", paused=True)
        elif "СЃР±СЂРѕС€РµРЅ" in result.casefold():
            await update_auto_scan_progress("Scan СЃР±СЂРѕС€РµРЅ. РЎРѕС…СЂР°РЅРµРЅРЅС‹Р№ РїСЂРѕРіСЂРµСЃСЃ РѕС‡РёС‰РµРЅ.", done=True)
        else:
            await update_auto_scan_progress("Scan Р·Р°РІРµСЂС€РµРЅ. С‚РѕРіРѕРІС‹Р№ РѕС‚С‡РµС‚ РіРѕС‚РѕРІ.", done=True)
        await client.send_message(chat_id, result)
        await send_latest_dashboard_to_chat_id(chat_id)
    except Exception:
        logging.exception("Scan auto-resume failed after priority command=%s", command_name)
        try:
            await client.send_message(
                chat_id,
                "РќРµ СѓРґР°Р»РѕСЃСЊ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё РїСЂРѕРґРѕР»Р¶РёС‚СЊ scan. РћС‚РїСЂР°РІСЊ `scan continue`, С‡С‚РѕР±С‹ РїСЂРѕРґРѕР»Р¶РёС‚СЊ РІСЂСѓС‡РЅСѓСЋ.",
            )
        except Exception:
            logging.exception("Failed to notify chat about scan auto-resume failure")
    finally:
        active_scan_cancel_event = None
        active_scan_owner_id = None
        active_scan_reset_requested = False
        active_scan_action_delay_seconds = settings.scan_action_delay_seconds
        active_scan_base_delay_seconds = settings.scan_action_delay_seconds


async def send_promo_value_and_read(bot, current_message, value: str, label: str):
    logging.info("Sending promo %s value=%r", label, value)
    previous_snapshot = message_snapshot(current_message)
    await send_conv_message_with_retry(bot, value)
    next_message = await wait_bot_update(bot, previous_snapshot)
    log_message(f"Promo after {label}", next_message)
    return next_message


def is_promo_created_message(message, promo_code: str) -> bool:
    expected_text = settings.promo_success_text.strip().casefold()
    if not expected_text:
        logging.warning("PROMO_SUCCESS_TEXT is empty; promo success cannot be confirmed for %s", promo_code)
        return False

    variants = collect_message_text_variants(message)
    haystack = "\n".join(variants).casefold()
    if expected_text in haystack:
        return True

    action = getattr(message, "action", None)
    action_name = type(action).__name__.casefold() if action is not None else ""
    text = (message.raw_text or "").strip()
    promo_code_lowered = promo_code.casefold()
    is_pin_notice = "pin" in action_name or "pin" in haystack or "Р·Р°РєСЂРµРї" in haystack
    is_promo_context = any(
        token in haystack
        for token in (
            promo_code_lowered,
            "promo",
            "РїСЂРѕРјРѕ",
            "РїСЂРѕРјРѕРєРѕРґ",
            "РґРѕР±Р°РІ",
            "СЃРѕР·РґР°РЅ",
            "СѓСЃРїРµС€",
        )
    )
    if is_pin_notice and (is_promo_context or not text):
        logging.info(
            "Promo success detected from pinned/service message promo_code=%s action=%s text=%r",
            promo_code,
            type(action).__name__ if action is not None else None,
            text,
        )
        return True
    return False


def message_contains_promo_code(message, promo_code: str) -> bool:
    promo_code_lowered = promo_code.casefold()
    variants = collect_message_text_variants(message)
    button_texts = [str(button["text"]) for button in extract_all_buttons(message)]
    haystack = "\n".join([*variants, *button_texts]).casefold()
    return promo_code_lowered in haystack


async def find_promo_message_in_dialog(bot, promo_code: str, *, min_id: int = 0, success_only: bool = False):
    checked = 0
    async for message in client.iter_messages(bot, min_id=max(0, min_id), limit=PROMO_CONFIRM_HISTORY_LIMIT):
        checked += 1
        if not is_incoming_bot_message(message):
            continue
        if success_only:
            if is_promo_created_message(message, promo_code):
                logging.info(
                    "Promo success found in dialog history promo_code=%s message_id=%s checked=%s",
                    promo_code,
                    getattr(message, "id", None),
                    checked,
                )
                return message
        elif message_contains_promo_code(message, promo_code):
            logging.info(
                "Promo code found in dialog history promo_code=%s message_id=%s checked=%s",
                promo_code,
                getattr(message, "id", None),
                checked,
            )
            return message
    logging.info(
        "Promo history scan finished promo_code=%s success_only=%s checked=%s limit=%s min_id=%s",
        promo_code,
        success_only,
        checked,
        PROMO_CONFIRM_HISTORY_LIMIT,
        min_id,
    )
    return None


async def click_optional_promo_back(bot, message):
    button = find_button_by_keywords(
        message,
        ((settings.back_button_text, "РЅР°Р·Р°Рґ", "back", "return"),),
        exclude_keywords=(settings.cancel_button_text,),
    )
    if not button:
        logging.warning("Promo fallback: back button not found")
        return message
    try:
        return await click_keyword_button_and_read(
            bot,
            message,
            ((settings.back_button_text, "РЅР°Р·Р°Рґ", "back", "return"),),
            label="promo fallback back",
            exclude_keywords=(settings.cancel_button_text,),
        )
    except Exception:
        logging.exception("Promo fallback: failed to click back")
        return await latest_bot_message(bot)


async def click_optional_all_promocodes(bot, message):
    candidates = (
        (("РІСЃРµ", "all"), ("РїСЂРѕРјРѕРєРѕРґ", "promo", "coupon", "РєРѕРґ")),
        (("СЃРїРёСЃ", "list"), ("РїСЂРѕРјРѕРєРѕРґ", "promo", "coupon", "РєРѕРґ")),
        (("РїСЂРѕРјРѕРєРѕРґ", "promo", "coupon"),),
    )
    exclude_keywords = (
        settings.cancel_button_text,
        settings.back_button_text,
        "СЃРѕР·Рґ",
        "РґРѕР±Р°РІ",
        "new",
        "create",
    )
    for required_groups in candidates:
        if not find_button_by_keywords(message, required_groups, exclude_keywords=exclude_keywords):
            continue
        try:
            return await click_keyword_button_and_read(
                bot,
                message,
                required_groups,
                label="promo fallback all promocodes",
                exclude_keywords=exclude_keywords,
            )
        except Exception:
            logging.exception("Promo fallback: failed to open all promocodes with groups=%s", required_groups)
            try:
                message = await latest_bot_message(bot)
            except Exception:
                return message
    logging.warning("Promo fallback: all promocodes button not found")
    return message


async def confirm_promo_created_after_submit(
    bot,
    current_message,
    promo_code: str,
    flow_start_message_id: int,
    progress_callback: ProgressCallback | None,
    user_id: str,
):
    success_message = await find_promo_message_in_dialog(
        bot,
        promo_code,
        min_id=flow_start_message_id,
        success_only=True,
    )
    if success_message:
        return success_message

    await emit_process_progress(
        progress_callback,
        "Promo",
        PROMO_STEPS,
        7,
        user_id=user_id,
        extra_lines=[
            "РўРµРєСЃС‚ СѓСЃРїРµС…Р° РЅРµ РЅР°Р№РґРµРЅ РІ РёСЃС‚РѕСЂРёРё.",
            "РџСЂРѕРІРµСЂСЏСЋ СЃРїРёСЃРѕРє РїСЂРѕРјРѕРєРѕРґРѕРІ.",
        ],
    )
    latest_message = current_message or await latest_bot_message(bot)

    menu_message = await click_optional_promo_back(bot, latest_message)
    if message_contains_promo_code(menu_message, promo_code):
        return menu_message

    list_message = await click_optional_all_promocodes(bot, menu_message)
    if message_contains_promo_code(list_message, promo_code):
        return list_message

    history_message = await find_promo_message_in_dialog(
        bot,
        promo_code,
        min_id=flow_start_message_id,
        success_only=False,
    )
    if history_message:
        return history_message

    raise RuntimeError(
        f"Promo {promo_code} was not confirmed: success text was not found and code is absent in promocodes list."
    )


async def create_promo_code_in_admin_bot(
    user_id: str,
    promo_code: str,
    progress_callback: ProgressCallback | None = None,
) -> str:
    await emit_process_progress(
        progress_callback,
        "Promo",
        PROMO_STEPS,
        1,
        user_id=user_id,
        extra_lines=["РћР¶РёРґР°СЋ СЃРІРѕР±РѕРґРЅС‹Р№ Р°РґРјРёРЅ-РїСЂРѕС†РµСЃСЃ"],
    )
    async with admin_flow_context(
        "Promo",
        user_id=user_id,
        progress_callback=progress_callback,
        progress_title="Promo",
        progress_steps=PROMO_STEPS,
        progress_step=1,
    ):
        await emit_process_progress(
            progress_callback,
            "Promo",
            PROMO_STEPS,
            1,
            user_id=user_id,
            extra_lines=[f"РђРґРјРёРЅ-Р±РѕС‚: @{settings.admin_bot_username}"],
        )
        bot = await get_admin_bot_entity()
        logging.info("Starting promo creation user_id=%s promo_code=%s", user_id, promo_code)

        async with admin_conversation(bot) as conv:
            admin_message = await send_admin_and_get_menu(conv, bot)
            promo_flow_start_message_id = int(getattr(admin_message, "id", 0) or 0)

            await emit_process_progress(
                progress_callback,
                "Promo",
                PROMO_STEPS,
                2,
                user_id=user_id,
                extra_lines=[f"С‰Сѓ СЂР°Р·РґРµР»: {settings.promo_button_text}"],
            )
            admin_message = await ensure_message_with_keyword_button(
                conv,
                bot,
                admin_message,
                ((settings.promo_button_text, "РїСЂРѕРјРѕРєРѕРґ", "promo", "coupon"),),
                label="promo section",
                optional_keywords=("СЃРєРёРґ", "РєРѕРґ"),
                exclude_keywords=(settings.cancel_button_text, settings.back_button_text),
            )
            promo_menu_message = await click_keyword_button_and_read(
                bot,
                admin_message,
                ((settings.promo_button_text, "РїСЂРѕРјРѕРєРѕРґ", "promo", "coupon"),),
                label="promo section",
                optional_keywords=("СЃРєРёРґ", "РєРѕРґ"),
                exclude_keywords=(settings.cancel_button_text, settings.back_button_text),
            )

            await emit_process_progress(
                progress_callback,
                "Promo",
                PROMO_STEPS,
                3,
                user_id=user_id,
                extra_lines=[f"С‰Сѓ РєРЅРѕРїРєСѓ: {settings.promo_create_button_text}"],
            )
            create_form_message = await click_keyword_button_and_read(
                bot,
                promo_menu_message,
                ((settings.promo_create_button_text, "СЃРѕР·Рґ", "РґРѕР±Р°РІ", "new", "create"),),
                label="create promo",
                optional_keywords=("РїСЂРѕРјРѕРєРѕРґ", "promo", "coupon"),
                exclude_keywords=(settings.cancel_button_text, settings.back_button_text),
            )

            await emit_process_progress(
                progress_callback,
                "Promo",
                PROMO_STEPS,
                4,
                user_id=user_id,
                extra_lines=[f"РќР°Р·РІР°РЅРёРµ: {promo_code}"],
            )
            budget_message = await send_promo_value_and_read(bot, create_form_message, promo_code, "code")

            await emit_process_progress(
                progress_callback,
                "Promo",
                PROMO_STEPS,
                5,
                user_id=user_id,
                extra_lines=[f"Р‘СЋРґР¶РµС‚: {settings.promo_budget_rub}"],
            )
            amount_message = await send_promo_value_and_read(bot, budget_message, settings.promo_budget_rub, "budget")

            await emit_process_progress(
                progress_callback,
                "Promo",
                PROMO_STEPS,
                6,
                user_id=user_id,
                extra_lines=[f"Р Р°Р·РјРµСЂ СЃСѓРјРјС‹: {settings.promo_amount_rub}"],
            )
            submit_message = await send_promo_value_and_read(bot, amount_message, settings.promo_amount_rub, "amount")

            await emit_process_progress(
                progress_callback,
                "Promo",
                PROMO_STEPS,
                7,
                user_id=user_id,
                extra_lines=[
                    f"РљРЅРѕРїРєР°: {settings.promo_submit_button_text}",
                    "РџРѕСЃР»Рµ РєР»РёРєР° РїСЂРѕРІРµСЂСЋ СЃРїРёСЃРѕРє РїСЂРѕРјРѕРєРѕРґРѕРІ.",
                ],
            )
            final_message = await click_keyword_button_and_settle(
                bot,
                submit_message,
                ((settings.promo_submit_button_text, "СЃРѕР·Рґ", "СЃРѕС…СЂР°РЅ", "РіРѕС‚РѕРІ", "create", "save"),),
                label="submit promo",
                settle_seconds=PROMO_AFTER_SUBMIT_SETTLE_SECONDS,
                optional_keywords=("РїСЂРѕРјРѕРєРѕРґ", "promo", "coupon"),
                exclude_keywords=(settings.cancel_button_text, settings.back_button_text),
            )
            final_message = await confirm_promo_created_after_submit(
                bot,
                final_message,
                promo_code,
                promo_flow_start_message_id,
                progress_callback,
                user_id,
            )
            log_message("Promo final response", final_message)

    result_text = "\n".join(
        (
            f"Promo СЃРѕР·РґР°РЅ: {promo_code}",
            f"РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ: {user_id}",
            f"Р‘СЋРґР¶РµС‚: {settings.promo_budget_rub}",
            f"РЎСѓРјРјР°: {settings.promo_amount_rub}",
        )
    )
    logging.info("Promo creation finished user_id=%s promo_code=%s", user_id, promo_code)
    return result_text


async def send_mail2_to_users_without_subscriptions(
    message_text: str,
    progress_callback: ProgressCallback | None = None,
    cancel_event: asyncio.Event | None = None,
) -> str:
    users = load_users_without_subscriptions_from_database()
    total = len(users)
    await emit_process_progress(
        progress_callback,
        "Mail2 Р±РµР· РїРѕРґРїРёСЃРєРё",
        MAIL2_STEPS,
        1,
        extra_lines=[
            f"SQLite: {database_path()}",
            f"РќР°Р№РґРµРЅРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ Р±РµР· РїРѕРґРїРёСЃРєРё: {total}",
            f"Р”Р»РёРЅР° С‚РµРєСЃС‚Р°: {len(message_text)} СЃРёРјРІРѕР»РѕРІ",
        ],
    )
    if not users:
        return "Mail2: РІ Р±Р°Р·Рµ РЅРµС‚ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ Р±РµР· РїРѕРґРїРёСЃРєРё. Р—Р°РїСѓСЃС‚Рё `scan new`, РµСЃР»Рё Р±Р°Р·Р° СѓСЃС‚Р°СЂРµР»Р°."

    sent: list[str] = []
    failed: list[dict[str, str]] = []
    stopped = False
    for index, user_id in enumerate(users, start=1):
        if cancel_event and cancel_event.is_set():
            stopped = True
            break
        await emit_process_progress(
            progress_callback,
            "Mail2 Р±РµР· РїРѕРґРїРёСЃРєРё",
            MAIL2_STEPS,
            4,
            user_id=user_id,
            extra_lines=[
                f"РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ {index}/{total}",
                f"РћС‚РїСЂР°РІР»РµРЅРѕ: {len(sent)}",
                f"РћС€РёР±РѕРє: {len(failed)}",
            ],
        )
        try:
            await send_mail_to_user_in_admin_bot(user_id, message_text)
            sent.append(user_id)
            logging.info("Mail2 sent user_id=%s progress=%s/%s", user_id, index, total)
        except Exception as error:
            failed.append(
                {
                    "user_id": user_id,
                    "error": f"{type(error).__name__}: {error}",
                }
            )
            logging.exception("Mail2 failed user_id=%s progress=%s/%s", user_id, index, total)

        if cancel_event and cancel_event.is_set():
            stopped = True
            break

        if settings.mail2_send_delay_seconds > 0 and index < total:
            await asyncio.sleep(settings.mail2_send_delay_seconds)

    await emit_process_progress(
        progress_callback,
        "Mail2 Р±РµР· РїРѕРґРїРёСЃРєРё",
        MAIL2_STEPS,
        5,
        extra_lines=[
            f"Р’СЃРµРіРѕ РЅР°Р№РґРµРЅРѕ: {total}",
            f"РћС‚РїСЂР°РІР»РµРЅРѕ: {len(sent)}",
            f"РћС€РёР±РѕРє: {len(failed)}",
            "РћСЃС‚Р°РЅРѕРІР»РµРЅРѕ СЂР°РґРё РґСЂСѓРіРѕР№ Р°РґРјРёРЅ-РєРѕРјР°РЅРґС‹" if stopped else "",
        ],
        done=not failed,
        failed=bool(failed),
    )

    lines = [
        "Mail2 РѕСЃС‚Р°РЅРѕРІР»РµРЅ" if stopped else "Mail2 Р·Р°РІРµСЂС€РµРЅ",
        f"РўРµРєСЃС‚: {len(message_text)} СЃРёРјРІРѕР»РѕРІ",
        f"РџРѕР»СЊР·РѕРІР°С‚РµР»РµР№ Р±РµР· РїРѕРґРїРёСЃРєРё: {total}",
        f"РћС‚РїСЂР°РІР»РµРЅРѕ: {len(sent)}",
        f"РћС€РёР±РѕРє: {len(failed)}",
    ]
    if stopped:
        lines.append("РџСЂРёС‡РёРЅР°: РїСЂРёС€Р»Р° РґСЂСѓРіР°СЏ Р°РґРјРёРЅ-РєРѕРјР°РЅРґР°, Р°РґРјРёРЅ-РїСЂРѕС†РµСЃСЃ РѕСЃРІРѕР±РѕР¶РґРµРЅ.")
    if sent:
        lines.append("")
        lines.append("РћС‚РїСЂР°РІР»РµРЅРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏРј:")
        lines.extend(f"- {user_id}" for user_id in sent[:50])
        if len(sent) > 50:
            lines.append(f"...Рё РµС‰Рµ {len(sent) - 50}")
    if failed:
        lines.append("")
        lines.append("РћС€РёР±РєРё:")
        for item in failed[:50]:
            lines.append(f"- {item['user_id']}: {item['error'][:180]}")
        if len(failed) > 50:
            lines.append(f"...Рё РµС‰Рµ РѕС€РёР±РѕРє: {len(failed) - 50}")
    return "\n".join(lines)


async def send_mail_to_user_in_admin_bot(
    user_id: str,
    message_text: str,
    progress_callback: ProgressCallback | None = None,
) -> str:
    await emit_process_progress(
        progress_callback,
        "Mail РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
        MAIL_STEPS,
        1,
        user_id=user_id,
        extra_lines=["РћР¶РёРґР°СЋ СЃРІРѕР±РѕРґРЅС‹Р№ Р°РґРјРёРЅ-РїСЂРѕС†РµСЃСЃ"],
    )
    async with admin_flow_context(
        "Mail РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
        user_id=user_id,
        progress_callback=progress_callback,
        progress_title="Mail РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
        progress_steps=MAIL_STEPS,
        progress_step=1,
    ):
        await emit_process_progress(
            progress_callback,
            "Mail РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
            MAIL_STEPS,
            1,
            user_id=user_id,
            extra_lines=[f"РџРѕР»СѓС‡Р°СЋ Telegram entity @{settings.admin_bot_username}"],
        )
        bot = await get_admin_bot_entity()
        logging.info("Starting admin mail for user_id=%s in @%s", user_id, settings.admin_bot_username)

        async with admin_conversation(bot) as conv:
            result_message = await open_user_in_admin_bot(
                conv,
                bot,
                user_id,
                progress_callback=progress_callback,
                progress_title="Mail РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
                progress_steps=MAIL_STEPS,
            )
            await emit_process_progress(
                progress_callback,
                "Mail РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
                MAIL_STEPS,
                4,
                user_id=user_id,
                extra_lines=[f"РљРЅРѕРїРєР° РїРёСЃСЊРјР°: {settings.write_user_button_text}"],
            )
            write_message = await click_and_read(bot, result_message, settings.write_user_button_text)

            await emit_process_progress(
                progress_callback,
                "Mail РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
                MAIL_STEPS,
                5,
                user_id=user_id,
                extra_lines=[
                    f"Р”Р»РёРЅР° С‚РµРєСЃС‚Р°: {len(message_text)} СЃРёРјРІРѕР»РѕРІ",
                    f"РџСЂРµРґРїСЂРѕСЃРјРѕС‚СЂ: {message_text[:120]}",
                ],
            )
            logging.info("Sending mail text to admin bot for user_id=%s text=%r", user_id, message_text)
            previous_snapshot = message_snapshot(write_message)
            await send_conv_message_with_retry(bot, message_text)
            preview_message = await wait_bot_update(bot, previous_snapshot)
            log_message("Mail sent response", preview_message)

            await emit_process_progress(
                progress_callback,
                "Mail РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
                MAIL_STEPS,
                6,
                user_id=user_id,
                extra_lines=[f"РљРЅРѕРїРєР° РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ: {settings.mail_next_button_text}"],
            )
            final_message = await click_and_read(bot, preview_message, settings.mail_next_button_text)
            log_message("Mail final response", final_message)

        result_text = "\n".join(
            (
                f"1. Username \u0431\u043e\u0442\u0430: @{settings.admin_bot_username}",
                f"2. ID \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439: {user_id}",
                "3. Mail: \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u043e",
            )
        )
        print("\n===== USER MAIL RESULT =====")
        print(result_text)
        print("============================\n")
        logging.info("Admin mail finished for user_id=%s", user_id)
        return result_text


async def send_to_wizard_target(text: str) -> None:
    try:
        target = await get_wizard_target_entity()
        await client.send_message(target, text)
        note_success_action()
    except FloodWaitError as error:
        wait_seconds = int(getattr(error, "seconds", 1) or 1)
        note_floodwait(wait_seconds)
        logging.warning("FloodWait on wizard send: waiting %ss before retry", wait_seconds)
        await asyncio.sleep(wait_seconds + 1)
        target = await get_wizard_target_entity()
        await client.send_message(target, text)
        note_success_action()


async def ask_wizard_confirmation(
    event,
    *,
    sender_id: int,
    user_id: str,
    base_text: str,
    status_message,
    update_status,
) -> None:
    wizard_target = f"@{settings.wizard_target_username.lstrip('@')}"
    pending_wizard_requests[sender_id] = {
        "stage": "await_choice",
        "user_id": user_id,
        "base_text": base_text,
        "final_text": base_text,
        "status_message": status_message,
        "created_at": now_timestamp(),
    }
    await update_status(
        build_process_status(
            "Wizard",
            WIZARD_STEPS,
            6,
            user_id=user_id,
            target=wizard_target,
            extra_lines=[
                "РљР°СЂС‚РѕС‡РєР° РїРѕРґРіРѕС‚РѕРІР»РµРЅР°",
                "РџСЂРѕРІРµСЂСЊ С‚РµРєСЃС‚ РїРµСЂРµРґ РѕС‚РїСЂР°РІРєРѕР№",
                "РћС‚РІРµС‚СЊ: 1 - РѕС‚РїСЂР°РІРёС‚СЊ, 2 - РґРѕРїРёСЃР°С‚СЊ, 0 - РѕС‚РјРµРЅР°",
            ],
        )
    )
    await safe_event_reply(event, f"РџСЂРµРґРїСЂРѕСЃРјРѕС‚СЂ wizard:\n\n{base_text}")
    await safe_event_reply(
        event,
        "РћС‚РїСЂР°РІР»СЏС‚СЊ РІ wizard?",
        buttons=[
            [Button.text("1 РѕС‚РїСЂР°РІРёС‚СЊ"), Button.text("2 РґРѕРїРёСЃР°С‚СЊ")],
            [Button.text("0 РѕС‚РјРµРЅР°")],
        ],
    )


async def handle_roots_command(event, sender) -> None:
    sender_id = int(event.sender_id or 0)
    sender_user = sender_username(sender)
    text = (event.raw_text or "").strip()
    parts = [part for part in text.split() if part]

    if len(parts) == 1 or (len(parts) > 1 and parts[1].casefold() in {"list", "show", "СЃРїРёСЃРѕРє"}):
        await safe_event_reply(event, build_roots_text(), buttons=build_roots_buttons())
        return

    action = parts[1].casefold()
    if action in {"help", "РїРѕРјРѕС‰СЊ"}:
        await safe_event_reply(event, build_roots_text(), buttons=build_roots_buttons())
        return

    if action in {"add", "РґРѕР±Р°РІРёС‚СЊ"}:
        if len(parts) < 3:
            await safe_event_reply(event, "Р¤РѕСЂРјР°С‚: /roots add <user_id|@username|me> [РєРѕРјРјРµРЅС‚Р°СЂРёР№]")
            return
        target = parts[2].strip()
        note = " ".join(parts[3:]).strip()
        if target.casefold() == "me":
            target = str(sender_id)
            if not note:
                note = "owner"
        try:
            lookup_key = upsert_requester(
                target,
                username=sender_user if target == str(sender_id) else "",
                note=note,
                added_by=str(sender_id),
            )
        except ValueError as error:
            await safe_event_reply(event, f"РќРµ СЃРјРѕРі РґРѕР±Р°РІРёС‚СЊ Р·Р°РїСЂРѕСЃРЅРёРєР°: {error}")
            return
        await safe_event_reply(event, f"Р—Р°РїСЂРѕСЃРЅРёРє РґРѕР±Р°РІР»РµРЅ: {lookup_key}\n\n{build_roots_text()}")
        return

    if action in {"del", "delete", "remove", "rm", "СѓРґР°Р»РёС‚СЊ"}:
        if len(parts) < 3:
            await safe_event_reply(event, "Р¤РѕСЂРјР°С‚: /roots del <user_id|@username>")
            return
        target = parts[2].strip()
        if target.casefold() == "me":
            target = str(sender_id)
        removed = delete_requester(target)
        await safe_event_reply(
            event,
            ("Р—Р°РїСЂРѕСЃРЅРёРє СѓРґР°Р»РµРЅ." if removed else "РўР°РєРѕРіРѕ Р·Р°РїСЂРѕСЃРЅРёРєР° РЅРµ РЅР°С€РµР».") + f"\n\n{build_roots_text()}",
        )
        return

    if action in {"clear", "РѕС‡РёСЃС‚РёС‚СЊ"}:
        if len(parts) < 3 or parts[2].casefold() not in {"yes", "confirm", "РґР°"}:
            await safe_event_reply(event, "Р§С‚РѕР±С‹ РѕС‡РёСЃС‚РёС‚СЊ РІРµСЃСЊ СЃРїРёСЃРѕРє Р·Р°РїСЂРѕСЃРЅРёРєРѕРІ, РѕС‚РїСЂР°РІСЊ: /roots clear yes")
            return
        with connect_database() as conn:
            initialize_database(conn)
            conn.execute("DELETE FROM requesters")
            conn.commit()
        await safe_event_reply(event, "РЎРїРёСЃРѕРє Р·Р°РїСЂРѕСЃРЅРёРєРѕРІ РѕС‡РёС‰РµРЅ. Р§С‚РѕР±С‹ СЃРЅРѕРІР° РґРѕР±Р°РІРёС‚СЊ СЃРµР±СЏ: /roots add me")
        return

    await safe_event_reply(event, "РќРµ РїРѕРЅСЏР» РєРѕРјР°РЅРґСѓ /roots. РћС‚РїСЂР°РІСЊ /roots, С‡С‚РѕР±С‹ РїРѕСЃРјРѕС‚СЂРµС‚СЊ СЃРїРёСЃРѕРє Рё РїРѕРґСЃРєР°Р·РєРё.")


async def handle_gpt_prompt(
    event: events.NewMessage.Event,
    sender_id: int,
    prompt: str,
    status_message=None,
    *,
    compact_status: bool = False,
    reveal_unavailable: bool = True,
) -> None:
    await safe_event_reply(event, support_operator_contact_text())
    return

    log_action_event(
        "gpt_request_start",
        sender_id=sender_id,
        chat_id=getattr(event, "chat_id", None),
        compact_status=compact_status,
        reveal_unavailable=reveal_unavailable,
        prompt=prompt,
    )
    if not prompt.strip():
        log_action_event("gpt_request_empty", sender_id=sender_id, chat_id=getattr(event, "chat_id", None))
        await safe_event_reply(event, assistant_compact_reply("РќР°РїРёС€РёС‚Рµ РІРѕРїСЂРѕСЃ.", "РЇ СЃСЂР°Р·Сѓ РЅР°С‡РЅСѓ РіРѕС‚РѕРІРёС‚СЊ РѕС‚РІРµС‚."))
        return

    local_answer = local_gpt_answer(prompt)
    if local_answer:
        log_action_event(
            "gpt_request_local_answer",
            sender_id=sender_id,
            chat_id=getattr(event, "chat_id", None),
            prompt=prompt,
        )
        if status_message:
            edited = await edit_status_message(status_message, local_answer, force=True)
            if not edited:
                await safe_event_reply(event, local_answer)
        else:
            await safe_event_reply(event, local_answer)
        return

    cached_answer = get_cached_gpt_answer(prompt)
    if cached_answer:
        log_action_event(
            "gpt_request_cache_hit",
            sender_id=sender_id,
            chat_id=getattr(event, "chat_id", None),
            prompt=prompt,
        )
        if status_message:
            edited = await edit_status_message(status_message, cached_answer, force=True)
            if not edited:
                await safe_event_reply(event, cached_answer)
        else:
            await safe_event_reply(event, cached_answer)
        return

    if status_message is None:
        if compact_status:
            status_message = await safe_event_reply(event, gpt_processing_message())
        else:
            status_message = await safe_event_reply(
                event,
                build_process_status(
                    "KBR_GPT",
                    GPT_STEPS,
                    1,
                    extra_lines=[f"РњРѕРґРµР»СЊ: {settings.openai_model}", f"Р’РѕРїСЂРѕСЃ: {len(prompt)} СЃРёРјРІРѕР»РѕРІ"],
                ),
            )

    async def update_gpt_status(text: str, *, force: bool = False) -> None:
        if status_message:
            if compact_status:
                await edit_status_message(status_message, text, force=True)
            else:
                await edit_status_message(status_message, text, force=force)
        else:
            await safe_event_reply(event, text)

    if not settings.openai_api_key:
        logging.warning(
            "KBR_GPT unavailable for sender_id=%s compact=%s reveal_unavailable=%s",
            sender_id,
            compact_status,
            reveal_unavailable,
        )
        log_action_event(
            "gpt_request_unavailable",
            sender_id=sender_id,
            chat_id=getattr(event, "chat_id", None),
            reason="missing_openai_api_key",
        )
        try:
            sender = await event.get_sender()
            save_unresolved_from_event(
                event,
                sender,
                source="gpt",
                reason="gpt_not_configured",
                question_text=prompt,
            )
        except Exception:
            logging.exception("Failed to save unresolved GPT request without API key sender_id=%s", sender_id)
        if compact_status:
            await update_gpt_status(
                gpt_unavailable_message() if reveal_unavailable else gpt_public_fallback_message(),
                force=True,
            )
        else:
            await update_gpt_status(
                build_process_status(
                    "KBR_GPT",
                    GPT_STEPS,
                    1,
                    extra_lines=["OPENAI_API_KEY РЅРµ Р·Р°РґР°РЅ РІ .env РЅР° СЃРµСЂРІРµСЂРµ"],
                    failed=True,
                ),
                force=True,
            )
            await safe_event_reply(event, "KBR_GPT РЅРµ РЅР°СЃС‚СЂРѕРµРЅ: РґРѕР±Р°РІСЊ `OPENAI_API_KEY` РІ `.env` РЅР° СЃРµСЂРІРµСЂРµ Рё РїРµСЂРµР·Р°РїСѓСЃС‚Рё Р±РѕС‚Р°.")
        return

    previous_response_id = gpt_chat_sessions.get(sender_id)
    request_id = uuid.uuid4().hex
    if gpt_request_lock.locked():
        gpt_waiting_request_ids.append(request_id)
        queue_position = len(gpt_waiting_request_ids)
        estimated_wait = queue_position * GPT_QUEUE_WAIT_SECONDS_PER_REQUEST
        pending_gpt_requests[sender_id] = {
            "request_id": request_id,
            "stage": "queue",
            "created_at": now_timestamp(),
            "position": queue_position,
            "prompt": prompt[:200],
        }
        log_action_event(
            "gpt_request_queued",
            sender_id=sender_id,
            chat_id=getattr(event, "chat_id", None),
            previous_response=bool(previous_response_id),
            request_id=request_id,
            position=queue_position,
            estimated_wait_seconds=int(round(estimated_wait)),
        )
        if compact_status:
            await update_gpt_status(gpt_queue_message(queue_position, estimated_wait), force=True)
        else:
            await update_gpt_status(
                build_process_status(
                    "KBR_GPT",
                    GPT_STEPS,
                    1,
                    extra_lines=[
                        f"Р—Р°РїСЂРѕСЃ РїРѕСЃС‚Р°РІР»РµРЅ РІ РѕС‡РµСЂРµРґСЊ: РїРѕР·РёС†РёСЏ {queue_position}",
                        f"РџСЂРёРјРµСЂРЅРѕРµ РѕР¶РёРґР°РЅРёРµ: {int(round(estimated_wait))} СЃРµРє",
                    ],
                ),
                force=True,
            )

    async with gpt_request_lock:
        if request_id in gpt_waiting_request_ids:
            gpt_waiting_request_ids.remove(request_id)
        pending_gpt_requests.pop(sender_id, None)
        request_state = {
            "stage": "request",
            "user_id": "-",
            "created_at": now_timestamp(),
            "canceled": False,
            "suppress_output": False,
            "request_id": request_id,
        }
        active_gpt_requests[sender_id] = request_state
        log_action_event(
            "gpt_request_active",
            sender_id=sender_id,
            chat_id=getattr(event, "chat_id", None),
            previous_response=bool(previous_response_id),
        )
        rate_limit_deadline = time.monotonic() + GPT_RATE_LIMIT_RETRY_WINDOW_SECONDS
        rate_limit_wait_total = 0.0
        rate_limit_retries = 0
        timeout_retry_deadline = time.monotonic() + GPT_TIMEOUT_RETRY_WINDOW_SECONDS
        timeout_wait_total = 0.0
        timeout_retries = 0

        async def wait_with_countdown(wait_seconds: float, *, reason: str) -> bool:
            remaining_seconds = max(1, int(round(wait_seconds)))
            while remaining_seconds > 0:
                if request_state.get("canceled") or request_state.get("suppress_output"):
                    return False
                if compact_status:
                    if reason == "timeout":
                        await update_gpt_status(gpt_timeout_wait_message(remaining_seconds), force=True)
                    else:
                        await update_gpt_status(gpt_retry_message(remaining_seconds), force=True)
                else:
                    extra_lines = [
                        (
                            f"РўР°Р№РјР°СѓС‚ РѕС‚РІРµС‚Р°, РїРѕРІС‚РѕСЂ С‡РµСЂРµР· {remaining_seconds} СЃРµРє"
                            if reason == "timeout"
                            else f"Р›РёРјРёС‚ Р·Р°РїСЂРѕСЃРѕРІ, РїРѕРІС‚РѕСЂ С‡РµСЂРµР· {remaining_seconds} СЃРµРє"
                        ),
                        (
                            f"РџРѕРїС‹С‚РєР° timeout-РїРѕРІС‚РѕСЂР°: {timeout_retries}"
                            if reason == "timeout"
                            else f"РџРѕРїС‹С‚РєР°: {rate_limit_retries}"
                        ),
                    ]
                    await update_gpt_status(
                        build_process_status(
                            "KBR_GPT",
                            GPT_STEPS,
                            2,
                            extra_lines=extra_lines,
                        ),
                        force=True,
                    )
                await asyncio.sleep(1)
                remaining_seconds -= 1
            return True

        try:
            if compact_status:
                await update_gpt_status(gpt_processing_message())
            else:
                await update_gpt_status(
                    build_process_status(
                        "KBR_GPT",
                        GPT_STEPS,
                        2,
                        extra_lines=[
                            f"РњРѕРґРµР»СЊ: {settings.openai_model}",
                            "РљРѕРЅС‚РµРєСЃС‚: " + ("РїСЂРѕРґРѕР»Р¶Р°СЋ РїСЂРѕС€Р»С‹Р№ РґРёР°Р»РѕРі" if previous_response_id else "РЅРѕРІС‹Р№ РґРёР°Р»РѕРі"),
                        ],
                    )
                )
            while True:
                try:
                    answer_text, response_id = await ask_chatgpt(prompt, previous_response_id)
                    break
                except Exception as retry_error:
                    error_text = str(retry_error)
                    if is_daily_limit_error_text(error_text):
                        raise
                    if is_rate_limit_error_text(error_text):
                        now_monotonic = time.monotonic()
                        remaining = rate_limit_deadline - now_monotonic
                        if remaining <= 0:
                            raise RuntimeError(
                                f"KBR_GPT_RATE_LIMIT_TIMEOUT after {int(rate_limit_wait_total)}s: {error_text[:300]}"
                            ) from retry_error
                        wait_seconds = min(parse_retry_seconds_from_error_text(error_text), remaining)
                        rate_limit_retries += 1
                        rate_limit_wait_total += wait_seconds
                        log_action_event(
                            "gpt_request_retry",
                            sender_id=sender_id,
                            chat_id=getattr(event, "chat_id", None),
                            retry_number=rate_limit_retries,
                            wait_seconds=int(round(wait_seconds)),
                            error=error_text,
                        )
                        if not await wait_with_countdown(wait_seconds, reason="rate_limit"):
                            return
                        continue
                    if is_timeout_error_text(error_text):
                        now_monotonic = time.monotonic()
                        remaining = timeout_retry_deadline - now_monotonic
                        if remaining <= 0:
                            raise RuntimeError(
                                f"KBR_GPT_TIMEOUT_RETRY_EXHAUSTED after {int(timeout_wait_total)}s: {error_text[:300]}"
                            ) from retry_error
                        wait_seconds = min(GPT_TIMEOUT_RETRY_DELAY_SECONDS, remaining)
                        timeout_retries += 1
                        timeout_wait_total += wait_seconds
                        log_action_event(
                            "gpt_request_timeout_retry",
                            sender_id=sender_id,
                            chat_id=getattr(event, "chat_id", None),
                            retry_number=timeout_retries,
                            wait_seconds=int(round(wait_seconds)),
                            error=error_text,
                        )
                        if not await wait_with_countdown(wait_seconds, reason="timeout"):
                            return
                        continue
                    raise
            if request_state.get("canceled") or request_state.get("suppress_output"):
                logging.info(
                    "KBR_GPT output suppressed sender_id=%s reason=%s",
                    sender_id,
                    request_state.get("reason") or "",
                )
                log_action_event(
                    "gpt_request_suppressed",
                    sender_id=sender_id,
                    chat_id=getattr(event, "chat_id", None),
                    reason=str(request_state.get("reason") or ""),
                )
                return
            if response_id:
                gpt_chat_sessions[sender_id] = response_id
            log_action_event(
                "gpt_request_success",
                sender_id=sender_id,
                chat_id=getattr(event, "chat_id", None),
                response_id=response_id,
                answer_length=len(answer_text),
            )
            store_cached_gpt_answer(prompt, answer_text)
            if compact_status:
                await update_gpt_status(assistant_compact_reply("РћС‚РІРµС‚ РіРѕС‚РѕРІ.", "РћС‚РїСЂР°РІР»СЏСЋ РµРіРѕ РІ С‡Р°С‚."), force=True)
            else:
                await update_gpt_status(
                    build_process_status(
                        "KBR_GPT",
                        GPT_STEPS,
                        len(GPT_STEPS),
                        extra_lines=[f"РћС‚РІРµС‚: {len(answer_text)} СЃРёРјРІРѕР»РѕРІ"],
                        done=True,
                    ),
                    force=True,
                )
            final_answer_text = answer_text.strip() or "Р“РѕС‚РѕРІРѕ."
            edited_in_place = False
            if status_message:
                edited_in_place = await edit_status_message(status_message, final_answer_text, force=True)
            if not edited_in_place:
                await safe_event_reply(event, final_answer_text)
        except Exception as error:
            logging.exception("KBR_GPT request failed sender_id=%s", sender_id)
            error_text = str(error)
            is_rate_limit_timeout = "KBR_GPT_RATE_LIMIT_TIMEOUT" in error_text or (
                is_rate_limit_error_text(error_text) and rate_limit_wait_total >= GPT_RATE_LIMIT_RETRY_WINDOW_SECONDS
            )
            is_timeout_retry_exhausted = "KBR_GPT_TIMEOUT_RETRY_EXHAUSTED" in error_text or (
                is_timeout_error_text(error_text) and timeout_wait_total >= GPT_TIMEOUT_RETRY_WINDOW_SECONDS
            )
            log_action_event(
                "gpt_request_error",
                sender_id=sender_id,
                chat_id=getattr(event, "chat_id", None),
                error=error_text,
                rate_limit_timeout=is_rate_limit_timeout,
                timeout_retry_exhausted=is_timeout_retry_exhausted,
                retries=rate_limit_retries,
                waited_seconds=int(rate_limit_wait_total),
                timeout_retries=timeout_retries,
                timeout_waited_seconds=int(timeout_wait_total),
            )
            if request_state.get("canceled") or request_state.get("suppress_output"):
                logging.info(
                    "KBR_GPT error suppressed sender_id=%s reason=%s error=%s",
                    sender_id,
                    request_state.get("reason") or "",
                    error_text[:300],
                )
                log_action_event(
                    "gpt_request_error_suppressed",
                    sender_id=sender_id,
                    chat_id=getattr(event, "chat_id", None),
                    reason=str(request_state.get("reason") or ""),
                    error=error_text,
                )
                return
            try:
                sender = await event.get_sender()
                save_unresolved_from_event(
                    event,
                    sender,
                    source="gpt",
                    reason="gpt_rate_limit_timeout" if is_rate_limit_timeout else "gpt_error",
                    question_text=prompt,
                    payload={
                        "error_text": error_text[:500],
                        "waited_seconds": int(rate_limit_wait_total),
                        "retries": int(rate_limit_retries),
                        "timeout_waited_seconds": int(timeout_wait_total),
                        "timeout_retries": int(timeout_retries),
                    },
                )
            except Exception:
                logging.exception("Failed to save unresolved GPT failure sender_id=%s", sender_id)
            if is_rate_limit_timeout and compact_status:
                try:
                    sender = await event.get_sender()
                    sender_username_value = sender_username(sender)
                    sender_full_name_value = sender_full_name(sender)
                    await send_to_wizard_target(
                        "\n".join(
                            (
                                "Р­СЃРєР°Р»Р°С†РёСЏ KBR_GPT РІ РїРѕРґРґРµСЂР¶РєСѓ (Р»РёРјРёС‚ > 2 РјРёРЅСѓС‚)",
                                f"Р’СЂРµРјСЏ: {datetime.now().isoformat(timespec='seconds')}",
                                f"РћС‚РїСЂР°РІРёС‚РµР»СЊ Telegram ID: {sender_id}",
                                (
                                    f"РћС‚РїСЂР°РІРёС‚РµР»СЊ username: @{sender_username_value}"
                                    if sender_username_value
                                    else "РћС‚РїСЂР°РІРёС‚РµР»СЊ username: РЅРµС‚"
                                ),
                                (
                                    f"РћС‚РїСЂР°РІРёС‚РµР»СЊ РёРјСЏ: {sender_full_name_value}"
                                    if sender_full_name_value
                                    else "РћС‚РїСЂР°РІРёС‚РµР»СЊ РёРјСЏ: РЅРµС‚"
                                ),
                                "",
                                "РўРµРєСЃС‚ Р·Р°РїСЂРѕСЃР°:",
                                prompt.strip() or "[РїСѓСЃС‚Рѕ]",
                            )
                        )
                    )
                except Exception:
                    logging.exception("Failed to forward GPT rate-limit escalation to support sender_id=%s", sender_id)
            if compact_status:
                if is_rate_limit_timeout:
                    await update_gpt_status(gpt_escalated_message(), force=True)
                else:
                    await update_gpt_status(gpt_failed_message(error_text), force=True)
            else:
                await update_gpt_status(
                    build_process_status(
                        "KBR_GPT",
                        GPT_STEPS,
                        len(GPT_STEPS),
                        extra_lines=[
                            "Р—Р°РїСЂРѕСЃ Р·Р°РІРµСЂС€РёР»СЃСЏ РѕС€РёР±РєРѕР№",
                            f"РћР¶РёРґР°РЅРёРµ СЂРµС‚СЂР°РµРІ: {int(rate_limit_wait_total)} СЃРµРє" if rate_limit_wait_total > 0 else "",
                            error_text[:300],
                        ],
                        failed=True,
                    ),
                    force=True,
                )
                if is_rate_limit_timeout:
                    await safe_event_reply(
                        event,
                        assistant_user_message(
                            f"РЎРµСЂРІРёСЃ РїРµСЂРµРіСЂСѓР¶РµРЅ Р±РѕР»РµРµ 2 РјРёРЅСѓС‚. РџРµСЂРµРґР°СЋ РІ РїРѕРґРґРµСЂР¶РєСѓ.\nРЎРІСЏР¶РёС‚РµСЃСЊ СЃ @{SUPPORT_OPERATOR_USERNAME}"
                        ),
                    )
                else:
                    await safe_event_reply(event, gpt_failed_message(error_text))
        finally:
            log_action_event(
                "gpt_request_finish",
                sender_id=sender_id,
                chat_id=getattr(event, "chat_id", None),
                request_id=request_id,
            )
            if request_id in gpt_waiting_request_ids:
                gpt_waiting_request_ids.remove(request_id)
            pending_gpt_requests.pop(sender_id, None)
            active_gpt_requests.pop(sender_id, None)


@client.on(events.CallbackQuery(data=SCAN_CANCEL_CALLBACK_DATA))
async def handle_scan_cancel(event: events.CallbackQuery.Event) -> None:
    if not active_scan_cancel_event:
        await event.answer("Scan СЃРµР№С‡Р°СЃ РЅРµ РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ.", alert=False)
        return

    if active_scan_owner_id is not None and event.sender_id != active_scan_owner_id:
        await event.answer("РџРѕСЃС‚Р°РІРёС‚СЊ scan РЅР° РїР°СѓР·Сѓ РјРѕР¶РµС‚ С‚РѕР»СЊРєРѕ С‚РѕС‚, РєС‚Рѕ РµРіРѕ Р·Р°РїСѓСЃС‚РёР».", alert=True)
        return

    active_scan_cancel_event.set()
    await event.answer("РџР°СѓР·Р° РїСЂРёРЅСЏС‚Р°. Р—Р°РІРµСЂС€Сѓ С‚РµРєСѓС‰РµРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ Рё СЃРѕС…СЂР°РЅСЋ РїСЂРѕРіСЂРµСЃСЃ.", alert=False)


@client.on(events.CallbackQuery(pattern=b"^poc:"))
async def handle_poc_callback(event: events.CallbackQuery.Event) -> None:
    data = bytes(event.data or b"")
    changed = False
    if data == POC_SCAN_PAUSE_CALLBACK_DATA:
        if active_scan_cancel_event and not active_scan_cancel_event.is_set():
            active_scan_cancel_event.set()
            changed = True
            await event.answer("Scan РїРѕСЃС‚Р°РІР»РµРЅ РЅР° РїР°СѓР·Сѓ.", alert=False)
        else:
            await event.answer("Scan СЃРµР№С‡Р°СЃ РЅРµ Р°РєС‚РёРІРµРЅ.", alert=False)
    elif data == POC_MAIL2_STOP_CALLBACK_DATA:
        if active_mail2_cancel_event and not active_mail2_cancel_event.is_set():
            active_mail2_cancel_event.set()
            changed = True
            await event.answer("Mail2 РїРѕР»СѓС‡РёР» РєРѕРјР°РЅРґСѓ РѕСЃС‚Р°РЅРѕРІРєРё.", alert=False)
        else:
            await event.answer("Mail2 СЃРµР№С‡Р°СЃ РЅРµ Р°РєС‚РёРІРµРЅ.", alert=False)
    elif data == POC_CLEAR_WIZARD_CALLBACK_DATA:
        count = len(pending_wizard_requests)
        pending_wizard_requests.clear()
        changed = count > 0
        await event.answer(f"Wizard pending РѕС‡РёС‰РµРЅРѕ: {count}.", alert=False)
    elif data == POC_CLEAR_MAIL2_PENDING_CALLBACK_DATA:
        count = len(pending_mail2_requests)
        pending_mail2_requests.clear()
        changed = count > 0
        await event.answer(f"Mail2 pending РѕС‡РёС‰РµРЅРѕ: {count}.", alert=False)
    elif data == b"poc:clear_mail_pending":
        count = len(pending_direct_mail_requests)
        pending_direct_mail_requests.clear()
        changed = count > 0
        await event.answer(f"Mail pending РѕС‡РёС‰РµРЅРѕ: {count}.", alert=False)
    elif data == POC_CLEAR_GPT_PENDING_CALLBACK_DATA:
        count = len(pending_gpt_requests) + len(gpt_waiting_request_ids)
        pending_gpt_requests.clear()
        gpt_waiting_request_ids.clear()
        changed = count > 0
        await event.answer(f"GPT pending РѕС‡РёС‰РµРЅРѕ: {count}.", alert=False)
    elif data == b"poc:clear_smart_pending":
        count = len(pending_smart_actions)
        pending_smart_actions.clear()
        changed = count > 0
        await event.answer(f"Smart pending РѕС‡РёС‰РµРЅРѕ: {count}.", alert=False)
    elif data == POC_CLEAR_ALL_PENDING_CALLBACK_DATA:
        count = (
            len(pending_wizard_requests)
            + len(pending_mail2_requests)
            + len(pending_direct_mail_requests)
            + len(pending_gpt_requests)
            + len(gpt_waiting_request_ids)
            + len(pending_smart_actions)
        )
        pending_wizard_requests.clear()
        pending_mail2_requests.clear()
        pending_direct_mail_requests.clear()
        pending_gpt_requests.clear()
        gpt_waiting_request_ids.clear()
        pending_smart_actions.clear()
        changed = count > 0
        await event.answer(f"Pending РѕС‡РёС‰РµРЅРѕ: {count}.", alert=False)
    elif data == POC_REFRESH_CALLBACK_DATA:
        await event.answer("РћР±РЅРѕРІР»СЏСЋ РїСЂРѕС†РµСЃСЃС‹.", alert=False)
    else:
        await event.answer("РќРµРёР·РІРµСЃС‚РЅР°СЏ РєРѕРјР°РЅРґР° РїСЂРѕС†РµСЃСЃРѕРІ.", alert=True)
        return

    logging.info("Process callback data=%r sender_id=%s changed=%s", data, event.sender_id, changed)
    try:
        await event.edit(build_poc_text(), buttons=build_poc_buttons())
    except MessageNotModifiedError:
        pass
    except FloodWaitError as error:
        wait_seconds = int(getattr(error, "seconds", 1) or 1)
        note_floodwait(wait_seconds)
        logging.warning("FloodWait on POC callback edit: %ss", wait_seconds)
    except Exception:
        logging.exception("Failed to edit POC message")


@client.on(events.NewMessage)
async def handle_private_message(event: events.NewMessage.Event) -> None:
    global active_scan_cancel_event, active_scan_owner_id, active_scan_menu_owner_id, active_scan_action_delay_seconds, active_scan_base_delay_seconds, active_scan_reset_requested
    global active_mail2_cancel_event

    if not event.is_private:
        return

    if event.out:
        return

    admin_bot = await get_admin_bot_entity()
    if event.chat_id == getattr(admin_bot, "id", None):
        return

    prune_expired_pending_requests()

    sender = await event.get_sender()
    if not event.out and getattr(sender, "bot", False):
        return
    sender_id = int(event.sender_id or 0)
    incoming_text = (event.raw_text or "").strip()
    requester_allowed = is_requester_allowed(sender_id, sender)
    simulate_public_mode = False
    simulated_text = incoming_text
    if requester_allowed and incoming_text:
        lowered = incoming_text.casefold()
        if lowered == "-p" or lowered.startswith("-p "):
            simulate_public_mode = True
            simulated_text = incoming_text[2:].strip()

    log_action_event(
        "incoming_message",
        sender_id=sender_id,
        chat_id=getattr(event, "chat_id", None),
        username=sender_username(sender),
        full_name=sender_full_name(sender),
        is_requester=requester_allowed,
        simulate_public_mode=simulate_public_mode,
        is_voice=is_voice_or_audio_message(event),
        text=incoming_text,
    )

    if simulate_public_mode:
        if not simulated_text:
            await safe_event_reply(
                event,
                assistant_compact_reply(
                    "Режим -p активирован.",
                    "Напишите после -p текст пользователя, например: -p не работает впн",
                ),
            )
            return
        log_action_event("route", sender_id=sender_id, route="simulate_non_requester", text=simulated_text)
        await handle_non_requester_message(event, sender, sender_id, simulated_text)
        return

    roots_command = is_roots_command(incoming_text)
    roots_empty = requester_count() == 0
    if roots_command and (roots_empty or requester_allowed):
        log_action_event("route", sender_id=sender_id, route="roots_command", text=incoming_text)
        await handle_roots_command(event, sender)
        return

    if not requester_allowed:
        log_action_event("route", sender_id=sender_id, route="non_requester", text=incoming_text)
        await handle_non_requester_message(event, sender, sender_id, incoming_text)
        return

    incoming_is_explicit_command = is_explicit_requester_command_input(incoming_text, sender_id)
    if incoming_is_explicit_command:
        log_action_event("route", sender_id=sender_id, route="explicit_requester_command", text=incoming_text)
        pending_smart_actions.pop(sender_id, None)
        pending_gpt_requests.pop(sender_id, None)
        if mark_active_gpt_request(sender_id, suppress_output=True, reason="interrupted_by_command"):
            active_gpt_requests.pop(sender_id, None)

    pending_smart = pending_smart_actions.get(sender_id)
    if pending_smart:
        log_action_event("route", sender_id=sender_id, route="pending_smart", text=incoming_text)
        cleaned = incoming_text.strip().casefold()
        if cleaned in {"1", "РґР°", "yes", "y", "РІС‹РїРѕР»РЅРёС‚СЊ", "РѕС‚РїСЂР°РІРёС‚СЊ", "send"}:
            pending_smart_actions.pop(sender_id, None)
            await execute_smart_action(event, sender_id, dict(pending_smart.get("action") or {}), confirmed=True)
            return
        if cleaned in {"0", "РЅРµС‚", "no", "n", "РѕС‚РјРµРЅР°", "cancel", "/cancel"}:
            pending_smart_actions.pop(sender_id, None)
            await safe_event_reply(event, "РЈРјРЅРѕРµ РґРµР№СЃС‚РІРёРµ РѕС‚РјРµРЅРµРЅРѕ.")
            return
        if incoming_text:
            pending_smart_actions.pop(sender_id, None)
            await handle_smart_request(event, sender_id, incoming_text, source="text correction")
            return
        await safe_event_reply(event, "РџРѕРґС‚РІРµСЂРґРё РґРµР№СЃС‚РІРёРµ: `1 РІС‹РїРѕР»РЅРёС‚СЊ` РёР»Рё `0 РѕС‚РјРµРЅР°`.")
        return

    pending_wizard = pending_wizard_requests.get(sender_id)
    if pending_wizard:
        log_action_event("route", sender_id=sender_id, route="pending_wizard", stage=str(pending_wizard.get("stage") or ""), text=incoming_text)
        stage = str(pending_wizard.get("stage") or "")
        pending_wizard_user_id = str(pending_wizard.get("user_id") or "")
        wizard_target = f"@{settings.wizard_target_username.lstrip('@')}"
        pending_status_message = pending_wizard.get("status_message")

        async def update_pending_wizard_status(text: str) -> None:
            if pending_status_message:
                await edit_status_message(pending_status_message, text)
            else:
                await safe_event_reply(event, text)

        if stage == "await_choice":
            choice = parse_wizard_reply_choice(incoming_text)
            if choice == "cancel":
                pending_wizard_requests.pop(sender_id, None)
                await update_pending_wizard_status(
                    build_process_status(
                        "Wizard",
                        WIZARD_STEPS,
                        6,
                        user_id=pending_wizard_user_id,
                        target=wizard_target,
                        extra_lines=["РћС‚РїСЂР°РІРєР° РѕС‚РјРµРЅРµРЅР° РїРѕР»СЊР·РѕРІР°С‚РµР»РµРј"],
                        done=True,
                    )
                )
                return
            if choice == "send_now":
                try:
                    await update_pending_wizard_status(
                        build_process_status(
                            "Wizard",
                            WIZARD_STEPS,
                            7,
                            user_id=pending_wizard_user_id,
                            target=wizard_target,
                            extra_lines=["РћС‚РїСЂР°РІР»СЏСЋ РїРѕРґРіРѕС‚РѕРІР»РµРЅРЅСѓСЋ РєР°СЂС‚РѕС‡РєСѓ Р±РµР· РґРѕРїРѕР»РЅРµРЅРёСЏ"],
                        )
                    )
                    await send_to_wizard_target(str(pending_wizard.get("final_text") or pending_wizard["base_text"]))
                    pending_wizard_requests.pop(sender_id, None)
                    await update_pending_wizard_status(
                        build_process_status(
                            "Wizard",
                            WIZARD_STEPS,
                            7,
                            user_id=pending_wizard_user_id,
                            target=wizard_target,
                            extra_lines=["РљР°СЂС‚РѕС‡РєР° РѕС‚РїСЂР°РІР»РµРЅР°"],
                            done=True,
                        )
                    )
                except Exception:
                    logging.exception("Wizard send failed sender_id=%s", sender_id)
                    await update_pending_wizard_status(
                        build_process_status(
                            "Wizard",
                            WIZARD_STEPS,
                            7,
                            user_id=pending_wizard_user_id,
                            target=wizard_target,
                            extra_lines=["РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚РїСЂР°РІРёС‚СЊ РєР°СЂС‚РѕС‡РєСѓ", "РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі"],
                            failed=True,
                        )
                    )
                    await safe_event_reply(event, "РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚РїСЂР°РІРёС‚СЊ РІ wizard. РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё РІ Р»РѕРіРµ.")
                return
            if choice == "add_text":
                pending_wizard["stage"] = "await_extra_text"
                await update_pending_wizard_status(
                    build_process_status(
                        "Wizard",
                        WIZARD_STEPS,
                        6,
                        user_id=pending_wizard_user_id,
                        target=wizard_target,
                        extra_lines=[
                            "РћР¶РёРґР°СЋ РґРѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Р№ С‚РµРєСЃС‚",
                            "РЎР»РµРґСѓСЋС‰РµРµ СЃРѕРѕР±С‰РµРЅРёРµ Р±СѓРґРµС‚ РґРѕР±Р°РІР»РµРЅРѕ Рє РєР°СЂС‚РѕС‡РєРµ",
                            "Р”Р»СЏ РѕС‚РјРµРЅС‹ РѕС‚РїСЂР°РІСЊС‚Рµ 0",
                        ],
                    )
                )
                return
            await update_pending_wizard_status(
                build_process_status(
                    "Wizard",
                    WIZARD_STEPS,
                    6,
                    user_id=pending_wizard_user_id,
                    target=wizard_target,
                    extra_lines=["РќРµ РїРѕРЅСЏР» РѕС‚РІРµС‚. РќР°РїРёС€РёС‚Рµ 1, 2 РёР»Рё 0"],
                )
            )
            return
        elif stage == "await_extra_text":
            choice = parse_wizard_reply_choice(incoming_text)
            if choice == "cancel":
                pending_wizard_requests.pop(sender_id, None)
                await update_pending_wizard_status(
                    build_process_status(
                        "Wizard",
                        WIZARD_STEPS,
                        6,
                        user_id=pending_wizard_user_id,
                        target=wizard_target,
                        extra_lines=["РћС‚РїСЂР°РІРєР° РѕС‚РјРµРЅРµРЅР° РїРѕР»СЊР·РѕРІР°С‚РµР»РµРј"],
                        done=True,
                    )
                )
                return

            extra_text = incoming_text
            full_text = "\n\n".join(
                (
                    str(pending_wizard["base_text"]),
                    f"Р”РѕРїРѕР»РЅРµРЅРёРµ:\n{extra_text}",
                )
            )
            pending_wizard["extra_text"] = extra_text
            pending_wizard["final_text"] = full_text
            pending_wizard["stage"] = "await_final_choice"
            await update_pending_wizard_status(
                build_process_status(
                    "Wizard",
                    WIZARD_STEPS,
                    6,
                    user_id=pending_wizard_user_id,
                    target=wizard_target,
                    extra_lines=[
                        "Р”РѕРїРѕР»РЅРµРЅРёРµ РґРѕР±Р°РІР»РµРЅРѕ",
                        "РџСЂРѕРІРµСЂСЊ РёС‚РѕРіРѕРІС‹Р№ С‚РµРєСЃС‚",
                        "РћС‚РІРµС‚СЊ: 1 - РѕС‚РїСЂР°РІРёС‚СЊ, 2 - РёР·РјРµРЅРёС‚СЊ РґРѕРїРёСЃРєСѓ, 0 - РѕС‚РјРµРЅР°",
                    ],
                )
            )
            await safe_event_reply(event, f"С‚РѕРіРѕРІС‹Р№ РїСЂРµРґРїСЂРѕСЃРјРѕС‚СЂ wizard:\n\n{full_text}")
            await safe_event_reply(
                event,
                "РћС‚РїСЂР°РІР»СЏС‚СЊ СЌС‚РѕС‚ РІР°СЂРёР°РЅС‚?",
                buttons=[
                    [Button.text("1 РѕС‚РїСЂР°РІРёС‚СЊ"), Button.text("2 РёР·РјРµРЅРёС‚СЊ РґРѕРїРёСЃРєСѓ")],
                    [Button.text("0 РѕС‚РјРµРЅР°")],
                ],
            )
            return

        elif stage == "await_final_choice":
            choice = parse_wizard_reply_choice(incoming_text)
            if choice == "cancel":
                pending_wizard_requests.pop(sender_id, None)
                await update_pending_wizard_status(
                    build_process_status(
                        "Wizard",
                        WIZARD_STEPS,
                        6,
                        user_id=pending_wizard_user_id,
                        target=wizard_target,
                        extra_lines=["РћС‚РїСЂР°РІРєР° РѕС‚РјРµРЅРµРЅР° РїРѕР»СЊР·РѕРІР°С‚РµР»РµРј"],
                        done=True,
                    )
                )
                return
            if choice == "add_text":
                pending_wizard["stage"] = "await_extra_text"
                await update_pending_wizard_status(
                    build_process_status(
                        "Wizard",
                        WIZARD_STEPS,
                        6,
                        user_id=pending_wizard_user_id,
                        target=wizard_target,
                        extra_lines=[
                            "РћР¶РёРґР°СЋ РЅРѕРІС‹Р№ РґРѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Р№ С‚РµРєСЃС‚",
                            "РЎР»РµРґСѓСЋС‰РµРµ СЃРѕРѕР±С‰РµРЅРёРµ Р·Р°РјРµРЅРёС‚ РїСЂРѕС€Р»СѓСЋ РґРѕРїРёСЃРєСѓ",
                            "Р”Р»СЏ РѕС‚РјРµРЅС‹ РѕС‚РїСЂР°РІСЊС‚Рµ 0",
                        ],
                    )
                )
                return
            if choice != "send_now":
                await update_pending_wizard_status(
                    build_process_status(
                        "Wizard",
                        WIZARD_STEPS,
                        6,
                        user_id=pending_wizard_user_id,
                        target=wizard_target,
                        extra_lines=["РќРµ РїРѕРЅСЏР» РѕС‚РІРµС‚. РќР°РїРёС€РёС‚Рµ 1, 2 РёР»Рё 0"],
                    )
                )
                return

            try:
                await update_pending_wizard_status(
                    build_process_status(
                        "Wizard",
                        WIZARD_STEPS,
                        7,
                        user_id=pending_wizard_user_id,
                        target=wizard_target,
                        extra_lines=[
                            "РџРѕРґС‚РІРµСЂР¶РґРµРЅРёРµ РїРѕР»СѓС‡РµРЅРѕ",
                            f"Р”Р»РёРЅР° РёС‚РѕРіРѕРІРѕРіРѕ С‚РµРєСЃС‚Р°: {len(str(pending_wizard.get('final_text') or ''))} СЃРёРјРІРѕР»РѕРІ",
                            "РћС‚РїСЂР°РІР»СЏСЋ РІ wizard",
                        ],
                    )
                )
                await send_to_wizard_target(str(pending_wizard.get("final_text") or pending_wizard["base_text"]))
                pending_wizard_requests.pop(sender_id, None)
                await update_pending_wizard_status(
                    build_process_status(
                        "Wizard",
                        WIZARD_STEPS,
                        7,
                        user_id=pending_wizard_user_id,
                        target=wizard_target,
                        extra_lines=["РљР°СЂС‚РѕС‡РєР° РѕС‚РїСЂР°РІР»РµРЅР° РїРѕСЃР»Рµ РїРѕРґС‚РІРµСЂР¶РґРµРЅРёСЏ"],
                        done=True,
                    )
                )
            except Exception:
                logging.exception("Wizard send with extra failed sender_id=%s", sender_id)
                await update_pending_wizard_status(
                    build_process_status(
                        "Wizard",
                        WIZARD_STEPS,
                        7,
                        user_id=pending_wizard_user_id,
                        target=wizard_target,
                        extra_lines=["РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚РїСЂР°РІРёС‚СЊ РєР°СЂС‚РѕС‡РєСѓ СЃ РґРѕРїРѕР»РЅРµРЅРёРµРј", "РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі"],
                        failed=True,
                    )
                )
                await safe_event_reply(event, "РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚РїСЂР°РІРёС‚СЊ РІ wizard. РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё РІ Р»РѕРіРµ.")
            return

    pending_mail2 = pending_mail2_requests.get(sender_id)
    if pending_mail2:
        if incoming_text.strip().casefold() in {"0", "РѕС‚РјРµРЅР°", "cancel", "/cancel"}:
            pending_mail2_requests.pop(sender_id, None)
            status_message = pending_mail2.get("status_message")
            cancel_text = build_process_status(
                "Mail2 Р±РµР· РїРѕРґРїРёСЃРєРё",
                MAIL2_STEPS,
                3,
                extra_lines=["Р Р°СЃСЃС‹Р»РєР° РѕС‚РјРµРЅРµРЅР° РїРѕР»СЊР·РѕРІР°С‚РµР»РµРј"],
                done=True,
            )
            if status_message:
                await edit_status_message(status_message, cancel_text, force=True)
            else:
                await safe_event_reply(event, cancel_text)
            return

        message_text = incoming_text.strip()
        if not message_text:
            await safe_event_reply(event, "РџСЂРёС€Р»Рё С‚РµРєСЃС‚ РґР»СЏ /mail2 РёР»Рё `0 РѕС‚РјРµРЅР°`.")
            return

        pending_mail2_requests.pop(sender_id, None)
        status_message = pending_mail2.get("status_message")

        async def update_pending_mail2_status(text: str) -> None:
            if status_message:
                await edit_status_message(status_message, text)
            else:
                await safe_event_reply(event, text)

        scan_interruption = await request_scan_pause_for_priority_command(event, "mail2")
        active_mail2_cancel_event = asyncio.Event()
        try:
            result = await send_mail2_to_users_without_subscriptions(
                message_text,
                progress_callback=update_pending_mail2_status,
                cancel_event=active_mail2_cancel_event,
            )
            await safe_event_reply(event, result)
        except Exception:
            logging.exception("Mail2 failed after pending text sender_id=%s", sender_id)
            await update_pending_mail2_status(
                build_process_status(
                    "Mail2 Р±РµР· РїРѕРґРїРёСЃРєРё",
                    MAIL2_STEPS,
                    len(MAIL2_STEPS),
                    extra_lines=["Р Р°СЃСЃС‹Р»РєР° Р·Р°РІРµСЂС€РёР»Р°СЃСЊ РѕС€РёР±РєРѕР№", "РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі"],
                    failed=True,
                )
            )
            await safe_event_reply(event, "РќРµ СѓРґР°Р»РѕСЃСЊ РІС‹РїРѕР»РЅРёС‚СЊ /mail2. РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі.")
        finally:
            active_mail2_cancel_event = None
            schedule_scan_auto_resume(scan_interruption)
        return

    pending_gpt_requests.pop(sender_id, None)

    pending_direct_mail = pending_direct_mail_requests.get(sender_id)
    if pending_direct_mail:
        log_action_event(
            "route",
            sender_id=sender_id,
            route="pending_direct_mail",
            user_id=str(pending_direct_mail.get("user_id") or ""),
            text=incoming_text,
        )
        direct_mail_user_id = str(pending_direct_mail.get("user_id") or "").strip()
        plain_text = incoming_text.strip()
        if plain_text.casefold() in {"0", "РѕС‚РјРµРЅР°", "cancel", "/cancel"}:
            pending_direct_mail_requests.pop(sender_id, None)
            await safe_event_reply(
                event,
                assistant_compact_reply(
                    "РћС‚РїСЂР°РІРєСѓ РѕС‚РјРµРЅРёР».",
                    f"РЎРѕРѕР±С‰РµРЅРёРµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ {direct_mail_user_id} РЅРµ РѕС‚РїСЂР°РІР»СЏР».",
                ),
            )
            return
        if plain_text and is_explicit_requester_command_input(plain_text, sender_id):
            pending_direct_mail_requests.pop(sender_id, None)
            await safe_event_reply(
                event,
                assistant_compact_reply(
                    "РџРѕРґРіРѕС‚РѕРІРєСѓ СЃРѕРѕР±С‰РµРЅРёСЏ РѕС‚РјРµРЅРёР».",
                    "РџРµСЂРµС…РѕР¶Сѓ Рє РЅРѕРІРѕР№ РєРѕРјР°РЅРґРµ.",
                ),
            )
        else:
            if not plain_text:
                await safe_event_reply(event, requester_mail_text_prompt(direct_mail_user_id))
                return
            pending_direct_mail_requests.pop(sender_id, None)
            await execute_text_command(event, f"/send {direct_mail_user_id} {plain_text}")
            return

    active_command_name = current_command_execution_name(sender_id)
    if active_command_name:
        log_action_event("route", sender_id=sender_id, route="active_command_guard", command_name=active_command_name, text=incoming_text)
        if is_voice_or_audio_message(event):
            await safe_event_reply(event, command_reply_guard_message(active_command_name))
            return
        plain_text = (event.raw_text or "").strip()
        if plain_text and not is_explicit_requester_command_input(plain_text, sender_id):
            await safe_event_reply(event, command_reply_guard_message(active_command_name))
            return

    if is_voice_or_audio_message(event):
        log_action_event("route", sender_id=sender_id, route="requester_voice", text=incoming_text)
        status_message = await safe_event_reply(
            event,
            build_process_status(
                "Р“РѕР»РѕСЃРѕРІРѕР№ РїРѕРјРѕС‰РЅРёРє",
                SMART_STEPS,
                2,
                extra_lines=[
                    f"РњРѕРґРµР»СЊ СЂР°СЃРїРѕР·РЅР°РІР°РЅРёСЏ: {settings.openai_transcribe_model}",
                    "РЎРєР°С‡РёРІР°СЋ Рё СЂР°СЃРїРѕР·РЅР°СЋ РіРѕР»РѕСЃРѕРІРѕРµ",
                ],
            ),
        )
        try:
            transcript = await transcribe_telegram_voice(event)
            await edit_status_message(
                status_message,
                build_process_status(
                    "Р“РѕР»РѕСЃРѕРІРѕР№ РїРѕРјРѕС‰РЅРёРє",
                    SMART_STEPS,
                    3,
                    extra_lines=[f"Р Р°СЃРїРѕР·РЅР°РЅРѕ: {transcript[:500]}"],
                    done=True,
                ),
                force=True,
            )
            await safe_event_reply(event, f"Р Р°СЃРїРѕР·РЅР°Р» РіРѕР»РѕСЃ:\n\n{transcript}")
            await handle_smart_request(event, sender_id, transcript, source="voice")
        except Exception:
            logging.exception("Voice smart request failed sender_id=%s", sender_id)
            record_voice_failure(event, sender, incoming_text, sender_id=sender_id)
            await edit_status_message(
                status_message,
                build_process_status(
                    "Р“РѕР»РѕСЃРѕРІРѕР№ РїРѕРјРѕС‰РЅРёРє",
                    SMART_STEPS,
                    2,
                    extra_lines=["РќРµ СѓРґР°Р»РѕСЃСЊ СЂР°СЃРїРѕР·РЅР°С‚СЊ РіРѕР»РѕСЃРѕРІРѕРµ", "РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі"],
                    failed=True,
                ),
                force=True,
            )
            await safe_event_reply(event, "РќРµ СѓРґР°Р»РѕСЃСЊ СЂР°СЃРїРѕР·РЅР°С‚СЊ РіРѕР»РѕСЃРѕРІРѕРµ. РџРѕРїСЂРѕР±СѓР№ РµС‰Рµ СЂР°Р· РёР»Рё РѕС‚РїСЂР°РІСЊ С‚РµРєСЃС‚РѕРј.")
        return

    if is_command_menu_command(event.raw_text or ""):
        log_action_event("route", sender_id=sender_id, route="command_menu")
        await safe_event_reply(event, build_command_menu_text(), buttons=build_command_menu_buttons())
        return

    if is_requester_capabilities_question(event.raw_text or ""):
        log_action_event("route", sender_id=sender_id, route="requester_capabilities")
        await safe_event_reply(event, build_requester_capabilities_text(), buttons=build_command_menu_buttons())
        return

    if is_version_command(event.raw_text or ""):
        log_action_event("route", sender_id=sender_id, route="version")
        await safe_event_reply(event, build_runtime_version_text())
        return

    if is_diagnostics_command(event.raw_text or ""):
        log_action_event("route", sender_id=sender_id, route="diagnostics")
        await safe_event_reply(event, build_diagnostics_text())
        return

    template_command = parse_template_command(event.raw_text or "")
    if template_command is not None:
        template_key, template_rest = template_command
        log_action_event("route", sender_id=sender_id, route="templates", key=template_key)
        await safe_event_reply(event, resolve_template_text(template_key, template_rest))
        return

    unresolved_command = parse_unresolved_command(event.raw_text or "")
    if unresolved_command and await handle_unresolved_command_event(event, unresolved_command):
        log_action_event("route", sender_id=sender_id, route="unresolved", command=unresolved_command)
        return

    if is_poc_command(event.raw_text or ""):
        log_action_event("route", sender_id=sender_id, route="poc")
        await safe_event_reply(event, build_poc_text(), buttons=build_poc_buttons())
        return

    logs_lines = parse_logs_command(event.raw_text or "")
    if logs_lines is not None:
        log_action_event("route", sender_id=sender_id, route="logs", lines=logs_lines)
        await safe_event_reply(event, build_recent_logs_text(logs_lines))
        return

    if is_status_command(event.raw_text or ""):
        log_action_event("route", sender_id=sender_id, route="status")
        await safe_event_reply(event, "[STATUS] РЎРѕР±РёСЂР°СЋ dashboard РёР· SQL Р±Р°Р·С‹...")
        await send_status_dashboard_from_database(event)
        return

    if is_admin_site_command(event.raw_text or ""):
        log_action_event("route", sender_id=sender_id, route="adminsite")
        await send_live_admin_dashboard_link(event)
        return

    if is_root_panel_command(event.raw_text or ""):
        log_action_event("route", sender_id=sender_id, route="root_panel")
        await send_live_root_panel_link(event)
        return

    gpt_command = parse_gpt_command(event.raw_text or "")
    if gpt_command:
        log_action_event("route", sender_id=sender_id, route="gpt_command", action=gpt_command.action, prompt=gpt_command.prompt)
        if gpt_command.action == "reset":
            gpt_chat_sessions.pop(sender_id, None)
            pending_gpt_requests.pop(sender_id, None)
            had_active_request = mark_active_gpt_request(sender_id, canceled=True, suppress_output=True, reason="gpt_reset")
            if had_active_request:
                active_gpt_requests.pop(sender_id, None)
            reset_message = "РљРѕРЅС‚РµРєСЃС‚ KBR_GPT РѕС‡РёС‰РµРЅ."
            if had_active_request:
                reset_message += "\nРўРµРєСѓС‰РёР№ Р·Р°РїСЂРѕСЃ РѕСЃС‚Р°РЅРѕРІР»РµРЅ, РµРіРѕ РѕС‚РІРµС‚ Р±РѕР»СЊС€Рµ РЅРµ РїСЂРёРґРµС‚ РІ С‡Р°С‚."
            else:
                reset_message += "\nРЎР»РµРґСѓСЋС‰РёР№ /gpt РЅР°С‡РЅРµС‚ РЅРѕРІС‹Р№ РґРёР°Р»РѕРі."
            await safe_event_reply(event, reset_message)
            return
        if not gpt_command.prompt:
            status_message = await safe_event_reply(
                event,
                build_process_status(
                    "KBR_GPT",
                    GPT_STEPS,
                    1,
                    extra_lines=[
                        f"РњРѕРґРµР»СЊ: {settings.openai_model}",
                        "Р–РґСѓ РІРѕРїСЂРѕСЃ СЃР»РµРґСѓСЋС‰РёРј СЃРѕРѕР±С‰РµРЅРёРµРј",
                        "Р”Р»СЏ РѕС‚РјРµРЅС‹ РѕС‚РїСЂР°РІСЊ 0",
                    ],
                ),
            )
            pending_gpt_requests[sender_id] = {
                "stage": "await_prompt",
                "status_message": status_message,
                "created_at": now_timestamp(),
            }
            await safe_event_reply(event, "РќР°РїРёС€Рё РІРѕРїСЂРѕСЃ РґР»СЏ KBR_GPT СЃР»РµРґСѓСЋС‰РёРј СЃРѕРѕР±С‰РµРЅРёРµРј.")
            return
        await handle_gpt_prompt(event, sender_id, gpt_command.prompt, compact_status=True)
        return

    scan_menu_action = parse_scan_menu_action(
        event.raw_text or "",
        allow_numeric=active_scan_menu_owner_id == event.sender_id,
    )
    if scan_menu_action == "menu":
        log_action_event("route", sender_id=sender_id, route="scan_menu")
        active_scan_menu_owner_id = event.sender_id
        await safe_event_reply(event, build_scan_menu_text_fast(), buttons=build_scan_menu_buttons())
        return

    if scan_menu_action == "results":
        log_action_event("route", sender_id=sender_id, route="scan_results")
        active_scan_menu_owner_id = event.sender_id
        await safe_event_reply(event, build_scan_results_text())
        await send_latest_dashboard_to_chat(event)
        return

    if scan_menu_action in {"pause", "pause_results"}:
        if active_scan_cancel_event and not active_scan_cancel_event.is_set():
            if active_scan_owner_id is not None and event.sender_id != active_scan_owner_id:
                await safe_event_reply(event, "РџРѕСЃС‚Р°РІРёС‚СЊ scan РЅР° РїР°СѓР·Сѓ РјРѕР¶РµС‚ С‚РѕР»СЊРєРѕ С‚РѕС‚, РєС‚Рѕ РµРіРѕ Р·Р°РїСѓСЃС‚РёР».")
                return
            active_scan_cancel_event.set()
            reply_text = "РџР°СѓР·Р° scan РїСЂРёРЅСЏС‚Р°. Р—Р°РІРµСЂС€Сѓ С‚РµРєСѓС‰РµРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ Рё СЃРѕС…СЂР°РЅСЋ РїСЂРѕРіСЂРµСЃСЃ."
            if scan_menu_action == "pause_results":
                reply_text = f"{reply_text}\n\n{build_scan_results_text()}"
            await safe_event_reply(event, reply_text)
        else:
            if scan_menu_action == "pause_results":
                await safe_event_reply(event, build_scan_results_text())
                await send_latest_dashboard_to_chat(event)
            else:
                await safe_event_reply(event, "Scan СЃРµР№С‡Р°СЃ РЅРµ РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ. Р”Р»СЏ РІС‹Р±РѕСЂР° РѕС‚РїСЂР°РІСЊ `scan`.")
        return

    if scan_menu_action == "reset":
        if active_scan_cancel_event and not active_scan_cancel_event.is_set():
            if active_scan_owner_id is not None and event.sender_id != active_scan_owner_id:
                await safe_event_reply(event, "РЎР±СЂРѕСЃРёС‚СЊ Р°РєС‚РёРІРЅС‹Р№ scan РјРѕР¶РµС‚ С‚РѕР»СЊРєРѕ С‚РѕС‚, РєС‚Рѕ РµРіРѕ Р·Р°РїСѓСЃС‚РёР».")
                return
            active_scan_reset_requested = True
            active_scan_cancel_event.set()
            clear_scan_checkpoint()
            reset_scan_database()
            await safe_event_reply(event, "РЎР±СЂРѕСЃ scan РїСЂРёРЅСЏС‚. РћСЃС‚Р°РЅР°РІР»РёРІР°СЋ С‚РµРєСѓС‰РёР№ РѕР±С…РѕРґ Рё РѕС‡РёС‰Р°СЋ СЃРѕС…СЂР°РЅРµРЅРЅС‹Р№ РїСЂРѕРіСЂРµСЃСЃ Рё SQL Р±Р°Р·Сѓ.")
        else:
            clear_scan_checkpoint()
            reset_scan_database()
            await safe_event_reply(event, "РЎРѕС…СЂР°РЅРµРЅРЅС‹Р№ РїСЂРѕРіСЂРµСЃСЃ scan Рё SQL Р±Р°Р·Р° РѕС‡РёС‰РµРЅС‹. РЎС‚Р°СЂС‹Рµ РіРѕС‚РѕРІС‹Рµ РѕС‚С‡РµС‚С‹ РѕСЃС‚Р°РІР»РµРЅС‹.")
        return

    mail2_text = parse_mail2_command(event.raw_text or "")
    if mail2_text is not None:
        log_action_event("route", sender_id=sender_id, route="mail2", has_text=bool(mail2_text))
        if not mail2_text:
            status_message = await safe_event_reply(
                event,
                build_process_status(
                    "Mail2 Р±РµР· РїРѕРґРїРёСЃРєРё",
                    MAIL2_STEPS,
                    3,
                    extra_lines=[
                        "Р–РґСѓ С‚РµРєСЃС‚ СЂР°СЃСЃС‹Р»РєРё СЃР»РµРґСѓСЋС‰РёРј СЃРѕРѕР±С‰РµРЅРёРµРј",
                        "Р”Р»СЏ РѕС‚РјРµРЅС‹ РѕС‚РїСЂР°РІСЊ: 0 РѕС‚РјРµРЅР°",
                    ],
                ),
            )
            pending_mail2_requests[sender_id] = {
                "status_message": status_message,
                "created_at": now_timestamp(),
            }
            await safe_event_reply(
                event,
                "РџСЂРёС€Р»Рё С‚РµРєСЃС‚, РєРѕС‚РѕСЂС‹Р№ РЅСѓР¶РЅРѕ РѕС‚РїСЂР°РІРёС‚СЊ РІСЃРµРј РїРѕР»СЊР·РѕРІР°С‚РµР»СЏРј Р±РµР· РїРѕРґРїРёСЃРєРё РёР· Р±Р°Р·С‹.\n\nРћС‚РјРµРЅР°: `0 РѕС‚РјРµРЅР°`",
            )
            return

        logging.info("Received mail2 command from chat_id=%s sender_id=%s", event.chat_id, event.sender_id)
        status_message = await safe_event_reply(
            event,
            build_process_status(
                "Mail2 Р±РµР· РїРѕРґРїРёСЃРєРё",
                MAIL2_STEPS,
                1,
                extra_lines=[f"РўРµРєСЃС‚: {len(mail2_text)} СЃРёРјРІРѕР»РѕРІ"],
            ),
        )

        async def update_mail2_status(text: str) -> None:
            await edit_status_message(status_message, text)

        scan_interruption = await request_scan_pause_for_priority_command(event, "mail2")
        active_mail2_cancel_event = asyncio.Event()
        try:
            result = await send_mail2_to_users_without_subscriptions(
                mail2_text,
                progress_callback=update_mail2_status,
                cancel_event=active_mail2_cancel_event,
            )
            await safe_event_reply(event, result)
        except Exception:
            logging.exception("Mail2 command failed sender_id=%s", sender_id)
            await update_mail2_status(
                build_process_status(
                    "Mail2 Р±РµР· РїРѕРґРїРёСЃРєРё",
                    MAIL2_STEPS,
                    len(MAIL2_STEPS),
                    extra_lines=["Р Р°СЃСЃС‹Р»РєР° Р·Р°РІРµСЂС€РёР»Р°СЃСЊ РѕС€РёР±РєРѕР№", "РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі"],
                    failed=True,
                )
            )
            await safe_event_reply(event, "РќРµ СѓРґР°Р»РѕСЃСЊ РІС‹РїРѕР»РЅРёС‚СЊ /mail2. РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі.")
        finally:
            active_mail2_cancel_event = None
            schedule_scan_auto_resume(scan_interruption)
        return

    promo_command = parse_promo_command(event.raw_text or "")
    if promo_command:
        log_action_event("route", sender_id=sender_id, route="promo", user_id=str(promo_command[0]), promo_code=str(promo_command[1]))
        user_id, promo_code, promo_mail_text = promo_command
        logging.info(
            "Received promo command user_id=%s promo_code=%s from chat_id=%s sender_id=%s",
            user_id,
            promo_code,
            event.chat_id,
            event.sender_id,
        )
        status_message = await safe_event_reply(
            event,
            build_process_status(
                "Promo",
                PROMO_STEPS,
                1,
                user_id=user_id,
                extra_lines=[
                    f"РџСЂРѕРјРѕРєРѕРґ: {promo_code}",
                    f"Р‘СЋРґР¶РµС‚: {settings.promo_budget_rub}",
                    f"РЎСѓРјРјР°: {settings.promo_amount_rub}",
                ],
            ),
        )

        async def update_promo_status(text: str) -> None:
            await edit_status_message(status_message, text)

        await request_mail2_stop_for_priority_command(event, f"promo {user_id}")
        scan_interruption = await request_scan_pause_for_priority_command(event, f"promo {user_id}")
        try:
            promo_result = await create_promo_code_in_admin_bot(
                user_id,
                promo_code,
                progress_callback=update_promo_status,
            )
            await update_promo_status(
                build_process_status(
                    "Promo",
                    PROMO_STEPS,
                    8,
                    user_id=user_id,
                    extra_lines=["РџСЂРѕРјРѕРєРѕРґ СЃРѕР·РґР°РЅ", "РћС‚РїСЂР°РІР»СЏСЋ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ С‡РµСЂРµР· mail"],
                )
            )
            mail_result = await send_mail_to_user_in_admin_bot(
                user_id,
                promo_mail_text,
                progress_callback=update_promo_status,
            )
            await update_promo_status(
                build_process_status(
                    "Promo",
                    PROMO_STEPS,
                    len(PROMO_STEPS),
                    user_id=user_id,
                    extra_lines=[
                        f"РџСЂРѕРјРѕРєРѕРґ: {promo_code}",
                        "РџСЂРѕРјРѕРєРѕРґ СЃРѕР·РґР°РЅ Рё РѕС‚РїСЂР°РІР»РµРЅ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
                    ],
                    done=True,
                )
            )
            await safe_event_reply(event, f"{promo_result}\n\n{mail_result}")
        except Exception:
            logging.exception("Promo flow failed for user_id=%s promo_code=%s", user_id, promo_code)
            await update_promo_status(
                build_process_status(
                    "Promo",
                    PROMO_STEPS,
                    len(PROMO_STEPS),
                    user_id=user_id,
                    extra_lines=["РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕР·РґР°С‚СЊ РёР»Рё РѕС‚РїСЂР°РІРёС‚СЊ РїСЂРѕРјРѕРєРѕРґ", "РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі"],
                    failed=True,
                )
            )
            await safe_event_reply(event, "РќРµ СѓРґР°Р»РѕСЃСЊ РІС‹РїРѕР»РЅРёС‚СЊ promo. РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі.")
        finally:
            schedule_scan_auto_resume(scan_interruption)
        return

    mail_command = parse_mail_command(event.raw_text or "")
    if mail_command:
        user_id, message_text = mail_command
        logging.info("Received mail command user_id=%s from chat_id=%s sender_id=%s", user_id, event.chat_id, event.sender_id)
        status_message = await safe_event_reply(
            event,
            build_process_status(
                "Mail РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
                MAIL_STEPS,
                1,
                user_id=user_id,
                extra_lines=[f"РђРґРјРёРЅ-Р±РѕС‚: @{settings.admin_bot_username}", f"РўРµРєСЃС‚: {len(message_text)} СЃРёРјРІРѕР»РѕРІ"],
            )
        )

        async def update_mail_status(text: str) -> None:
            await edit_status_message(status_message, text)

        await request_mail2_stop_for_priority_command(event, f"mail {user_id}")
        scan_interruption = await request_scan_pause_for_priority_command(event, f"mail {user_id}")
        try:
            result = await send_mail_to_user_in_admin_bot(
                user_id,
                message_text,
                progress_callback=update_mail_status,
            )
            await update_mail_status(
                build_process_status(
                    "Mail РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
                    MAIL_STEPS,
                    len(MAIL_STEPS),
                    user_id=user_id,
                    extra_lines=["РџРёСЃСЊРјРѕ РѕС‚РїСЂР°РІР»РµРЅРѕ С‡РµСЂРµР· Р°РґРјРёРЅ-Р±РѕС‚", "С‚РѕРі РѕС‚РїСЂР°РІР»РµРЅ РѕС‚РґРµР»СЊРЅС‹Рј СЃРѕРѕР±С‰РµРЅРёРµРј"],
                    done=True,
                )
            )
            await safe_event_reply(event, result)
        except Exception:
            logging.exception("Admin mail failed for user_id=%s", user_id)
            await update_mail_status(
                build_process_status(
                    "Mail РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ",
                    MAIL_STEPS,
                    len(MAIL_STEPS),
                    user_id=user_id,
                    extra_lines=["РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РІРµСЂС€РёС‚СЊ РѕС‚РїСЂР°РІРєСѓ", "РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі"],
                    failed=True,
                )
            )
            await safe_event_reply(event, "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c mail. \u041f\u043e\u0434\u0440\u043e\u0431\u043d\u043e\u0441\u0442\u0438 \u0437\u0430\u043f\u0438\u0441\u0430\u043d\u044b \u0432 \u043b\u043e\u0433.")
        finally:
            schedule_scan_auto_resume(scan_interruption)
        return

    if is_help_overview_command(event.raw_text or ""):
        await safe_event_reply(event, build_command_menu_text(), buttons=build_command_menu_buttons())
        return

    wizard_user_id = parse_wizard_command(event.raw_text or "")
    if wizard_user_id:
        logging.info(
            "Received wizard command user_id=%s from chat_id=%s sender_id=%s",
            wizard_user_id,
            event.chat_id,
            event.sender_id,
        )
        wizard_target = f"@{settings.wizard_target_username.lstrip('@')}"
        status_message = await safe_event_reply(
            event,
            build_process_status(
                "Wizard",
                WIZARD_STEPS,
                1,
                user_id=wizard_user_id,
                target=wizard_target,
                extra_lines=[f"РђРґРјРёРЅ-Р±РѕС‚: @{settings.admin_bot_username}"],
            )
        )

        async def update_wizard_status(text: str) -> None:
            await edit_status_message(status_message, text)

        scan_interruption = None
        try:
            cached_record = load_latest_record_from_database(wizard_user_id)
            if cached_record:
                await ask_wizard_confirmation(
                    event,
                    sender_id=sender_id,
                    user_id=wizard_user_id,
                    base_text=format_user_summary_from_record(cached_record),
                    status_message=status_message,
                    update_status=update_wizard_status,
                )
                return

            await request_mail2_stop_for_priority_command(event, f"wizard {wizard_user_id}")
            scan_interruption = await request_scan_pause_for_priority_command(event, f"wizard {wizard_user_id}")
            result = await find_user_in_admin_bot(
                wizard_user_id,
                progress_callback=update_wizard_status,
                progress_title="Wizard",
                progress_steps=WIZARD_STEPS,
            )
            await ask_wizard_confirmation(
                event,
                sender_id=sender_id,
                user_id=wizard_user_id,
                base_text=result,
                status_message=status_message,
                update_status=update_wizard_status,
            )
        except Exception:
            logging.exception("Wizard search failed for user_id=%s", wizard_user_id)
            await update_wizard_status(
                build_process_status(
                    "Wizard",
                    WIZARD_STEPS,
                    5,
                    user_id=wizard_user_id,
                    target=wizard_target,
                    extra_lines=["РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕРґРіРѕС‚РѕРІРёС‚СЊ РєР°СЂС‚РѕС‡РєСѓ", "РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі"],
                    failed=True,
                )
            )
            await safe_event_reply(event, "РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕРґРіРѕС‚РѕРІРёС‚СЊ СЃРѕРѕР±С‰РµРЅРёРµ РґР»СЏ wizard. РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё РІ Р»РѕРіРµ.")
        finally:
            schedule_scan_auto_resume(scan_interruption)
        return

    scan_action = scan_menu_action or parse_scan_command(event.raw_text or "")
    if scan_action in {"new", "continue"}:
        log_action_event("route", sender_id=sender_id, route="scan_run", action=scan_action)
        active_scan_menu_owner_id = event.sender_id
        logging.info(
            "Received scan command action=%s from chat_id=%s sender_id=%s",
            scan_action,
            event.chat_id,
            event.sender_id,
        )
        await request_mail2_stop_for_priority_command(event, f"scan {scan_action}")
        if active_scan_cancel_event and not active_scan_cancel_event.is_set():
            await safe_event_reply(event, "Scan СѓР¶Рµ РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ. РњРѕР¶РЅРѕ РїРѕСЃС‚Р°РІРёС‚СЊ РЅР° РїР°СѓР·Сѓ: `scan pause`.")
            return

        if scan_action == "new":
            clear_scan_checkpoint()
            reset_scan_database()
            clear_scan_outputs()
        start_text = (
            "Запускаю новый scan: очистил базу и старые отчёты, начинаю с первого пользователя."
            if scan_action == "new"
            else "РџСЂРѕРґРѕР»Р¶Р°СЋ scan СЃ СЃРѕС…СЂР°РЅРµРЅРЅРѕРіРѕ РјРµСЃС‚Р°. Р•СЃР»Рё checkpoint РїСѓСЃС‚РѕР№, РЅР°С‡РЅСѓ СЃ РїРµСЂРІРѕР№ СЃС‚СЂР°РЅРёС†С‹."
        )
        active_scan_cancel_event = asyncio.Event()
        active_scan_owner_id = event.sender_id
        active_scan_reset_requested = False
        active_scan_base_delay_seconds = max(
            0.08,
            min(settings.scan_action_delay_seconds, settings.scan_turbo_delay_seconds),
        )
        active_scan_action_delay_seconds = active_scan_base_delay_seconds

        progress_interval_seconds = max(0.25, env_float("SCAN_PROGRESS_INTERVAL_SECONDS", 0.5))
        progress_message = await safe_event_reply(
            event,
            build_scan_status(
                f"{start_text} Р“РѕС‚РѕРІР»СЋ Р°РґРјРёРЅ-Р±РѕС‚ Рє РѕР±С…РѕРґСѓ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№.",
                checkpoint_text=format_scan_checkpoint_text(),
            ),
            buttons=[[Button.inline("РџР°СѓР·Р° scan", data=SCAN_CANCEL_CALLBACK_DATA)]],
        )

        async def update_scan_progress(
            text: str,
            *,
            done: bool = False,
            failed: bool = False,
            paused: bool = False,
        ) -> None:
            buttons = None if done or failed or paused else [[Button.inline("РџР°СѓР·Р° scan", data=SCAN_CANCEL_CALLBACK_DATA)]]
            await edit_status_message(
                progress_message,
                build_scan_status(
                    text,
                    checkpoint_text=format_scan_checkpoint_text(),
                    done=done,
                    failed=failed,
                    paused=paused,
                ),
                buttons=buttons,
                force=done or failed or paused,
            )

        try:
            result = await scan_all_users_in_admin_bot(
                progress_callback=update_scan_progress,
                progress_interval_seconds=progress_interval_seconds,
                cancel_event=active_scan_cancel_event,
            )
            if "РЅР° РїР°СѓР·Рµ" in result.casefold():
                await update_scan_progress("Scan РЅР° РїР°СѓР·Рµ. РџСЂРѕРіСЂРµСЃСЃ СЃРѕС…СЂР°РЅРµРЅ, С‡Р°СЃС‚РёС‡РЅС‹Р№ РѕС‚С‡РµС‚ РѕС‚РїСЂР°РІР»РµРЅ РЅРёР¶Рµ.", paused=True)
            elif "СЃР±СЂРѕС€РµРЅ" in result.casefold():
                await update_scan_progress("Scan СЃР±СЂРѕС€РµРЅ. РЎРѕС…СЂР°РЅРµРЅРЅС‹Р№ РїСЂРѕРіСЂРµСЃСЃ РѕС‡РёС‰РµРЅ.", done=True)
            else:
                await update_scan_progress("Scan Р·Р°РІРµСЂС€РµРЅ. С‚РѕРіРѕРІС‹Р№ РѕС‚С‡РµС‚ РіРѕС‚РѕРІ Рё РѕС‚РїСЂР°РІР»РµРЅ РЅРёР¶Рµ.", done=True)
            await safe_event_reply(event, result)
            await send_latest_dashboard_to_chat(event)
        except Exception:
            logging.exception("Admin scan failed")
            await update_scan_progress("Scan Р·Р°РІРµСЂС€РёР»СЃСЏ СЃ РѕС€РёР±РєРѕР№. РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі.", failed=True)
            await safe_event_reply(event, "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0432\u044b\u043f\u043e\u043b\u043d\u0438\u0442\u044c scan. \u041f\u043e\u0434\u0440\u043e\u0431\u043d\u043e\u0441\u0442\u0438 \u0437\u0430\u043f\u0438\u0441\u0430\u043d\u044b \u0432 \u043b\u043e\u0433.")
        finally:
            active_scan_cancel_event = None
            active_scan_owner_id = None
            active_scan_reset_requested = False
            active_scan_action_delay_seconds = settings.scan_action_delay_seconds
            active_scan_base_delay_seconds = settings.scan_action_delay_seconds
        return

    info_lookup = parse_info_command(event.raw_text or "")
    if info_lookup:
        log_action_event("route", sender_id=sender_id, route="info", query=info_lookup.query, use_database=info_lookup.use_database)
        user_id = info_lookup.query
        logging.info(
            "Received info command query=%s database=%s from chat_id=%s sender_id=%s",
            user_id,
            info_lookup.use_database,
            event.chat_id,
            event.sender_id,
        )
        status_message = await safe_event_reply(
            event,
            build_process_status(
                "Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
                INFO_STEPS,
                1,
                user_id=user_id,
                extra_lines=[f"РђРґРјРёРЅ-Р±РѕС‚: @{settings.admin_bot_username}"],
            )
        )

        async def update_info_status(text: str) -> None:
            await edit_status_message(status_message, text)

        scan_interruption = None
        try:
            if info_lookup.use_database:
                await update_info_status(
                    build_process_status(
                        "Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
                        INFO_STEPS,
                        len(INFO_STEPS),
                        user_id=user_id,
                        extra_lines=["Р§РёС‚Р°СЋ SQLite Р±Р°Р·Сѓ", "РђРґРјРёРЅ-Р±РѕС‚ РЅРµ С‚СЂРѕРіР°СЋ"],
                        done=True,
                    )
                )
                record = load_latest_record_by_lookup_from_database(user_id)
                if not record:
                    await safe_event_reply(
                        event,
                        "Р’ Р±Р°Р·Рµ РЅРµС‚ С‚Р°РєРѕРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ. Р—Р°РїСѓСЃС‚Рё `scan new` РёР»Рё РїРѕРїСЂРѕР±СѓР№ Р±РµР· `-b`, С‡С‚РѕР±С‹ РёСЃРєР°С‚СЊ С‡РµСЂРµР· Р°РґРјРёРЅ-Р±РѕС‚Р°.",
                    )
                    return
                result = format_subscription_info_from_record_html(record)
            else:
                await request_mail2_stop_for_priority_command(event, f"info {user_id}")
                scan_interruption = await request_scan_pause_for_priority_command(event, f"info {user_id}")
                result = await get_user_subscriptions_info_in_admin_bot(
                    user_id,
                    progress_callback=update_info_status,
                )
            await update_info_status(
                build_process_status(
                    "Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
                    INFO_STEPS,
                    len(INFO_STEPS),
                    user_id=user_id,
                    extra_lines=["РџРѕР»РЅС‹Р№ РѕС‚С‡РµС‚ СЃРѕР±СЂР°РЅ", "С‚РѕРі РѕС‚РїСЂР°РІР»РµРЅ РѕС‚РґРµР»СЊРЅС‹Рј СЃРѕРѕР±С‰РµРЅРёРµРј"],
                    done=True,
                )
            )
            await safe_event_reply(event, result, parse_mode="html")
        except Exception:
            logging.exception("Info failed for query=%s database=%s", user_id, info_lookup.use_database)
            await update_info_status(
                build_process_status(
                    "Info РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
                    INFO_STEPS,
                    len(INFO_STEPS),
                    user_id=user_id,
                    extra_lines=["РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕР»СѓС‡РёС‚СЊ РїРѕР»РЅС‹Р№ РѕС‚С‡РµС‚", "РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі"],
                    failed=True,
                )
            )
            await safe_event_reply(event, "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043f\u043e\u043b\u0443\u0447\u0438\u0442\u044c info. \u041f\u043e\u0434\u0440\u043e\u0431\u043d\u043e\u0441\u0442\u0438 \u0437\u0430\u043f\u0438\u0441\u0430\u043d\u044b \u0432 \u043b\u043e\u0433.")
        finally:
            schedule_scan_auto_resume(scan_interruption)
        return

    help_lookup = parse_help_command(event.raw_text or "")
    if help_lookup:
        log_action_event("route", sender_id=sender_id, route="help", query=help_lookup.query, use_database=help_lookup.use_database)
        user_id = help_lookup.query
        logging.info(
            "Received help command query=%s database=%s from chat_id=%s sender_id=%s",
            user_id,
            help_lookup.use_database,
            event.chat_id,
            event.sender_id,
        )
        status_message = await safe_event_reply(
            event,
            build_process_status(
                "РџРѕРёСЃРє РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
                SEARCH_STEPS,
                1,
                user_id=user_id,
                extra_lines=[f"РђРґРјРёРЅ-Р±РѕС‚: @{settings.admin_bot_username}"],
            )
        )

        async def update_help_status(text: str) -> None:
            await edit_status_message(status_message, text)

        scan_interruption = None
        try:
            if help_lookup.use_database:
                await update_help_status(
                    build_process_status(
                        "РџРѕРёСЃРє РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
                        SEARCH_STEPS,
                        len(SEARCH_STEPS),
                        user_id=user_id,
                        extra_lines=["Р§РёС‚Р°СЋ SQLite Р±Р°Р·Сѓ", "РђРґРјРёРЅ-Р±РѕС‚ РЅРµ С‚СЂРѕРіР°СЋ"],
                        done=True,
                    )
                )
                record = load_latest_record_by_lookup_from_database(user_id)
                if not record:
                    await safe_event_reply(
                        event,
                        "Р’ Р±Р°Р·Рµ РЅРµС‚ С‚Р°РєРѕРіРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ. Р—Р°РїСѓСЃС‚Рё `scan new` РёР»Рё РїРѕРїСЂРѕР±СѓР№ Р±РµР· `-b`, С‡С‚РѕР±С‹ РёСЃРєР°С‚СЊ С‡РµСЂРµР· Р°РґРјРёРЅ-Р±РѕС‚Р°.",
                    )
                    return
                result = format_user_summary_from_record(record)
            else:
                await request_mail2_stop_for_priority_command(event, f"help {user_id}")
                scan_interruption = await request_scan_pause_for_priority_command(event, f"help {user_id}")
                result = await find_user_in_admin_bot(
                    user_id,
                    progress_callback=update_help_status,
                )
            await update_help_status(
                build_process_status(
                    "РџРѕРёСЃРє РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
                    SEARCH_STEPS,
                    len(SEARCH_STEPS),
                    user_id=user_id,
                    extra_lines=["РљРѕСЂРѕС‚РєР°СЏ РєР°СЂС‚РѕС‡РєР° РіРѕС‚РѕРІР°", "С‚РѕРі РѕС‚РїСЂР°РІР»РµРЅ РѕС‚РґРµР»СЊРЅС‹Рј СЃРѕРѕР±С‰РµРЅРёРµРј"],
                    done=True,
                )
            )
            await safe_event_reply(event, result)
        except Exception:
            logging.exception("Help search failed for query=%s database=%s", user_id, help_lookup.use_database)
            await update_help_status(
                build_process_status(
                    "РџРѕРёСЃРє РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ",
                    SEARCH_STEPS,
                    len(SEARCH_STEPS),
                    user_id=user_id,
                    extra_lines=["РќРµ СѓРґР°Р»РѕСЃСЊ РЅР°Р№С‚Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ", "РџРѕРґСЂРѕР±РЅРѕСЃС‚Рё Р·Р°РїРёСЃР°РЅС‹ РІ Р»РѕРі"],
                    failed=True,
                )
            )
            await safe_event_reply(event, "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043d\u0430\u0439\u0442\u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f. \u041f\u043e\u0434\u0440\u043e\u0431\u043d\u043e\u0441\u0442\u0438 \u0437\u0430\u043f\u0438\u0441\u0430\u043d\u044b \u0432 \u043b\u043e\u0433.")
        finally:
            schedule_scan_auto_resume(scan_interruption)
        return

    if (event.raw_text or "").strip():
        raw_text = (event.raw_text or "").strip()
        lowered_text = (event.raw_text or "").casefold()
        if raw_text.startswith("/"):
            log_action_event("route", sender_id=sender_id, route="unknown_slash_command", text=raw_text)
            await safe_event_reply(event, unknown_slash_command_message())
            return
        if is_control_reply_text(raw_text):
            log_action_event("route", sender_id=sender_id, route="control_reply_without_context", text=raw_text)
            workflow_name = current_pending_workflow_name(sender_id)
            if workflow_name:
                await safe_event_reply(event, command_reply_guard_message(workflow_name))
            else:
                await safe_event_reply(
                    event,
                    assistant_compact_reply(
                        "РљРѕСЂРѕС‚РєРёР№ РѕС‚РІРµС‚ РЅРµ СЂР°СЃРїРѕР·РЅР°РЅ.",
                        "СЃРїРѕР»СЊР·СѓР№С‚Рµ РїРѕР»РЅСѓСЋ РєРѕРјР°РЅРґСѓ РёР»Рё СЃРЅР°С‡Р°Р»Р° РѕС‚РєСЂРѕР№С‚Рµ РЅСѓР¶РЅС‹Р№ СЃС†РµРЅР°СЂРёР№.",
                    ),
                )
            return
        try:
            if await forward_problem_report_to_wizard(event, sender, event.raw_text or ""):
                log_action_event("route", sender_id=sender_id, route="auto_problem_report", text=raw_text)
                await safe_event_reply(event, "РџСЂРѕР±Р»РµРјСѓ РїСЂРёРЅСЏР». РљР°СЂС‚РѕС‡РєСѓ РѕС‚РїСЂР°РІРёР» РІ wizard РґР»СЏ РѕР±СЂР°Р±РѕС‚РєРё.")
                return
        except Exception:
            logging.exception("Failed to auto-forward problem report sender_id=%s", sender_id)
            await safe_event_reply(
                event,
                "РќРµ СѓРґР°Р»РѕСЃСЊ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё РѕС‚РїСЂР°РІРёС‚СЊ РїСЂРѕР±Р»РµРјСѓ РІ wizard. РџРѕРїСЂРѕР±СѓР№ РµС‰Рµ СЂР°Р· РёР»Рё РѕС‚РїСЂР°РІСЊ /wizard <id>.",
            )
            return
        if "scan" in lowered_text or "СЃРєР°РЅ" in lowered_text:
            log_action_event("route", sender_id=sender_id, route="scan_keyword")
            active_scan_menu_owner_id = event.sender_id
            await safe_event_reply(event, build_scan_menu_text_fast(), buttons=build_scan_menu_buttons())
            return
        direct_mail_user_id = parse_requester_mail_target_only(raw_text)
        if direct_mail_user_id:
            log_action_event("route", sender_id=sender_id, route="direct_mail_prompt", user_id=direct_mail_user_id)
            pending_direct_mail_requests[sender_id] = {
                "user_id": direct_mail_user_id,
                "created_at": now_timestamp(),
            }
            await safe_event_reply(event, requester_mail_text_prompt(direct_mail_user_id))
            return
        direct_smart_action = detect_direct_smart_action(raw_text)
        if direct_smart_action is not None and str(direct_smart_action.get("action") or "") != "chat":
            log_action_event(
                "route",
                sender_id=sender_id,
                route="direct_smart_local",
                action=str(direct_smart_action.get("action") or ""),
                text=raw_text,
            )
            await execute_smart_action(event, sender_id, direct_smart_action)
            return
        requester_text_intent = detect_non_requester_intent(raw_text)
        if requester_text_intent == "greeting":
            log_action_event("route", sender_id=sender_id, route="requester_greeting")
            await safe_event_reply(event, requester_greeting_message())
            return
        if requester_text_intent == "thanks":
            log_action_event("route", sender_id=sender_id, route="requester_thanks")
            await safe_event_reply(event, support_thanks_message())
            return
        if requester_text_intent == "vpn_setup_help":
            log_action_event("route", sender_id=sender_id, route="vpn_setup_help")
            await safe_event_reply(event, vpn_setup_help_message())
            return
        if requester_text_intent == "profile_id_help":
            log_action_event("route", sender_id=sender_id, route="profile_id_help")
            await safe_event_reply(event, profile_id_help_message())
            return
        if looks_like_requester_action_text(raw_text):
            log_action_event("route", sender_id=sender_id, route="smart_request", text=raw_text)
            await handle_smart_request(event, sender_id, event.raw_text or "", source="text", compact_status=True)
            return
        log_action_event("route", sender_id=sender_id, route="requester_gpt_fallback", text=raw_text)
        await handle_gpt_prompt(
            event,
            sender_id,
            event.raw_text or "",
            compact_status=True,
            reveal_unavailable=False,
        )


async def main() -> None:
    global own_user_id, admin_bot_entity_cache

    configure_logging()
    log_runtime_version()
    loop.set_exception_handler(
        lambda event_loop, context: logging.error(
            "Unhandled async error: %s",
            context.get("message", context),
            exc_info=context.get("exception"),
        )
    )

    ensure_database_file()
    seed_requesters_from_settings()
    logging.info("SQLite database file: %s", database_path())
    logging.info("Requesters configured: %s", requester_count())
    start_dashboard_http_server()

    me = await client.get_me()
    own_user_id = me.id
    admin_bot_entity_cache = await get_admin_bot_entity()
    health_task = asyncio.create_task(monitor_admin_bot_health())
    logging.info("Userbot started as %s", me.username or me.first_name or me.id)
    logging.info("Send /user <user_id|username> or /help in private chat to run commands.")
    logging.info("Full log file: %s", Path(settings.log_file).resolve())
    logging.info("Press Ctrl+C to stop.")
    try:
        await client.run_until_disconnected()
    finally:
        health_task.cancel()
        await asyncio.gather(health_task, return_exceptions=True)


if __name__ == "__main__":
    startup_cleanup()
    configure_logging()
    log_runtime_version()
    with client:
        loop.run_until_complete(main())



