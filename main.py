import asyncio
import html
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import shutil
import sqlite3
import subprocess
import threading
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Awaitable, Callable
from urllib.parse import quote, unquote, urlsplit

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
    admin_bot_username: str
    admin_command: str
    users_button_text: str
    find_user_button_text: str
    subscriptions_button_text: str
    write_user_button_text: str
    mail_next_button_text: str
    cancel_button_text: str
    back_button_text: str
    next_page_button_text: str
    report_dir: str
    database_path: str
    mail_text: str
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


@dataclass(frozen=True)
class UserLookupCommand:
    query: str
    use_database: bool

    @property
    def is_username(self) -> bool:
        return bool(normalize_username(self.query))


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
    if any(marker in value for marker in ("Р", "СЊ", "СЏ", "С€", "С‹")):
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


def fix_mojibake(text: str) -> str:
    if not text:
        return text
    if "Р" not in text and "С" not in text:
        return text
    try:
        fixed = text.encode("cp1251").decode("utf-8")
    except Exception:
        return text
    return fixed if fixed else text


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
        admin_bot_username=os.getenv("ADMIN_BOT_USERNAME", "vpn_kbr_bot"),
        admin_command=os.getenv("ADMIN_COMMAND", "/admin"),
        users_button_text=env_text("USERS_BUTTON_TEXT", "\u041f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0438"),
        find_user_button_text=env_text("FIND_USER_BUTTON_TEXT", "\u041d\u0430\u0439\u0442\u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f"),
        subscriptions_button_text=env_text("SUBSCRIPTIONS_BUTTON_TEXT", "\u041f\u043e\u0434\u043f\u0438\u0441\u043a\u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f"),
        write_user_button_text=env_text("WRITE_USER_BUTTON_TEXT", "\u041d\u0430\u043f\u0438\u0441\u0430\u0442\u044c \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044e"),
        mail_next_button_text=env_text("MAIL_NEXT_BUTTON_TEXT", "\u0414\u0430\u043b\u0435\u0435"),
        cancel_button_text=env_text("CANCEL_BUTTON_TEXT", "\u041e\u0442\u043c\u0435\u043d\u0438\u0442\u044c"),
        back_button_text=env_text("BACK_BUTTON_TEXT", "\u041d\u0430\u0437\u0430\u0434"),
        next_page_button_text=env_text("NEXT_PAGE_BUTTON_TEXT", "\u0414\u0430\u043b\u0435\u0435"),
        report_dir=os.getenv("REPORT_DIR", "reports"),
        database_path=os.getenv("DATABASE_PATH", "scan-data.sqlite3"),
        mail_text=env_text("MAIL_TEXT", "\u0417\u0434\u0440\u0430\u0432\u0441\u0442\u0432\u0443\u0439\u0442\u0435!"),
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
        dashboard_title=env_text("DASHBOARD_TITLE", "Понятный отчёт по подпискам"),
        dashboard_subtitle=env_text(
            "DASHBOARD_SUBTITLE",
            "Просто смотри на цифры: сколько людей, сколько подписок и сколько денег ждём.",
        ),
        dashboard_hint_primary=env_text(
            "DASHBOARD_HINT_PRIMARY",
            "1) Смотри карточку «Доход в следующем месяце» — это главная сумма.",
        ),
        dashboard_hint_secondary=env_text(
            "DASHBOARD_HINT_SECONDARY",
            "2) Блок «Заканчивается скоро» показывает, с кем связаться в первую очередь.",
        ),
        dashboard_hint_tertiary=env_text(
            "DASHBOARD_HINT_TERTIARY",
            "3) Графики ниже показывают рост: сплошная линия — прошлое, пунктир — прогноз.",
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
scan_auto_resume_lock = asyncio.Lock()
own_user_id: int | None = None
admin_bot_entity_cache = None
wizard_target_entity_cache = None
SCAN_ACTION_DELAY_SECONDS = settings.scan_action_delay_seconds
active_scan_action_delay_seconds = SCAN_ACTION_DELAY_SECONDS
active_scan_base_delay_seconds = SCAN_ACTION_DELAY_SECONDS
SCAN_CANCEL_CALLBACK_DATA = b"scan_cancel"
active_scan_cancel_event: asyncio.Event | None = None
active_scan_owner_id: int | None = None
active_scan_menu_owner_id: int | None = None
active_scan_reset_requested = False
active_scan_auto_resume_task: asyncio.Task | None = None
pending_wizard_requests: dict[int, dict[str, object]] = {}
ProgressCallback = Callable[[str], Awaitable[None]]
logging_is_configured = False
runtime_version_logged = False
startup_cleanup_done = False
dashboard_http_server: ThreadingHTTPServer | None = None
dashboard_http_thread: threading.Thread | None = None
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
FORECAST_PRICE_PER_SUBSCRIPTION_RUB = env_float("FORECAST_PRICE_PER_SUBSCRIPTION_RUB", 100.0)
FORECAST_RENEWAL_RATE_7_DAYS = env_float("FORECAST_RENEWAL_RATE_7_DAYS", 0.70)
FORECAST_RENEWAL_RATE_30_DAYS = env_float("FORECAST_RENEWAL_RATE_30_DAYS", 0.70)
FORECAST_WINBACK_RATE_EXPIRED = env_float("FORECAST_WINBACK_RATE_EXPIRED", 0.18)
MAX_SCAN_ACTION_DELAY_SECONDS = 2.5
SCAN_CHECKPOINT_USER_INTERVAL = max(1, env_int("SCAN_CHECKPOINT_USER_INTERVAL", 6))
SCAN_CHECKPOINT_MIN_INTERVAL_SECONDS = max(2.0, env_float("SCAN_CHECKPOINT_MIN_INTERVAL_SECONDS", 10.0))


class ScanCancelledError(Exception):
    pass


admin_bot_health = {
    "emoji": "[WAIT]",
    "status": "проверка",
    "detail": "ещё не проверял",
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
    status = str(admin_bot_health.get("status", "проверка"))
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


KEYWORD_REPLIES = {
    "\u043f\u0440\u0438\u0432\u0435\u0442": "\u041f\u0440\u0438\u0432\u0435\u0442! \u042f \u0441\u0435\u0439\u0447\u0430\u0441 \u0437\u0430\u043d\u044f\u0442, \u043e\u0442\u0432\u0435\u0447\u0443 \u0447\u0443\u0442\u044c \u043f\u043e\u0437\u0436\u0435.",
    "\u0437\u0434\u0440\u0430\u0432\u0441\u0442\u0432\u0443\u0439\u0442\u0435": "\u0417\u0434\u0440\u0430\u0432\u0441\u0442\u0432\u0443\u0439\u0442\u0435! \u0421\u0435\u0439\u0447\u0430\u0441 \u043d\u0435 \u043c\u043e\u0433\u0443 \u043e\u0442\u0432\u0435\u0442\u0438\u0442\u044c, \u043d\u0430\u043f\u0438\u0448\u0443 \u043f\u043e\u0437\u0436\u0435.",
    "\u0446\u0435\u043d\u0430": "\u041d\u0430\u043f\u0438\u0448\u0438\u0442\u0435, \u043f\u043e\u0436\u0430\u043b\u0443\u0439\u0441\u0442\u0430, \u0447\u0442\u043e \u0438\u043c\u0435\u043d\u043d\u043e \u0432\u0430\u0441 \u0438\u043d\u0442\u0435\u0440\u0435\u0441\u0443\u0435\u0442. \u042f \u043e\u0442\u0432\u0435\u0447\u0443 \u043f\u043e\u0437\u0436\u0435.",
}

SEARCH_STEPS = [
    "Подключаюсь к админ-боту",
    "Открываю раздел пользователей",
    "Ищу пользователя по ID",
    "Открываю подписки пользователя",
    "Формирую ответ",
]
INFO_STEPS = [
    "Подключаюсь к админ-боту",
    "Открываю раздел пользователей",
    "Ищу пользователя по ID",
    "Открываю список подписок",
    "Читаю подробности каждой подписки",
    "Формирую полный отчет",
]
MAIL_STEPS = [
    "Подключаюсь к админ-боту",
    "Открываю раздел пользователей",
    "Ищу пользователя по ID",
    "Открываю форму сообщения",
    "Передаю текст письма",
    "Подтверждаю отправку",
]
WIZARD_STEPS = [
    "Подключаюсь к админ-боту",
    "Открываю раздел пользователей",
    "Ищу пользователя по ID",
    "Открываю подписки пользователя",
    "Готовлю карточку",
    "Жду ответ: 1 отправить, 2 добавить, 0 отмена",
    "Отправляю в wizard",
]

REQUESTER_DENY_MESSAGE = (
    "Этот аккаунт не предназначен для получения сообшений, это сообщение "
    "сгенерировано автоматически, отвечать на него не нужно"
)


def make_progress_bar(done_units: int, total_units: int, width: int = 16) -> tuple[str, int]:
    total_units = max(total_units, 1)
    done_units = max(0, min(done_units, total_units))
    if width <= 0:
        width = max(12, min(30, 12 + len(str(total_units)) * 2))
    percent = int(round((done_units / total_units) * 100))
    filled = int(round((done_units / total_units) * width))
    return f"[{'█' * filled}{'·' * (width - filled)}]", percent


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
    if failed:
        status = "ошибка"
    elif done:
        status = "завершено"
    else:
        status = "выполняется"

    total_steps = max(len(steps), 1)
    current_step = max(1, min(active_step, total_steps))
    done_units = total_steps if done else max(current_step - 1 if failed else current_step, 0)
    bar, percent = make_progress_bar(done_units, total_steps, width=0)
    step_text = steps[current_step - 1] if steps else title
    title_text = decorate_status_title(title, done=done, failed=failed)
    status_icon = animated_symbol("error" if failed else "done" if done else "waiting")

    lines = [
        title_text,
        f"{bar} {percent}%",
        f"{status_icon} Статус: {status}",
        f"STEP {current_step}/{total_steps}: {step_text}",
    ]
    if user_id:
        lines.append(f"ID пользователя: {user_id}")
    if target:
        lines.append(f"Получатель: {target}")
    if extra_lines:
        lines.extend(str(line) for line in extra_lines if str(line).strip())

    return "\n".join(lines)


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


def is_final_status_text(text: str) -> bool:
    markers = (
        "Статус: завершено",
        "Статус: ошибка",
        "Статус: пауза",
        "Scan завершен",
        "Scan на паузе",
        "Scan сброшен",
    )
    return any(marker in text for marker in markers)


def extract_scan_position(text: str) -> tuple[int, int] | None:
    patterns = (
        r"Сканирование по ID:\s*(\d+)\s*/\s*(\d+)",
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


async def edit_status_message(message, text: str, *, buttons=None, parse_mode=None, force: bool = False) -> None:
    if not message:
        return
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
    except MessageNotModifiedError:
        status_edit_state[key] = (loop.time(), text)
    except FloodWaitError as error:
        wait_seconds = int(getattr(error, "seconds", 1) or 1)
        note_floodwait(wait_seconds)
        status_edit_state[key] = (loop.time() + wait_seconds, text)
        logging.warning("FloodWait on status edit: skipping edits for %ss", wait_seconds)
    except Exception:
        logging.exception("Failed to edit status message")


async def safe_event_reply(event, *args, **kwargs):
    if args and isinstance(args[0], str) and len(args[0]) > TELEGRAM_SAFE_TEXT_LIMIT and "file" not in kwargs:
        return await reply_with_text_file(event, args[0], **kwargs)
    try:
        return await event.reply(*args, **kwargs)
    except MessageTooLongError:
        if args and isinstance(args[0], str):
            logging.warning("Reply text is too long; sending it as a txt file")
            return await reply_with_text_file(event, args[0], **kwargs)
        logging.exception("Failed to send reply: message is too long")
        return None
    except FloodWaitError as error:
        wait_seconds = int(getattr(error, "seconds", 1) or 1)
        note_floodwait(wait_seconds)
        logging.warning("FloodWait on reply: message suppressed for %ss", wait_seconds)
        return None
    except Exception:
        logging.exception("Failed to send reply")
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
            "Полный текст слишком большой для Telegram. Отправляю файлом.",
            f"Файл: {path.name}",
            "",
            preview,
        )
    )
    try:
        return await event.reply(short_text, file=str(path), **file_kwargs)
    except MediaCaptionTooLongError:
        logging.warning("File caption is too long; retrying with minimal caption")
        try:
            return await event.reply(f"Полный текст в файле: {path.name}", file=str(path), **file_kwargs)
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
    checkpoint_text: str = "нет",
    done: bool = False,
    failed: bool = False,
    paused: bool = False,
) -> str:
    if failed:
        status = "ошибка"
    elif paused:
        status = "пауза"
    elif done:
        status = "завершено"
    else:
        status = "выполняется"

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
        "Scan пользователей",
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
        f"{status_icon} Статус: {status}",
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


def pick_reply(text: str) -> str:
    lowered = text.lower()
    for keyword, reply in KEYWORD_REPLIES.items():
        if keyword in lowered:
            return reply
    return settings.default_reply


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


def parse_user_lookup_command(command: str, text: str) -> UserLookupCommand | None:
    match = re.match(rf"^\s*/?{re.escape(command)}\s+(.+?)\s*$", text or "", flags=re.IGNORECASE)
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
    match = re.match(r"^\s*mail\s+(\d{1,20})(?:\s+([\s\S]+))?\s*$", text, flags=re.IGNORECASE)
    if not match:
        return None

    user_id = match.group(1)
    message_text = (match.group(2) or "").strip() or settings.mail_text
    return user_id, message_text


def parse_help_command(text: str) -> UserLookupCommand | None:
    return parse_user_lookup_command("help", text)


def is_help_overview_command(text: str) -> bool:
    return bool(re.match(r"^\s*/?help\s*$", text, flags=re.IGNORECASE))


def is_command_menu_command(text: str) -> bool:
    return bool(re.match(r"^\s*/?(?:menu|commands|команды|меню)\s*$", text, flags=re.IGNORECASE))


def is_status_command(text: str) -> bool:
    return bool(re.match(r"^\s*/?(?:status|статус)\s*$", text, flags=re.IGNORECASE))


def is_version_command(text: str) -> bool:
    return bool(re.match(r"^\s*/?(?:version|версия|v)\s*$", text, flags=re.IGNORECASE))


def is_roots_command(text: str) -> bool:
    return bool(re.match(r"^\s*/?roots(?:\s+.*)?$", text or "", flags=re.IGNORECASE))


def is_stop_scan_command(text: str) -> bool:
    return bool(
        re.match(
            r"^\s*(?:/stopscan|stop\s+scan|stop\s+скан|стоп\s+скан|scan\s+pause|scan\s+stop)\s*$",
            text,
            flags=re.IGNORECASE,
        )
    )


def parse_scan_menu_action(text: str, allow_numeric: bool = False) -> str | None:
    cleaned = text.strip().casefold()
    if cleaned in {"/scan", "scan", "скан"}:
        return "menu"
    if cleaned in {"/scan_new", "scan new", "new scan", "новый scan", "новый скан", "начать скан", "запусти скан"}:
        return "new"
    if cleaned in {
        "/scan_start",
        "/scan_continue",
        "scan start",
        "scan continue",
        "continue scan",
        "start scan",
        "продолжить scan",
        "продолжить скан",
        "продолжи скан",
    }:
        return "continue"
    if cleaned in {
        "/stopscan",
        "stop scan",
        "stop скан",
        "стоп скан",
        "scan stop",
        "scan pause",
        "pause scan",
        "пауза scan",
        "пауза скан",
        "остановить scan",
        "остановить скан",
    }:
        return "pause_results"
    mapping = {
        "/scan": "start",
        "/scanmenu": "menu",
        "scan menu": "menu",
        "scan status": "menu",
        "статус скана": "menu",
        "статус scan": "menu",
        "сканы": "menu",
        "меню скан": "menu",
        "меню сканов": "menu",
        "/scan_start": "start",
        "scan": "start",
        "скан": "start",
        "scan start": "start",
        "scan new": "start",
        "start scan": "start",
        "новый скан": "start",
        "новый scan": "start",
        "запусти скан": "start",
        "начать скан": "start",
        "продолжить скан": "start",
        "продолжи скан": "start",
        "/stopscan": "pause",
        "/scan_pause": "pause",
        "scan pause": "pause",
        "scan stop": "pause",
        "pause scan": "pause",
        "stop scan": "pause",
        "пауза скан": "pause",
        "поставь скан на паузу": "pause",
        "остановить скан": "pause",
        "/scan_reset": "reset",
        "scan reset": "reset",
        "reset scan": "reset",
        "сброс скана": "reset",
        "сбросить скан": "reset",
        "сброс scan": "reset",
        "/scan_results": "results",
        "scan results": "results",
        "scan result": "results",
        "results scan": "results",
        "результаты скана": "results",
        "показать результаты скана": "results",
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
    return parse_user_lookup_command("info", text)


def parse_wizard_command(text: str) -> str | None:
    match = re.match(r"^\s*wizard\s+(\d{1,20})\s*$", text, flags=re.IGNORECASE)
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
    if cleaned in {"1", "нет", "no", "n", "отправить", "send"}:
        return "send_now"
    if cleaned in {"2", "да", "yes", "y", "добавить", "add"}:
        return "add_text"
    if cleaned in {"0", "отмена", "cancel", "/cancel"}:
        return "cancel"
    return None


def parse_scan_command(text: str) -> str | None:
    return parse_scan_menu_action(text, allow_numeric=False)


def build_command_menu_text() -> str:
    return "\n".join(
        (
            "Меню команд",
            "",
            "Выбери кнопку или отправь команду вручную:",
            "help <user_id|username> - найти пользователя через админ-бот",
            "help <user_id|username> -b - взять пользователя из SQLite базы",
            "info <user_id|username> - подробная информация и подписки",
            "info <user_id|username> -b - подробная информация из SQLite базы",
            "wizard <user_id> - подготовить карточку и отправить в wizard",
            "mail <user_id> <текст> - отправить сообщение пользователю",
            "/roots - список запросников",
            "/roots add <user_id|@username|me> - добавить запросника",
            "/roots del <user_id|@username> - удалить запросника",
            "scan - меню скана",
            "scan new - новый скан с первой страницы",
            "scan continue - продолжить сохраненный скан",
            "stop скан - пауза и просмотр результатов",
            "scan results - результаты scan",
            "scan reset - сброс сохраненного scan",
            "/status - собрать dashboard из SQL базы и отправить в чат",
            "/version - показать версию, commit и дату запуска",
        )
    )


def build_command_menu_buttons():
    return [
        [Button.text("scan"), Button.text("scan results")],
        [Button.text("/status"), Button.text("/version"), Button.text("menu")],
        [Button.text("scan new"), Button.text("scan continue")],
        [Button.text("stop скан"), Button.text("scan reset")],
        [Button.text("help 123456789"), Button.text("info 123456789")],
        [Button.text("help username -b"), Button.text("info username -b")],
        [Button.text("wizard 123456789"), Button.text("mail 123456789")],
        [Button.text("/roots"), Button.text("/roots add me")],
    ]


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
    return any(word and word in lowered for word in words) or text.strip() in {"➡", "➡️", "»", ">>", "⏭"}


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
    candidates = {"⬅", "⬅️", "«", "<<", "⏮"}
    tokens = (
        settings.back_button_text.casefold(),
        settings.cancel_button_text.casefold(),
        "назад",
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


def get_next_page_button(message) -> dict[str, int | str] | None:
    if not message.buttons:
        return None

    expected = settings.next_page_button_text.casefold()
    candidates = {"➡", "➡️", "»", ">>", "⏭"}
    for row_index, row in enumerate(message.buttons):
        for column_index, button in enumerate(row):
            text = button.text.strip()
            lowered = text.casefold()
            if (expected and expected in lowered) or text in candidates:
                return {
                    "text": button.text,
                    "row": row_index,
                    "column": column_index,
                }

    nav_buttons = [button for button in extract_all_buttons(message) if is_navigation_button_text(str(button["text"]))]
    if nav_buttons:
        back_tokens = (
            settings.back_button_text.casefold(),
            "назад",
            "back",
            "return",
        )
        forward_buttons = [
            button
            for button in nav_buttons
            if not any(token and token in str(button["text"]).casefold() for token in back_tokens)
        ]
        pool = forward_buttons or nav_buttons
        pool.sort(key=lambda button: (int(button["row"]), int(button["column"])), reverse=True)
        return pool[0]
    return None


def is_users_page_message(message) -> bool:
    return bool(extract_user_buttons(message))


def users_page_user_ids_signature(message) -> tuple[str, ...]:
    return tuple(str(button["id"]) for button in extract_user_buttons(message))


def score_users_menu_button(text: str) -> int:
    lowered = text.casefold()
    score = 0
    if any(token in lowered for token in ("польз", "user", "users", "клиент", "абонент", "участ")):
        score += 30
    if any(symbol in text for symbol in ("👤", "👥", "🧑", "🙍")):
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
        text = str(button["text"])
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
    pattern = re.compile(r"(\d[\d\s.,]*)\s*(?:₽|руб|р\b|rub)", flags=re.IGNORECASE)
    match = pattern.search(line)
    if match:
        return parse_float_number(match.group(1))

    if any(token in line.casefold() for token in ("приб", "доход", "выруч", "profit", "revenue")):
        match = re.search(r"(\d[\d\s.,]*)", line)
        if match:
            return parse_float_number(match.group(1))
    return None


def detect_period_key(line: str) -> str | None:
    lowered = line.casefold()
    if any(token in lowered for token in ("сегодня", "за день", "день", "day", "daily")):
        return "day"
    if any(token in lowered for token in ("недел", "week", "weekly")):
        return "week"
    if any(token in lowered for token in ("3 мес", "3 month", "кварт", "quarter")):
        return "quarter"
    if any(token in lowered for token in ("6 мес", "полгод", "half-year", "half year")):
        return "half_year"
    if any(token in lowered for token in ("месяц", "month", "monthly")):
        return "month"
    if any(token in lowered for token in ("год", "year", "yearly", "annual")):
        return "year"
    if any(token in lowered for token in ("все время", "всё время", "all time", "total")):
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
        if any(token in lowered for token in ("польз", "user")):
            user_match = re.search(r"(\d{1,9})", line)
            if user_match:
                try:
                    users_by_period[period_key] = int(user_match.group(1))
                except ValueError:
                    pass

        if any(token in lowered for token in ("приб", "доход", "выруч", "profit", "revenue", "руб", "₽", "rub")):
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


def extract_vpn_count(text: str) -> int | None:
    patterns = (
        r"(?:кол(?:-?во|ичество)?\s*)?vpn\s*[:=\-]?\s*(\d{1,6})",
        r"vpn\s*(?:подписок|ключей|subscriptions)?\s*[:=\-]?\s*(\d{1,6})",
        r"(?:подписок|ключей)\s*vpn\s*[:=\-]?\s*(\d{1,6})",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        try:
            return int(match.group(1))
        except ValueError:
            continue
    return None


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
    subscriptions_text = ", ".join(subscription_numbers) if subscription_numbers else "подписок нет"
    user_number = extract_user_number(user_text, subscriptions_text_for_number)

    return "\n".join(
        (
            f"1. Username бота: @{settings.admin_bot_username}",
            f"2. ID пользователя: {user_number or user_id}",
            f"3. Username пользователя: @{username}" if username else "3. Username пользователя: нет в базе",
            f"4. Айди подписок: {subscriptions_text}",
            "5. Источник: SQLite база",
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
        f"1. Username бота: @{html.escape(settings.admin_bot_username)}",
        f"2. ID пользователя: {html.escape(user_number or user_id)}",
        (
            f"3. Username пользователя: @{html.escape(username)}"
            if username
            else "3. Username пользователя: нет в базе"
        ),
        (
            f"4. Дата регистрации: {html.escape(registration_date)}"
            if registration_date
            else "4. Дата регистрации: нет в базе"
        ),
        f"5. Подписок в базе: {len(subscriptions)}",
    ]

    if user_text.strip():
        lines.extend(("", "6. Карточка из базы:", html.escape(user_text.strip())))

    if not subscriptions:
        lines.append("\n7. Инфо подписок: подписок нет")
        return "\n".join(lines)

    lines.append("\n7. Инфо подписок:")
    for subscription in subscriptions:
        subscription_id = str(subscription.get("subscription_id") or "")
        button_text = str(subscription.get("button_text") or "")
        detail_text = str(subscription.get("detail_text") or "").strip()
        lines.append("")
        lines.append(f"[{html.escape(subscription_id)}] {html.escape(button_text)}")
        lines.append(make_keys_copyable_html(detail_text or "[empty subscription response]"))

    lines.append("\n8. Источник: SQLite база")
    return "\n".join(lines)


def log_message(label: str, message) -> None:
    logging.info("%s message_id=%s text=%r", label, getattr(message, "id", None), message.raw_text or "")
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
    set_admin_bot_health("[WAIT]", "проверка", "получаю entity")
    admin_bot_entity_cache = await client.get_entity(settings.admin_bot_username)
    set_admin_bot_health("[OK]", "отвечает", "entity получен")
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
            set_admin_bot_health("[WAIT]", "проверка", "читаю последнее сообщение")
            messages = await client.get_messages(bot, limit=limit)
            set_admin_bot_health("[OK]", "отвечает", "история доступна")
            break
        except FloodWaitError as error:
            wait_seconds = int(getattr(error, "seconds", 1) or 1)
            note_floodwait(wait_seconds)
            set_admin_bot_health("[WAIT]", "ожидание", f"FloodWait {wait_seconds}s")
            if attempt:
                raise
            logging.warning("FloodWait on latest_bot_message: sleeping %ss", wait_seconds)
            await asyncio.sleep(wait_seconds + 1)
    for message in messages:
        if is_incoming_bot_message(message):
            return message
    set_admin_bot_health("[ERR]", "ошибка", "нет входящих сообщений")
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
            set_admin_bot_health("[WAIT]", "ожидание", f"FloodWait {wait_seconds}s")
            await asyncio.sleep(min(wait_seconds + 1, BOT_HEALTH_POLL_INTERVAL_SECONDS * 2))
            continue
        except Exception as error:
            set_admin_bot_health("[ERR]", "ошибка", str(error)[:80])
            logging.warning("Admin bot health check failed: %s", error)
        await asyncio.sleep(BOT_HEALTH_POLL_INTERVAL_SECONDS)


def message_snapshot(message) -> tuple[int | None, str, tuple[tuple[str, ...], ...]]:
    buttons: tuple[tuple[str, ...], ...] = ()
    if message.buttons:
        buttons = tuple(tuple(button.text for button in row) for row in message.buttons)
    return getattr(message, "id", None), message.raw_text or "", buttons


def message_content_signature(message) -> tuple[str, tuple[tuple[str, ...], ...]]:
    buttons: tuple[tuple[str, ...], ...] = ()
    if message.buttons:
        buttons = tuple(tuple(button.text for button in row) for row in message.buttons)
    return message.raw_text or "", buttons


def is_intermediate_message(message) -> bool:
    return (message.raw_text or "").strip() == "\u23f3" and not message.buttons


async def wait_bot_update(bot, previous_snapshot=None, ready=None, timeout_seconds: float | None = None):
    future = loop.create_future()
    timeout_seconds = timeout_seconds or settings.bot_response_timeout_seconds
    set_admin_bot_health("[WAIT]", "ожидание", "жду ответ")

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
            set_admin_bot_health("[ERR]", "завис", f"нет ответа {timeout_seconds:.0f}s")
            raise TimeoutError(
                f"Admin bot @{settings.admin_bot_username} did not send an expected update "
                f"within {timeout_seconds:.0f}s."
            )
        for task in pending:
            task.cancel()
        result = done.pop().result()
        set_admin_bot_health("[OK]", "отвечает", "получен ответ")
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


def prune_dashboard_public_files() -> None:
    public_dir = dashboard_public_dir()
    files = [
        path
        for path in public_dir.glob("*.html")
        if path.is_file() and not path.name.startswith("latest-")
    ]
    files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    for old_path in files[settings.dashboard_public_retention:]:
        try:
            old_path.unlink()
        except OSError:
            logging.exception("Failed to prune old public dashboard: %s", old_path)


def publish_dashboard_file(source_path: Path, latest_name: str | None = None) -> tuple[Path, str]:
    source_path = Path(source_path)
    public_dir = dashboard_public_dir()
    public_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", source_path.name)
    if not public_name.endswith(".html"):
        public_name = f"{public_name}.html"
    public_path = public_dir / public_name
    shutil.copy2(source_path, public_path)

    if latest_name:
        latest_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", latest_name)
        if not latest_name.endswith(".html"):
            latest_name = f"{latest_name}.html"
        latest_path = public_dir / latest_name
        shutil.copy2(source_path, latest_path)

    prune_dashboard_public_files()
    return public_path, build_dashboard_public_url(public_name)


def public_dashboard_url_from_report_path(report_path: Path) -> str:
    public_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", Path(report_path).name)
    return build_dashboard_public_url(public_name)


def ensure_dashboard_public_url(report_path: Path, latest_name: str | None = None) -> str:
    public_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", Path(report_path).name)
    public_path = dashboard_public_dir() / public_name
    if not public_path.exists() and Path(report_path).exists():
        publish_dashboard_file(Path(report_path), latest_name=latest_name)
    elif latest_name and Path(report_path).exists():
        latest_path = dashboard_public_dir() / re.sub(r"[^A-Za-z0-9_.-]+", "-", latest_name)
        if not latest_path.exists():
            shutil.copy2(report_path, latest_path)
    return build_dashboard_public_url(public_name)


class DashboardRequestHandler(BaseHTTPRequestHandler):
    server_version = "VPNKBRDashboard/1.0"

    def log_message(self, format: str, *args) -> None:
        logging.info("Dashboard HTTP: " + format, *args)

    def do_GET(self) -> None:
        self.serve_dashboard(send_body=True)

    def do_HEAD(self) -> None:
        self.serve_dashboard(send_body=False)

    def serve_dashboard(self, *, send_body: bool) -> None:
        path = unquote(urlsplit(self.path).path)
        parts = [part for part in path.split("/") if part]
        prefix = settings.dashboard_public_path_prefix.strip("/")
        if not parts or parts[0] != prefix:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        parts = parts[1:]
        if settings.dashboard_public_token:
            if not parts or parts[0] != settings.dashboard_public_token:
                self.send_error(HTTPStatus.NOT_FOUND, "Not found")
                return
            parts = parts[1:]

        if len(parts) != 1:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        file_name = parts[0]
        if "/" in file_name or "\\" in file_name or not file_name.endswith(".html"):
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        public_dir = dashboard_public_dir().resolve()
        file_path = (public_dir / file_name).resolve()
        if public_dir not in file_path.parents or not file_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        content = file_path.read_bytes()
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
            raw_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (user_id, subscription_id),
            FOREIGN KEY (user_id) REFERENCES latest_users(user_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_users_run_user_id ON users(run_id, user_id);
        CREATE INDEX IF NOT EXISTS idx_subscriptions_run_user_id ON subscriptions(run_id, user_id);
        CREATE INDEX IF NOT EXISTS idx_subscriptions_expires_at ON subscriptions(expires_at);
        CREATE INDEX IF NOT EXISTS idx_scan_errors_run_user_id ON scan_errors(run_id, user_id);
        CREATE INDEX IF NOT EXISTS idx_requesters_user_id ON requesters(user_id);
        CREATE INDEX IF NOT EXISTS idx_requesters_username ON requesters(username);
        CREATE INDEX IF NOT EXISTS idx_latest_subscriptions_user_id ON latest_subscriptions(user_id);
        CREATE INDEX IF NOT EXISTS idx_latest_subscriptions_expires_at ON latest_subscriptions(expires_at);
        """
    )
    ensure_database_column(conn, "users", "username", "TEXT NOT NULL DEFAULT ''")
    ensure_database_column(conn, "latest_users", "username", "TEXT NOT NULL DEFAULT ''")
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
        DROP TABLE IF EXISTS users;
        DROP TABLE IF EXISTS scan_runs;
        """
    )
    conn.commit()
    initialize_database(conn)


def reset_scan_database() -> None:
    with connect_database() as conn:
        reset_database(conn)


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
    return bool(row)


def seed_requesters_from_settings() -> None:
    for lookup in settings.root_requester_ids:
        try:
            upsert_requester(lookup, note="seed from ROOT_REQUESTER_IDS", added_by="env")
        except ValueError:
            logging.warning("Invalid ROOT_REQUESTER_IDS item ignored: %r", lookup)


def build_roots_text() -> str:
    rows = load_requesters()
    lines = [
        "Список запросников",
        "",
        "Только эти аккаунты могут отправлять команды этому аккаунту.",
        "",
    ]
    if not rows:
        lines.extend(
            [
                "Список пуст.",
                "Чтобы добавить себя: /roots add me",
                "Чтобы добавить другого: /roots add 123456789 комментарий",
                "Можно добавить username: /roots add @username комментарий",
            ]
        )
        return "\n".join(lines)

    for index, row in enumerate(rows, start=1):
        identity = row["user_id"] or (f"@{row['username']}" if row["username"] else row["lookup_key"])
        note = f" — {row['note']}" if row["note"] else ""
        lines.append(f"{index}. {identity}{note}")
    lines.extend(
        [
            "",
            "Команды:",
            "/roots add me",
            "/roots add <user_id|@username> [комментарий]",
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
                    raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    str(record.get("user_id") or ""),
                    username,
                    str(record.get("user_button_text") or ""),
                    str(record.get("user_text") or ""),
                    record.get("registration_date"),
                    len(subscriptions),
                    json.dumps(record, ensure_ascii=False),
                ),
            )
            user_db_id = int(user_cursor.lastrowid)

            for subscription in subscriptions:
                expires_at = extract_expiration_date(str(subscription.get("detail_text") or ""))
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
                        raw_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    if not row:
        return None
    try:
        data = json.loads(str(row["stats_json"]))
    except json.JSONDecodeError:
        logging.exception("Failed to parse latest scan stats from database")
        return None
    return data if isinstance(data, dict) else None


def upsert_latest_record_with_conn(conn: sqlite3.Connection, record: dict, *, observed_at: str | None = None) -> None:
    observed_at = observed_at or datetime.now().isoformat(timespec="seconds")
    user_id = str(record.get("user_id") or "").strip()
    if not user_id:
        return

    subscriptions = list(record.get("subscriptions") or [])
    username = extract_username_from_record(record)
    conn.execute(
        """
        INSERT INTO latest_users (
            user_id,
            username,
            user_button_text,
            user_text,
            registration_date,
            subscriptions_count,
            raw_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            user_button_text=excluded.user_button_text,
            user_text=excluded.user_text,
            registration_date=excluded.registration_date,
            subscriptions_count=excluded.subscriptions_count,
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
            json.dumps(record, ensure_ascii=False),
            observed_at,
        ),
    )

    conn.execute("DELETE FROM latest_subscriptions WHERE user_id = ?", (user_id,))
    for subscription in subscriptions:
        expires_at = extract_expiration_date(str(subscription.get("detail_text") or ""))
        conn.execute(
            """
            INSERT INTO latest_subscriptions (
                user_id,
                subscription_id,
                button_text,
                location,
                detail_text,
                expires_at,
                raw_json,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                str(subscription.get("subscription_id") or ""),
                str(subscription.get("button_text") or ""),
                str(subscription.get("location") or ""),
                str(subscription.get("detail_text") or ""),
                expires_at.strftime("%Y-%m-%d") if expires_at else None,
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
            SELECT user_id, username, user_button_text, user_text, registration_date
            FROM latest_users
            ORDER BY CAST(user_id AS INTEGER)
            """
        ).fetchall()
        if not user_rows:
            return []

        sub_rows = conn.execute(
            """
            SELECT user_id, subscription_id, button_text, location, detail_text
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
                "subscriptions": subs_by_user.get(user_id, []),
            }
        )
    return records


def load_latest_record_from_database(user_id: str) -> dict | None:
    lookup_user_id = str(user_id).strip()
    if not lookup_user_id:
        return None

    with connect_database() as conn:
        initialize_database(conn)
        seed_latest_records_from_scan_runs(conn)
        row = conn.execute(
            """
            SELECT user_id, username, user_button_text, user_text, registration_date
            FROM latest_users
            WHERE user_id = ?
            """,
            (lookup_user_id,),
        ).fetchone()
        if not row:
            return None
        sub_rows = conn.execute(
            """
            SELECT subscription_id, button_text, location, detail_text
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
        "subscriptions": [
            {
                "subscription_id": str(sub_row["subscription_id"] or ""),
                "button_text": str(sub_row["button_text"] or ""),
                "location": str(sub_row["location"] or ""),
                "detail_text": str(sub_row["detail_text"] or ""),
            }
            for sub_row in sub_rows
        ],
    }


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
               OR lower(user_text) LIKE ?
               OR lower(raw_json) LIKE ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (username, f"%@{username}%", f"%@{username}%"),
        ).fetchone()

    if row:
        return load_latest_record_from_database(str(row["user_id"] or ""))

    for record in load_latest_records_from_database():
        if extract_username_from_record(record) == username:
            return record

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


def build_business_status_dashboard_html(stats: dict, analysis: dict) -> str:
    def esc(value) -> str:
        return html.escape(str(value))

    def fmt_int(value) -> str:
        return f"{int(round(float(value or 0))):,}".replace(",", " ")

    def fmt_money(value) -> str:
        return f"{float(value or 0):,.0f}".replace(",", " ")

    projections = list(analysis.get("projections") or [])
    monthly_rows = list(analysis.get("recent_monthly_rows") or [])
    projection_rows = "".join(
        (
            f"<tr><td>{item['months']} мес</td>"
            f"<td>{fmt_int(item['users'])}</td>"
            f"<td>{fmt_int(item['paid_users'])}</td>"
            f"<td>{fmt_int(item['subscriptions'])}</td>"
            f"<td>{fmt_money(item['revenue_rub'])} ₽</td></tr>"
        )
        for item in projections
    )
    monthly_table_rows = "".join(
        (
            f"<tr><td>{esc(item['month'])}</td>"
            f"<td>{fmt_int(item['users'])}</td>"
            f"<td>{fmt_int(item['paid_users'])}</td>"
            f"<td>{fmt_int(item['subscriptions'])}</td>"
            f"<td>{fmt_money(item['estimated_revenue_rub'])} ₽</td></tr>"
        )
        for item in monthly_rows
    ) or "<tr><td colspan='5'>Нет дат регистрации в данных scan</td></tr>"

    max_subs = max([int(item.get("subscriptions") or 0) for item in monthly_rows] or [1])
    bars = "".join(
        (
            f"<div class='bar-row'><span>{esc(item['month'])}</span>"
            f"<div class='bar'><i style='width:{max(4, int((item['subscriptions'] / max_subs) * 100))}%'></i></div>"
            f"<b>{fmt_int(item['subscriptions'])}</b></div>"
        )
        for item in monthly_rows
    ) or "<div class='muted'>Недостаточно исторических дат</div>"

    best_month = analysis.get("best_month") or {}
    growth_rate_text = f"{float(analysis.get('monthly_growth_rate') or 0) * 100:.1f}%"
    dashboard_html = f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Business Status Dashboard</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ margin:0; font-family: Arial, sans-serif; background:#08111f; color:#eef5ff; }}
    .wrap {{ max-width:1280px; margin:0 auto; padding:24px 18px 38px; }}
    h1 {{ margin:0 0 6px; font-size:30px; }}
    .muted {{ color:#9fb0c9; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:10px; margin:16px 0; }}
    .card,.panel {{ background:#111d30; border:1px solid #243650; border-radius:8px; padding:14px; }}
    .k {{ color:#9fb0c9; font-size:13px; }}
    .v {{ font-size:25px; font-weight:700; margin-top:7px; }}
    .good {{ color:#36d399; }} .warn {{ color:#f6c453; }} .blue {{ color:#5ab7ff; }}
    .cols {{ display:grid; grid-template-columns:1fr 1fr; gap:12px; }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th,td {{ padding:8px 9px; border-bottom:1px solid #243650; text-align:left; }}
    th {{ color:#9fb0c9; }}
    .bar-row {{ display:grid; grid-template-columns:80px 1fr 54px; gap:10px; align-items:center; margin:8px 0; }}
    .bar {{ height:12px; background:#0a1424; border:1px solid #243650; border-radius:7px; overflow:hidden; }}
    .bar i {{ display:block; height:100%; background:linear-gradient(90deg,#5ab7ff,#36d399); }}
    @media (max-width:900px) {{ .cols {{ grid-template-columns:1fr; }} h1 {{ font-size:25px; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Business Status</h1>
    <div class="muted">SQL dashboard · scan: {esc(analysis.get('source_scan_generated_at') or '-')} · generated: {esc(analysis.get('generated_at'))}</div>
    <div class="grid">
      <div class="card"><div class="k">Оценка MRR</div><div class="v good">{fmt_money(analysis.get('estimated_mrr_rub'))} ₽</div></div>
      <div class="card"><div class="k">База дохода / месяц</div><div class="v good">{fmt_money(analysis.get('baseline_month_revenue_rub'))} ₽</div></div>
      <div class="card"><div class="k">Пользователей</div><div class="v blue">{fmt_int(analysis.get('total_users'))}</div></div>
      <div class="card"><div class="k">Платящих</div><div class="v blue">{fmt_int(analysis.get('paid_users'))}</div></div>
      <div class="card"><div class="k">Подписок</div><div class="v warn">{fmt_int(analysis.get('total_subscriptions'))}</div></div>
      <div class="card"><div class="k">Рост подписок / мес</div><div class="v warn">{growth_rate_text}</div></div>
    </div>
    <div class="cols">
      <div class="panel">
        <h2>Прогноз на 1 / 3 / 6 / 9 / 12 месяцев</h2>
        <table><thead><tr><th>Горизонт</th><th>Users</th><th>Paid</th><th>Subs</th><th>Доход/мес</th></tr></thead><tbody>{projection_rows}</tbody></table>
      </div>
      <div class="panel">
        <h2>Прирост подписок по месяцам</h2>
        {bars}
      </div>
    </div>
    <div class="panel">
      <h2>История прироста за последние месяцы</h2>
      <table><thead><tr><th>Месяц</th><th>Новые users</th><th>Платящие</th><th>Подписки</th><th>Оценка дохода</th></tr></thead><tbody>{monthly_table_rows}</tbody></table>
    </div>
    <div class="grid">
      <div class="card"><div class="k">Период наблюдения</div><div class="v">{fmt_int(analysis.get('observation_days'))} дн.</div></div>
      <div class="card"><div class="k">Users / день</div><div class="v">{float(analysis.get('users_per_day') or 0):.2f}</div></div>
      <div class="card"><div class="k">Subs / день</div><div class="v">{float(analysis.get('subscriptions_per_day') or 0):.2f}</div></div>
      <div class="card"><div class="k">Лучший месяц</div><div class="v">{esc(best_month.get('month', '-'))}</div></div>
      <div class="card"><div class="k">Ошибки scan</div><div class="v">{fmt_int(analysis.get('scan_errors_total'))}</div></div>
    </div>
  </div>
</body>
</html>"""
    return fix_mojibake(dashboard_html)


def build_status_dashboard_from_database() -> tuple[Path, dict] | None:
    stats = load_latest_scan_stats_from_database()
    if not stats:
        return None
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
    atomic_write_text(dashboard_path, build_business_status_dashboard_html(stats, analysis))
    public_path, public_url = publish_dashboard_file(dashboard_path, latest_name="latest-status-dashboard.html")
    stats["dashboard_public_path"] = str(public_path)
    stats["dashboard_public_url"] = public_url
    return dashboard_path, stats


def build_status_summary_from_stats(stats: dict, dashboard_path: Path) -> str:
    analysis = dict(stats.get("business_analysis") or analyze_business_status(stats))
    projections = list(analysis.get("projections") or [])
    dashboard_url = str(stats.get("dashboard_public_url") or ensure_dashboard_public_url(dashboard_path, "latest-status-dashboard.html"))
    lines = [
        "Business status из SQL базы",
        f"Админ-бот: {format_admin_bot_health()}",
        f"SQLite: {database_path()}",
        f"Dashboard: {dashboard_url or dashboard_path}",
        "",
        f"Сформирован scan: {str(stats.get('generated_at') or '-').replace('T', ' ')}",
        f"Пользователей: {int(analysis.get('total_users') or 0)}",
        f"Платящих: {int(analysis.get('paid_users') or 0)}",
        f"Подписок: {int(analysis.get('total_subscriptions') or 0)}",
        f"MRR оценка: {float(analysis.get('estimated_mrr_rub') or 0):.0f} RUB",
        f"Рост подписок / месяц: {float(analysis.get('monthly_growth_rate') or 0) * 100:.1f}%",
        f"Ошибок scan: {int(analysis.get('scan_errors_total') or 0)}",
    ]
    if projections:
        lines.append("")
        lines.append("Прогноз дохода / мес:")
        for item in projections:
            lines.append(
                f"- {int(item['months'])} мес: {float(item['revenue_rub']):.0f} RUB, "
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
    progress_title: str = "Поиск пользователя",
    progress_steps: list[str] | None = None,
):
    steps = progress_steps or SEARCH_STEPS
    await emit_process_progress(
        progress_callback,
        progress_title,
        steps,
        1,
        user_id=user_id,
        extra_lines=[f"Админ-бот: @{settings.admin_bot_username}", f"Команда: {settings.admin_command}"],
    )
    admin_message = await send_admin_and_get_menu(conv, bot)
    admin_message = await reset_admin_state_if_needed(conv, bot, admin_message)

    await emit_process_progress(
        progress_callback,
        progress_title,
        steps,
        2,
        user_id=user_id,
        extra_lines=[f"Кнопка раздела: {settings.users_button_text}"],
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
        extra_lines=[f"Кнопка поиска: {settings.find_user_button_text}", f"Отправляю ID: {user_id}"],
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
    progress_title: str = "Поиск пользователя",
    progress_steps: list[str] | None = None,
) -> str:
    steps = progress_steps or SEARCH_STEPS
    await emit_process_progress(
        progress_callback,
        progress_title,
        steps,
        1,
        user_id=user_id,
        extra_lines=["Ожидаю свободный админ-процесс"],
    )
    async with admin_flow_lock:
        await emit_process_progress(
            progress_callback,
            progress_title,
            steps,
            1,
            user_id=user_id,
            extra_lines=[f"Получаю Telegram entity @{settings.admin_bot_username}"],
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
                extra_lines=[f"Кнопка подписок: {settings.subscriptions_button_text}"],
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
                f"Найдено подписок: {len(subscription_numbers)}",
                "Готовлю короткую карточку пользователя",
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


def format_subscription_info(user_id: str, user_text: str, subscriptions_message, details: list[tuple[str, str, str]]) -> str:
    subscriptions_text = subscriptions_message.raw_text or ""
    user_number = extract_user_number(user_text, subscriptions_text)

    lines = [
        f"1. Username \u0431\u043e\u0442\u0430: @{settings.admin_bot_username}",
        f"2. ID \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u0435\u0439: {user_number or user_id}",
    ]

    if not details:
        lines.append("3. \u0418\u043d\u0444\u043e \u043f\u043e\u0434\u043f\u0438\u0441\u043e\u043a: \u043f\u043e\u0434\u043f\u0438\u0441\u043e\u043a \u043d\u0435\u0442")
        return "\n".join(lines)

    lines.append("3. \u0418\u043d\u0444\u043e \u043f\u043e\u0434\u043f\u0438\u0441\u043e\u043a:")
    for subscription_id, button_text, detail_text in details:
        lines.append("")
        lines.append(f"[{subscription_id}] {button_text}")
        lines.append(detail_text.strip() or "[empty subscription response]")

    return "\n".join(lines)


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
    cleaned = re.sub(r"\[\d+\]", "", text).strip()
    return cleaned or "\u0431\u0435\u0437 \u043b\u043e\u043a\u0430\u0446\u0438\u0438"


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
        "дата регистрации",
        "зарегистрирован",
        "регистрация",
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
        if not any(token in line_lower for token in ("рег", "register", "created")):
            continue
        found = extract_expiration_date(line)
        if found and 2000 <= found.year <= datetime.now().year + 1:
            return found
    return None


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
        user_subscriptions = record["subscriptions"]
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
        sub_count = len(record.get("subscriptions") or [])
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
        "Отчет scan",
        f"Сформирован: {now.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Проверено ID: {pages_total}",
        f"Пользователей: {len(records)}",
        f"Пользователей с подписками: {users_with_subscriptions}",
        f"Пользователей без подписок: {len(users_without_subscriptions)}",
        f"Подписок: {total_subscriptions}",
        f"Среднее подписок на пользователя: {avg_subscriptions_per_user:.2f}",
        f"Среднее подписок на пользователя с подписками: {avg_subscriptions_per_active_user:.2f}",
        f"Подписок с датой окончания: {dated_subscriptions}",
        f"Подписок без даты окончания: {undated_subscriptions}",
        f"Самая ранняя дата окончания: {stats['earliest_expiration'] or 'нет'}",
        f"Самая поздняя дата окончания: {stats['latest_expiration'] or 'нет'}",
        "",
        "Доходность на следующий месяц (по реальным срокам истечения):",
        f"- Подписок с истечением в 30 дней: {expiring_30_total}",
        f"- Базовый сценарий (70% продлят): ~{fmt_money(scenario_base)} RUB",
        f"- Консервативный (60%): ~{fmt_money(scenario_low)} RUB",
        f"- Оптимистичный (80%): ~{fmt_money(scenario_high)} RUB",
        f"- Риск потери выручки при непродлении: ~{fmt_money(churn_risk_revenue_30d)} RUB",
        f"- Потенциал возврата уже истекших (winback): ~{fmt_money(expected_winback_revenue_30d)} RUB",
        "",
        "Финансовый прогноз (объединение статистики и подписок):",
        f"- Через 1 месяц: ~{fmt_money(financial_month_1)} RUB",
        f"- Через 6 месяцев: ~{fmt_money(financial_month_6)} RUB",
        f"- Через 12 месяцев: ~{fmt_money(financial_month_12)} RUB",
        f"- Источник базы: {'прибыль из статистики' if stats_month_profit > 0 else 'оценка MRR по подпискам'}",
        f"- История прибыли из статистики: месяц {fmt_money(stats_month_profit)} / полгода {fmt_money(stats_half_year_profit)} / год {fmt_money(stats_year_profit)} RUB",
        "",
        "Прогноз через 6 месяцев (по скорости прироста от даты регистрации):",
        f"- Период наблюдения: {observation_days} дней (с {observation_start.isoformat() if observation_start else 'нет данных'})",
        f"- Покрытие дат регистрации (все пользователи): {registration_coverage_users:.0%}",
        f"- Покрытие дат регистрации (платящие): {registration_coverage_paid:.0%}",
        f"- Скорость прироста пользователей: {users_growth_per_day:.2f}/день",
        f"- Скорость прироста платящих: {paid_users_growth_per_day:.2f}/день",
        f"- Скорость прироста подписок: {subscriptions_growth_per_day:.2f}/день",
        f"- Прогноз пользователей через 6м: ~{int(round(projected_users_6m))}",
        f"- Прогноз платящих через 6м: ~{int(round(projected_paid_users_6m))}",
        f"- Прогноз подписок через 6м: ~{int(round(projected_subscriptions_6m))}",
        f"- Прогноз MRR через 6м: ~{fmt_money(projected_mrr_6m)} RUB",
        "",
        "Распределение по срокам (активные и истекшие):",
        f"- Истекли: {timing_buckets['expired']}",
        f"- 0..3 дня: {timing_buckets['0_3_days']}",
        f"- 4..7 дней: {timing_buckets['4_7_days']}",
        f"- 8..14 дней: {timing_buckets['8_14_days']}",
        f"- 15..30 дней: {timing_buckets['15_30_days']}",
        f"- 31..60 дней: {timing_buckets['31_60_days']}",
        f"- 61+ дней: {timing_buckets['61_plus_days']}",
        f"- Без даты: {timing_buckets['without_date']}",
        "",
        "Локации:",
    ]
    if locations:
        lines.extend(f"- {location}: {count}" for location, count in locations.most_common())
    else:
        lines.append("- нет данных")

    lines.append("")
    lines.append("Доход следующего месяца по локациям (сценарий 70%):")
    if renewal_income_next_month_by_location:
        for location, amount in sorted(renewal_income_next_month_by_location.items(), key=lambda item: item[1], reverse=True):
            due_count = due_next_month_by_location[location]
            lines.append(f"- {location}: {fmt_money(amount)} RUB (истекает {due_count})")
    else:
        lines.append("- нет данных")

    lines.append("")
    lines.append("Топ пользователей по числу подписок:")
    if top_users_by_subscriptions:
        for user_id, count in top_users_by_subscriptions[:20]:
            lines.append(f"- user {user_id}: {count}")
    else:
        lines.append("- нет данных")

    lines.append("")
    lines.append("Заканчиваются в течение 3 дней:")
    if expiring_soon:
        for item in expiring_soon:
            lines.append(
                f"- user {item['user_id']}, sub {item['subscription_id']}, {item['location']}, {item['expires_at']}"
            )
    else:
        lines.append("- нет")

    lines.append("")
    lines.append("Заканчиваются в течение 7 дней:")
    if expiring_within_7_days:
        for item in expiring_within_7_days:
            lines.append(
                f"- user {item['user_id']}, sub {item['subscription_id']}, {item['location']}, {item['expires_at']}"
            )
    else:
        lines.append("- нет")

    lines.append("")
    lines.append("Заканчиваются в течение 30 дней:")
    if expiring_within_30_days:
        for item in expiring_within_30_days[:50]:
            lines.append(
                f"- user {item['user_id']}, sub {item['subscription_id']}, {item['location']}, {item['expires_at']}"
            )
    else:
        lines.append("- нет")

    lines.append("")
    lines.append("Уже истекли:")
    if expired_subscriptions:
        for item in expired_subscriptions:
            lines.append(
                f"- user {item['user_id']}, sub {item['subscription_id']}, {item['location']}, {item['expires_at']}"
            )
    else:
        lines.append("- нет")

    lines.append("")
    lines.append("Пользователи без подписок:")
    if users_without_subscriptions:
        lines.extend(f"- user {user_id}" for user_id in sorted(users_without_subscriptions))
    else:
        lines.append("- нет")

    lines.append("")
    lines.append("Допущения прогноза:")
    lines.append(f"- Цена подписки: {fmt_money(price)} RUB")
    lines.append(f"- Продление в 7 дней: {renew_7:.0%}")
    lines.append(f"- Продление в 30 дней: {renew_30:.0%}")
    lines.append(f"- Возврат истекших: {winback:.0%}")
    lines.append("- Для подписок без даты берется 50% как активные.")

    return "\n".join(lines), stats


def build_detailed_scan_report(records: list[dict]) -> str:
    lines = ["Подробный отчет scan", ""]
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

    location_rows = "".join(
        f"<tr><td>{esc(location)}</td><td>{fmt_int(count)}</td></tr>"
        for location, count in sorted(locations.items(), key=lambda item: item[1], reverse=True)[:5]
    ) or "<tr><td colspan='2'>Нет данных</td></tr>"

    top_user_rows = "".join(
        f"<tr><td>{esc(item.get('user_id', '-'))}</td><td>{fmt_int(item.get('subscriptions', 0))}</td></tr>"
        for item in top_users[:5]
    ) or "<tr><td colspan='2'>Нет данных</td></tr>"

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
        return rows or f"<tr><td colspan='{colspan}'>Нет данных</td></tr>"

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
            f"<tr><td>Истекли</td><td>{fmt_int(timing_buckets.get('expired', 0))}</td></tr>"
            f"<tr><td>0..3 дня</td><td>{fmt_int(timing_buckets.get('0_3_days', 0))}</td></tr>"
            f"<tr><td>4..7 дней</td><td>{fmt_int(timing_buckets.get('4_7_days', 0))}</td></tr>"
            f"<tr><td>8..14 дней</td><td>{fmt_int(timing_buckets.get('8_14_days', 0))}</td></tr>"
            f"<tr><td>15..30 дней</td><td>{fmt_int(timing_buckets.get('15_30_days', 0))}</td></tr>"
            f"<tr><td>31..60 дней</td><td>{fmt_int(timing_buckets.get('31_60_days', 0))}</td></tr>"
            f"<tr><td>61+ дней</td><td>{fmt_int(timing_buckets.get('61_plus_days', 0))}</td></tr>"
            f"<tr><td>Без даты</td><td>{fmt_int(timing_buckets.get('without_date', 0))}</td></tr>"
        )
    )

    due_location_rows = "".join(
        f"<tr><td>{esc(location)}</td><td>{fmt_int(due_count)}</td><td>{fmt_money(float(due_income_by_location.get(location, 0.0)))} ₽</td></tr>"
        for location, due_count in sorted(due_by_location.items(), key=lambda item: item[1], reverse=True)[:5]
    ) or "<tr><td colspan='3'>Нет данных</td></tr>"

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
    theme_bg = sanitize_hex_color(settings.dashboard_theme_bg, "#0b1020")
    theme_panel = sanitize_hex_color(settings.dashboard_theme_panel, "#141a30")
    theme_panel_soft = sanitize_hex_color(settings.dashboard_theme_panel_soft, "#1b2340")
    theme_text = sanitize_hex_color(settings.dashboard_theme_text, "#edf1ff")
    theme_muted = sanitize_hex_color(settings.dashboard_theme_muted, "#aeb9d6")
    theme_primary = sanitize_hex_color(settings.dashboard_theme_primary, "#56d4ff")
    theme_good = sanitize_hex_color(settings.dashboard_theme_good, "#34d399")
    theme_warn = sanitize_hex_color(settings.dashboard_theme_warn, "#f59e0b")
    theme_bad = sanitize_hex_color(settings.dashboard_theme_bad, "#f87171")
    theme_border = sanitize_hex_color(settings.dashboard_theme_border, "#2a3564")
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
      font-family: "Segoe UI", "Inter", Arial, sans-serif;
      background:
        radial-gradient(1200px 500px at 10% -20%, rgba(86,212,255,.16), transparent 60%),
        radial-gradient(1000px 400px at 90% -20%, rgba(52,211,153,.12), transparent 60%),
        var(--bg);
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
      background: linear-gradient(180deg, var(--panel), var(--panel-soft));
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 14px;
      min-height: 96px;
    }}
    .k {{ color: var(--muted); font-size: 13px; margin-bottom: 8px; }}
    .v {{ font-size: 26px; font-weight: 700; letter-spacing: .2px; }}
    .v.good {{ color: var(--good); }}
    .v.warn {{ color: var(--warn); }}
    .v.bad {{ color: var(--bad); }}
    .panel {{
      background: linear-gradient(180deg, var(--panel), var(--panel-soft));
      border: 1px solid var(--border);
      border-radius: 10px;
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
    code {{
      background: #101735;
      border: 1px solid var(--border);
      padding: 1px 6px;
      border-radius: 6px;
      color: #c4d7ff;
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
      <div class="card"><div class="k">Проверено ID</div><div class="v">{pages_total}</div></div>
      <div class="card"><div class="k">Пользователей</div><div class="v">{users_total}</div></div>
      <div class="card"><div class="k">Подписок</div><div class="v">{subscriptions_total}</div></div>
      <div class="card"><div class="k">Истекает в 30 дней</div><div class="v warn">{due_30_count}</div></div>
      <div class="card"><div class="k">Доход next month (70%)</div><div class="v good">{revenue_next_base} ₽</div></div>
      <div class="card"><div class="k">Прибыль 1 месяц (итог)</div><div class="v good">{profit_m1} ₽</div></div>
      <div class="card"><div class="k">Прибыль 6 месяцев (итог)</div><div class="v good">{profit_m6} ₽</div></div>
      <div class="card"><div class="k">Прибыль 12 месяцев (итог)</div><div class="v good">{profit_y1} ₽</div></div>
      <div class="card"><div class="k">Ожидаемые продления</div><div class="v good">{renewals_next_base}</div></div>
      <div class="card"><div class="k">Базовый MRR</div><div class="v">{mrr_estimate} ₽</div></div>
      <div class="card"><div class="k">Возвраты истекших 30 дней</div><div class="v warn">{winback_30} ₽</div></div>
      <div class="card"><div class="k">Риск потери 30 дней</div><div class="v bad">{churn_risk} ₽</div></div>
    </div>

    <div class="panel">
      <h2>Доходность на следующий месяц</h2>
      <table>
        <thead><tr><th>Сценарий</th><th>Ставка продления</th><th>Прогноз выручки</th></tr></thead>
        <tbody>
          <tr><td>Консервативный</td><td>60%</td><td>{revenue_next_low} ₽</td></tr>
          <tr><td>Базовый</td><td>70%</td><td>{revenue_next_base} ₽</td></tr>
          <tr><td>Оптимистичный</td><td>80%</td><td>{revenue_next_high} ₽</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h2>Исторические показатели из кнопки Статистика</h2>
      <table>
        <thead><tr><th>Период</th><th>Пользователи</th><th>Прибыль</th></tr></thead>
        <tbody>
          <tr><td>Месяц</td><td>{fmt_int(int(stats_users_period.get("month", 0)))}</td><td>{stats_profit_month} ₽</td></tr>
          <tr><td>Полгода</td><td>{fmt_int(int(stats_users_period.get("half_year", 0)))}</td><td>{stats_profit_half} ₽</td></tr>
          <tr><td>Год</td><td>{fmt_int(int(stats_users_period.get("year", 0)))}</td><td>{stats_profit_year} ₽</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h2>Прогноз на 6 месяцев (скорость прироста)</h2>
      <table>
        <thead><tr><th>Метрика</th><th>Значение</th></tr></thead>
        <tbody>
          <tr><td>Период наблюдения</td><td>{obs_days} дней (с {obs_start})</td></tr>
          <tr><td>Покрытие дат регистрации (все)</td><td>{reg_cov_all}</td></tr>
          <tr><td>Покрытие дат регистрации (платящие)</td><td>{reg_cov_paid}</td></tr>
          <tr><td>Прирост пользователей</td><td>{growth_users_day:.2f}/день</td></tr>
          <tr><td>Прирост платящих</td><td>{growth_paid_day:.2f}/день</td></tr>
          <tr><td>Прирост подписок</td><td>{growth_subs_day:.2f}/день</td></tr>
          <tr><td>Пользователи через 6м</td><td>{proj_users_6m}</td></tr>
          <tr><td>Платящие через 6м</td><td>{proj_paid_6m}</td></tr>
          <tr><td>Подписки через 6м</td><td>{proj_subs_6m}</td></tr>
          <tr><td>Прогноз MRR через 6м</td><td>{proj_mrr_6m} ₽</td></tr>
        </tbody>
      </table>
      <div class="chart-wrap">
        <div class="legend">Пользователи: сплошная линия — история, пунктир — прогноз</div>
        {users_chart_svg}
      </div>
      <div class="chart-wrap">
        <div class="legend">Подписки: сплошная линия — история, пунктир — прогноз</div>
        {subs_chart_svg}
      </div>
    </div>

    <div class="cols">
      <div class="panel">
        <h2>Локации (топ 5)</h2>
        <table>
          <thead><tr><th>Локация</th><th>Подписок</th></tr></thead>
          <tbody>{location_rows}</tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Топ пользователей (5)</h2>
        <table>
          <thead><tr><th>User ID</th><th>Подписок</th></tr></thead>
          <tbody>{top_user_rows}</tbody>
        </table>
      </div>
    </div>

    <div class="panel">
      <h2>Распределение сроков подписок</h2>
      <table>
        <thead><tr><th>Диапазон</th><th>Кол-во</th></tr></thead>
        <tbody>{timing_rows}</tbody>
      </table>
    </div>

    <div class="panel">
      <h2>Выручка next month по локациям (топ 5, 70%)</h2>
      <table>
        <thead><tr><th>Локация</th><th>Истекает в 30 дней</th><th>Прогноз выручки</th></tr></thead>
        <tbody>{due_location_rows}</tbody>
      </table>
    </div>

    <div class="panel">
      <h2>Критичные истечения (0..3 дня, топ 5)</h2>
      <table>
        <thead><tr><th>User</th><th>Sub</th><th>Локация</th><th>Дата</th><th>Дней до конца</th></tr></thead>
        <tbody>{build_expiration_rows(expiring_3, limit=5)}</tbody>
      </table>
    </div>

    <div class="panel">
      <h2>Истечения 0..30 дней (топ 5)</h2>
      <table>
        <thead><tr><th>User</th><th>Sub</th><th>Локация</th><th>Дата</th><th>Дней до конца</th></tr></thead>
        <tbody>{build_expiration_rows(expiring_30, limit=5)}</tbody>
      </table>
    </div>

    <div class="panel">
      <h2>Уже истекли (топ 5)</h2>
      <table>
        <thead><tr><th>User</th><th>Sub</th><th>Локация</th><th>Дата</th><th>Дней до конца</th></tr></thead>
        <tbody>{build_expiration_rows(expired, limit=5)}</tbody>
      </table>
    </div>

    <div class="panel">
      <h2>Допущения прогноза</h2>
      <div class="assumptions">
        Цена подписки: <code>{price} ₽</code><br>
        Доход next month считается только по подпискам, чей срок истечет в ближайшие 30 дней.<br>
        Продления в 30 дней (база): <code>{renew_30_rate}</code><br>
        Продление в 7 дней: <code>{renew_7_rate}</code><br>
        Возврат истекших: <code>{winback_rate}</code><br>
        Для подписок без даты используется доля активных: <code>50%</code>
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
    checkpoint_text = "нет"
    if checkpoint:
        next_user_id = int(checkpoint.get("next_user_id") or checkpoint.get("page_number") or 1)
        total_users_hint = int(checkpoint.get("total_users_hint") or 0)
        range_text = f"{next_user_id}" if total_users_hint <= 0 else f"{next_user_id}/{total_users_hint}"
        checkpoint_text = (
            f"ID позиция {range_text}, "
            f"пользователей {len(checkpoint.get('records') or [])}, "
            f"сохранен {checkpoint.get('saved_at', '-')}"
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

    running_text = "да" if active_scan_cancel_event and not active_scan_cancel_event.is_set() else "нет"
    lines = [
        "Меню scan",
        f"Активный scan: {running_text}",
        f"Админ-бот: {format_admin_bot_health()}",
        f"Checkpoint: {checkpoint_text}",
        "",
        "Выбери действие (можно цифрой, без команд):",
        "1 — Старт / продолжить scan",
        "2 — Пауза scan",
        "3 — Сброс scan",
        "4 — Результаты scan",
        "5 — Обновить статус",
    ]
    lines = [
        "Меню scan",
        f"Активный scan: {running_text}",
        f"Админ-бот: {format_admin_bot_health()}",
        f"Checkpoint: {checkpoint_text}",
        "",
        "Выбери действие кнопкой или цифрой:",
        "1 - Новый scan с первой страницы",
        "2 - Продолжить сохраненный scan",
        "3 - Stop scan: пауза и текущие результаты",
        "4 - Результаты scan",
        "5 - Сброс сохраненного scan",
        "6 - Обновить статус",
        "",
        "Команды: scan new, scan continue, stop скан, scan results, scan reset.",
    ]
    if recent_reports:
        lines.append("")
        lines.append("Последние отчеты:")
        lines.extend(f"- {name}" for name in recent_reports)
    return "\n".join(lines)


def build_scan_menu_buttons():
    return [
        [Button.text("scan new"), Button.text("scan continue")],
        [Button.text("stop скан"), Button.text("scan results")],
        [Button.text("scan reset"), Button.text("menu")],
    ]


def build_scan_menu_text_fast() -> str:
    checkpoint = load_scan_checkpoint()
    checkpoint_text = "нет"
    if checkpoint:
        next_user_id = int(checkpoint.get("next_user_id") or checkpoint.get("page_number") or 1)
        total_users_hint = int(checkpoint.get("total_users_hint") or 0)
        range_text = f"{next_user_id}" if total_users_hint <= 0 else f"{next_user_id}/{total_users_hint}"
        checkpoint_text = (
            f"позиция ID {range_text}, "
            f"пользователей {len(checkpoint.get('records') or [])}, "
            f"сохранен {checkpoint.get('saved_at', '-')}"
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

    running_text = "да" if active_scan_cancel_event and not active_scan_cancel_event.is_set() else "нет"
    lines = [
        "Меню scan",
        f"Активный scan: {running_text}",
        f"Checkpoint: {checkpoint_text}",
        "",
        "Выбери действие кнопкой или цифрой:",
        "1 - Новый scan с первого ID",
        "2 - Продолжить сохраненный scan",
        "3 - Stop scan: пауза и текущие результаты",
        "4 - Результаты scan",
        "5 - Сброс сохраненного scan",
        "6 - Обновить статус",
        "",
        "Команды: scan new, scan continue, stop скан, scan results, scan reset.",
    ]
    if recent_reports:
        lines.append("")
        lines.append("Последние отчеты:")
        lines.extend(f"- {name}" for name in recent_reports)
    return "\n".join(lines)


def format_scan_checkpoint_text() -> str:
    checkpoint = load_scan_checkpoint()
    if not checkpoint:
        return "нет"
    next_user_id = int(checkpoint.get("next_user_id") or checkpoint.get("page_number") or 1)
    total_users_hint = int(checkpoint.get("total_users_hint") or 0)
    range_text = f"{next_user_id}" if total_users_hint <= 0 else f"{next_user_id}/{total_users_hint}"
    return (
        f"{checkpoint.get('status', 'saved')}, "
        f"позиция ID {range_text}, "
        f"пользователей {len(checkpoint.get('records') or [])}, "
        f"ID проверено {int(checkpoint.get('pages_scanned') or 0)}, "
        f"сохранен {checkpoint.get('saved_at', '-')}"
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
        "Результаты scan",
        f"Активный scan: {'да' if active_scan_cancel_event and not active_scan_cancel_event.is_set() else 'нет'}",
        f"Админ-бот: {format_admin_bot_health()}",
        f"Сохраненный прогресс: {format_scan_checkpoint_text()}",
    ]

    if checkpoint_records:
        summary_text, _ = build_scan_report(
            checkpoint_records,
            int((checkpoint or {}).get("pages_scanned") or 0),
            admin_statistics=dict((checkpoint or {}).get("admin_statistics") or {}),
        )
        lines.extend(("", "Частичный прогресс:", summary_text))

    if txt_path:
        lines.extend(("", "Последний готовый отчет:", f"TXT: {txt_path}"))
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
                            "Ключевой прогноз на следующий месяц:",
                            f"- Подписок с истечением в 30 дней: {int(forecast.get('next_month_due_subscriptions_total', 0))}",
                            f"- Доход (база 70%): {float(forecast.get('next_month_projected_revenue_base_rub', 0.0)):.0f} RUB",
                            f"- Доход (60%): {float(forecast.get('next_month_projected_revenue_low_rub', 0.0)):.0f} RUB",
                            f"- Доход (80%): {float(forecast.get('next_month_projected_revenue_high_rub', 0.0)):.0f} RUB",
                        )
                    )
                    if financial:
                        lines.extend(
                            (
                                "",
                                "Итоговый прогноз прибыли:",
                                f"- Через 1 месяц: ~{float(financial.get('profit_projection_month_1_rub', 0.0)):.0f} RUB",
                                f"- Через 6 месяцев: ~{float(financial.get('profit_projection_month_6_rub', 0.0)):.0f} RUB",
                                f"- Через 12 месяцев: ~{float(financial.get('profit_projection_month_12_rub', 0.0)):.0f} RUB",
                            )
                        )
                    if six_month:
                        lines.extend(
                            (
                                "",
                                "Ключевой прогноз на 6 месяцев:",
                                f"- Пользователи: ~{int(round(float(six_month.get('users_total_projected_6m', 0.0))))}",
                                f"- Платящие: ~{int(round(float(six_month.get('users_with_subscriptions_projected_6m', 0.0))))}",
                                f"- Подписки: ~{int(round(float(six_month.get('subscriptions_total_projected_6m', 0.0))))}",
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
            lines.extend(("", "Краткий просмотр:", preview))
    elif not checkpoint_records:
        lines.extend(("", "Готовых отчетов пока нет."))

    return "\n".join(lines)


async def send_latest_dashboard_to_chat(event) -> bool:
    _, _, _, dashboard_path = latest_scan_report_paths()
    if not dashboard_path:
        return False
    dashboard_url = ensure_dashboard_public_url(dashboard_path, "latest-scan-dashboard.html")
    if not dashboard_url:
        _, dashboard_url = publish_dashboard_file(dashboard_path, latest_name="latest-scan-dashboard.html")
    sent = await safe_event_reply(event, f"Dashboard scan:\n{dashboard_url or dashboard_path}")
    return sent is not None


async def send_latest_dashboard_to_chat_id(chat_id: int) -> bool:
    _, _, _, dashboard_path = latest_scan_report_paths()
    if not dashboard_path:
        return False
    try:
        dashboard_url = ensure_dashboard_public_url(dashboard_path, "latest-scan-dashboard.html")
        if not dashboard_url:
            _, dashboard_url = publish_dashboard_file(dashboard_path, latest_name="latest-scan-dashboard.html")
        await client.send_message(chat_id, f"Dashboard scan:\n{dashboard_url or dashboard_path}")
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
            "SQL база пуста. Сначала запусти `scan new`, чтобы собрать данные.",
        )
        return False
    dashboard_path, stats = built
    summary_text = build_status_summary_from_stats(stats, dashboard_path)
    sent = await safe_event_reply(event, summary_text)
    return sent is not None


async def get_user_subscriptions_info_in_admin_bot(
    user_id: str,
    progress_callback: ProgressCallback | None = None,
) -> str:
    await emit_process_progress(
        progress_callback,
        "Info пользователя",
        INFO_STEPS,
        1,
        user_id=user_id,
        extra_lines=["Ожидаю свободный админ-процесс"],
    )
    async with admin_flow_lock:
        await emit_process_progress(
            progress_callback,
            "Info пользователя",
            INFO_STEPS,
            1,
            user_id=user_id,
            extra_lines=[f"Получаю Telegram entity @{settings.admin_bot_username}"],
        )
        bot = await get_admin_bot_entity()
        logging.info("Starting admin info for user_id=%s in @%s", user_id, settings.admin_bot_username)

        async with admin_conversation(bot) as conv:
            result_message = await open_user_in_admin_bot(
                conv,
                bot,
                user_id,
                progress_callback=progress_callback,
                progress_title="Info пользователя",
                progress_steps=INFO_STEPS,
            )
            await emit_process_progress(
                progress_callback,
                "Info пользователя",
                INFO_STEPS,
                4,
                user_id=user_id,
                extra_lines=[f"Кнопка подписок: {settings.subscriptions_button_text}"],
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
                "Info пользователя",
                INFO_STEPS,
                5,
                user_id=user_id,
                extra_lines=[f"Найдено подписок для чтения: {len(subscription_buttons)}"],
            )

            current_menu = subscriptions_message
            for index, subscription in enumerate(subscription_buttons, start=1):
                await emit_process_progress(
                    progress_callback,
                    "Info пользователя",
                    INFO_STEPS,
                    5,
                    user_id=user_id,
                    extra_lines=[
                        f"Подписка {index}/{len(subscription_buttons)}",
                        f"Кнопка: {subscription['text']}",
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
            "Info пользователя",
            INFO_STEPS,
            6,
            user_id=user_id,
            extra_lines=[f"Прочитано подписок: {len(details)}", "Собираю HTML-ответ"],
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

    await emit_collect_progress(f"Открываю поиск и запрашиваю ID {user_id}.")
    find_message = await click_and_read(bot, users_page_message, settings.find_user_button_text)
    previous_snapshot = message_snapshot(find_message)
    await send_conv_message_with_retry(bot, user_id)
    result_message = await wait_bot_update(bot, previous_snapshot)
    log_message(f"Search result for user_id={user_id}", result_message)

    if not has_button_text(result_message, settings.subscriptions_button_text):
        await emit_collect_progress(f"ID {user_id}: карточка не найдена или без доступа.")
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

    await emit_collect_progress("Карточка найдена. Читаю подписки.")
    subscriptions_message = await click_and_read(
        bot,
        result_message,
        settings.subscriptions_button_text,
    )
    subscriptions = []
    current_subscription_menu = subscriptions_message
    subscription_buttons = extract_subscription_buttons(subscriptions_message)
    await emit_collect_progress(f"Найдено подписок: {len(subscription_buttons)}.")
    for subscription_index, subscription_button in enumerate(subscription_buttons, start=1):
        await emit_collect_progress(
            f"Подписка {subscription_index}/{len(subscription_buttons)}: {subscription_button['text']}."
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

    await emit_collect_progress(f"Открываю карточку пользователя ID {user_id}.")
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
    await emit_collect_progress(f"Найдено подписок: {len(subscription_buttons)}. Читаю детали.")
    for subscription_index, subscription_button in enumerate(subscription_buttons, start=1):
        await emit_collect_progress(
            f"Подписка {subscription_index}/{len(subscription_buttons)}: {subscription_button['text']}."
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
    await emit_collect_progress("Возвращаюсь к списку пользователей.")
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
        await progress_callback("Ожидаю свободный админ-процесс для scan.")
    async with admin_flow_lock:
        if cancel_event and cancel_event.is_set():
            if active_scan_reset_requested:
                clear_scan_checkpoint()
                active_scan_reset_requested = False
                return "Scan сброшен. Сохраненный прогресс очищен."
            return "Scan на паузе. Новых действий не выполнено."

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
                    "Найден сохраненный прогресс scan по ID. "
                    f"Продолжаю с ID {start_user_id}, уже собрано пользователей: {len(records)}."
                ),
                force=True,
            )
        else:
            await emit_progress("Сканирование по ID запущено с чистого состояния.", force=True)

        while current_user_id <= (total_users or current_user_id):
            if cancel_event and cancel_event.is_set():
                reset_requested = active_scan_reset_requested
                paused = not reset_requested
                break

            try:
                async with admin_conversation(bot) as conv:
                    if not total_users:
                        await emit_progress("Открываю /admin статистику и считываю общее число пользователей.", force=True)
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
                                f"Не удалось обновить статистику, продолжаю по checkpoint total={total_users}.",
                                force=True,
                            )
                        await emit_progress(f"Всего пользователей по статистике: {total_users}.", force=True)
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
                                f"Сканирование по ID: {current_user_id}/{total_users}. "
                                f"Собрано записей: {len(records)}."
                            ),
                        )

                        if user_id in seen_users:
                            current_user_id += 1
                            continue

                        async def emit_user_progress(text: str) -> None:
                            await emit_progress(text)

                        try:
                            record, users_page_message = await collect_user_record_via_search(
                                conv,
                                bot,
                                users_page_message,
                                user_id,
                                progress_callback=emit_user_progress,
                                progress_context=f"ID {user_id}",
                            )
                        except Exception as error:
                            logging.exception("Failed to collect user_id=%s via search; resetting users page", user_id)
                            consecutive_failures += 1
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
                                    f"ID {user_id}: ошибка, пробую восстановиться. "
                                    f"Подряд ошибок: {consecutive_failures}/{SCAN_MAX_CONSECUTIVE_FAILURES}."
                                ),
                                force=True,
                            )
                            if consecutive_failures >= SCAN_MAX_CONSECUTIVE_FAILURES:
                                logging.warning(
                                    "Restarting admin conversation after %s consecutive failures at user_id=%s",
                                    consecutive_failures,
                                    user_id,
                                )
                                set_admin_bot_health("[WAIT]", "перезапуск", "много ошибок подряд")
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
                                set_admin_bot_health("[WAIT]", "перезапуск", "страница не восстановилась")
                                await asyncio.sleep(SCAN_SESSION_RESTART_DELAY_SECONDS)
                                break
                            current_user_id += 1
                            continue

                        if record:
                            records.append(record)
                            seen_users.add(user_id)
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
                        f"Сессия scan зависла/сломалась на ID {current_user_id}. "
                        f"Перезапуск {session_restarts}/{SCAN_MAX_SESSION_RESTARTS}."
                    ),
                    force=True,
                )
                set_admin_bot_health("[WAIT]", "перезапуск", f"scan session {session_restarts}")
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
                f"Перезапускаю scan-сессию и продолжаю с ID {current_user_id}.",
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
            await emit_progress("Scan сброшен. Сохраненный прогресс очищен.", force=True)
            return "Scan сброшен. Сохраненный прогресс очищен."

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
                    f"Scan на паузе: проверено ID {checked_ids_total}, "
                    f"обработано пользователей {len(records)}, следующая позиция ID {next_user_id}."
                ),
                force=True,
            )
        else:
            clear_scan_checkpoint()
            await emit_progress(
                f"Scan завершен: проверено ID {checked_ids_total}, пользователей собрано {len(records)}.",
                force=True,
            )
        return "\n".join(
            (
                "Scan на паузе. Частичный отчет сохранен:" if paused else "Scan завершен.",
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
            f"[SCAN] Активный scan временно ставлю на паузу для команды `{command_name}`.\n"
            "Завершу текущего пользователя, выполню команду и продолжу scan автоматически."
        ),
    )
    return interruption


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
            0.08,
            min(settings.scan_action_delay_seconds, settings.scan_turbo_delay_seconds),
        )
        active_scan_action_delay_seconds = active_scan_base_delay_seconds

        progress_interval_seconds = max(0.25, env_float("SCAN_PROGRESS_INTERVAL_SECONDS", 0.5))
        progress_message = await client.send_message(
            chat_id,
            build_scan_status(
                f"Продолжаю scan после команды `{command_name}`.",
                checkpoint_text=format_scan_checkpoint_text(),
            ),
            buttons=[[Button.inline("Пауза scan", data=SCAN_CANCEL_CALLBACK_DATA)]],
        )

        async def update_auto_scan_progress(
            text: str,
            *,
            done: bool = False,
            failed: bool = False,
            paused: bool = False,
        ) -> None:
            buttons = None if done or failed or paused else [[Button.inline("Пауза scan", data=SCAN_CANCEL_CALLBACK_DATA)]]
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
        if "на паузе" in result.casefold():
            await update_auto_scan_progress("Scan снова на паузе. Прогресс сохранен.", paused=True)
        elif "сброшен" in result.casefold():
            await update_auto_scan_progress("Scan сброшен. Сохраненный прогресс очищен.", done=True)
        else:
            await update_auto_scan_progress("Scan завершен. Итоговый отчет готов.", done=True)
        await client.send_message(chat_id, result)
        await send_latest_dashboard_to_chat_id(chat_id)
    except Exception:
        logging.exception("Scan auto-resume failed after priority command=%s", command_name)
        try:
            await client.send_message(
                chat_id,
                "Не удалось автоматически продолжить scan. Отправь `scan continue`, чтобы продолжить вручную.",
            )
        except Exception:
            logging.exception("Failed to notify chat about scan auto-resume failure")
    finally:
        active_scan_cancel_event = None
        active_scan_owner_id = None
        active_scan_reset_requested = False
        active_scan_action_delay_seconds = settings.scan_action_delay_seconds
        active_scan_base_delay_seconds = settings.scan_action_delay_seconds


async def send_mail_to_user_in_admin_bot(
    user_id: str,
    message_text: str,
    progress_callback: ProgressCallback | None = None,
) -> str:
    await emit_process_progress(
        progress_callback,
        "Mail пользователю",
        MAIL_STEPS,
        1,
        user_id=user_id,
        extra_lines=["Ожидаю свободный админ-процесс"],
    )
    async with admin_flow_lock:
        await emit_process_progress(
            progress_callback,
            "Mail пользователю",
            MAIL_STEPS,
            1,
            user_id=user_id,
            extra_lines=[f"Получаю Telegram entity @{settings.admin_bot_username}"],
        )
        bot = await get_admin_bot_entity()
        logging.info("Starting admin mail for user_id=%s in @%s", user_id, settings.admin_bot_username)

        async with admin_conversation(bot) as conv:
            result_message = await open_user_in_admin_bot(
                conv,
                bot,
                user_id,
                progress_callback=progress_callback,
                progress_title="Mail пользователю",
                progress_steps=MAIL_STEPS,
            )
            await emit_process_progress(
                progress_callback,
                "Mail пользователю",
                MAIL_STEPS,
                4,
                user_id=user_id,
                extra_lines=[f"Кнопка письма: {settings.write_user_button_text}"],
            )
            write_message = await click_and_read(bot, result_message, settings.write_user_button_text)

            await emit_process_progress(
                progress_callback,
                "Mail пользователю",
                MAIL_STEPS,
                5,
                user_id=user_id,
                extra_lines=[
                    f"Длина текста: {len(message_text)} символов",
                    f"Предпросмотр: {message_text[:120]}",
                ],
            )
            logging.info("Sending mail text to admin bot for user_id=%s text=%r", user_id, message_text)
            previous_snapshot = message_snapshot(write_message)
            await send_conv_message_with_retry(bot, message_text)
            preview_message = await wait_bot_update(bot, previous_snapshot)
            log_message("Mail sent response", preview_message)

            await emit_process_progress(
                progress_callback,
                "Mail пользователю",
                MAIL_STEPS,
                6,
                user_id=user_id,
                extra_lines=[f"Кнопка подтверждения: {settings.mail_next_button_text}"],
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
    }
    await update_status(
        build_process_status(
            "Wizard",
            WIZARD_STEPS,
            6,
            user_id=user_id,
            target=wizard_target,
            extra_lines=[
                "Карточка подготовлена",
                "Проверь текст перед отправкой",
                "Ответь: 1 - отправить, 2 - дописать, 0 - отмена",
            ],
        )
    )
    await safe_event_reply(event, f"Предпросмотр wizard:\n\n{base_text}")
    await safe_event_reply(
        event,
        "Отправлять в wizard?",
        buttons=[
            [Button.text("1 отправить"), Button.text("2 дописать")],
            [Button.text("0 отмена")],
        ],
    )


async def handle_roots_command(event, sender) -> None:
    sender_id = int(event.sender_id or 0)
    sender_user = sender_username(sender)
    text = (event.raw_text or "").strip()
    parts = [part for part in text.split() if part]

    if len(parts) == 1 or (len(parts) > 1 and parts[1].casefold() in {"list", "show", "список"}):
        await safe_event_reply(event, build_roots_text(), buttons=build_roots_buttons())
        return

    action = parts[1].casefold()
    if action in {"help", "помощь"}:
        await safe_event_reply(event, build_roots_text(), buttons=build_roots_buttons())
        return

    if action in {"add", "добавить"}:
        if len(parts) < 3:
            await safe_event_reply(event, "Формат: /roots add <user_id|@username|me> [комментарий]")
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
            await safe_event_reply(event, f"Не смог добавить запросника: {error}")
            return
        await safe_event_reply(event, f"Запросник добавлен: {lookup_key}\n\n{build_roots_text()}")
        return

    if action in {"del", "delete", "remove", "rm", "удалить"}:
        if len(parts) < 3:
            await safe_event_reply(event, "Формат: /roots del <user_id|@username>")
            return
        target = parts[2].strip()
        if target.casefold() == "me":
            target = str(sender_id)
        removed = delete_requester(target)
        await safe_event_reply(
            event,
            ("Запросник удален." if removed else "Такого запросника не нашел.") + f"\n\n{build_roots_text()}",
        )
        return

    if action in {"clear", "очистить"}:
        if len(parts) < 3 or parts[2].casefold() not in {"yes", "confirm", "да"}:
            await safe_event_reply(event, "Чтобы очистить весь список запросников, отправь: /roots clear yes")
            return
        with connect_database() as conn:
            initialize_database(conn)
            conn.execute("DELETE FROM requesters")
            conn.commit()
        await safe_event_reply(event, "Список запросников очищен. Чтобы снова добавить себя: /roots add me")
        return

    await safe_event_reply(event, "Не понял команду /roots. Отправь /roots, чтобы посмотреть список и подсказки.")


@client.on(events.CallbackQuery(data=SCAN_CANCEL_CALLBACK_DATA))
async def handle_scan_cancel(event: events.CallbackQuery.Event) -> None:
    if not active_scan_cancel_event:
        await event.answer("Scan сейчас не выполняется.", alert=False)
        return

    if active_scan_owner_id is not None and event.sender_id != active_scan_owner_id:
        await event.answer("Поставить scan на паузу может только тот, кто его запустил.", alert=True)
        return

    active_scan_cancel_event.set()
    await event.answer("Пауза принята. Завершу текущего пользователя и сохраню прогресс.", alert=False)


@client.on(events.NewMessage)
async def handle_private_message(event: events.NewMessage.Event) -> None:
    global active_scan_cancel_event, active_scan_owner_id, active_scan_menu_owner_id, active_scan_action_delay_seconds, active_scan_base_delay_seconds, active_scan_reset_requested

    if not event.is_private:
        return

    if event.out:
        return

    admin_bot = await get_admin_bot_entity()
    if event.chat_id == getattr(admin_bot, "id", None):
        return

    sender = await event.get_sender()
    if not event.out and getattr(sender, "bot", False):
        return
    sender_id = int(event.sender_id or 0)
    incoming_text = (event.raw_text or "").strip()
    roots_command = is_roots_command(incoming_text)
    roots_empty = requester_count() == 0
    if roots_command and (roots_empty or is_requester_allowed(sender_id, sender)):
        await handle_roots_command(event, sender)
        return

    if not is_requester_allowed(sender_id, sender):
        await safe_event_reply(event, REQUESTER_DENY_MESSAGE)
        logging.info(
            "Rejected private message from non-requester sender_id=%s username=%s text=%r",
            sender_id,
            sender_username(sender),
            incoming_text,
        )
        return

    pending_wizard = pending_wizard_requests.get(sender_id)
    if pending_wizard:
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
                        extra_lines=["Отправка отменена пользователем"],
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
                            extra_lines=["Отправляю подготовленную карточку без дополнения"],
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
                            extra_lines=["Карточка отправлена"],
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
                            extra_lines=["Не удалось отправить карточку", "Подробности записаны в лог"],
                            failed=True,
                        )
                    )
                    await safe_event_reply(event, "Не удалось отправить в wizard. Подробности в логе.")
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
                            "Ожидаю дополнительный текст",
                            "Следующее сообщение будет добавлено к карточке",
                            "Для отмены отправьте 0",
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
                    extra_lines=["Не понял ответ. Напишите 1, 2 или 0"],
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
                        extra_lines=["Отправка отменена пользователем"],
                        done=True,
                    )
                )
                return

            extra_text = incoming_text
            full_text = "\n\n".join(
                (
                    str(pending_wizard["base_text"]),
                    f"Дополнение:\n{extra_text}",
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
                        "Дополнение добавлено",
                        "Проверь итоговый текст",
                        "Ответь: 1 - отправить, 2 - изменить дописку, 0 - отмена",
                    ],
                )
            )
            await safe_event_reply(event, f"Итоговый предпросмотр wizard:\n\n{full_text}")
            await safe_event_reply(
                event,
                "Отправлять этот вариант?",
                buttons=[
                    [Button.text("1 отправить"), Button.text("2 изменить дописку")],
                    [Button.text("0 отмена")],
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
                        extra_lines=["Отправка отменена пользователем"],
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
                            "Ожидаю новый дополнительный текст",
                            "Следующее сообщение заменит прошлую дописку",
                            "Для отмены отправьте 0",
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
                        extra_lines=["Не понял ответ. Напишите 1, 2 или 0"],
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
                            "Подтверждение получено",
                            f"Длина итогового текста: {len(str(pending_wizard.get('final_text') or ''))} символов",
                            "Отправляю в wizard",
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
                        extra_lines=["Карточка отправлена после подтверждения"],
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
                        extra_lines=["Не удалось отправить карточку с дополнением", "Подробности записаны в лог"],
                        failed=True,
                    )
                )
                await safe_event_reply(event, "Не удалось отправить в wizard. Подробности в логе.")
            return

    if is_command_menu_command(event.raw_text or ""):
        await safe_event_reply(event, build_command_menu_text(), buttons=build_command_menu_buttons())
        return

    if is_version_command(event.raw_text or ""):
        await safe_event_reply(event, build_runtime_version_text())
        return

    if is_status_command(event.raw_text or ""):
        await safe_event_reply(event, "[STATUS] Собираю dashboard из SQL базы...")
        await send_status_dashboard_from_database(event)
        return

    scan_menu_action = parse_scan_menu_action(
        event.raw_text or "",
        allow_numeric=active_scan_menu_owner_id == event.sender_id,
    )
    if scan_menu_action == "menu":
        active_scan_menu_owner_id = event.sender_id
        await safe_event_reply(event, build_scan_menu_text_fast(), buttons=build_scan_menu_buttons())
        return

    if scan_menu_action == "results":
        active_scan_menu_owner_id = event.sender_id
        await safe_event_reply(event, build_scan_results_text())
        await send_latest_dashboard_to_chat(event)
        return

    if scan_menu_action in {"pause", "pause_results"}:
        if active_scan_cancel_event and not active_scan_cancel_event.is_set():
            if active_scan_owner_id is not None and event.sender_id != active_scan_owner_id:
                await safe_event_reply(event, "Поставить scan на паузу может только тот, кто его запустил.")
                return
            active_scan_cancel_event.set()
            reply_text = "Пауза scan принята. Завершу текущего пользователя и сохраню прогресс."
            if scan_menu_action == "pause_results":
                reply_text = f"{reply_text}\n\n{build_scan_results_text()}"
            await safe_event_reply(event, reply_text)
        else:
            if scan_menu_action == "pause_results":
                await safe_event_reply(event, build_scan_results_text())
                await send_latest_dashboard_to_chat(event)
            else:
                await safe_event_reply(event, "Scan сейчас не выполняется. Для выбора отправь `scan`.")
        return

    if scan_menu_action == "reset":
        if active_scan_cancel_event and not active_scan_cancel_event.is_set():
            if active_scan_owner_id is not None and event.sender_id != active_scan_owner_id:
                await safe_event_reply(event, "Сбросить активный scan может только тот, кто его запустил.")
                return
            active_scan_reset_requested = True
            active_scan_cancel_event.set()
            clear_scan_checkpoint()
            reset_scan_database()
            await safe_event_reply(event, "Сброс scan принят. Останавливаю текущий обход и очищаю сохраненный прогресс и SQL базу.")
        else:
            clear_scan_checkpoint()
            reset_scan_database()
            await safe_event_reply(event, "Сохраненный прогресс scan и SQL база очищены. Старые готовые отчеты оставлены.")
        return

    mail_command = parse_mail_command(event.raw_text or "")
    if mail_command:
        user_id, message_text = mail_command
        logging.info("Received mail command user_id=%s from chat_id=%s sender_id=%s", user_id, event.chat_id, event.sender_id)
        status_message = await safe_event_reply(
            event,
            build_process_status(
                "Mail пользователю",
                MAIL_STEPS,
                1,
                user_id=user_id,
                extra_lines=[f"Админ-бот: @{settings.admin_bot_username}", f"Текст: {len(message_text)} символов"],
            )
        )

        async def update_mail_status(text: str) -> None:
            await edit_status_message(status_message, text)

        scan_interruption = await request_scan_pause_for_priority_command(event, f"mail {user_id}")
        try:
            result = await send_mail_to_user_in_admin_bot(
                user_id,
                message_text,
                progress_callback=update_mail_status,
            )
            await update_mail_status(
                build_process_status(
                    "Mail пользователю",
                    MAIL_STEPS,
                    len(MAIL_STEPS),
                    user_id=user_id,
                    extra_lines=["Письмо отправлено через админ-бот", "Итог отправлен отдельным сообщением"],
                    done=True,
                )
            )
            await safe_event_reply(event, result)
        except Exception:
            logging.exception("Admin mail failed for user_id=%s", user_id)
            await update_mail_status(
                build_process_status(
                    "Mail пользователю",
                    MAIL_STEPS,
                    len(MAIL_STEPS),
                    user_id=user_id,
                    extra_lines=["Не удалось завершить отправку", "Подробности записаны в лог"],
                    failed=True,
                )
            )
            await safe_event_reply(event, "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c mail. \u041f\u043e\u0434\u0440\u043e\u0431\u043d\u043e\u0441\u0442\u0438 \u0437\u0430\u043f\u0438\u0441\u0430\u043d\u044b \u0432 \u043b\u043e\u0433.")
        finally:
            schedule_scan_auto_resume(scan_interruption)
        return

    if is_help_overview_command(event.raw_text or ""):
        await safe_event_reply(
            event,
            "\n".join(
                (
                    "Доступные команды:",
                    "/help — показать список всех команд",
                    "/version — показать версию, commit и дату запуска",
                    "help <user_id|username> — найти пользователя в админ-боте",
                    "help <user_id|username> -b — взять пользователя из SQLite базы",
                    "wizard <user_id> — найти и отправить карточку в @wizardvpn_manager (с подтверждением)",
                    "info <user_id|username> — получить подробную информацию и подписки",
                    "info <user_id|username> -b — получить подробную информацию из SQLite базы",
                    "mail <user_id> <текст> — отправить сообщение пользователю",
                    "mail <user_id> — отправить сообщение по умолчанию (MAIL_TEXT)",
                    "/roots — список запросников",
                    "/roots add <user_id|@username|me> — добавить запросника",
                    "/roots del <user_id|@username> — удалить запросника",
                    "scan или scan start — старт / продолжение scan",
                    "scan pause или /stopscan — поставить scan на паузу",
                    "scan reset — сбросить сохраненный прогресс scan",
                    "scan results — показать результаты scan",
                    "/scanmenu — открыть меню scan",
                )
            )
        )
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
                extra_lines=[f"Админ-бот: @{settings.admin_bot_username}"],
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
                    extra_lines=["Не удалось подготовить карточку", "Подробности записаны в лог"],
                    failed=True,
                )
            )
            await safe_event_reply(event, "Не удалось подготовить сообщение для wizard. Подробности в логе.")
        finally:
            schedule_scan_auto_resume(scan_interruption)
        return

    scan_action = scan_menu_action or parse_scan_command(event.raw_text or "")
    if scan_action in {"new", "continue"}:
        active_scan_menu_owner_id = event.sender_id
        logging.info(
            "Received scan command action=%s from chat_id=%s sender_id=%s",
            scan_action,
            event.chat_id,
            event.sender_id,
        )
        if active_scan_cancel_event and not active_scan_cancel_event.is_set():
            await safe_event_reply(event, "Scan уже выполняется. Можно поставить на паузу: `scan pause`.")
            return

        if scan_action == "new":
            clear_scan_checkpoint()
            reset_scan_database()
        start_text = (
            "Запускаю новый scan с первой страницы."
            if scan_action == "new"
            else "Продолжаю scan с сохраненного места. Если checkpoint пустой, начну с первой страницы."
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
                f"{start_text} Готовлю админ-бот к обходу пользователей.",
                checkpoint_text=format_scan_checkpoint_text(),
            ),
            buttons=[[Button.inline("Пауза scan", data=SCAN_CANCEL_CALLBACK_DATA)]],
        )

        async def update_scan_progress(
            text: str,
            *,
            done: bool = False,
            failed: bool = False,
            paused: bool = False,
        ) -> None:
            buttons = None if done or failed or paused else [[Button.inline("Пауза scan", data=SCAN_CANCEL_CALLBACK_DATA)]]
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
            if "на паузе" in result.casefold():
                await update_scan_progress("Scan на паузе. Прогресс сохранен, частичный отчет отправлен ниже.", paused=True)
            elif "сброшен" in result.casefold():
                await update_scan_progress("Scan сброшен. Сохраненный прогресс очищен.", done=True)
            else:
                await update_scan_progress("Scan завершен. Итоговый отчет готов и отправлен ниже.", done=True)
            await safe_event_reply(event, result)
            await send_latest_dashboard_to_chat(event)
        except Exception:
            logging.exception("Admin scan failed")
            await update_scan_progress("Scan завершился с ошибкой. Подробности записаны в лог.", failed=True)
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
                "Info пользователя",
                INFO_STEPS,
                1,
                user_id=user_id,
                extra_lines=[f"Админ-бот: @{settings.admin_bot_username}"],
            )
        )

        async def update_info_status(text: str) -> None:
            await edit_status_message(status_message, text)

        scan_interruption = None
        try:
            if info_lookup.use_database:
                await update_info_status(
                    build_process_status(
                        "Info пользователя",
                        INFO_STEPS,
                        len(INFO_STEPS),
                        user_id=user_id,
                        extra_lines=["Читаю SQLite базу", "Админ-бот не трогаю"],
                        done=True,
                    )
                )
                record = load_latest_record_by_lookup_from_database(user_id)
                if not record:
                    await safe_event_reply(
                        event,
                        "В базе нет такого пользователя. Запусти `scan new` или попробуй без `-b`, чтобы искать через админ-бота.",
                    )
                    return
                result = format_subscription_info_from_record_html(record)
            else:
                scan_interruption = await request_scan_pause_for_priority_command(event, f"info {user_id}")
                result = await get_user_subscriptions_info_in_admin_bot(
                    user_id,
                    progress_callback=update_info_status,
                )
            await update_info_status(
                build_process_status(
                    "Info пользователя",
                    INFO_STEPS,
                    len(INFO_STEPS),
                    user_id=user_id,
                    extra_lines=["Полный отчет собран", "Итог отправлен отдельным сообщением"],
                    done=True,
                )
            )
            await safe_event_reply(event, result, parse_mode="html")
        except Exception:
            logging.exception("Info failed for query=%s database=%s", user_id, info_lookup.use_database)
            await update_info_status(
                build_process_status(
                    "Info пользователя",
                    INFO_STEPS,
                    len(INFO_STEPS),
                    user_id=user_id,
                    extra_lines=["Не удалось получить полный отчет", "Подробности записаны в лог"],
                    failed=True,
                )
            )
            await safe_event_reply(event, "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043f\u043e\u043b\u0443\u0447\u0438\u0442\u044c info. \u041f\u043e\u0434\u0440\u043e\u0431\u043d\u043e\u0441\u0442\u0438 \u0437\u0430\u043f\u0438\u0441\u0430\u043d\u044b \u0432 \u043b\u043e\u0433.")
        finally:
            schedule_scan_auto_resume(scan_interruption)
        return

    help_lookup = parse_help_command(event.raw_text or "")
    if help_lookup:
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
                "Поиск пользователя",
                SEARCH_STEPS,
                1,
                user_id=user_id,
                extra_lines=[f"Админ-бот: @{settings.admin_bot_username}"],
            )
        )

        async def update_help_status(text: str) -> None:
            await edit_status_message(status_message, text)

        scan_interruption = None
        try:
            if help_lookup.use_database:
                await update_help_status(
                    build_process_status(
                        "Поиск пользователя",
                        SEARCH_STEPS,
                        len(SEARCH_STEPS),
                        user_id=user_id,
                        extra_lines=["Читаю SQLite базу", "Админ-бот не трогаю"],
                        done=True,
                    )
                )
                record = load_latest_record_by_lookup_from_database(user_id)
                if not record:
                    await safe_event_reply(
                        event,
                        "В базе нет такого пользователя. Запусти `scan new` или попробуй без `-b`, чтобы искать через админ-бота.",
                    )
                    return
                result = format_user_summary_from_record(record)
            else:
                scan_interruption = await request_scan_pause_for_priority_command(event, f"help {user_id}")
                result = await find_user_in_admin_bot(
                    user_id,
                    progress_callback=update_help_status,
                )
            await update_help_status(
                build_process_status(
                    "Поиск пользователя",
                    SEARCH_STEPS,
                    len(SEARCH_STEPS),
                    user_id=user_id,
                    extra_lines=["Короткая карточка готова", "Итог отправлен отдельным сообщением"],
                    done=True,
                )
            )
            await safe_event_reply(event, result)
        except Exception:
            logging.exception("Help search failed for query=%s database=%s", user_id, help_lookup.use_database)
            await update_help_status(
                build_process_status(
                    "Поиск пользователя",
                    SEARCH_STEPS,
                    len(SEARCH_STEPS),
                    user_id=user_id,
                    extra_lines=["Не удалось найти пользователя", "Подробности записаны в лог"],
                    failed=True,
                )
            )
            await safe_event_reply(event, "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043d\u0430\u0439\u0442\u0438 \u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u0435\u043b\u044f. \u041f\u043e\u0434\u0440\u043e\u0431\u043d\u043e\u0441\u0442\u0438 \u0437\u0430\u043f\u0438\u0441\u0430\u043d\u044b \u0432 \u043b\u043e\u0433.")
        finally:
            schedule_scan_auto_resume(scan_interruption)
        return

    if (event.raw_text or "").strip():
        lowered_text = (event.raw_text or "").casefold()
        if "scan" in lowered_text or "скан" in lowered_text:
            active_scan_menu_owner_id = event.sender_id
            await safe_event_reply(event, build_scan_menu_text_fast(), buttons=build_scan_menu_buttons())
            return
        await safe_event_reply(event, "\u0422\u0430\u043a\u043e\u0439 \u043a\u043e\u043c\u0430\u043d\u0434\u044b \u043d\u0435\u0442 \u0432 \u0441\u043f\u0438\u0441\u043a\u0435 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u0438\u0432\u0430\u0435\u043c\u044b\u0445.")


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
    logging.info("Send help <user_id> in private chat to run admin search.")
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
