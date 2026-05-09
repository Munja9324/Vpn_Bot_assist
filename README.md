# Vpn_Bot_assist

`Vpn_Bot_assist` - Telegram userbot на Telethon для работы с админ-панелью VPN-бота через личный Telegram-аккаунт.

Разработчик: `DevM29`

## Что умеет

- искать пользователей по ID и username;
- открывать краткую карточку пользователя и подробную информацию по подпискам;
- брать данные либо напрямую из админ-бота, либо из локальной SQLite-базы;
- сканировать базу пользователей и обновлять локальную БД;
- строить dashboard и live admin site;
- отправлять сообщения, промокоды и карточки в wizard;
- принимать голос, распознавать его и запускать безопасные сценарии;
- работать как KBR_GPT-помощник;
- сохранять все неразобранные случаи в `unresolved_requests`.

## Быстрый старт

1. Получи `API_ID` и `API_HASH` на [my.telegram.org](https://my.telegram.org).
2. Установи Python 3.10+.
3. Установи зависимости:

```powershell
pip install -r requirements.txt
```

4. Скопируй `.env.example` в `.env` и заполни настройки.
5. Запусти:

```powershell
python vpn_kbr.py
```

6. При первом запуске введи номер Telegram, код входа и пароль 2FA, если он включен.
7. После запуска добавь себя в список разрешенных аккаунтов:

```text
/roots add me
```

## Основные команды

```text
menu
/dashboard
/adminsite
/diag
/processes
/unresolved
/version
/gpt
/gpt reset
/user <id|username>
/user <id|username> -b
/subs <id|username>
/subs <id|username> -b
/wizard <id>
/send <id> <текст>
/broadcast <текст>
/coupon <id>
scan
scan new
scan continue
scan results
scan reset
stop scan
/roots
```

Старые алиасы вроде `help`, `info`, `mail`, `mail2`, `promo`, `status`, `poc`, `logs` тоже поддерживаются.

## Важные файлы

- [vpn_kbr.py](C:/Project/vpn_kbr.py) - точка входа
- [kbrbot/app.py](C:/Project/kbrbot/app.py) - основная логика
- [docs/RU_FULL_GUIDE.md](C:/Project/docs/RU_FULL_GUIDE.md) - полная инструкция
- `scan-data.sqlite3` - локальная база проекта
- `userbot.log` - рабочий лог
- `.env` - секреты и конфиг
- `*.session` - Telethon-сессия аккаунта

## Безопасность

Никогда не выкладывай в GitHub:

- `.env`
- `.env.txt`
- `*.session`
- `*.sqlite3`
- `reports/`
- `*.log`
- приватные токены, ключи и пароли

Полная инструкция по установке, настройке, GitHub и серверу лежит в [docs/RU_FULL_GUIDE.md](C:/Project/docs/RU_FULL_GUIDE.md).
