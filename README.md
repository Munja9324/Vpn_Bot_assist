# Vpn_Bot_assist

Телеграм userbot для администрирования VPN-проекта через личный Telegram-аккаунт (Telethon).

Разработчик: `DevM29`

## Быстрый старт

1. Получите `API_ID` и `API_HASH` на [my.telegram.org](https://my.telegram.org).
2. Установите зависимости:

```powershell
pip install -r requirements.txt
```

3. Создайте `.env`:

```powershell
Copy-Item .env.example .env
```

4. Заполните минимум:
- `API_ID`
- `API_HASH`
- `SESSION_NAME`
- `ADMIN_BOT_USERNAME`
- `WIZARD_TARGET_USERNAME`

5. Запуск:

```powershell
python vpn_kbr.py
```

6. После первого входа добавьте себя в запросники:

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
/version
/tail
/unresolved

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
/roots add <id|@username|me>
/roots del <id|@username>
```

## Режим теста обычного пользователя

Если вы запросник, можно проверить пользовательский сценарий так:

```text
-p не работает впн
```

Любой текст после `-p` будет обработан как сообщение обычного пользователя.

## Важные файлы

- `vpn_kbr.py` — точка входа
- `kbrbot/app.py` — основная логика
- `docs/RU_FULL_GUIDE.md` — полная инструкция
- `.env` — конфиг и секреты
- `scan-data.sqlite3` — локальная база
- `userbot.log` — лог
- `*.session` — Telegram-сессия Telethon

## Безопасность

Никогда не коммитьте в Git:
- `.env`
- `*.session`
- `*.sqlite3`
- `*.log`
- `reports/`
- любые ключи и токены
