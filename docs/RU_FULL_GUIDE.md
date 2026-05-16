# Полная инструкция по Vpn_Bot_assist

Документ актуален для проекта `Vpn_Bot_assist`.

## 1) Что это

`Vpn_Bot_assist` — это userbot на Telethon.  
Он работает от имени вашего Telegram-аккаунта и управляет админ-логикой VPN-бота.

## 2) Требования

- Python 3.10+
- Git
- доступ к Telegram-аккаунту
- `API_ID` и `API_HASH` с [my.telegram.org](https://my.telegram.org)

Проверка:

```powershell
python --version
git --version
```

## 3) Получение API_ID и API_HASH

1. Откройте [my.telegram.org](https://my.telegram.org)
2. Войдите по номеру Telegram
3. Откройте `API development tools`
4. Создайте приложение
5. Сохраните `api_id` и `api_hash`

Важно: `api_hash` — секрет.

## 4) Установка

```powershell
git clone https://github.com/Munja9324/Vpn_Bot_assist.git C:\Project
cd C:\Project
pip install -r requirements.txt
Copy-Item .env.example .env
```

## 5) Настройка `.env` (минимум)

Обязательные поля:

```env
API_ID=123456
API_HASH=your_hash
SESSION_NAME=vpn_kbr_session
ADMIN_BOT_USERNAME=vpn_kbr_bot
WIZARD_TARGET_USERNAME=wizardvpn_manager
```

Рекомендуемые:

```env
DATABASE_PATH=scan-data.sqlite3
LOG_FILE=userbot.log
REPORT_DIR=reports
```

## 6) Первый запуск

```powershell
python vpn_kbr.py
```

При первом запуске Telethon запросит:
- номер телефона
- код из Telegram
- пароль 2FA (если включен)

После входа добавьте себя в запросники:

```text
/roots add me
```

## 7) Права доступа

- **Запросники**: полный доступ к служебным командам.
- **Обычные пользователи**: только поддержка по своему профилю и подпискам.

Управление запросниками:

```text
/roots
/roots add <id|@username|me>
/roots del <id|@username>
```

## 8) Основные команды

### Сервис

```text
menu
/dashboard
/adminsite
/diag
/processes
/version
/tail
/unresolved
```

### Пользователи и подписки

```text
/user <id|username>
/user <id|username> -b
/subs <id|username>
/subs <id|username> -b
```

`-b` = взять данные из локальной БД, без запроса к админ-боту.

### Коммуникации

```text
/wizard <id>
/send <id> <текст>
/broadcast <текст>
/coupon <id>
```

### Скан

```text
scan
scan new
scan continue
scan results
scan reset
stop scan
```

## 9) Тест сценария обычного пользователя

Если вы запросник и хотите проверить пользовательский диалог:

```text
-p не работает впн
```

Префикс `-p` заставляет бота обработать текст как от обычного пользователя.

## 10) Деплой

Локальный деплой-скрипт:

```powershell
.\deploy.ps1 -Message "update docs"
```

Скрипт делает precheck, push и рестарт сервиса на сервере.

## 11) Безопасность

Никогда не отправляйте в Git:
- `.env`
- `*.session`
- `*.sqlite3`
- `*.log`
- приватные ключи/пароли/токены

## 12) Где смотреть проблемы

- `userbot.log` — runtime-ошибки
- `/diag` — диагностика
- `/tail` — хвост логов
- `/unresolved` — непросмотренные сложные кейсы
