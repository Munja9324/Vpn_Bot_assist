# Полная инструкция по установке и настройке Vpn_Bot_assist

Документация написана для проекта `Vpn_Bot_assist`. Разработчик: `DevM29`. Это не обычный Bot API бот, а userbot: программа входит в Telegram под твоим личным аккаунтом и через Telethon работает с админ-ботом VPN-сервиса.

## 1. Что делает проект

Проект автоматизирует работу с админ-панелью VPN-бота:

- открывает админ-бота, например `@vpn_kbr_bot`;
- заходит в `/admin`;
- ищет пользователей по ID, Telegram ID, username, WID или email, если это поддерживает админ-бот;
- собирает карточки пользователей и подписки;
- сохраняет найденные данные в SQLite базу;
- строит отчеты и HTML dashboard;
- отдает dashboard по ссылке с сервера;
- отправляет подготовленную карточку в wizard-чат только после твоего подтверждения;
- принимает команды только от разрешенных аккаунтов из списка `/roots`.

## 2. Важные понятия

`API_ID` и `API_HASH` - это ключи приложения Telegram API. Они нужны Telethon, чтобы подключиться к Telegram.

`SESSION_NAME` - имя файла сессии Telethon. В этом файле хранится вход в Telegram. Файл `*.session` равен доступу к аккаунту, его нельзя выкладывать в GitHub.

`ADMIN_BOT_USERNAME` - username твоего VPN-бота, где есть админ-панель.

`WIZARD_TARGET_USERNAME` - username аккаунта или чата, куда отправляется подготовленная карточка wizard.

`scan-data.sqlite3` - локальная база проекта. В ней хранятся последние найденные пользователи, подписки, ошибки скана и список запросников.

`/roots` - список аккаунтов, которым разрешено писать команды userbot-аккаунту.

## 3. Получение API_ID и API_HASH

Официальный способ описан в документации Telethon: нужно войти на `my.telegram.org`, открыть `API Development tools`, создать приложение и сохранить `api_id` и `api_hash`.

Пошагово:

1. Открой [https://my.telegram.org](https://my.telegram.org).
2. Введи номер Telegram-аккаунта, под которым будет работать userbot.
3. В Telegram придет код входа. Введи его на сайте.
4. Открой раздел `API development tools`.
5. Нажми создание нового приложения.
6. Заполни минимум:
   - `App title` - любое понятное имя, например `VPN KBR Admin`;
   - `Short name` - короткое латинское имя, например `vpnkbradmin`.
7. URL можно не указывать, если сайт не требует.
8. Нажми `Create application`.
9. Сохрани:
   - `api_id`;
   - `api_hash`.

Важно:

- `api_hash` секретный;
- не отправляй `api_hash` в чат;
- не коммить `.env` в GitHub;
- не используй чужие `api_id/api_hash`.

## 4. Подготовка Windows-локальной машины

Установи:

- Python 3.10 или новее;
- Git;
- PowerShell;
- при необходимости OpenSSH Client.

Проверь:

```powershell
python --version
git --version
ssh -V
```

Клонируй проект:

```powershell
git clone https://github.com/OWNER/REPO.git C:\Project
cd C:\Project
```

Если проект уже лежит в `C:\Project`, просто перейди в папку:

```powershell
cd C:\Project
```

Установи зависимости:

```powershell
pip install -r requirements.txt
```

## 5. Настройка `.env`

Создай `.env`:

```powershell
Copy-Item .env.example .env
```

Открой `.env` и заполни основные поля:

```env
API_ID=123456
API_HASH=your_api_hash_here
SESSION_NAME=my_profile_session

ADMIN_BOT_USERNAME=vpn_kbr_bot
ADMIN_COMMAND=/admin
WIZARD_TARGET_USERNAME=wizardvpn_manager
```

Под свой VPN-бот поменяй:

```env
ADMIN_BOT_USERNAME=your_vpn_bot_username
USERS_BUTTON_TEXT=Пользователи
FIND_USER_BUTTON_TEXT=Найти пользователя
SUBSCRIPTIONS_BUTTON_TEXT=Подписки пользователя
WRITE_USER_BUTTON_TEXT=Написать пользователю
MAIL_NEXT_BUTTON_TEXT=Далее
CANCEL_BUTTON_TEXT=Отменить
BACK_BUTTON_TEXT=Назад
NEXT_PAGE_BUTTON_TEXT=Далее
```

Если у твоего VPN-бота другие тексты кнопок, укажи их в `.env`. При скане и поиске код также старается ориентироваться по фактически полученным кнопкам, но базовые тексты лучше настроить.

Настрой путь к базе и отчетам:

```env
REPORT_DIR=reports
DATABASE_PATH=scan-data.sqlite3
```

Настрой доступ к dashboard по ссылке:

```env
DASHBOARD_HTTP_ENABLED=true
DASHBOARD_HTTP_HOST=0.0.0.0
DASHBOARD_HTTP_PORT=8088
DASHBOARD_PUBLIC_BASE_URL=http://YOUR_SERVER_IP:8088
DASHBOARD_PUBLIC_PATH_PREFIX=dashboard
DASHBOARD_PUBLIC_TOKEN=long_random_token
DASHBOARD_PUBLIC_DIR=reports/public
DASHBOARD_PUBLIC_RETENTION=30
```

`DASHBOARD_PUBLIC_TOKEN` лучше сделать длинным случайным значением. Пример:

```powershell
[guid]::NewGuid().ToString("N")
```

## 6. Первый запуск под свой номер Telegram

Запусти:

```powershell
python main.py
```

При первом запуске Telethon попросит:

1. номер телефона Telegram;
2. код из Telegram;
3. пароль 2FA, если включен.

После успешного входа появится файл:

```text
my_profile_session.session
```

Это файл входа в Telegram. Его нельзя выкладывать в GitHub.

## 7. Первичная защита через `/roots`

После запуска отправь userbot-аккаунту в личку:

```text
/roots add me
```

Теперь команды будут приниматься только от тебя.

Посмотреть список:

```text
/roots
```

Добавить другого запросника:

```text
/roots add 123456789 админ
/roots add @username менеджер
```

Удалить:

```text
/roots del 123456789
/roots del @username
```

Очистить список:

```text
/roots clear yes
```

Если пишет неразрешенный аккаунт, он получает:

```text
Этот аккаунт не предназначен для получения сообшений, это сообщение сгенерировано автоматически, отвечать на него не нужно
```

Если список `/roots` пустой, разрешена только команда `/roots`, чтобы можно было добавить себя.

## 8. Основные команды

Открыть меню:

```text
menu
```

Показать версию:

```text
/version
```

Собрать dashboard из SQLite базы:

```text
/status
```

## 9. Команда `help`

Живой поиск через админ-бота:

```text
help 123456789
help @username
```

Поиск по базе без обращения к админ-боту:

```text
help 123456789 -b
help @username -b
```

`-b` значит `base`, то есть брать из SQLite базы. Это быстрее и безопаснее по лимитам Telegram, но данные будут актуальны только настолько, насколько свежий последний scan или последний `info`.

## 10. Команда `info`

Живой подробный сбор:

```text
info 123456789
info @username
```

Из базы:

```text
info 123456789 -b
info @username -b
```

`info` собирает карточку пользователя и подробности подписок.

## 11. Команда `wizard`

Команда готовит карточку и отправляет ее в `WIZARD_TARGET_USERNAME`, но только после проверки.

```text
wizard 123456789
```

Порядок работы:

1. userbot берет карточку из SQLite базы, если она уже есть;
2. если в базе нет, ищет через админ-бота;
3. показывает предпросмотр;
4. предлагает:
   - `1 отправить`;
   - `2 дописать`;
   - `0 отмена`;
5. если выбран пункт `2`, следующее сообщение будет добавлено к карточке;
6. после дописки бот снова показывает итоговый предпросмотр;
7. только после `1 отправить` карточка уходит в wizard.

Ничего не отправляется автоматически без подтверждения.

## 12. Команда `mail`

Отправить пользователю текст через админ-бота:

```text
mail 123456789 Текст сообщения
```

Если текст не указан, используется `MAIL_TEXT` из `.env`:

```text
mail 123456789
```

## 13. Scan

Открыть меню scan:

```text
scan
```

Новый scan:

```text
scan new
```

Продолжить:

```text
scan continue
```

Пауза:

```text
scan pause
stop scan
/stopscan
```

Результаты:

```text
scan results
```

Сброс:

```text
scan reset
```

Как работает scan:

1. открывает `/admin`;
2. заходит в статистику;
3. берет общее количество пользователей;
4. возвращается в пользователей;
5. ищет пользователей по ID через поиск, не листая страницы вручную;
6. собирает карточки и подписки;
7. после каждого найденного пользователя обновляет `latest_users/latest_subscriptions`;
8. при паузе сохраняет checkpoint;
9. после завершения сохраняет отчеты и dashboard.

Во время scan в статусе показывается текущий ID вида `1037/1250`.

## 14. SQLite база

База по умолчанию:

```text
scan-data.sqlite3
```

В ней есть:

- `scan_runs` - последние полные результаты scan;
- `users` - пользователи из scan;
- `subscriptions` - подписки из scan;
- `scan_errors` - ошибки scan;
- `latest_users` - актуальный последний снимок пользователей;
- `latest_subscriptions` - актуальный последний снимок подписок;
- `requesters` - список `/roots`.

Эта база нужна для быстрых команд:

```text
help 123 -b
info 123 -b
```

Базу нельзя коммитить в GitHub.

## 15. Dashboard по ссылке

Dashboard больше не обязан отправляться файлом. Он публикуется на сервере и открывается по ссылке.

Настройки:

```env
DASHBOARD_HTTP_ENABLED=true
DASHBOARD_HTTP_HOST=0.0.0.0
DASHBOARD_HTTP_PORT=8088
DASHBOARD_PUBLIC_BASE_URL=http://YOUR_SERVER_IP:8088
DASHBOARD_PUBLIC_PATH_PREFIX=dashboard
DASHBOARD_PUBLIC_TOKEN=long_random_token
DASHBOARD_PUBLIC_DIR=reports/public
```

Пример ссылки:

```text
http://YOUR_SERVER_IP:8088/dashboard/long_random_token/latest-scan-dashboard.html
```

Важно:

- ссылка защищена только длинным токеном в URL;
- не публикуй ссылку в открытых местах;
- если ссылка утекла, поменяй `DASHBOARD_PUBLIC_TOKEN` и перезапусти сервис.

## 16. Настройка GitHub

Создай репозиторий на GitHub.

Локально:

```powershell
cd C:\Project
git init
git remote add origin https://github.com/OWNER/REPO.git
git add .gitignore README.md BOT_TEMPLATE.md main.py requirements.txt .env.example docs
git commit -m "initial commit"
git branch -M main
git push -u origin main
```

Проверь `.gitignore`. В GitHub не должны попадать:

```text
.env
*.session
*.session-journal
scan-data.sqlite3
*.db
reports/
*.log
runtime-version.txt
```

Если случайно закоммитил секреты:

1. сразу поменяй секреты;
2. удали их из истории Git;
3. force-push только если понимаешь последствия;
4. считай, что секрет уже мог быть скопирован.

## 17. Настройка KVM-сервера

Пример для Ubuntu/Debian.

Подключись:

```powershell
ssh root@YOUR_SERVER_IP
```

Установи пакеты:

```bash
apt update
apt install -y git python3 python3-venv python3-pip curl openssh-client
```

Склонируй проект:

```bash
cd /root
git clone https://github.com/OWNER/REPO.git vol29app
cd /root/vol29app
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
```

Создай серверный `.env`:

```bash
cp .env.example .env
nano .env
```

Заполни реальные значения. Секреты должны жить только на сервере.

## 18. Перенос сессии на сервер

Есть два варианта.

Вариант A: создать сессию локально и перенести:

```powershell
scp C:\Project\my_profile_session.session root@YOUR_SERVER_IP:/root/vol29app/my_profile_session.session
```

Вариант B: запустить на сервере интерактивно:

```bash
cd /root/vol29app
./venv/bin/python main.py
```

Введи номер, код и 2FA. После успешного входа останови `Ctrl+C`.

## 19. systemd сервис

Создай файл:

```bash
nano /etc/systemd/system/vol29app.service
```

Пример:

```ini
[Unit]
Description=Vol29 Telegram Bot
After=network.target
StartLimitIntervalSec=60
StartLimitBurst=10

[Service]
Type=simple
WorkingDirectory=/root/vol29app
Environment=PYTHONUNBUFFERED=1
ExecStart=/root/vol29app/venv/bin/python /root/vol29app/main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Запусти:

```bash
systemctl daemon-reload
systemctl enable vol29app
systemctl start vol29app
systemctl status vol29app --no-pager -l
```

Логи:

```bash
journalctl -u vol29app -n 100 --no-pager
journalctl -u vol29app -f
```

## 20. Автообновление с GitHub перед стартом

Создай серверный скрипт:

```bash
mkdir -p /root/vol29app/deploy
nano /root/vol29app/deploy/update_from_github.sh
chmod +x /root/vol29app/deploy/update_from_github.sh
```

Пример:

```bash
#!/usr/bin/env bash
set -euo pipefail

cd /root/vol29app
REMOTE="${GIT_REMOTE:-origin}"
BRANCH="${GIT_BRANCH:-main}"

echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) checking GitHub updates remote=$REMOTE branch=$BRANCH head=$(git rev-parse HEAD)"

git fetch --quiet "$REMOTE" "+$BRANCH:refs/remotes/$REMOTE/$BRANCH"
LOCAL="$(git rev-parse HEAD)"
REMOTE_HEAD="$(git rev-parse "$REMOTE/$BRANCH")"

if [[ "$LOCAL" == "$REMOTE_HEAD" ]]; then
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) already up to date"
  exit 0
fi

BACKUP="/root/vol29app-backups/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$BACKUP"
cp -a .env *.session* scan-data.sqlite3 reports "$BACKUP"/ 2>/dev/null || true

git reset --hard "$REMOTE/$BRANCH"

cp -a "$BACKUP"/.env . 2>/dev/null || true
cp -a "$BACKUP"/*.session* . 2>/dev/null || true
cp -a "$BACKUP"/scan-data.sqlite3 . 2>/dev/null || true
cp -a "$BACKUP"/reports . 2>/dev/null || true

if git diff --name-only "$LOCAL" "$REMOTE_HEAD" | grep -q '^requirements.txt$'; then
  ./venv/bin/pip install -r requirements.txt
fi

echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) GitHub update completed"
```

Добавь в systemd перед запуском:

```ini
ExecStartPre=/root/vol29app/deploy/update_from_github.sh
```

После изменения сервиса:

```bash
systemctl daemon-reload
systemctl restart vol29app
```

## 21. Подключение xray/SOCKS5 прокси

Если Telegram плохо подключается с сервера, можно использовать локальный SOCKS5 прокси xray.

В `.env`:

```env
TELEGRAM_PROXY_ENABLED=true
TELEGRAM_PROXY_TYPE=socks5
TELEGRAM_PROXY_HOST=127.0.0.1
TELEGRAM_PROXY_PORT=1080
TELEGRAM_PROXY_RDNS=true
```

Проверь, что xray слушает порт:

```bash
ss -lntp | grep ':1080'
```

Если `vol29app` должен стартовать только после xray, systemd можно настроить так:

```ini
[Unit]
Requires=xray.service
After=network.target xray.service

[Service]
ExecStartPre=/bin/systemctl is-active --quiet xray.service
```

## 22. Локальный деплой с Windows через `deploy.ps1`

В проекте есть `deploy.ps1`. Он:

1. проверяет безопасный набор файлов;
2. делает commit;
3. пушит в GitHub;
4. перезапускает сервис на сервере.

Пример:

```powershell
powershell -ExecutionPolicy Bypass -File C:\Project\deploy.ps1 -Message "update docs"
```

Перед деплоем убедись, что `.env`, база, session и логи не staged:

```powershell
git status --short
```

## 23. Настройка под своего VPN-бота

Минимально поменяй:

```env
ADMIN_BOT_USERNAME=your_vpn_bot_username
ADMIN_COMMAND=/admin
WIZARD_TARGET_USERNAME=your_wizard_username
```

Проверь кнопки в админ-боте:

```env
USERS_BUTTON_TEXT=Пользователи
FIND_USER_BUTTON_TEXT=Найти пользователя
SUBSCRIPTIONS_BUTTON_TEXT=Подписки пользователя
WRITE_USER_BUTTON_TEXT=Написать пользователю
MAIL_NEXT_BUTTON_TEXT=Далее
CANCEL_BUTTON_TEXT=Отменить
BACK_BUTTON_TEXT=Назад
NEXT_PAGE_BUTTON_TEXT=Далее
```

Если твой бот использует другие названия, впиши их точно как в Telegram.

После настройки проверь:

```text
menu
help 1
info 1
scan results
/status
```

## 24. Логи и диагностика

### Автоочистка перед запуском

При каждом запуске и перезапуске `vol29app` проект может очищать временные файлы и локальные логи. Это включено по умолчанию:

```env
CLEANUP_ON_START_ENABLED=true
CLEANUP_LOGS_ON_START=true
CLEANUP_TEMP_ON_START=true
```

Что удаляется:

- `userbot.log`;
- `userbot-crash.log`;
- ротации этих логов;
- `runtime-version.txt`;
- `__pycache__`;
- `.pytest_cache`;
- `.mypy_cache`;
- `.ruff_cache`;
- временные файлы в корне проекта: `*.tmp`, `*.temp`, `*.part`, `*.pyc`, `*.pyo`, `*.swp`, `*.swo`.

Что не удаляется:

- `.env`;
- `*.session`;
- `scan-data.sqlite3`;
- `reports/`;
- dashboard HTML;
- серверные backups.

Если нужно временно выключить очистку:

```env
CLEANUP_ON_START_ENABLED=false
```

Если нужно чистить временные файлы, но оставлять логи:

```env
CLEANUP_ON_START_ENABLED=true
CLEANUP_LOGS_ON_START=false
CLEANUP_TEMP_ON_START=true
```

Локально:

```powershell
Get-Content C:\Project\userbot.log -Tail 100
```

На сервере:

```bash
journalctl -u vol29app -n 120 --no-pager
journalctl -u vol29app -f
```

Проверить версию в Telegram:

```text
/version
```

Проверить commit на сервере:

```bash
cd /root/vol29app
git rev-parse --short HEAD
```

Проверить dashboard:

```bash
ss -lntp | grep ':8088'
curl -I http://YOUR_SERVER_IP:8088/dashboard/YOUR_TOKEN/latest-scan-dashboard.html
```

## 25. Частые ошибки

### `FloodWaitError`

Telegram просит подождать. Код старается снижать частоту действий, но частые запросы все равно могут ловить лимиты. Используй `-b`, если данные уже есть в базе:

```text
info 123 -b
help 123 -b
```

### `ValueError: Too many incoming messages`

Обычно возникает при слишком активной переписке с админ-ботом. В проекте используется прямое ожидание обновлений и увеличенный `ADMIN_CONVERSATION_MAX_MESSAGES`, но при сбоях помогает перезапуск сервиса.

### Telethon session schema error

Если после обновления Python/Telethon сессия ругается на структуру SQLite, код пытается починить session автоматически и сделать backup. Если не помогло, создай новую сессию через повторный вход.

### Dashboard не открывается

Проверь:

```bash
systemctl status vol29app --no-pager -l
ss -lntp | grep ':8088'
grep '^DASHBOARD_' /root/vol29app/.env
```

Проверь, открыт ли порт в firewall/панели провайдера.

### Команды не принимаются

Проверь `/roots`. Если список пустой, отправь:

```text
/roots add me
```

Если ты случайно удалил себя, очисти таблицу на сервере:

```bash
cd /root/vol29app
sqlite3 scan-data.sqlite3 "DELETE FROM requesters;"
systemctl restart vol29app
```

Потом снова отправь:

```text
/roots add me
```

## 26. Безопасность

Никогда не коммить:

- `.env`;
- `*.session`;
- `scan-data.sqlite3`;
- `reports/`;
- `*.log`;
- SSH ключи;
- пароли;
- токены;
- dashboard token.

Если секрет попал в GitHub, считай его скомпрометированным.

Что нужно сделать:

1. поменять секрет;
2. удалить секрет из истории Git;
3. force-push очищенную историю;
4. проверить серверные `.env` и backup-файлы;
5. при утечке `*.session` выйти из всех сессий в официальном Telegram-клиенте.

## 27. Минимальный порядок запуска с нуля

1. Получить `API_ID/API_HASH` на `my.telegram.org`.
2. Установить Python и Git.
3. Склонировать репозиторий.
4. Создать `.env`.
5. Настроить `ADMIN_BOT_USERNAME` под своего VPN-бота.
6. Запустить `python main.py`.
7. Войти по номеру Telegram.
8. Отправить `/roots add me`.
9. Проверить `help 1`.
10. Запустить `scan new`.
11. Проверить `scan results`.
12. Настроить сервер.
13. Перенести `.env`, `.session`, базу при необходимости.
14. Настроить `vol29app.service`.
15. Настроить автообновление из GitHub.
16. Настроить dashboard link.
17. Проверить `/version`, `/status`, `wizard 1`.

## 28. Источники

- Telethon: получение `API_ID/API_HASH` и вход в аккаунт: [docs.telethon.dev](https://docs.telethon.dev/en/stable/basic/signing-in.html)
- Telegram API tools: [my.telegram.org](https://my.telegram.org)
