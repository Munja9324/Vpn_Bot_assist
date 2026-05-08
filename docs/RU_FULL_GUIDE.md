# Полная инструкция по Vpn_Bot_assist

Документ для проекта `Vpn_Bot_assist`.  
Разработчик: `DevM29`.

## 1. Что это за проект

`Vpn_Bot_assist` - это не Bot API-бот, а userbot. Он входит в Telegram под вашим личным аккаунтом и через Telethon работает с админ-ботом VPN-сервиса.

Проект нужен, чтобы:

- быстро искать пользователей и подписки;
- вести локальную базу в SQLite;
- строить dashboard и live admin site;
- отправлять сообщения, промокоды и карточки в wizard;
- автоматически помогать обычным пользователям как виртуальный помощник VPN_KBR;
- сохранять сложные или неразобранные случаи в отдельный список.

## 2. Как устроен проект

Основные части:

- `main.py` - вся бизнес-логика;
- `.env` - настройки и секреты;
- `scan-data.sqlite3` - локальная база;
- `reports/` - HTML-отчеты и dashboard;
- `userbot.log` - лог работы;
- `*.session` - сессия Telegram-аккаунта.

Логика общения разделена на три потока:

1. Команды запросников  
   Команды вроде `/user`, `/subs`, `/wizard`, `scan`, `/broadcast`.

2. Обычные пользователи  
   Вежливая поддержка, уточнение проблемы, помощь по подключению, передача в wizard при необходимости.

3. KBR_GPT  
   Отдельный поток с очередью и повторными попытками при rate limit.

## 3. Что нужно для запуска

- Windows или Linux-сервер;
- Python 3.10+;
- Git;
- доступ к Telegram-аккаунту;
- `API_ID` и `API_HASH` с `my.telegram.org`.

Проверка:

```powershell
python --version
git --version
```

## 4. Как получить API_ID и API_HASH

1. Открой [https://my.telegram.org](https://my.telegram.org)
2. Войди по своему номеру Telegram.
3. Введи код, который придет в Telegram.
4. Открой `API development tools`.
5. Создай новое приложение.
6. Заполни:
   - `App title` - любое понятное имя, например `VPN KBR Admin`
   - `Short name` - короткое латинское имя, например `vpnkbradmin`
7. Сохрани:
   - `api_id`
   - `api_hash`

Важно:

- `api_hash` - секрет;
- не отправляй его в чаты;
- не коммить `.env` в GitHub.

## 5. Установка проекта

Клонирование:

```powershell
git clone https://github.com/OWNER/REPO.git C:\Project
cd C:\Project
```

Если проект уже лежит в `C:\Project`:

```powershell
cd C:\Project
```

Установка зависимостей:

```powershell
pip install -r requirements.txt
```

## 6. Настройка `.env`

Создай конфиг:

```powershell
Copy-Item .env.example .env
```

Минимально обязательные поля:

```env
API_ID=123456
API_HASH=your_api_hash_here
SESSION_NAME=my_profile_session

ADMIN_BOT_USERNAME=vpn_kbr_bot
ADMIN_COMMAND=/admin
WIZARD_TARGET_USERNAME=wizardvpn_manager
```

Основные параметры OpenAI:

```env
OPENAI_API_KEY=
OPENAI_MODEL=gpt-5.4-mini
OPENAI_TIMEOUT_SECONDS=60
OPENAI_MAX_OUTPUT_TOKENS=2048
OPENAI_REASONING_EFFORT=none
OPENAI_TRANSCRIBE_MODEL=gpt-4o-mini-transcribe
SMART_CONTROLLER_ENABLED=true
```

Основные кнопки админ-бота:

```env
USERS_BUTTON_TEXT=Пользователи
FIND_USER_BUTTON_TEXT=Найти пользователя
SUBSCRIPTIONS_BUTTON_TEXT=Подписки пользователя
WRITE_USER_BUTTON_TEXT=Написать пользователю
MAIL_NEXT_BUTTON_TEXT=Далее
PROMO_BUTTON_TEXT=Промокоды
PROMO_CREATE_BUTTON_TEXT=Создать промокод
PROMO_SUBMIT_BUTTON_TEXT=Создать
PROMO_SUCCESS_TEXT=Промокод успешно добавлен
BACK_BUTTON_TEXT=Назад
NEXT_PAGE_BUTTON_TEXT=Далее
CANCEL_BUTTON_TEXT=Отменить
```

Пути и отчеты:

```env
REPORT_DIR=reports
DATABASE_PATH=scan-data.sqlite3
LOG_FILE=userbot.log
```

Настройки dashboard:

```env
DASHBOARD_HTTP_ENABLED=false
DASHBOARD_HTTP_HOST=0.0.0.0
DASHBOARD_HTTP_PORT=8088
DASHBOARD_PUBLIC_BASE_URL=
DASHBOARD_PUBLIC_PATH_PREFIX=dashboard
DASHBOARD_PUBLIC_TOKEN=
DASHBOARD_PUBLIC_DIR=reports/public
DASHBOARD_PUBLIC_RETENTION=30
```

Если хочешь публичную ссылку на dashboard, включи HTTP и укажи внешний URL сервера.

Настройки очистки:

```env
CLEANUP_ON_START_ENABLED=true
CLEANUP_LOGS_ON_START=true
CLEANUP_TEMP_ON_START=true
```

## 7. Первый запуск

Запуск:

```powershell
python main.py
```

При первом запуске Telethon попросит:

1. номер телефона;
2. код из Telegram;
3. пароль 2FA, если включен.

После этого появится файл сессии:

```text
my_profile_session.session
```

Его нельзя публиковать.

## 8. Первичная защита через `/roots`

После первого входа отправь userbot-аккаунту:

```text
/roots add me
```

Теперь команды будут приниматься только от разрешенных аккаунтов.

Полезные варианты:

```text
/roots
/roots add me
/roots add 123456789 админ
/roots add @username менеджер
/roots del 123456789
/roots clear yes
```

Если список `/roots` пустой, разрешена только команда `/roots`, чтобы можно было добавить себя.

## 9. Основные команды

### Служебные

```text
menu
/version
/diag
/processes
/tail
/unresolved
```

### Dashboard

```text
/dashboard
/adminsite
/status
```

`/status` и `/dashboard` используются для быстрого открытия отчетного маршрута.  
`/adminsite` открывает live admin site.

### Пользователи

```text
/user <id|username>
/user <id|username> -b
/subs <id|username>
/subs <id|username> -b
```

Старые алиасы:

```text
help <id|username>
help <id|username> -b
info <id|username>
info <id|username> -b
```

`-b` означает брать данные из SQLite, без похода в админ-бот.

### Wizard

```text
/wizard <id>
```

Сценарий:

1. бот собирает карточку;
2. показывает предпросмотр;
3. предлагает:
   - `1` отправить;
   - `2` дописать;
   - `0` отменить.

### Сообщения

```text
/send <id> <текст>
/send <id>
/broadcast <текст>
/broadcast
```

Старые алиасы:

```text
mail ...
mail2 ...
```

### Промокод

```text
/coupon <id>
```

Бот:

1. создает промокод вида `<id>nPromo`;
2. ждет подтверждение успешного создания;
3. только потом отправляет сообщение пользователю.

Старый алиас:

```text
promo <id>
```

### Scan

```text
scan
scan new
scan continue
scan results
scan reset
stop scan
```

Логика scan:

- получает общее число пользователей через `/admin` -> `Статистика`;
- идет по пользователям быстрее, чем постраничный обход;
- пишет все найденное в SQLite;
- умеет паузу, продолжение и сброс;
- строит отчеты и dashboard на базе локальной БД.

## 10. KBR_GPT

Команды:

```text
/gpt
/gpt <вопрос>
/gpt reset
```

Особенности:

- у GPT отдельная очередь;
- пока идет командный сценарий, GPT не должен перехватывать короткие ответы;
- при rate limit бот ждет и повторяет запрос;
- если ожидание слишком долгое, сложный случай сохраняется в `unresolved_requests`.

## 11. Как работает поддержка обычных пользователей

Если пишет не запросник, бот работает как виртуальный помощник VPN_KBR:

- приветствует;
- помогает с подключением;
- подсказывает, где взять ID;
- понимает проблему по VPN;
- проверяет наличие подписок;
- если подписок несколько, просит уточнить;
- если проблема описана слишком размыто, просит детали;
- если требуется оператор, дает `@Aloneinthepluto`;
- если нужно, отправляет карточку в wizard.

Бот старается не спамить и чаще редактирует текущий рабочий ответ, а не шлет пачку сообщений.

## 12. Неразобранные случаи

Все случаи, где бот не смог уверенно ответить сам, сохраняются в SQLite.

Команды:

```text
/unresolved
/unresolved all
/unresolved 15
/unresolved done 15
/unresolved done 15 разобрано вручную
```

Это полезно для:

- GPT rate limit;
- голосовых ошибок;
- проблем, ушедших в поддержку;
- ручной доработки сценариев.

## 13. Dashboard и admin site

В проекте есть два формата просмотра:

1. Обычный dashboard
2. Live admin site

Открытие:

```text
/dashboard
/adminsite
```

Dashboard использует локальную базу и показывает:

- число пользователей;
- подписки;
- продления;
- доходные прогнозы;
- историю и прогноз;
- короткие топы вместо огромных списков.

## 14. GitHub и сервер

Рекомендуемый порядок:

1. вносишь изменения локально;
2. проверяешь сборку;
3. пушишь в GitHub;
4. сервер обновляет проект;
5. сервис перезапускается.

На GitHub нельзя отправлять:

- `.env`
- `.env.txt`
- `*.session`
- `*.sqlite3`
- `reports/`
- `*.log`

## 15. Перед запуском и перезапуском

Проект умеет чистить:

- временные файлы;
- старые логи;
- мусорные артефакты.

Это задается через:

```env
CLEANUP_ON_START_ENABLED=true
CLEANUP_LOGS_ON_START=true
CLEANUP_TEMP_ON_START=true
```

## 16. Типовые проблемы

### `FloodWaitError`

Причина: слишком частые действия в Telegram.  
Что делать: уменьшить частоту, дать боту время, не запускать лишние параллельные команды.

### `MessageNotModifiedError`

Причина: попытка отредактировать сообщение тем же текстом.  
Обычно безопасно, но код должен избегать лишних редактирований.

### `ValueError: Too many incoming messages`

Причина: перегруженный или слишком длинный conversation flow.  
Лечится ограничением сценариев, очередями и меньшим количеством лишних запросов.

### `GPT недоступен`

Проверь:

- `OPENAI_API_KEY`
- прокси или доступ к API
- rate limit организации

### `SQL пустой`

Проверь:

- существует ли `scan-data.sqlite3`
- был ли выполнен scan
- правильный ли путь в `DATABASE_PATH`

## 17. Что проверять после любого изменения

Минимум:

```powershell
python -m py_compile main.py
```

Потом:

1. запуск бота;
2. `/diag`;
3. `menu`;
4. `/user ... -b`;
5. `/adminsite` или `/dashboard`;
6. один реальный сценарий поддержки.

## 18. Куда смотреть при проблемах

- [main.py](C:/Project/main.py)
- [README.md](C:/Project/README.md)
- [docs/RU_FULL_GUIDE.md](C:/Project/docs/RU_FULL_GUIDE.md)
- `userbot.log`
- `scan-data.sqlite3`

Если проект меняется дальше, лучше поддерживать именно эти три вещи в актуальном состоянии:

1. код;
2. `.env.example`;
3. документацию.
