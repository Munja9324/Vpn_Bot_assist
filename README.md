# Telegram userbot

Userbot for a personal Telegram profile using Telethon.

## Setup

1. Install Python 3.10+.
2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Copy `.env.example` to `.env`.
4. Put your `API_ID` and `API_HASH` into `.env`.
5. Run:

```powershell
python main.py
```

On the first run, Telegram will ask for your phone number, login code, and possibly your 2FA password. The script stores a local `*.session` file so you do not need to log in every time.

## What it does

- When someone sends `help 123456789` in private chat, opens `@vpn_kbr_bot`, sends `/admin`, clicks `Пользователи`, clicks `Найти пользователя`, sends the id, clicks `Подписки пользователя`, prints the result in the console, logs every step, and replies with the user number and subscriptions.
- Replies only in private messages.
- Ignores bots.
- Can reply once per chat to avoid noisy behavior.
- Supports simple keyword replies in `KEYWORD_REPLIES` inside `main.py`.
- Long commands edit one compact progress-bar status message.

Admin bot settings can be changed in `.env`:

```env
ADMIN_BOT_USERNAME=vpn_kbr_bot
ADMIN_COMMAND=/admin
WIZARD_TARGET_USERNAME=wizardvpn_manager
USERS_BUTTON_TEXT=Пользователи
FIND_USER_BUTTON_TEXT=Найти пользователя
SUBSCRIPTIONS_BUTTON_TEXT=Подписки пользователя
WRITE_USER_BUTTON_TEXT=Написать пользователю
MAIL_NEXT_BUTTON_TEXT=Далее
CANCEL_BUTTON_TEXT=Отменить
MAIL_TEXT=Здравствуйте!
LOG_FILE=userbot.log
```

Help command:

```text
help 123456789
```

Mail command:

```text
mail 123456789 Текст сообщения
```

Unsupported commands reply:

```text
Такой команды нет в списке поддерживаемых.
```

## Safety

Info command:

```text
info 123456789
```

Scan commands:

```text
scan          # start or continue the scan
scan pause    # pause after the current user and save progress
scan reset    # clear saved progress
scan results  # show saved progress and latest reports
/scanmenu     # show scan menu
```

Do not use this for spam or mass messaging. Telegram can restrict or ban accounts for abusive automation.
