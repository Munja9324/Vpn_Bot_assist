# Vpn_Bot_assist

`Vpn_Bot_assist` - это Telegram userbot на Telethon для работы с админ-панелью VPN-бота через личный Telegram-аккаунт.

Разработчик: `DevM29`.

Он умеет:

- искать пользователей в админ-боте по ID или username;
- получать краткую карточку `help` и подробную карточку `info`;
- брать данные из SQLite базы через флаг `-b`;
- сканировать всех пользователей и подписки;
- строить HTML dashboard с прогнозом денег, пользователей и подписок;
- отдавать dashboard по ссылке с сервера;
- отправлять карточки в `wizard` только после предпросмотра и подтверждения;
- ограничивать доступ к командам через список запросников `/roots`;
- автоматически деплоить изменения через GitHub на KVM-сервер.

Полная документация на русском:

[docs/RU_FULL_GUIDE.md](docs/RU_FULL_GUIDE.md)

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
python main.py
```

6. При первом запуске введи номер телефона Telegram, код входа и 2FA пароль, если он включен.
7. После запуска отправь боту:

```text
/roots add me
```

После этого команды будут приниматься только от аккаунтов из списка `/roots`.

## Главные команды

```text
menu
/roots
help 123456789
help @username -b
info 123456789
info @username -b
wizard 123456789
promo 123456789
/mail2 Текст рассылки
scan
scan results
/status
/version
```

## Важно про секреты

Нельзя выкладывать в GitHub:

- `.env`;
- `*.session`;
- `scan-data.sqlite3`;
- `reports/`;
- `userbot.log`;
- любые пароли, токены, API hash и приватные ключи.

Эти файлы должны жить только локально или на сервере.
