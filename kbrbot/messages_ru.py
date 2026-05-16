from kbrbot.core.text_sanitize import sanitize_outgoing_text


MESSAGES_RU = {
    "wizard.preview_title": "Предпросмотр wizard:",
    "wizard.final_preview_title": "Итоговый предпросмотр wizard:",
    "wizard.send_variant_prompt": "Отправлять этот вариант?",
    "wizard.await_extra": "Ожидаю дополнительный текст",
    "wizard.extra_added": "Дополнение добавлено",
    "wizard.cancelled": "Отправка отменена пользователем",
    "wizard.sent": "Карточка отправлена",
    "wizard.failed": "Не удалось отправить карточку",
    "wizard.choice_help": "Не понял ответ. Напишите 1, 2 или 0",
    "wizard.await_choice_hint": "Ответьте: 1 - отправить, 2 - добавить, 0 - отмена",
    "wizard.await_final_choice_hint": "Ответьте: 1 - отправить, 2 - изменить дописку, 0 - отмена",
    "wizard.prepared": "Карточка подготовлена",
    "wizard.review_before_send": "Проверьте текст перед отправкой",
    "wizard.sending_without_extra": "Отправляю подготовленную карточку без дополнения",
    "wizard.send_failed_log": "Не удалось отправить карточку. Подробности в логе.",
    "wizard.await_extra_note": "Следующее сообщение будет добавлено к карточке",
    "wizard.cancel_hint": "Для отмены отправьте 0",
    "wizard.extra_title": "Дополнение:",
    "wizard.final_review": "Проверьте итоговый текст",
    "wizard.await_new_extra": "Ожидаю новый дополнительный текст",
    "wizard.replace_extra_note": "Следующее сообщение заменит прошлую дописку",
    "wizard.confirmation_received": "Подтверждение получено",
    "wizard.final_text_length": "Длина итогового текста: {length} символов",
    "wizard.sending_now": "Отправляю в wizard",
    "wizard.sent_after_confirm": "Карточка отправлена после подтверждения",
    "wizard.send_extra_failed": "Не удалось отправить карточку с дополнением",
    "scan.started_clean": "Сканирование по ID запущено с чистого состояния.",
    "scan.resume_found": "Найден сохраненный прогресс scan по ID.",
    "status.long_text_as_file": "Полный текст слишком большой для Telegram. Отправляю файлом.",
}


def msg(key: str, **kwargs) -> str:
    template = MESSAGES_RU.get(key, key)
    try:
        rendered = template.format(**kwargs)
    except Exception:
        rendered = template
    return sanitize_outgoing_text(rendered)
