from kbrbot.messages_ru import msg


def wizard_preview_text(base_text: str) -> str:
    return f"{msg('wizard.preview_title')}\n\n{base_text}"


def wizard_final_preview_text(full_text: str) -> str:
    return f"{msg('wizard.final_preview_title')}\n\n{full_text}"

