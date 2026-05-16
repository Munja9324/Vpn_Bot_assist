from kbrbot.features.wizard import wizard_final_preview_text, wizard_preview_text
from kbrbot.messages_ru import MESSAGES_RU, msg


def test_wizard_preview_text_uses_dictionary():
    text = wizard_preview_text("Тест")
    assert text.startswith("Предпросмотр wizard:")
    assert "Тест" in text


def test_wizard_final_preview_text_uses_dictionary():
    text = wizard_final_preview_text("Итог")
    assert text.startswith("Итоговый предпросмотр wizard:")
    assert "Итог" in text


def test_msg_always_sanitizes_output():
    key = "__tmp_mojibake__"
    MESSAGES_RU[key] = "Привет".encode("utf-8").decode("latin1")
    try:
        assert msg(key) == "Привет"
    finally:
        MESSAGES_RU.pop(key, None)
