from kbrbot.core.text_sanitize import sanitize_outgoing_text


def test_repair_utf8_as_latin1_mojibake():
    source = "Я виртуальный помощник VPN_KBR."
    broken = source.encode("utf-8").decode("latin1")
    fixed = sanitize_outgoing_text(broken)
    assert fixed == source


def test_repair_utf8_as_cp1251_mojibake():
    broken = "РџСЂРёРІРµС‚"
    fixed = sanitize_outgoing_text(broken)
    assert fixed == "Привет"


def test_sanitize_is_idempotent():
    value = "Проверка текста".encode("utf-8").decode("latin1")
    once = sanitize_outgoing_text(value)
    twice = sanitize_outgoing_text(once)
    assert once == twice
