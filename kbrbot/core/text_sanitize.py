import re
from typing import Any

try:
    from telethon import Button
except Exception:  # pragma: no cover
    Button = None  # type: ignore


MOJIBAKE_MARKERS = (
    "\u0420\u00a0",
    "\u0420\u040e",
    "\u0420\u040f",
    "\u0421\u20ac",
    "\u0421\u2039",
    "\u0421\u0451",
    "\u0432\u0402\u201d",
    "\u0432\u201e",
    "\u0432\u201a",
    "Ð",
    "Ñ",
    "Ã",
    "â",
)


def cyrillic_letters_count(text: str) -> int:
    return sum(1 for char in text if ("\u0410" <= char <= "\u044f") or char in {"\u0401", "\u0451"})


def mojibake_score(text: str) -> int:
    return sum(text.count(marker) for marker in MOJIBAKE_MARKERS)


def text_quality_score(text: str) -> int:
    sample = str(text or "")
    cyr = cyrillic_letters_count(sample)
    words = len(re.findall(r"[\u0410-\u044f\u0401\u0451]{3,}", sample))
    moj = mojibake_score(sample)
    weird = sum(sample.count(ch) for ch in "�ЂЃ‚ѓ„…†‡€‰Љ‹ЊЌЋЏ")
    latin_noise = sample.count("Ð") + sample.count("Ñ") + sample.count("Ã") + sample.count("â")
    return (words * 20) + (cyr * 2) - (moj * 10) - (weird * 8) - (latin_noise * 6)


def looks_like_mojibake_text(text: str) -> bool:
    sample = str(text or "")
    if not sample:
        return False
    if mojibake_score(sample) >= 2:
        return True
    if "\u0420\u00a7\u0420\u00b5\u0420\u0458" in sample or "\u0432\u0402\u201d" in sample:
        return True
    if re.search(r"(?:Ð.|Ñ.){2,}", sample):
        return True
    if re.search(r"(?:Ã.|â.){2,}", sample):
        return True
    return bool(re.search(r"(?:\u0420.|\u0421.){3,}", sample))


def repair_mojibake_text(text: str) -> str:
    original = str(text or "")
    if not original:
        return original

    def repair_piece(piece: str) -> str:
        candidates = [piece]
        # Typical UTF-8 text that was decoded as latin-1/cp1252 (Ð¢ÐµÐºÑÑ -> Текст)
        for encoding in ("latin1", "cp1252"):
            try:
                candidate = piece.encode(encoding, errors="replace").decode("utf-8", errors="replace")
            except Exception:
                continue
            if candidate:
                candidates.append(candidate)

        # Typical UTF-8 text that was decoded as cp1251 (РўРµРєСЃС‚ -> Текст)
        try:
            candidate = piece.encode("cp1251", errors="replace").decode("utf-8", errors="replace")
            if candidate:
                candidates.append(candidate)
        except Exception:
            pass

        # Fallback inverse direction for rare edge-cases.
        for encoding in ("cp1251", "latin1"):
            try:
                candidate = piece.encode(encoding, errors="replace").decode("utf-8", errors="replace")
            except Exception:
                continue
            if candidate:
                candidates.append(candidate)
        for encoding in ("cp1251", "latin1"):
            try:
                candidate = piece.encode("utf-8", errors="replace").decode(encoding, errors="replace")
            except Exception:
                continue
            if candidate:
                candidates.append(candidate)

        best = piece
        best_value = text_quality_score(piece)
        for candidate in candidates[1:]:
            value = text_quality_score(candidate)
            if value > best_value:
                best = candidate
                best_value = value
        return best

    repaired_lines = []
    for line in original.splitlines(keepends=True):
        repaired_lines.append(repair_piece(line) if looks_like_mojibake_text(line) else line)
    repaired = "".join(repaired_lines)
    return repair_piece(repaired) if looks_like_mojibake_text(repaired) else repaired


def sanitize_outgoing_text(text: str) -> str:
    current = str(text or "")
    for _ in range(3):
        repaired = repair_mojibake_text(current)
        if repaired == current:
            break
        current = repaired
    return current.replace("\r\n", "\n").replace("\r", "\n")


def sanitize_outgoing_payload(value: Any) -> Any:
    if isinstance(value, str):
        return sanitize_outgoing_text(value)
    if isinstance(value, list):
        return [sanitize_outgoing_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize_outgoing_payload(item) for item in value)
    if isinstance(value, dict):
        return {key: sanitize_outgoing_payload(item) for key, item in value.items()}
    return value


def sanitize_buttons(buttons):
    if not buttons:
        return buttons
    if Button is None:  # pragma: no cover
        return buttons

    def sanitize_button(btn):
        try:
            text = sanitize_outgoing_text(str(getattr(btn, "text", "") or ""))
            data = getattr(btn, "data", None)
            url = getattr(btn, "url", None)
            if data is not None:
                return Button.inline(text, data=data)
            if url:
                return Button.url(text, url)
            return Button.text(text)
        except Exception:
            return btn

    rows = []
    for row in buttons:
        if isinstance(row, (list, tuple)):
            rows.append([sanitize_button(btn) for btn in row])
        else:
            rows.append([sanitize_button(row)])
    return rows
