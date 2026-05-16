import os
import re


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return float(value.strip().replace(",", "."))
    except ValueError:
        return default


def env_text(name: str, default: str) -> str:
    value = os.getenv(name)
    if not value:
        return default
    if re.search(r"(?:\u0420.|\u0421.){3,}", value):
        return default
    if re.search(r"(?:Ð.|Ñ.){2,}", value):
        return default
    if re.search(r"(?:Ã.|â.){2,}", value):
        return default
    return value
