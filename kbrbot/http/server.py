from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class JsonResponse:
    status: int
    body: str
    content_type: str = "application/json; charset=utf-8"

    def parse(self) -> dict[str, Any]:
        return json.loads(self.body)


def build_json_response(payload: dict[str, Any], *, status: int = 200) -> JsonResponse:
    return JsonResponse(status=status, body=json.dumps(payload, ensure_ascii=False))


class ReadOnlyApiCache:
    def __init__(self, *, enabled: bool = True, ttl_seconds: float = 8.0) -> None:
        self.enabled = bool(enabled)
        self.ttl_seconds = max(1.0, float(ttl_seconds))
        self._lock = threading.Lock()
        self._store: dict[str, tuple[float, dict[str, Any]]] = {}

    def get(self, key: str) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        now_ts = time.time()
        with self._lock:
            item = self._store.get(key)
            if not item:
                return None
            expires_at, payload = item
            if now_ts >= expires_at:
                self._store.pop(key, None)
                return None
            return dict(payload)

    def set(self, key: str, payload: dict[str, Any]) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._store[key] = (time.time() + self.ttl_seconds, dict(payload))

    def invalidate(self, prefixes: tuple[str, ...] | None = None) -> None:
        with self._lock:
            if not prefixes:
                self._store.clear()
                return
            for key in list(self._store.keys()):
                if any(key.startswith(prefix) for prefix in prefixes):
                    self._store.pop(key, None)
