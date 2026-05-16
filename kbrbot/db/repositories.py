from typing import Iterable


def dedup_subscriptions_count(records: Iterable[dict]) -> int:
    total = 0
    for record in records:
        seen: set[str] = set()
        for sub in list(record.get("subscriptions") or []):
            sub_id = str(sub.get("subscription_id") or "").strip()
            btn = str(sub.get("button_text") or "").strip()
            loc = str(sub.get("location") or "").strip()
            key = sub_id or f"{btn}|{loc}"
            if not key or key in seen:
                continue
            seen.add(key)
            total += 1
    return total

