from kbrbot.db.repositories import dedup_subscriptions_count


def consistent_totals(records: list[dict]) -> dict[str, int]:
    users_total = len(records)
    subscriptions_total = dedup_subscriptions_count(records)
    return {
        "users_total": users_total,
        "subscriptions_total": subscriptions_total,
    }

