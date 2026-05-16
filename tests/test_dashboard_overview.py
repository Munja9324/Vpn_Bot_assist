from kbrbot.db.repositories import dedup_subscriptions_count
from kbrbot.features.dashboard_stats import consistent_totals


def test_consistent_totals_uses_deduplicated_subscriptions():
    records = [
        {
            "user_id": "1",
            "subscriptions": [
                {"subscription_id": "sub-1", "button_text": "A", "location": "FI"},
                {"subscription_id": "sub-1", "button_text": "A", "location": "FI"},
            ],
        },
        {
            "user_id": "2",
            "subscriptions": [
                {"subscription_id": "", "button_text": "B", "location": "US"},
                {"subscription_id": "", "button_text": "B", "location": "US"},
                {"subscription_id": "", "button_text": "C", "location": "US"},
            ],
        },
    ]
    totals = consistent_totals(records)
    assert totals["users_total"] == 2
    assert totals["subscriptions_total"] == 3


def test_dedup_subscriptions_count_empty():
    assert dedup_subscriptions_count([]) == 0
