def should_skip_user(user_id: str, seen_users: set[str]) -> bool:
    return str(user_id) in seen_users


def next_user_checkpoint(current_user_id: int, total_users: int) -> int:
    if total_users <= 0:
        return max(1, int(current_user_id))
    return min(total_users + 1, max(1, int(current_user_id)))

