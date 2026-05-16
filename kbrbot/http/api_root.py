def root_users_cache_key(query: str) -> str:
    return f"root-api:users:{query.strip().casefold()}"


def root_user_detail_cache_key(lookup: str) -> str:
    return f"root-api:user:{lookup.strip().casefold()}"

