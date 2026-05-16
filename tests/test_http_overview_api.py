import time

from kbrbot.http.api_admin import admin_overview_cache_key
from kbrbot.http.api_root import root_user_detail_cache_key, root_users_cache_key
from kbrbot.http.server import ReadOnlyApiCache, build_json_response


def test_api_key_builders_are_stable():
    assert admin_overview_cache_key() == "admin-api:overview"
    assert root_users_cache_key(" User ") == "root-api:users:user"
    assert root_user_detail_cache_key("@Name") == "root-api:user:@name"


def test_build_json_response_returns_valid_json():
    payload = {"ok": True, "users": 12}
    response = build_json_response(payload, status=200)
    assert response.status == 200
    assert response.parse() == payload


def test_read_only_cache_ttl_and_invalidation():
    cache = ReadOnlyApiCache(enabled=True, ttl_seconds=1)
    cache.set("admin-api::overview::", {"ok": True})
    assert cache.get("admin-api::overview::") == {"ok": True}
    cache.invalidate(("admin-api::",))
    assert cache.get("admin-api::overview::") is None

    cache.set("root-api::users::all", {"rows": 1})
    time.sleep(1.1)
    assert cache.get("root-api::users::all") is None
