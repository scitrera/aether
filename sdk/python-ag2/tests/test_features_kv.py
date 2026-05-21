"""Unit tests for AetherKVNamespace (mock-only, no aetherlite required)."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from scitrera_aether_ag2.features.kv import AetherKVNamespace


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_kv_response(*, success: bool = True, value: bytes = b"", keys: list[str] | None = None, counter_value: int = 0) -> MagicMock:
    resp = MagicMock()
    resp.success = success
    resp.value = value
    resp.keys = keys or []
    resp.counter_value = counter_value
    return resp


def _make_client(*, get_resp=None, put_resp=None, delete_resp=None, list_resp=None,
                 increment_resp=None, decrement_resp=None) -> AsyncMock:
    client = AsyncMock()
    client.kv_get.return_value = get_resp
    client.kv_put.return_value = put_resp
    client.kv_delete.return_value = delete_resp
    client.kv_list.return_value = list_resp
    client.kv_increment.return_value = increment_resp
    client.kv_decrement.return_value = decrement_resp
    return client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_put_get_roundtrip_dict():
    value = {"model": "gpt-4o", "temperature": 0.7}
    encoded = json.dumps(value).encode()
    client = _make_client(
        put_resp=_make_kv_response(success=True),
        get_resp=_make_kv_response(success=True, value=encoded),
    )
    ns = AetherKVNamespace(client, scope="user-workspace", workspace="ws1")
    await ns.put("config", value)
    result = await ns.get("config")
    assert result == value
    client.kv_put.assert_awaited_once_with("config", encoded, scope="user-workspace", workspace="ws1")
    client.kv_get.assert_awaited_once_with("config", scope="user-workspace", workspace="ws1")


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [
    [1, 2, 3],
    "hello world",
    42,
])
async def test_put_get_roundtrip_types(value):
    encoded = json.dumps(value).encode()
    client = _make_client(
        put_resp=_make_kv_response(success=True),
        get_resp=_make_kv_response(success=True, value=encoded),
    )
    ns = AetherKVNamespace(client, scope="global")
    await ns.put("k", value)
    assert await ns.get("k") == value


@pytest.mark.asyncio
async def test_get_missing_returns_default():
    client = _make_client(get_resp=_make_kv_response(success=False, value=b""))
    ns = AetherKVNamespace(client, scope="global")
    result = await ns.get("nonexistent", default="sentinel")
    assert result == "sentinel"


@pytest.mark.asyncio
async def test_get_none_response_returns_default():
    client = _make_client(get_resp=None)
    ns = AetherKVNamespace(client, scope="global")
    assert await ns.get("k", default=99) == 99


@pytest.mark.asyncio
async def test_list_returns_keys():
    client = _make_client(
        list_resp=_make_kv_response(success=True, keys=["alpha", "beta", "gamma"]),
    )
    ns = AetherKVNamespace(client, scope="workspace", workspace="myws")
    keys = await ns.list()
    assert keys == ["alpha", "beta", "gamma"]
    client.kv_list.assert_awaited_once_with("", scope="workspace", workspace="myws")


@pytest.mark.asyncio
async def test_list_filters_by_namespace_prefix():
    """When namespace has a prefix, list() returns keys with that prefix stripped."""
    stored_keys = ["ns:key1", "ns:key2", "other:key3"]
    client = _make_client(
        list_resp=_make_kv_response(success=True, keys=stored_keys),
    )
    ns = AetherKVNamespace(client, scope="global", prefix="ns:")
    keys = await ns.list()
    assert keys == ["key1", "key2"]


@pytest.mark.asyncio
async def test_delete_then_get_returns_default():
    client = _make_client(
        delete_resp=_make_kv_response(success=True),
        get_resp=_make_kv_response(success=False, value=b""),
    )
    ns = AetherKVNamespace(client, scope="user", workspace="")
    await ns.delete("gone")
    result = await ns.get("gone", default="missing")
    assert result == "missing"
    client.kv_delete.assert_awaited_once_with("gone", scope="user")


@pytest.mark.asyncio
async def test_invalid_scope_raises():
    client = AsyncMock()
    with pytest.raises(ValueError, match="invalid scope"):
        AetherKVNamespace(client, scope="bad_scope")


@pytest.mark.asyncio
async def test_increment_returns_new_value():
    client = _make_client(increment_resp=_make_kv_response(success=True, counter_value=5))
    ns = AetherKVNamespace(client, scope="global")
    result = await ns.increment("counter")
    assert result == 5


@pytest.mark.asyncio
async def test_decrement_returns_new_value():
    client = _make_client(decrement_resp=_make_kv_response(success=True, counter_value=3))
    ns = AetherKVNamespace(client, scope="global")
    result = await ns.decrement("counter")
    assert result == 3


@pytest.mark.asyncio
async def test_aiter_yields_all_keys():
    client = _make_client(
        list_resp=_make_kv_response(success=True, keys=["a", "b", "c"]),
    )
    ns = AetherKVNamespace(client, scope="global")
    collected = [k async for k in ns]
    assert collected == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_prefix_applied_to_put_and_get():
    encoded = json.dumps("val").encode()
    client = _make_client(
        put_resp=_make_kv_response(success=True),
        get_resp=_make_kv_response(success=True, value=encoded),
    )
    ns = AetherKVNamespace(client, scope="global", prefix="ag2:")
    await ns.put("mykey", "val")
    client.kv_put.assert_awaited_once_with("ag2:mykey", encoded, scope="global")
    await ns.get("mykey")
    client.kv_get.assert_awaited_once_with("ag2:mykey", scope="global")
