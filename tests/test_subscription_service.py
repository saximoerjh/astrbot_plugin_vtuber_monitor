import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_vtuber_monitor.bili_client import BiliError
from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import FollowLevel, Subscription, VtuberState, validate_uid
from astrbot_plugin_vtuber_monitor.services.subscription_service import SubscriptionService


@pytest.mark.parametrize("uid", [0, -1, True, "", "1.0", "abc", "1 OR 1=1", 2**63])
def test_bad_uid(uid):
    with pytest.raises(ValueError):
        validate_uid(uid)


@pytest.mark.asyncio
async def test_crud_restart_and_session_isolation(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    client = AsyncMock()
    client.get_user_info.return_value = VtuberState(123, "主播", 456, False)
    service = SubscriptionService(data, client)
    await service.subscribe("123", "qq:GroupMessage:a")
    first = await data.get_subscription(123, "qq:GroupMessage:a")
    await service.subscribe("123", "qq:GroupMessage:a")
    await service.subscribe("123", "qq:GroupMessage:b")
    assert len(await data.get_subscriptions_by_uid(123)) == 2
    assert (await data.get_subscription(123, first.umo)).created_at == first.created_at
    assert Subscription(**json.loads(json.dumps(first.to_dict()))) == first
    reopened = DataManager(tmp_path)
    await reopened.initialize()
    assert len(await reopened.get_subscriptions_by_uid(123)) == 2
    assert (await reopened.get_vtuber_state(123)).name == "主播"
    assert await service.unsubscribe(123, first.umo)
    assert not await service.unsubscribe(123, first.umo)
    assert len(await service.list_subscriptions("qq:GroupMessage:b")) == 1


@pytest.mark.asyncio
async def test_failure_does_not_write(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    client = AsyncMock()
    client.get_user_info.side_effect = BiliError("timeout")
    service = SubscriptionService(data, client)
    with pytest.raises(BiliError):
        await service.subscribe(123, "umo")
    assert await data.get_subscription(123, "umo") is None
    assert await data.get_vtuber_state(123) is None
    for uid, umo, level in [(0, "umo", "normal"), (123, "", "normal"), (123, "umo", "invalid")]:
        with pytest.raises(ValueError):
            await service.subscribe(uid, umo, level)
    assert client.get_user_info.await_count == 1


@pytest.mark.asyncio
async def test_concurrent_upserts_and_level_roundtrip(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    state = VtuberState(123, "主播", 456, False)
    await asyncio.gather(*(data.add_subscription(state, "umo") for _ in range(10)))
    assert len(await data.get_subscriptions_by_uid(123)) == 1
    await data.add_subscription(VtuberState(123, "新名称", 456, True), "umo", FollowLevel.SPECIAL)
    assert await data.get_special_vtubers() == [123]
    assert (await data.get_vtuber_state(123)).is_live is False
    await data.add_subscription(state, "umo", FollowLevel.NORMAL)
    assert await data.get_special_vtubers() == []
