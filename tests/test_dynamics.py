import asyncio
import time
from dataclasses import replace
from unittest.mock import AsyncMock

import httpx
import pytest

from astrbot_plugin_vtuber_monitor.bili_client import BiliClient, BiliError
from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import DynamicPost, VtuberState
from astrbot_plugin_vtuber_monitor.services.dynamic_listener import DynamicListener
from astrbot_plugin_vtuber_monitor.services.subscription_service import (
    SubscriptionService,
)


def post(id, uid=1):
    return DynamicPost(uid, str(id), "本周安排", int(time.time()))


def item(id):
    return {
        "id_str": str(id),
        "modules": {
            "module_author": {"mid": 1, "pub_ts": 1700000000},
            "module_dynamic": {
                "desc": {"text": "本周安排"},
                "major": {
                    "draw": {"items": [{"src": "https://i0.hdslb.com/test.jpg"}]}
                },
            },
        },
    }


@pytest.mark.asyncio
async def test_dynamic_api_cookie_order_and_forward():
    def handler(request):
        assert request.url.host == "api.bilibili.com"
        assert request.url.params["host_mid"] == "1"
        assert request.headers["cookie"] == "SESSDATA=fake-test-secret"
        forwarded = item(2)
        forwarded["orig"] = {"text": "不应读取原作者的调播信息"}
        return httpx.Response(
            200, json={"code": 0, "data": {"items": [forwarded, item(1), item(2)]}}
        )

    client = BiliClient(transport=httpx.MockTransport(handler))
    client.set_credentials({"SESSDATA": "fake-test-secret"})
    try:
        posts = await client.get_latest_dynamics(1)
        assert [p.id for p in posts] == ["1", "2"]
        assert posts[1].text == "本周安排"
        assert posts[0].images == ("https://i0.hdslb.com/test.jpg",)
        assert (
            "cookie"
            not in client.http.build_request("GET", "https://example.com").headers
        )
    finally:
        await client.close()


@pytest.mark.parametrize(
    "bad",
    [{}, {"items": None}, {"items": [{}]}, {"items": [dict(item(1), id_str="oops")]}],
)
@pytest.mark.asyncio
async def test_invalid_dynamic_payload_rejected(bad):
    client = BiliClient(
        max_retry=0,
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"code": 0, "data": bad})
        ),
    )
    try:
        with pytest.raises(BiliError):
            await client.get_latest_dynamics(1)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_invalid_credentials_not_retried_or_logged(caplog):
    count = 0

    def handler(request):
        nonlocal count
        count += 1
        return httpx.Response(200, json={"code": -101, "message": "fake-test-secret"})

    client = BiliClient(transport=httpx.MockTransport(handler))
    client.set_credentials({"SESSDATA": "fake-test-secret"})
    try:
        with pytest.raises(BiliError, match="登录凭据") as error:
            await client.get_latest_dynamics(1)
        assert count == 1
        assert "fake-test-secret" not in str(error.value) + caplog.text
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_catchup_pages_until_checkpoint_and_ignores_pinned_boundary():
    calls = []

    def handler(request):
        offset = request.url.params.get("offset")
        calls.append(offset)
        pinned = item(2)
        pinned["modules"]["module_tag"] = {"text": "置顶"}
        data = (
            {"items": [item(15), item(14), pinned], "has_more": True, "offset": "older"}
            if not offset
            else {
                "items": [item(13), item(12), item(10)],
                "has_more": True,
                "offset": "older-again",
            }
        )
        return httpx.Response(200, json={"code": 0, "data": data})

    client = BiliClient(transport=httpx.MockTransport(handler))
    try:
        posts = await client.get_dynamics_since(1, "10")
    finally:
        await client.close()
    assert calls == [None, "older"]
    assert {p.id for p in posts} == {"2", "10", "12", "13", "14", "15"}


@pytest.mark.asyncio
async def test_late_posts_and_edits_enqueue_without_reopening_the_baseline(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    await data.add_subscription(
        VtuberState(1, "one"), "group", "special", initial_dynamics=[post(10)]
    )
    assert [p.id for p in await data.ingest_dynamics(1, [post(15)])] == ["15"]
    old = (await data.pending_adjustments(1))[0]
    changed = replace(post(15), text="改到晚上八点")
    assert [
        p.id for p in await data.ingest_dynamics(1, [post(8), post(12), changed])
    ] == ["12", "15"]
    await data.finish_adjustment(1, "15", True, payload=old["payload"])
    assert [j["dynamic_id"] for j in await data.pending_adjustments(1)] == ["12", "15"]
    assert not await data.ingest_dynamics(1, [changed])


@pytest.mark.asyncio
async def test_special_baseline_new_pages_restart_and_pins(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    client = AsyncMock()
    client.get_user_info.return_value = VtuberState(1, "主播", 10, False)
    client.get_latest_dynamics.return_value = [post(10), post(20)]
    service = SubscriptionService(data, client)
    await service.subscribe(1, "a", "special")
    assert (await data.get_dynamic_checkpoint(1))["latest_id"] == "20"
    listener = DynamicListener(data, client)
    client.get_dynamics_since.return_value = [post(30), post(5), post(21), post(30)]
    await listener.poll_once()
    assert listener.received == 2
    await listener.poll_once()
    assert listener.received == 2
    reopened = DataManager(tmp_path)
    await reopened.initialize()
    other = DynamicListener(reopened, client)
    await other.poll_once()
    assert other.received == 0
    # 其他会话不会重置已建立的检查点。
    await service.subscribe(1, "b", "special")
    assert (await data.get_dynamic_checkpoint(1))["latest_id"] == "30"
    await service.change_level(1, "a", "normal")
    assert await data.get_special_vtubers() == [1]
    await service.unsubscribe(1, "b")
    assert await data.get_special_vtubers() == []
    client.get_latest_dynamics.return_value = [post(40)]
    await service.change_level(1, "a", "special")
    assert (await data.get_dynamic_checkpoint(1))["latest_id"] == "40"


@pytest.mark.asyncio
async def test_empty_baseline_failure_isolation_and_normal_skipped(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    await data.add_subscription(
        VtuberState(1, "one"), "a", "special", initial_dynamics=[]
    )
    await data.add_subscription(
        VtuberState(2, "two"), "a", "special", initial_dynamics=[]
    )
    await data.add_subscription(VtuberState(3, "normal"), "a")
    client = AsyncMock()
    client.get_dynamics_since.side_effect = [BiliError("412"), [post(10, 2)]]
    listener = DynamicListener(data, client)
    await listener.poll_once()
    assert listener.failures == 1 and listener.received == 1
    assert client.get_dynamics_since.await_count == 2
    assert (await data.get_dynamic_checkpoint(1))["latest_id"] == "0"
    assert (await data.get_dynamic_checkpoint(2))["latest_id"] == "10"
    with pytest.raises(ValueError):
        await data.ingest_dynamics(1, [post(100), post(200, 2)])
    assert (await data.get_dynamic_checkpoint(1))["latest_id"] == "0"


@pytest.mark.asyncio
async def test_bootstrap_failure_preserves_existing_subscription(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    state = VtuberState(1, "one")
    await data.add_subscription(state, "a")
    client = AsyncMock()
    client.get_user_info.return_value = state
    client.get_latest_dynamics.side_effect = BiliError("expired")
    service = SubscriptionService(data, client)
    with pytest.raises(BiliError):
        await service.change_level(1, "a", "special")
    assert (await data.get_subscription(1, "a")).level == "normal"
    assert await data.get_dynamic_checkpoint(1) is None


@pytest.mark.asyncio
async def test_first_poll_silent_atomic_dedup_and_cancellation(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    await data.add_subscription(VtuberState(1, "one"), "a", "special")
    assert await data.ingest_dynamics(1, [post(10)]) == []
    results = await asyncio.gather(
        *(data.ingest_dynamics(1, [post(20)]) for _ in range(4))
    )
    assert sum(len(result) for result in results) == 1
    client = AsyncMock()
    entered = asyncio.Event()

    async def hang(uid, latest_id):
        entered.set()
        await asyncio.Event().wait()

    client.get_dynamics_since.side_effect = hang
    listener = DynamicListener(data, client)
    task = asyncio.create_task(listener.run())
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
