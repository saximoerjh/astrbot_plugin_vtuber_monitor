import asyncio
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from astrbot_plugin_vtuber_monitor.bili_client import (
    BiliClient,
    BiliError,
    BiliRateLimited,
)
from astrbot_plugin_vtuber_monitor.core.models import VtuberState


def good_body():
    return {
        "code": 0,
        "data": {
            "123": {
                "uid": 123,
                "uname": "主播",
                "room_id": 456,
                "live_status": 1,
            }
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cover,keyframe,expected",
    [
        (
            "http://i0.hdslb.com/cover.jpg",
            "https://i0.hdslb.com/frame.jpg",
            "https://i0.hdslb.com/cover.jpg",
        ),
        (None, "https://i0.hdslb.com/frame.jpg", "https://i0.hdslb.com/frame.jpg"),
        ("https://example.com/a.jpg", None, ""),
    ],
)
async def test_live_title_cover_optional_fields(cover, keyframe, expected):
    body = good_body()
    body["data"]["123"].update(
        title="  新直播  ", cover_from_user=cover, keyframe=keyframe
    )
    client = BiliClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body))
    )
    try:
        state = await client.get_live_room_info(123)
        assert state.live_title == "新直播" and state.live_cover == expected
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_metadata_and_close():
    def handler(request):
        assert request.url.params["uids[]"] == "123"
        return httpx.Response(200, json=good_body())

    client = BiliClient(transport=httpx.MockTransport(handler))
    try:
        state = await client.get_user_info(123)
        assert (state.name, state.room_id, state.is_live) == ("主播", 456, True)
    finally:
        await client.close()
    assert client.http.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("name", [None, "", " ", "当前昵称"])
async def test_live_observation_uses_cached_name_only_when_missing(name):
    body = good_body()
    if name is None:
        body["data"]["123"].pop("uname")
    else:
        body["data"]["123"]["uname"] = name
    cache = AsyncMock(return_value=VtuberState(123, "已保存昵称", 789, False))
    client = BiliClient(
        get_cached_state=cache,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body)),
    )
    try:
        state = await client.get_live_room_info(123)
    finally:
        await client.close()
    assert state.is_live and state.room_id == 456
    if name and name.strip():
        assert state.name == name
        cache.assert_not_awaited()
    else:
        assert state.name == "已保存昵称"
        cache.assert_awaited_once_with(123)


@pytest.mark.asyncio
async def test_gif_download_uses_cdn_static_jpeg_and_validates_format():
    jpeg = b"\xff\xd8\xfffixture"

    def handler(request):
        assert request.url.path == "/poster.gif@1600w_90q.jpg"
        assert request.url.params["source"] == "dynamic"
        assert "cookie" not in request.headers
        return httpx.Response(200, headers={"Content-Type": "image/jpeg"}, content=jpeg)

    client = BiliClient(transport=httpx.MockTransport(handler))
    client.set_credentials({"SESSDATA": "fake"})
    try:
        assert (
            await client.download_image(
                "https://i0.hdslb.com/poster.gif?source=dynamic"
            )
            == jpeg
        )
    finally:
        await client.close()


@pytest.mark.parametrize("failure", ["timeout", "500", "json", "missing"])
@pytest.mark.asyncio
async def test_retry_then_recovery(failure):
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if calls > 1:
            return httpx.Response(200, json=good_body())
        if failure == "timeout":
            raise httpx.ReadTimeout("test", request=request)
        if failure == "500":
            return httpx.Response(500)
        if failure == "-352":
            return httpx.Response(200, json={"code": -352})
        if failure == "json":
            return httpx.Response(200, text="not JSON")
        return httpx.Response(200, json={"code": 0})

    client = BiliClient(max_retry=2, transport=httpx.MockTransport(handler))
    try:
        with patch(
            "astrbot_plugin_vtuber_monitor.bili_client.asyncio.sleep",
            new_callable=AsyncMock,
        ) as sleep:
            assert (await client.get_user_info(123)).uid == 123
            sleep.assert_awaited_once_with(1)
        assert calls == 2
    finally:
        await client.close()


@pytest.mark.parametrize(
    "body",
    [
        {"code": 0, "data": {}},
        {"code": 0, "data": {"123": {}}},
        {"code": -101},
        {"code": 0, "data": []},
    ],
)
@pytest.mark.asyncio
async def test_bad_data_rejected(body):
    client = BiliClient(
        max_retry=0,
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=body)),
    )
    try:
        with pytest.raises(BiliError):
            await client.get_user_info(123)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_retry_bounded_and_cancellation_propagates():
    client = BiliClient(
        max_retry=2, transport=httpx.MockTransport(lambda r: httpx.Response(500))
    )
    try:
        with patch(
            "astrbot_plugin_vtuber_monitor.bili_client.asyncio.sleep",
            new_callable=AsyncMock,
        ) as sleep:
            with pytest.raises(BiliError):
                await client.get_user_info(123)
            assert [c.args[0] for c in sleep.await_args_list] == [1, 2]
        with patch(
            "astrbot_plugin_vtuber_monitor.bili_client.asyncio.sleep",
            side_effect=asyncio.CancelledError,
        ):
            with pytest.raises(asyncio.CancelledError):
                await client.get_user_info(123)
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [412, 429, -352, -412, -509])
async def test_rate_limit_stops_retries_and_blocks_other_uid(code):
    calls = 0

    def respond(request):
        nonlocal calls
        calls += 1
        return (
            httpx.Response(code)
            if code > 0
            else httpx.Response(200, json={"code": code})
        )

    client = BiliClient(transport=httpx.MockTransport(respond))
    try:
        with patch(
            "astrbot_plugin_vtuber_monitor.bili_client.time.monotonic", return_value=100
        ):
            with pytest.raises(BiliRateLimited) as error:
                await client.get_user_info(123)
            assert error.value.retry_after == 300
            with pytest.raises(BiliRateLimited):
                await client.get_user_info(456)
            assert calls == 1
        with patch(
            "astrbot_plugin_vtuber_monitor.bili_client.time.monotonic", return_value=401
        ):
            with pytest.raises(BiliRateLimited) as error:
                await client.get_user_info(123)
            assert error.value.retry_after == 600 and calls == 2
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_retry_after_and_success_reset_backoff():
    responses = iter(
        [
            httpx.Response(429, headers={"Retry-After": "900"}),
            httpx.Response(200, json=good_body()),
            httpx.Response(412),
        ]
    )
    client = BiliClient(transport=httpx.MockTransport(lambda request: next(responses)))
    try:
        with patch(
            "astrbot_plugin_vtuber_monitor.bili_client.time.monotonic", return_value=100
        ):
            with pytest.raises(BiliRateLimited) as error:
                await client.get_user_info(123)
            assert error.value.retry_after == 900
        with patch(
            "astrbot_plugin_vtuber_monitor.bili_client.time.monotonic",
            return_value=1001,
        ):
            assert (await client.get_user_info(123)).is_live
            with pytest.raises(BiliRateLimited) as error:
                await client.get_user_info(123)
            assert error.value.retry_after == 300
    finally:
        await client.close()
