import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from astrbot_plugin_vtuber_monitor.bili_client import BiliClient, BiliError
from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.services.login_service import LoginService


def credential():
    return {"SESSDATA": "fake-session", "bili_jct": "fake-csrf", "DedeUserID": "123"}


@pytest.mark.asyncio
async def test_qr_generation_and_statuses():
    replies = [
        {"url": "https://account.bilibili.com/h5/account-h5/auth/scan-web?key=fake", "qrcode_key": "fake"},
        {"code": 86101}, {"code": 86090}, {"code": 86038},
        {"code": 0, "url": "https://passport.bilibili.com/login?SESSDATA=fake-session&bili_jct=fake-csrf&DedeUserID=123"},
    ]
    client = BiliClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"code": 0, "data": replies.pop(0)})))
    try:
        _, key = await client.generate_login_qr()
        for expected in (86101, 86090, 86038):
            assert await client.poll_login_qr(key) == (expected, None)
        assert await client.poll_login_qr(key) == (0, credential())
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_cross_domain_ticket_cookies_and_untrusted_redirect():
    def handler(request):
        if request.url.path.endswith("/poll"):
            return httpx.Response(200, json={"code": 0, "data": {
                "code": 0, "url": "https://passport.biligame.com/crossDomain?ticket=fake"}})
        return httpx.Response(200, headers=[("set-cookie", f"{key}={value}; Path=/") for key, value in credential().items()])
    client = BiliClient(transport=httpx.MockTransport(handler))
    try:
        assert await client.poll_login_qr("fake") == (0, credential())
    finally:
        await client.close()
    client = BiliClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"code": 0, "data": {
        "code": 0, "url": "https://evil.example/crossDomain?ticket=fake"}})))
    try:
        with pytest.raises(BiliError, match="地址无效"):
            await client.poll_login_qr("fake")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_bad_login_response_no_secret_in_error():
    client = BiliClient(max_retry=0, transport=httpx.MockTransport(lambda r: httpx.Response(200, json={
        "code": 0, "data": {"code": 0, "url": "https://passport.bilibili.com/login?SESSDATA=do-not-leak"}})))
    try:
        with pytest.raises(BiliError) as caught:
            await client.poll_login_qr("fake")
        assert "do-not-leak" not in str(caught.value)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_httpx_logs_redact_login_key(caplog):
    caplog.set_level(logging.INFO, logger="httpx")
    client = BiliClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"code": 0, "data": {"code": 86101}})))
    try:
        await client.poll_login_qr("do-not-log-this-key")
        assert "do-not-log-this-key" not in caplog.text
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_login_success_persistence_and_cleanup(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    client = SimpleNamespace(generate_login_qr=AsyncMock(return_value=("https://account.bilibili.com/test", "fake")),
                             poll_login_qr=AsyncMock(side_effect=[(86101, None), (86090, None), (0, credential())]),
                             close=AsyncMock())
    monitor = Mock()
    notify, send_image = AsyncMock(), AsyncMock()
    service = LoginService(data, monitor, notify, client_factory=lambda: client, interval=0)
    await service.start("private", send_image)
    await asyncio.wait_for(service.task, 1)
    assert await data.get_credentials() == credential()
    monitor.set_credentials.assert_called_once_with(credential())
    assert notify.await_count == 2
    assert not list((tmp_path / "login_qr").glob("*.png"))
    assert await DataManager(tmp_path).get_credentials() == credential()
    await service.close()


@pytest.mark.parametrize("state", [86038, "error"])
@pytest.mark.asyncio
async def test_login_expired_or_failed_preserves_credentials(tmp_path, state):
    data = DataManager(tmp_path)
    await data.initialize()
    await data.save_credentials(credential())
    client = SimpleNamespace(generate_login_qr=AsyncMock(return_value=("https://account.bilibili.com/test", "fake")),
                             poll_login_qr=AsyncMock(), close=AsyncMock())
    if state == "error":
        client.poll_login_qr.side_effect = BiliError("failed")
    else:
        client.poll_login_qr.return_value = (state, None)
    monitor = Mock()
    service = LoginService(data, monitor, AsyncMock(), client_factory=lambda: client)
    await service.start("private", AsyncMock())
    await asyncio.wait_for(service.task, 1)
    assert await data.get_credentials() == credential()
    monitor.set_credentials.assert_not_called()
    assert not list((tmp_path / "login_qr").glob("*.png"))


@pytest.mark.asyncio
async def test_login_single_session_cancellation_and_send_failure(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    entered = asyncio.Event()
    async def hang(key):
        entered.set()
        await asyncio.Event().wait()
    client = SimpleNamespace(generate_login_qr=AsyncMock(return_value=("https://account.bilibili.com/test", "fake")),
                             poll_login_qr=hang, close=AsyncMock())
    service = LoginService(data, Mock(), AsyncMock(), client_factory=lambda: client)
    await service.start("private", AsyncMock())
    await asyncio.wait_for(entered.wait(), 1)
    with pytest.raises(BiliError, match="已有"):
        await service.start("another", AsyncMock())
    task = service.task
    await service.close()
    assert task.cancelled()
    assert await data.get_credentials() is None
    assert not list((tmp_path / "login_qr").glob("*.png"))
    service = LoginService(data, Mock(), AsyncMock(), client_factory=lambda: client)
    with pytest.raises(RuntimeError):
        await service.start("private", AsyncMock(side_effect=RuntimeError("send failed")))
    assert not list((tmp_path / "login_qr").glob("*.png"))
