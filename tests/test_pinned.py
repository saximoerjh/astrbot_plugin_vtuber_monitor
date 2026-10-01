import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_vtuber_monitor.bili_client import BiliClient, BiliError
from astrbot_plugin_vtuber_monitor.core.models import DynamicPost
from astrbot_plugin_vtuber_monitor.services.pinned_service import PinnedService
from astrbot_plugin_vtuber_monitor.services.pinned_screenshot import PinnedScreenshot, ScreenshotError


def components(images=("https://i0.hdslb.com/a.png", "https://i0.hdslb.com/b.png")):
    post = DynamicPost(1, "123", "置顶全文", 0, images, True)
    bili = SimpleNamespace(get_pinned_dynamic=AsyncMock(return_value=post),
                           download_image=AsyncMock(), browser_cookies=lambda: [],
                           dynamic_url=BiliClient.dynamic_url, preview_image_url=BiliClient.preview_image_url)
    screenshot = SimpleNamespace(capture=AsyncMock(return_value=b"screenshot"))
    selector = SimpleNamespace(select=AsyncMock(return_value=list(images)))
    return bili, screenshot, PinnedService(bili, screenshot, selector)


@pytest.mark.asyncio
async def test_pinned_order_only_confirmed_schedules_no_trailing_text():
    bili, screenshot, service = components()
    async def download(url):
        if url.endswith("a.png"):
            await asyncio.sleep(0.01)
        return url.encode()
    bili.download_image.side_effect = download
    parts = await service.build(1)
    assert [part.image for part in parts] == [b"screenshot", b"https://i0.hdslb.com/a.png", b"https://i0.hdslb.com/b.png"]
    assert all(not part.text for part in parts)
    screenshot.capture.assert_awaited_once_with(bili.get_pinned_dynamic.return_value, [])
    assert not service._tasks


@pytest.mark.asyncio
async def test_screenshot_failure_and_single_image_failure_are_explicit():
    bili, screenshot, service = components()
    screenshot.capture.side_effect = ScreenshotError("浏览器不可用")
    bili.download_image.side_effect = [BiliError("too large"), b"preview", BiliError("failed"), BiliError("failed")]
    parts = await service.build(1)
    assert "未生成" in parts[0].text and parts[0].image is None
    assert parts[1].image == b"preview" and "压缩预览" in parts[1].text
    assert "图片 2 下载失败" in parts[2].text and parts[2].image is None
    assert len(parts) == 3


@pytest.mark.asyncio
async def test_no_pinned_and_no_images():
    bili, screenshot, service = components(images=())
    parts = await service.build(1)
    assert len(parts) == 1 and parts[0].image == b"screenshot"
    bili.download_image.assert_not_awaited()
    bili.get_pinned_dynamic.return_value = None
    assert await service.build(1) is None
    assert screenshot.capture.await_count == 1


@pytest.mark.asyncio
async def test_pinned_excludes_unrecognized_images():
    bili, screenshot, service = components()
    service.selector.select.return_value = ["https://i0.hdslb.com/b.png"]
    bili.download_image.return_value = b"schedule"
    parts = await service.build(1)
    assert [part.image for part in parts] == [b"screenshot", b"schedule"]
    bili.download_image.assert_awaited_once_with("https://i0.hdslb.com/b.png")
    service.selector.select.return_value = []
    assert len(await service.build(1)) == 1


@pytest.mark.asyncio
async def test_shutdown_cancels_screenshot_and_rejects_new_requests():
    bili, screenshot, service = components()
    entered, closed = asyncio.Event(), asyncio.Event()
    async def capture(*args):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()
    screenshot.capture.side_effect = capture
    task = asyncio.create_task(service.build(1))
    await entered.wait()
    await service.close()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()
    with pytest.raises(BiliError, match="停止"):
        await service.build(1)


@pytest.mark.asyncio
async def test_screenshot_timeout_import_error_and_cancel_do_not_leak_details():
    screenshot = PinnedScreenshot(timeout=0.01)
    screenshot._capture = AsyncMock(side_effect=RuntimeError("fake-sensitive-browser-error"))
    with pytest.raises(ScreenshotError) as error:
        await screenshot.capture(None, [])
    assert "fake-sensitive" not in str(error.value)
    screenshot._capture.side_effect = ImportError("missing")
    with pytest.raises(ScreenshotError, match="playwright"):
        await screenshot.capture(None, [])
    screenshot._capture.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await screenshot.capture(None, [])
    async def hang(*args):
        await asyncio.Event().wait()
    screenshot._capture.side_effect = hang
    with pytest.raises(ScreenshotError, match="超时"):
        await screenshot.capture(None, [])


@pytest.mark.asyncio
async def test_browser_cookies_are_domain_scoped_and_preview_restricted():
    bili = BiliClient()
    bili.set_credentials({"SESSDATA": "fake"})
    try:
        bili.http.cookies.set("foreign", "secret", domain="example.com")
        cookies = bili.browser_cookies()
        assert len(cookies) == 1 and cookies[0]["name"] == "SESSDATA"
        assert cookies[0]["domain"] == ".bilibili.com"
        assert BiliClient.preview_image_url("http://i0.hdslb.com/a.png") == "https://i0.hdslb.com/a.png@1600w_90q.webp"
        with pytest.raises(BiliError):
            BiliClient.preview_image_url("https://example.com/a.png")
    finally:
        await bili.close()
