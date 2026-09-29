import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import DynamicPost
from astrbot_plugin_vtuber_monitor.services.schedule_image_selector import ScheduleImageSelector
from astrbot_plugin_vtuber_monitor.services.schedule_parser import ScheduleParseError
from astrbot_plugin_vtuber_monitor.bili_client import BiliClient


@pytest.mark.asyncio
async def test_reuse_needs_date_reject_stale_records_and_cache_classification(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    post = DynamicPost(1, "123", "intro", 0, tuple(f"https://i0.hdslb.com/{i}.png" for i in range(3)), True)
    fingerprint = hashlib.sha256(post.text.encode()).hexdigest()
    await data.save_schedule_candidate({"uid": 1, "dynamic_id": "123", "image_url": post.images[0],
                                        "text_fingerprint": fingerprint, "status": "needs_date"})
    await data.save_schedule_candidate({"uid": 1, "dynamic_id": "123", "image_url": post.images[1],
                                        "text_fingerprint": "old text", "status": "parsed"})
    bili = SimpleNamespace(download_image=AsyncMock(return_value=b"\x89PNGtest"), preview_image_url=BiliClient.preview_image_url)
    parser = SimpleNamespace(provider_id="vision", is_schedule_image=AsyncMock(side_effect=[False, True]))
    selector = ScheduleImageSelector(data, bili, parser)
    assert await selector.select(post) == [post.images[0], post.images[2]]
    assert await selector.select(post) == [post.images[0], post.images[2]]
    assert parser.is_schedule_image.await_count == 2
    assert await data.get_weekly_schedule(1) is None
    assert await data.get_schedule_history(1) == []
    reopened = DataManager(tmp_path)
    await reopened.initialize()
    assert await reopened.get_image_classifications(1, "123", fingerprint) == {post.images[1]: False, post.images[2]: True}


@pytest.mark.asyncio
async def test_missing_model_or_failed_classification_does_not_accept_images(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    post = DynamicPost(1, "123", "intro", 0, ("https://i0.hdslb.com/0.png",), True)
    bili = SimpleNamespace(download_image=AsyncMock(return_value=b"\x89PNGtest"), preview_image_url=BiliClient.preview_image_url)
    parser = SimpleNamespace(provider_id="", is_schedule_image=AsyncMock(side_effect=ScheduleParseError("model_error")))
    selector = ScheduleImageSelector(data, bili, parser)
    assert await selector.select(post) == []
    bili.download_image.assert_not_awaited()
    parser.provider_id = "vision"
    assert await selector.select(post) == []
    assert await data.get_image_classifications(1, "123", hashlib.sha256(post.text.encode()).hexdigest()) == {}
