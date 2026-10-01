"""V0.4 草案组件的离线测试；尚未接入命令与监听器。"""
from datetime import date

import httpx
import pytest

from astrbot_plugin_vtuber_monitor.bili_client import BiliClient, BiliError
from astrbot_plugin_vtuber_monitor.core.models import DynamicPost
from astrbot_plugin_vtuber_monitor.core.schedule_models import schedule_from_parser
from astrbot_plugin_vtuber_monitor.services.schedule_discovery import ScheduleDiscovery


def payload():
    return {"week_start": "2026-09-28", "streams": [
        {"date": "2026-09-28", "start_time": "20:00", "title": "歌回"}]}


def test_schedule_validation_and_pinned_candidates():
    schedule = schedule_from_parser(1, payload(), date(2026, 9, 28))
    assert schedule.to_dict()["streams"][0]["title"] == "歌回"
    discovery = ScheduleDiscovery(None, None, None, None)
    assert discovery.is_candidate(DynamicPost(1, "2", "置顶图片", 0, ("https://i0.hdslb.com/a.png",), True))
    assert not discovery.is_candidate(DynamicPost(1, "2", "普通图片", 0, ("https://i0.hdslb.com/a.png",)))
    assert not discovery.is_candidate(DynamicPost(1, "2", "周表", 0))


@pytest.mark.parametrize("case", ["empty", "duplicate", "bad_time", "outside_week", "stale"])
def test_invalid_schedule_rejected(case):
    data = payload()
    if case == "empty":
        data["streams"] = []
    elif case == "duplicate":
        data["streams"] *= 2
    elif case == "bad_time":
        data["streams"][0]["start_time"] = "25:30"
    elif case == "outside_week":
        data["streams"][0]["date"] = "2026-10-05"
    else:
        data["week_start"] = "2026-09-21"
    with pytest.raises(ValueError):
        schedule_from_parser(1, data, date(2026, 9, 28))


@pytest.mark.asyncio
async def test_image_download_domain_signature_and_cookies():
    def handler(request):
        assert "cookie" not in request.headers
        return httpx.Response(200, headers={"content-type": "image/png"}, content=b"\x89PNG\r\n\x1a\nfixture")
    client = BiliClient(transport=httpx.MockTransport(handler))
    client.set_credentials({"SESSDATA": "fake"})
    try:
        assert (await client.download_image("http://i0.hdslb.com/a.png")).startswith(b"\x89PNG")
        with pytest.raises(BiliError):
            await client.download_image("https://example.com/a.png")
    finally:
        await client.close()
