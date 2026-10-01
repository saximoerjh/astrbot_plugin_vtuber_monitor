"""空间资料：抓取、去重下载、失败保留旧图与 data URI 组装。"""
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_vtuber_monitor.bili_client import BiliError
from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.schedule_models import china_today
from astrbot_plugin_vtuber_monitor.services.profile_service import (
    PROFILE_TTL_DAYS, PROFILE_VERSION, SWEEP_AT, CHINA, ProfileService, image_mime,
    is_fresh, next_sweep, rendition_url)

AVATAR = "https://i1.hdslb.com/bfs/face/abc.jpg@128w_128h_1c_1s.webp"
HEADER = "https://i0.hdslb.com/bfs/space/def.jpg@3840w_400h_1c_100q.webp"
PNG = b"\x89PNG\r\n\x1a\n" + b"avatar-bytes"
WEBP = b"RIFF" + b"\x00" * 4 + b"WEBP" + b"header-bytes"


class FakeFetcher:
    def __init__(self, assets):
        self.assets = assets
        self.calls = []

    async def fetch(self, uids):
        self.calls.append(list(uids))
        # 与真实抓取一致：页面读成功带 ok，失败只回 ok=False。
        return {uid: ({"ok": True, **self.assets[uid]} if self.assets.get(uid) else {"ok": False})
                for uid in uids}


async def boot(tmp_path, assets):
    data = DataManager(tmp_path)
    await data.initialize()
    fetcher = FakeFetcher(assets)
    bili = AsyncMock()
    bili.download_image = AsyncMock(side_effect=lambda url: PNG if "face" in url else WEBP)
    service = ProfileService(data, bili, fetcher=fetcher)
    return service, bili, fetcher


def test_rendition_url_forces_a_small_known_size_and_cdn_host():
    assert rendition_url(AVATAR, "avatar").endswith("/bfs/face/abc.jpg@256w_256h_1c_1s.webp")
    assert rendition_url(HEADER, "header").endswith("/bfs/space/def.jpg@1680w_350h_1c_80q.webp")
    for bad in ("http://i0.hdslb.com/bfs/space/a.jpg", "https://evil.com/a.jpg",
                "https://i0.hdslb.com:8443/bfs/space/a.jpg"):
        with pytest.raises(BiliError):
            rendition_url(bad, "header")


def test_image_mime_sniffs_supported_formats():
    assert image_mime(PNG) == "image/png"
    assert image_mime(WEBP) == "image/webp"
    assert image_mime(b"\xff\xd8\xffabc") == "image/jpeg"
    assert image_mime(b"GIF89a") == ""


def test_is_fresh_uses_the_ttl_and_treats_damaged_records_as_stale():
    today = date(2026, 9, 30)
    assert is_fresh({"fetched_on": today.isoformat()}, today)
    assert is_fresh({"fetched_on": (today - timedelta(days=PROFILE_TTL_DAYS - 1)).isoformat()}, today)
    assert not is_fresh({"fetched_on": (today - timedelta(days=PROFILE_TTL_DAYS)).isoformat()}, today)
    for broken in (None, {}, {"fetched_on": ""}, {"fetched_on": "昨天"}, {"fetched_on": 20260930}):
        assert not is_fresh(broken, today), broken


def test_next_sweep_targets_early_morning_and_rolls_to_tomorrow():
    assert next_sweep(datetime(2026, 9, 30, 0, 10, tzinfo=CHINA)) == \
        datetime.combine(date(2026, 9, 30), SWEEP_AT, CHINA)
    assert next_sweep(datetime(2026, 9, 30, 4, 29, tzinfo=CHINA)) == \
        datetime.combine(date(2026, 9, 30), SWEEP_AT, CHINA)
    # 正好到点或已过点都排到第二天，避免同一天扫两遍。
    assert next_sweep(datetime.combine(date(2026, 9, 30), SWEEP_AT, CHINA)) == \
        datetime.combine(date(2026, 10, 1), SWEEP_AT, CHINA)
    assert next_sweep(datetime(2026, 9, 30, 23, 0, tzinfo=CHINA)) == \
        datetime.combine(date(2026, 10, 1), SWEEP_AT, CHINA)


@pytest.mark.asyncio
async def test_refresh_stores_assets_and_banner_returns_data_uris(tmp_path):
    service, _, _ = await boot(tmp_path, {1: {"avatar": AVATAR, "header": HEADER}})
    record = await service.refresh(1)
    assert record["fetched_on"] and record["errors"] == []
    assert Path(record["avatar"]).suffix == ".png" and Path(record["header"]).suffix == ".webp"
    banner = await service.banner(1)
    assert banner["avatar"].startswith("data:image/png;base64,")
    assert banner["header"].startswith("data:image/webp;base64,")


@pytest.mark.asyncio
async def test_ensure_does_not_refetch_when_assets_exist(tmp_path):
    service, bili, fetcher = await boot(tmp_path, {1: {"avatar": AVATAR, "header": HEADER}})
    await service.ensure(1)
    await service.ensure(1)
    assert len(fetcher.calls) == 1
    # 地址没变时不会重复下载同一个文件。
    await service.refresh(1)
    assert bili.download_image.await_count == 2


@pytest.mark.asyncio
async def test_failed_fetch_keeps_previous_images_and_skips_same_day_retry(tmp_path):
    service, _, fetcher = await boot(tmp_path, {1: {"avatar": AVATAR, "header": HEADER}})
    await service.refresh(1)
    before = await service.load(1)
    fetcher.assets = {}
    await service.refresh(1)
    after = await service.load(1)
    assert after["avatar"] == before["avatar"] and after["header"] == before["header"]
    assert "avatar_missing" in after["errors"] and "header_missing" in after["errors"]
    # 当天已经试过而且手上有图，就不再重试抓取。
    calls = len(fetcher.calls)
    await service.ensure(1)
    assert len(fetcher.calls) == calls
    assert set(await service.banner(1)) == {"avatar", "header"}


@pytest.mark.asyncio
async def test_ensure_fetches_once_when_nothing_is_cached(tmp_path):
    service, bili, fetcher = await boot(tmp_path, {1: {"avatar": AVATAR, "header": HEADER}})
    assert await service.banner(1)
    assert len(fetcher.calls) == 1
    assert bili.download_image.await_count == 2


@pytest.mark.asyncio
async def test_download_failure_is_isolated_and_reported(tmp_path):
    service, bili, _ = await boot(tmp_path, {1: {"avatar": AVATAR, "header": HEADER}})
    bili.download_image = AsyncMock(side_effect=BiliError("CDN 拒绝"))
    record = await service.refresh(1)
    assert not record.get("avatar")
    assert "avatar_download_failed" in record["errors"]
    assert await service.banner(1) == {}


@pytest.mark.asyncio
async def test_refresh_all_uses_one_batch_and_isolates_failures(tmp_path):
    service, _, fetcher = await boot(tmp_path, {1: {"avatar": AVATAR, "header": HEADER}})
    assert await service.refresh_all([1, 2]) == 2
    assert fetcher.calls == [[1, 2]]
    assert (await service.load(1))["header"]
    assert "header_missing" in (await service.load(2))["errors"]


@pytest.mark.asyncio
async def test_refresh_stale_skips_fresh_entries_without_a_browser(tmp_path):
    service, _, fetcher = await boot(tmp_path, {1: {"avatar": AVATAR, "header": HEADER},
                                               2: {"avatar": AVATAR, "header": HEADER}})
    await service.refresh_all([1, 2])
    fetcher.calls.clear()
    # 两个都还在 TTL 内：扫描必须一个都不抓，也不开浏览器。
    assert await service.refresh_stale([1, 2]) == 0
    assert fetcher.calls == []


@pytest.mark.asyncio
async def test_refresh_stale_only_picks_missing_or_expired_entries(tmp_path):
    service, _, fetcher = await boot(tmp_path, {1: {"avatar": AVATAR, "header": HEADER},
                                               2: {"avatar": AVATAR, "header": HEADER}})
    await service.refresh_all([1, 2])
    # 让 2 号整体过期（最后一次成功与最后一次尝试都在 TTL 之外），3 号从未抓过。
    record = await service.load(2)
    record["fetched_on"] = (china_today() - timedelta(days=PROFILE_TTL_DAYS)).isoformat()
    record["attempted_on"] = record["fetched_on"]
    _, path = service._paths(2)
    path.write_text(json.dumps(record), encoding="utf-8")
    fetcher.calls.clear()
    assert await service.refresh_stale([1, 2, 3]) == 2
    assert fetcher.calls == [[2, 3]]


@pytest.mark.asyncio
async def test_failed_page_read_does_not_advance_the_ttl(tmp_path):
    service, _, fetcher = await boot(tmp_path, {1: {"avatar": AVATAR, "header": HEADER}})
    await service.refresh(1)
    fresh = await service.load(1)
    fetcher.assets = {}
    await service.refresh(1)
    after = await service.load(1)
    assert after["fetched_on"] == fresh["fetched_on"]
    # 把最后一次成功与最后一次尝试都挪到 TTL 之外，扫描必须再次尝试。
    after["attempted_on"] = (china_today() - timedelta(days=1)).isoformat()
    after["fetched_on"] = (china_today() - timedelta(days=PROFILE_TTL_DAYS + 1)).isoformat()
    _, path = service._paths(1)
    path.write_text(json.dumps(after), encoding="utf-8")
    fetcher.calls.clear()
    assert await service.should_refresh(1) is True
    assert await service.refresh_stale([1]) == 1
    assert fetcher.calls == [[1]]


@pytest.mark.asyncio
async def test_should_refresh_skips_a_record_that_failed_today(tmp_path):
    service, _, _ = await boot(tmp_path, {})
    await service.refresh(1)
    record = await service.load(1)
    assert not record.get("avatar") and record["attempted_on"] == china_today().isoformat()
    assert await service.should_refresh(1) is False
    # 从来没有记录的主播仍然要抓。
    assert await service.should_refresh(9) is True


@pytest.mark.asyncio
async def test_older_record_version_forces_one_refetch(tmp_path):
    """提取逻辑升级后，久缓存里“没找到头图”的旧结论必须能自愈。"""
    service, _, fetcher = await boot(tmp_path, {1: {"avatar": AVATAR, "header": HEADER}})
    await service.refresh(1)
    record = await service.load(1)
    assert record["version"] == PROFILE_VERSION
    assert await service.should_refresh(1) is False
    record.pop("header")
    record["header_url"] = ""
    record["version"] = PROFILE_VERSION - 1
    _, path = service._paths(1)
    path.write_text(json.dumps(record), encoding="utf-8")
    assert await service.should_refresh(1) is True
    fetcher.calls.clear()
    await service.refresh_stale([1])
    assert fetcher.calls == [[1]]
    back = await service.load(1)
    assert back["header"] and back["version"] == PROFILE_VERSION
