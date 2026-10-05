"""候选原图的保留策略：schedule_images/ 不能无限增长。"""
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import utc_now
from astrbot_plugin_vtuber_monitor.core.schedule_models import china_today


async def candidate(data, *, dynamic_id, url, path, status, week, checked_at):
    await data.save_schedule_candidate({
        "uid": 1, "dynamic_id": dynamic_id, "image_url": url, "status": status,
        "local_image_path": path, "week_start": week, "checked_at": checked_at})


async def archive(data, *, week, path):
    await data.save_weekly_schedule(
        {"uid": 1, "week_start": week, "streams": [], "revision": 1, "local_image_path": path},
        expected=None, source_id="", reason="test")


@pytest.mark.asyncio
async def test_prune_keeps_recent_schedules_and_drops_the_rest(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    today = china_today()
    recent, long_ago = today.isoformat(), (today - timedelta(weeks=8)).isoformat()
    stale = (datetime.fromisoformat(utc_now()) - timedelta(days=8)).isoformat()

    archived = await data.save_schedule_image(1, "100", "u1", b"\x89PNGarchived")
    await archive(data, week=recent, path=archived)
    aged_archive = await data.save_schedule_image(1, "90", "u0", b"\x89PNGold-archive")
    await archive(data, week=long_ago, path=aged_archive)
    parsed = await data.save_schedule_image(1, "101", "u2", b"\x89PNGparsed")
    await candidate(data, dynamic_id="101", url="u2", path=parsed, status="parsed",
                    week=recent, checked_at=stale)
    skipped = await data.save_schedule_image(1, "102", "u3", b"\x89PNGskipped")
    await candidate(data, dynamic_id="102", url="u3", path=skipped, status="skipped",
                    week=recent, checked_at=stale)
    failed = await data.save_schedule_image(1, "103", "u4", b"\x89PNGfailed")
    await candidate(data, dynamic_id="103", url="u4", path=failed, status="failed",
                    week=recent, checked_at=utc_now())          # 宽限期内，先留着
    orphan = tmp_path / "schedule_images" / "orphan.png"
    orphan.write_bytes(b"orphan")

    result = await data.prune_schedule_images()

    remaining = {path.name for path in (tmp_path / "schedule_images").glob("*")}
    assert remaining == {Path(archived).name, Path(parsed).name, Path(failed).name}
    assert result["images"] == 3 and result["freed"] > 0
    # 被删掉的那条候选行不能留悬空路径。
    rows = {row["image_url"]: row for row in await data.get_schedule_candidates(1)}
    assert rows["u3"]["local_image_path"] == "" and rows["u3"]["status"] == "skipped"
    assert rows["u2"]["local_image_path"] == parsed


@pytest.mark.asyncio
async def test_prune_caps_notice_images(tmp_path):
    """通知附带的动态图只按数量保留最近若干张，避免又长成一个无上限目录。"""
    data = DataManager(tmp_path)
    await data.initialize()
    for index in range(4):
        await data.save_notice_image(1, f"dynamic-{index}", f"image-{index}".encode())
    result = await data.prune_schedule_images(keep_notices=2)
    assert result["notices"] == 2
    assert len(list((tmp_path / "notice_images").glob("*"))) == 2


@pytest.mark.asyncio
async def test_prune_keeps_the_image_a_pending_scan_is_waiting_for(tmp_path):
    """待确认周次的任务还要用这张图重试，不能被保留策略当垃圾清掉。"""
    data = DataManager(tmp_path)
    await data.initialize()
    pending = await data.save_schedule_image(1, "100", "u9", b"\x89PNGpending")
    await data.save_schedule_tracking(1, {"week": "2026-09-28",
                                          "pending": {"path": pending, "fingerprint": "f"}})
    orphan = tmp_path / "schedule_images" / "orphan.png"
    orphan.write_bytes(b"orphan")
    result = await data.prune_schedule_images()
    remaining = {path.name for path in (tmp_path / "schedule_images").glob("*")}
    assert remaining == {Path(pending).name}
    assert result["images"] == 1


@pytest.mark.asyncio
async def test_classification_scratch_is_wiped_by_prune(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    scratch = await data.save_classification_image(b"\x89PNGscratch")
    assert Path(scratch).parent.name == "classify_tmp" and Path(scratch).is_file()
    # 临时图不占候选原图目录。
    assert list((tmp_path / "schedule_images").glob("*")) == []
    result = await data.prune_schedule_images()
    assert result["scratch"] == 1 and not Path(scratch).exists()


@pytest.mark.asyncio
async def test_prune_rejects_negative_retention(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    with pytest.raises(ValueError):
        await data.prune_schedule_images(keep_weeks=-1)
