"""定时扫描：时间点调度、扫描范围、基准建立与更新落位。"""
import asyncio
from dataclasses import replace
from datetime import datetime, time, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import DynamicPost, FollowLevel, VtuberState
from astrbot_plugin_vtuber_monitor.core.schedule_models import china_today, schedule_from_parser
from astrbot_plugin_vtuber_monitor.services.schedule_service import ScheduleService
from astrbot_plugin_vtuber_monitor.services.schedule_watch import (
    CHINA, ScheduleWatch, latest_due_slot, monday, next_scan_at, parse_scan_times)

TIMES = ["00:30", "12:30", "20:30"]
URL = "https://i0.hdslb.com/week.png"


def at(day, clock):
    hour, minute = (int(part) for part in clock.split(":"))
    return datetime.combine(day, time(hour, minute), CHINA)


async def setup(tmp_path, *, offset=-1, remember=True, times=TIMES, auto_parse_normal=False):
    data = DataManager(tmp_path)
    await data.initialize()
    start = monday(china_today()) + timedelta(weeks=offset)
    post = DynamicPost(1, "100", "周表", 1, (URL,), True)

    def parsed(week, title="直播"):
        payload = {"week_start": week.isoformat(),
                   "streams": [{"date": week.isoformat(), "start_time": "20:00", "title": title}]}
        return replace(schedule_from_parser(1, payload, allow_history=True),
                       source_dynamic_id="100", source_image_url=URL)

    parser = SimpleNamespace(
        provider_id="vision",
        parse=AsyncMock(return_value=parsed(start + timedelta(days=7))),
        is_schedule_image=AsyncMock(return_value=True))
    bili = SimpleNamespace(get_latest_dynamics=AsyncMock(return_value=[post]),
                           download_image=AsyncMock(return_value=b"\x89PNGnew"))
    service = ScheduleService(data)
    discovery = SimpleNamespace(
        data=data, parser=parser, bili=bili, schedules=service, _lock=asyncio.Lock(),
        is_candidate=lambda item: bool(item.images) and (item.is_pinned or "周表" in item.text),
        prune_images=AsyncMock(return_value={"images": 0, "scratch": 0, "freed": 0}))
    watch = ScheduleWatch(discovery, scan_times=times, auto_parse_normal=auto_parse_normal)
    initial = parsed(start)
    await service.store_parsed_schedule(initial)
    if remember:
        await watch.remember(post, initial, b"\x89PNGold")
    return watch, discovery, start, parsed


def test_scan_times_and_slots():
    times = parse_scan_times(["20:30", "00:30", "12:30", "00:30"])
    assert [item.strftime("%H:%M") for item in times] == ["00:30", "12:30", "20:30"]
    assert parse_scan_times([]) == ()
    for bad in ("25:00", "1:2", "", None if False else "abc"):
        with pytest.raises(ValueError):
            parse_scan_times([bad])
    assert latest_due_slot(at(china_today(), "00:10"), times) is None
    assert latest_due_slot(at(china_today(), "13:00"), times) == f"{china_today().isoformat()}T12:30"
    assert next_scan_at(at(china_today(), "13:00"), times) == at(china_today(), "20:30")
    assert next_scan_at(at(china_today(), "23:00"), times) == at(china_today() + timedelta(days=1), "00:30")
    assert next_scan_at(at(china_today(), "13:00"), ()) is None


def test_select_posts_keeps_pinned_first_and_limits_recent():
    discovery = SimpleNamespace(data=None, is_candidate=lambda item: bool(item.images))
    watch = ScheduleWatch(discovery, scan_times=TIMES)
    posts = [DynamicPost(1, str(index), "x", index, (URL,), index == 3) for index in range(1, 10)]
    selected = watch.select_posts(posts)
    assert [item.id for item in selected] == ["3", "9", "8", "7", "6"]


@pytest.mark.asyncio
async def test_scan_runs_once_per_slot_and_not_before_the_first_slot(tmp_path):
    watch, d, day, _ = await setup(tmp_path)
    await watch.check(1, now=at(day, "00:10"))
    assert d.bili.get_latest_dynamics.await_count == 0
    await watch.check(1, now=at(day, "12:45"))
    assert d.bili.get_latest_dynamics.await_count == 1
    # 同一个时间点不重复扫描，重启（新建 watch）也一样。
    await watch.check(1, now=at(day, "13:00"))
    await ScheduleWatch(d, scan_times=TIMES).check(1, now=at(day, "13:30"))
    assert d.bili.get_latest_dynamics.await_count == 1
    await watch.check(1, now=at(day, "20:45"))
    assert d.bili.get_latest_dynamics.await_count == 2


@pytest.mark.asyncio
async def test_special_without_baseline_scans_but_normal_waits_for_manual(tmp_path):
    watch, d, day, _ = await setup(tmp_path, remember=False)
    d.scan = AsyncMock()
    await watch.check(1, now=at(day, "12:45"), special=True)
    d.scan.assert_awaited_once_with(1)
    d.scan.reset_mock()
    await watch.check(2, now=at(day, "12:45"), special=False)
    d.scan.assert_not_awaited()
    # 开了普通关注自动解析后同样会扫描。
    watch.auto_parse_normal = True
    await watch.check(2, now=at(day, "12:45"), special=False)
    d.scan.assert_awaited_once_with(2)


@pytest.mark.asyncio
async def test_changed_image_advances_the_week(tmp_path):
    watch, d, start, _ = await setup(tmp_path)
    await watch.check(1, now=at(start, "12:45"))
    # 上周周表发现新图 → 落位到下一周（本周）。
    expected = (start + timedelta(days=7)).isoformat()
    assert d.parser.parse.await_args.kwargs["week_start"] == expected
    state = await d.data.get_schedule_tracking(1)
    assert state["week"] == expected and state["pending"] is None
    assert await d.data.get_historical_schedule(1, expected)


@pytest.mark.asyncio
async def test_second_change_in_the_same_week_uses_update_context(tmp_path):
    watch, d, start, parsed = await setup(tmp_path)
    await watch.check(1, now=at(start, "12:45"))
    d.bili.download_image.return_value = b"\x89PNGsecond"
    current = start + timedelta(days=7)
    d.parser.parse.return_value = parsed(current, "修订")
    await watch.check(1, now=at(start, "20:45"))
    options = d.parser.parse.await_args.kwargs
    assert "week_start" not in options
    assert options["update_context"]["updates_this_week"] == 2
    stored = await d.data.get_historical_schedule(1, current.isoformat())
    assert stored["streams"][0]["title"] == "修订"


@pytest.mark.asyncio
async def test_failed_parse_keeps_baseline_and_retries_next_slot(tmp_path):
    watch, d, start, _ = await setup(tmp_path)
    d.parser.parse.side_effect = ValueError("ambiguous")
    await watch.check(1, now=at(start, "12:45"))
    state = await d.data.get_schedule_tracking(1)
    assert state["error"] and state["week"] == start.isoformat()
    kept = await d.data.get_historical_schedule(1, start.isoformat())
    assert kept["streams"][0]["title"] == "直播"
    d.parser.parse.side_effect = None
    await watch.check(1, now=at(start, "20:45"))
    assert (await d.data.get_schedule_tracking(1))["pending"] is None


@pytest.mark.asyncio
async def test_non_schedule_image_is_classified_once(tmp_path):
    watch, d, day, _ = await setup(tmp_path)
    other = DynamicPost(1, "101", "周表补充", 2, ("https://i0.hdslb.com/art.png",), False)
    d.bili.get_latest_dynamics.return_value = [d.bili.get_latest_dynamics.return_value[0], other]
    d.bili.download_image.side_effect = lambda url: b"\x89PNGold" if url == URL else b"\x89PNGart"
    d.parser.is_schedule_image.return_value = False
    await watch.check(1, now=at(day, "12:45"))
    assert d.parser.is_schedule_image.await_count == 1
    assert (await d.data.get_schedule_candidates(1))[0]["status"] == "skipped"
    # 不是周表的配图不再落成候选原图，只在临时目录里判定。
    assert list((tmp_path / "schedule_images").glob("*")) == []
    # 下一个时间点：已判定过的图不再下载也不再分类。
    await watch.check(1, now=at(day, "20:45"))
    assert d.parser.is_schedule_image.await_count == 1
    assert d.bili.download_image.await_count == 3


@pytest.mark.asyncio
async def test_new_weekly_image_in_a_recent_post_is_adopted(tmp_path):
    watch, d, day, _ = await setup(tmp_path)
    fresh = DynamicPost(1, "102", "本周周表", 3, ("https://i0.hdslb.com/new-week.png",), False)
    d.bili.get_latest_dynamics.return_value = [d.bili.get_latest_dynamics.return_value[0], fresh]
    d.bili.download_image.side_effect = lambda url: b"\x89PNGold" if url == URL else b"\x89PNGweek"
    await watch.check(1, now=at(day, "12:45"))
    state = await d.data.get_schedule_tracking(1)
    assert state["dynamic_id"] == "102" and state["image_url"].endswith("new-week.png")


@pytest.mark.asyncio
async def test_run_once_covers_specials_and_only_manually_parsed_normals(tmp_path):
    watch, d, start, parsed = await setup(tmp_path, remember=False)
    await d.data.add_subscription(VtuberState(1, "特别", 10), "a", FollowLevel.SPECIAL)
    await d.data.add_subscription(VtuberState(2, "普通", 11), "a")
    await d.data.add_subscription(VtuberState(3, "已解析", 12), "a")
    # 3 号是普通关注，但手动解析过（有基准）→ 也要检查。
    await watch.remember(DynamicPost(3, "300", "周表", 1, (URL,), True), parsed(start), b"\x89PNGold")
    d.scan = AsyncMock()
    d.bili.get_latest_dynamics = AsyncMock(return_value=[])
    await watch.run_once(now=at(start, "12:45"))
    assert [call.args[0] for call in d.scan.await_args_list] == [1]
