from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import VtuberState
from astrbot_plugin_vtuber_monitor.core.schedule_models import china_today, schedule_from_parser
from astrbot_plugin_vtuber_monitor.services.live_schedule import LiveScheduleRecorder
from astrbot_plugin_vtuber_monitor.services.schedule_display import format_stream
from astrbot_plugin_vtuber_monitor.services.schedule_service import ScheduleService

CHINA = timezone(timedelta(hours=8))


def monday():
    today = china_today()
    return today - timedelta(days=today.weekday())


def at(day, hour, minute=0):
    return datetime.fromisoformat(f"{day.isoformat()}T{hour:02d}:{minute:02d}:00").replace(tzinfo=CHINA)


def poster(day, slots, dynamic_id="900"):
    payload = {"week_start": monday().isoformat(),
               "streams": [{"date": day.isoformat(), "start_time": start, "end_time": end, "title": title}
                           for start, end, title in slots]}
    return replace(schedule_from_parser(1, payload), source_dynamic_id=dynamic_id,
                   source_image_url=f"https://i0.hdslb.com/{dynamic_id}.png")


async def boot(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    recorder = LiveScheduleRecorder(data)
    return data, ScheduleService(data, reconciler=recorder), recorder


async def observe(data, start, end, title=""):
    await data.save_vtuber_state(
        VtuberState(1, "主播", 10, True, live_title=title, live_started_at=start.isoformat()))
    with patch("astrbot_plugin_vtuber_monitor.core.data_manager.utc_now", return_value=end.isoformat()):
        await data.save_vtuber_state(VtuberState(1, "主播", 10, False))


def planned(schedule):
    return [stream for stream in schedule["streams"] if stream["source"] != "live_observation"]


def extras(schedule):
    return [stream for stream in schedule["streams"] if stream["source"] == "live_observation"]


@pytest.mark.asyncio
async def test_poster_stored_after_the_stream_backfills_it(tmp_path):
    data, service, _ = await boot(tmp_path)
    day = monday()
    await observe(data, at(day, 8), at(day, 10))
    # 观测与周表之间没有发生轮询，回填完全由导入触发。
    assert await service.store_parsed_schedule(poster(day, [("08:00", "10:00", "早播")])) == "parsed"
    schedule = await service.get_weekly_schedule(1)
    stream = planned(schedule)[0]
    assert stream["status"] == "completed"
    assert [item["start"] for item in stream["actual_intervals"]] == [at(day, 8).isoformat()]
    assert extras(schedule) == []
    assert await data.pending_live_sessions(1) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("minutes,matched", [(-60, True), (-61, False), (60, True), (61, False)])
async def test_tolerance_is_one_hour_each_side(tmp_path, minutes, matched):
    data, service, _ = await boot(tmp_path)
    day = monday()
    start = at(day, 8) + timedelta(minutes=minutes)
    await observe(data, start, start + timedelta(hours=2))
    await service.store_parsed_schedule(poster(day, [("08:00", "10:00", "早播")]))
    schedule = await service.get_weekly_schedule(1)
    if matched:
        assert planned(schedule)[0]["actual_intervals"] and extras(schedule) == []
    else:
        assert planned(schedule)[0]["actual_intervals"] == []
        assert len(extras(schedule)) == 1


@pytest.mark.asyncio
async def test_unmatched_stream_becomes_a_surprise_entry(tmp_path):
    data, service, recorder = await boot(tmp_path)
    day = monday()
    await observe(data, at(day, 8), at(day, 10), title="深夜杂谈")
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "晚播")]))
    schedule = await service.get_weekly_schedule(1)
    extra = extras(schedule)[0]
    assert extra["date"] == day.isoformat() and extra["start_time"] == "08:00"
    assert extra["title"] == "深夜杂谈" and extra["status"] == "completed"
    text = format_stream(extra)
    assert "突击直播" in text and "深夜杂谈" in text and "原定：" not in text
    # 重复落位不能产生重复区间或重复条目。
    await recorder.sync(1)
    await recorder.sync(1)
    schedule = await service.get_weekly_schedule(1)
    assert len(extras(schedule)) == 1 and len(extras(schedule)[0]["actual_intervals"]) == 1


@pytest.mark.asyncio
async def test_corrected_poster_re_homes_the_surprise_entry(tmp_path):
    data, service, _ = await boot(tmp_path)
    day = monday()
    await observe(data, at(day, 8), at(day, 10))
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "晚播")], dynamic_id="901"))
    assert len(extras(await service.get_weekly_schedule(1))) == 1
    await service.store_parsed_schedule(poster(day, [("08:00", "10:00", "早播")], dynamic_id="902"))
    schedule = await service.get_weekly_schedule(1)
    assert extras(schedule) == []
    assert len(planned(schedule)) == 1 and len(planned(schedule)[0]["actual_intervals"]) == 1


@pytest.mark.asyncio
async def test_week_without_poster_keeps_the_observation_pending(tmp_path):
    data, _, recorder = await boot(tmp_path)
    day = monday()
    await observe(data, at(day, 8), at(day, 10))
    await recorder.sync(1)
    assert len(await data.pending_live_sessions(1)) == 1
    assert await recorder.week_summary(1, day.isoformat()) == {
        "recorded": 0, "pending": 1, "extra": 0, "unknown": 0}
    assert (await data.get_work_status())["live_sessions"]["pending"] == 1


@pytest.mark.asyncio
async def test_unknown_start_is_retired_and_counted(tmp_path):
    data, service, recorder = await boot(tmp_path)
    day = monday()
    await service.store_parsed_schedule(poster(day, [("08:00", "10:00", "早播")]))
    # 开始监控时已经在播：接口没有开播时间，也没有观测到状态变化。
    await data.save_vtuber_state(VtuberState(1, "主播", 10, True))
    await data.save_vtuber_state(VtuberState(1, "主播", 10, False))
    await recorder.sync(1)
    rows = await data.live_sessions_for_uid(1)
    assert rows[0]["start_time"] is None and rows[0]["synced"] == 2
    assert await data.pending_live_sessions(1) == []
    assert planned(await service.get_weekly_schedule(1))[0]["actual_intervals"] == []
    assert (await recorder.week_summary(1, day.isoformat()))["unknown"] == 1
    assert (await data.get_work_status())["live_sessions"]["skipped"] == 1


@pytest.mark.asyncio
async def test_live_title_is_optional_for_surprise_entries(tmp_path):
    data, service, _ = await boot(tmp_path)
    day = monday()
    await observe(data, at(day, 8), at(day, 10))
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "晚播")]))
    assert extras(await service.get_weekly_schedule(1))[0]["title"] == "突击直播"
