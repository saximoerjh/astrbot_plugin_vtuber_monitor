import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import VtuberState
from astrbot_plugin_vtuber_monitor.core.schedule_models import china_today, schedule_from_parser
from astrbot_plugin_vtuber_monitor.core.schedule_models import is_pending_title
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
        "recorded": 0, "pending": 1, "extra": 0, "unknown": 0, "unfulfilled": 0}
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


@pytest.mark.asyncio
async def test_pending_title_is_replaced_by_observed_live_title(tmp_path):
    data, service, recorder = await boot(tmp_path)
    day = monday()
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "直播（内容待定）")]))
    await observe(data, at(day, 20, 5), at(day, 22), title="深夜歌回：随机点歌")
    await recorder.sync(1)
    stream = planned(await service.get_weekly_schedule(1))[0]
    assert stream["title"] == "深夜歌回：随机点歌"
    assert "深夜歌回：随机点歌" in format_stream(stream)
    # 落位依然只发生一次，重复处理不会改变结果。
    await recorder.sync(1)
    assert planned(await service.get_weekly_schedule(1))[0]["title"] == "深夜歌回：随机点歌"


@pytest.mark.asyncio
async def test_observed_title_does_not_overwrite_a_real_schedule_title(tmp_path):
    data, service, _ = await boot(tmp_path)
    day = monday()
    await observe(data, at(day, 20), at(day, 22), title="直播间标题和排期不一致")
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "歌回")]))
    stream = planned(await service.get_weekly_schedule(1))[0]
    assert stream["title"] == "歌回"


@pytest.mark.asyncio
async def test_missing_live_title_keeps_the_pending_placeholder(tmp_path):
    data, service, _ = await boot(tmp_path)
    day = monday()
    await observe(data, at(day, 20), at(day, 22))
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "直播（内容待定）")]))
    stream = planned(await service.get_weekly_schedule(1))[0]
    assert stream["title"] == "直播（内容待定）"
    assert stream["actual_intervals"]


@pytest.mark.asyncio
async def test_reparse_still_pending_keeps_the_enriched_title(tmp_path):
    data, service, recorder = await boot(tmp_path)
    day = monday()
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "直播（内容待定）")]))
    await observe(data, at(day, 20), at(day, 22), title="深夜歌回")
    await recorder.sync(1)
    # 同一张图重新解析仍是“内容待定”，不能把已经补全的标题退回去。
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "直播（内容待定）")], dynamic_id="901"))
    stream = planned(await service.get_weekly_schedule(1))[0]
    assert stream["title"] == "深夜歌回" and stream["actual_intervals"]
    # 更正后的周表写了真实标题时，以周表为准。
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "深夜歌回特别篇")], dynamic_id="902"))
    assert planned(await service.get_weekly_schedule(1))[0]["title"] == "深夜歌回特别篇"


def test_pending_title_matches_placeholders_but_not_real_programmes():
    for title in ("直播（内容待定）", "直播(内容待定)", "待定", "联动（待定）", " 内容待定 "):
        assert is_pending_title(title), title
    for title in ("歌回", "歌回（待定曲目）", "杂谈：聊聊最近的安排", "", "   ", None):
        assert not is_pending_title(title), title


@pytest.mark.asyncio
async def test_scheduled_stream_without_broadcast_is_marked_unfulfilled(tmp_path):
    data, service, recorder = await boot(tmp_path)
    day = monday()
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "晚播")]))
    # 到点两小时整即可判定；提前一分钟还不能判。
    assert await recorder.mark_unfulfilled(1, now=at(day, 21, 59)) == 0
    assert planned(await service.get_weekly_schedule(1))[0]["status"] == "scheduled"
    assert await recorder.mark_unfulfilled(1, now=at(day, 22)) == 1
    stream = planned(await service.get_weekly_schedule(1))[0]
    assert stream["status"] == "unfulfilled"
    assert "未兑现" in format_stream(stream)
    assert (await recorder.week_summary(1, day.isoformat()))["unfulfilled"] == 1
    # 重复判定必须幂等，不会反复改状态。
    assert await recorder.mark_unfulfilled(1, now=at(day, 23)) == 0


@pytest.mark.asyncio
async def test_rescheduled_or_unpromised_streams_are_not_marked(tmp_path):
    data, service, recorder = await boot(tmp_path)
    day = monday()
    await service.store_parsed_schedule(poster(day, [
        ("20:00", "22:00", "晚播"),
        (None, None, "待定联动"),
    ]))
    # 第一条被动态调播、第三条时间待定，都不参与未兑现判定。
    await service.reschedule_stream(1, planned(await service.get_weekly_schedule(1))[0]["id"],
                                    day.isoformat(), "22:30", "推迟")
    streams = planned(await service.get_weekly_schedule(1))
    unknown = next(item for item in streams if item["title"] == "待定联动")
    assert unknown["start_time"] is None and unknown["status"] == "unknown"
    assert await recorder.mark_unfulfilled(1, now=at(day, 23, 59)) == 0
    assert all(item["status"] != "unfulfilled"
               for item in planned(await service.get_weekly_schedule(1)))


@pytest.mark.asyncio
async def test_late_broadcast_same_day_revokes_the_unfulfilled_mark(tmp_path):
    data, service, recorder = await boot(tmp_path)
    day = monday()
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "晚播")]))
    assert await recorder.mark_unfulfilled(1, now=at(day, 22, 30)) == 1
    stream = planned(await service.get_weekly_schedule(1))[0]
    assert stream["status"] == "unfulfilled"
    # 当天 23:30 才开播：回填这条并把状态改回已结束，不再另开突击条目。
    await observe(data, at(day, 23, 30), at(day, 23, 59), title="迟到开播")
    await recorder.sync(1, now=at(day, 23, 59))
    schedule = await service.get_weekly_schedule(1)
    stream = planned(schedule)[0]
    assert stream["status"] == "completed"
    # 周表原本写了真实节目名，迟到的实测标题不覆盖它。
    assert stream["title"] == "晚播"
    assert [item["start"] for item in stream["actual_intervals"]] == [at(day, 23, 30).isoformat()]
    assert extras(schedule) == []
    assert (await recorder.week_summary(1, day.isoformat()))["unfulfilled"] == 0


@pytest.mark.asyncio
async def test_next_day_broadcast_does_not_revoke_the_mark(tmp_path):
    data, service, recorder = await boot(tmp_path)
    day = monday()
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "晚播")]))
    assert await recorder.mark_unfulfilled(1, now=at(day, 22, 30)) == 1
    # 次日才补播：未兑现定稿，补播另记突击条目。
    await observe(data, at(day + timedelta(days=1), 21), at(day + timedelta(days=1), 23))
    await recorder.sync(1, now=at(day + timedelta(days=1), 23))
    schedule = await service.get_weekly_schedule(1)
    assert planned(schedule)[0]["status"] == "unfulfilled"
    assert len(extras(schedule)) == 1


@pytest.mark.asyncio
async def test_reparse_keeps_unfulfilled_state(tmp_path):
    data, service, recorder = await boot(tmp_path)
    day = monday()
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "晚播")]))
    assert await recorder.mark_unfulfilled(1, now=at(day, 22, 30)) == 1
    await service.store_parsed_schedule(poster(day, [("20:00", "22:00", "晚播")], dynamic_id="903"))
    assert planned(await service.get_weekly_schedule(1))[0]["status"] == "unfulfilled"


@pytest.mark.asyncio
async def test_backfill_takes_the_session_off_the_surprise_entry(tmp_path):
    """周表写 19:00、实际 21:39 才开播：先落成突击条目，回填后不能两边都留。

    这就是实测遇到的情况：同一场直播既挂在周表条目上（已结束，有结束时间），
    又留在突击条目上（开放区间，图片里显示“直播中”）。
    """
    data, service, recorder = await boot(tmp_path)
    day = monday()
    await service.store_parsed_schedule(poster(day, [("19:00", "21:00", "随便看看")]))
    await data.save_vtuber_state(VtuberState(1, "主播", 10, True, live_title="不要笑挑战",
                                             live_started_at=at(day, 21, 39).isoformat()))
    await recorder.sync(1, now=at(day, 21, 40))
    schedule = await service.get_weekly_schedule(1)
    # 超出 ±1 小时匹配窗口，先成为突击直播。
    assert [item["title"] for item in extras(schedule)] == ["不要笑挑战"]
    assert (await data.live_sessions_for_uid(1))[0]["id"] == 1
    # 此时 19:00 那条已过 21:00 的判定线、又没有实际开播记录 → 判未兑现；
    # 于是同一场直播暂时分裂成「未兑现 + 突击直播」两条。
    assert planned(schedule)[0]["status"] == "unfulfilled"
    assert await recorder.mark_unfulfilled(1, now=at(day, 22, 5)) == 0
    # 23:30 下播：撤销标记并回填，突击条目必须交出这条区间。
    with patch("astrbot_plugin_vtuber_monitor.core.data_manager.utc_now",
               return_value=at(day, 23, 30).isoformat()):
        await data.save_vtuber_state(VtuberState(1, "主播", 10, False))
    await recorder.sync(1, now=at(day, 23, 31))
    schedule = await service.get_weekly_schedule(1)
    stream = planned(schedule)[0]
    assert stream["status"] == "completed"
    assert stream["actual_intervals"][0]["end"] == at(day, 23, 30).isoformat()
    assert extras(schedule) == []
    owners = [item["id"] for item in schedule["streams"]
              if any(entry["session_id"] == 1 for entry in item.get("actual_intervals") or ())]
    assert owners == [stream["id"]]


@pytest.mark.asyncio
async def test_reconcile_cleans_a_session_left_on_two_entries(tmp_path):
    """修复前写坏的数据（同一次直播挂在两条上）在重新落位后必须收敛成一条。"""
    data, service, recorder = await boot(tmp_path)
    day = monday()
    await service.store_parsed_schedule(poster(day, [("19:00", "21:00", "随便看看")]))
    await data.save_vtuber_state(VtuberState(1, "主播", 10, True, live_title="不要笑挑战",
                                             live_started_at=at(day, 21, 39).isoformat()))
    await recorder.sync(1, now=at(day, 21, 40))
    with patch("astrbot_plugin_vtuber_monitor.core.data_manager.utc_now",
               return_value=at(day, 23, 30).isoformat()):
        await data.save_vtuber_state(VtuberState(1, "主播", 10, False))
    await recorder.sync(1, now=at(day, 23, 31))
    schedule = await service.get_weekly_schedule(1)
    assert extras(schedule) == []
    # 手工造出旧版本会留下的重复：同一条区间也留在一条突击条目上（且没有结束时间）。
    interval = planned(schedule)[0]["actual_intervals"][0]
    broken = json.loads(json.dumps(schedule))
    broken["streams"].append({**broken["streams"][0], "id": "legacy-extra",
                              "date": day.isoformat(), "start_time": "21:39",
                              "title": "不要笑挑战", "source": "live_observation",
                              "status": "scheduled",
                              "actual_intervals": [{**interval, "end": None, "end_basis": None}]})
    assert await data.save_weekly_schedule(broken, expected=schedule, source_id="",
                                           reason="live_observation")
    assert len((await service.get_weekly_schedule(1))["streams"]) == 2
    # 重新解析/回填该周后应回到一条，且区间带回结束时间。
    await recorder.reconcile_week(1, day.isoformat())
    schedule = await service.get_weekly_schedule(1)
    assert len(schedule["streams"]) == 1
    assert schedule["streams"][0]["actual_intervals"][0]["end"] == at(day, 23, 30).isoformat()
