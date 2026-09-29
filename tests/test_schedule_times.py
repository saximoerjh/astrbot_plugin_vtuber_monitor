import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import VtuberState
from astrbot_plugin_vtuber_monitor.core.schedule_models import china_today, schedule_from_parser
from astrbot_plugin_vtuber_monitor.services.schedule_service import ScheduleService
from astrbot_plugin_vtuber_monitor.services.live_schedule import LiveScheduleRecorder
from astrbot_plugin_vtuber_monitor.services.schedule_display import format_stream

CHINA = timezone(timedelta(hours=8))


async def setup(tmp_path, *, time="20:00", end="22:00", duplicate=False):
    data = DataManager(tmp_path)
    await data.initialize()
    today = china_today()
    week = (today - timedelta(days=today.weekday())).isoformat()
    item = {"date": today.isoformat(), "start_time": time, "end_time": end, "title": "歌回"}
    payload = {"week_start": week, "streams": [item]}
    if duplicate:
        payload["streams"].append({**item, "title": "游戏"})
    schedule = replace(schedule_from_parser(1, payload), source_dynamic_id="100", source_image_url="https://i0.hdslb.com/week.png")
    service = ScheduleService(data)
    await service.store_parsed_schedule(schedule)
    await data.add_subscription(VtuberState(1, "主播", 10, False), "a")
    return data, service, schedule


@pytest.mark.asyncio
async def test_original_range_survives_multiple_adjustments_and_reparse(tmp_path):
    data, service, original = await setup(tmp_path)
    stream = original.streams[0]
    for time in ("21:00", "21:30"):
        await service.reschedule_stream(1, stream.id, stream.date, time, "推迟")
    current = (await service.get_weekly_schedule(1))["streams"][0]
    assert (current["original_start_time"], current["original_end_time"]) == ("20:00", "22:00")
    assert current["rescheduled_start_time"] == "21:30"
    await service.store_parsed_schedule(replace(original, source_image_url="https://i0.hdslb.com/new.png"))
    current = (await service.get_weekly_schedule(1))["streams"][0]
    assert current["start_time"] == "21:30" and current["original_start_time"] == "20:00"
    text = format_stream(current)
    assert "20:00–22:00" in text and "调播：" in text and "21:30" in text


@pytest.mark.asyncio
async def test_actual_start_end_persist_dedup_and_survive_restart(tmp_path):
    data, service, original = await setup(tmp_path)
    started = datetime.fromisoformat(original.streams[0].date + "T20:05:00+08:00")
    recorder = LiveScheduleRecorder(data)
    live = VtuberState(1, "主播", 10, True, live_started_at=started.isoformat())
    with patch("astrbot_plugin_vtuber_monitor.core.data_manager.utc_now", return_value=(started + timedelta(minutes=2)).isoformat()):
        assert await data.save_vtuber_state(live)
        await data.save_vtuber_state(live)
    await recorder.sync(1)
    item = (await service.get_weekly_schedule(1))["streams"][0]
    assert len(item["actual_intervals"]) == 1
    assert item["actual_intervals"][0]["start"] == started.isoformat()
    assert "直播中" in format_stream(item)
    data = DataManager(tmp_path)
    await data.initialize()
    ended = started + timedelta(hours=5)
    with patch("astrbot_plugin_vtuber_monitor.core.data_manager.utc_now", return_value=ended.isoformat()):
        assert await data.save_vtuber_state(VtuberState(1, "主播", 10, False))
    recorder = LiveScheduleRecorder(data)
    await recorder.sync(1)
    await recorder.sync(1)
    item = (await service.get_weekly_schedule(1))["streams"][0]
    assert len(item["actual_intervals"]) == 1 and item["actual_intervals"][0]["end"] == ended.isoformat()
    assert item["status"] == "completed"
    assert "调播：" not in format_stream(item)
    assert "观测" in format_stream(item)
    await service.store_parsed_schedule(replace(original, source_image_url="https://i0.hdslb.com/changed.png"))
    assert (await service.get_weekly_schedule(1))["streams"][0]["actual_intervals"] == item["actual_intervals"]


@pytest.mark.asyncio
async def test_duplicate_slots_bind_to_the_nearest_one(tmp_path):
    data, service, original = await setup(tmp_path, duplicate=True)
    started = original.streams[0].date + "T20:00:00+08:00"
    await data.save_vtuber_state(VtuberState(1, "主播", 10, True, live_started_at=started))
    await LiveScheduleRecorder(data).sync(1)
    # 就近匹配取代了旧的“歧义即不绑定”规则。
    recorded = [s for s in (await service.get_weekly_schedule(1))["streams"] if s["actual_intervals"]]
    assert len(recorded) == 1
    assert recorded[0]["actual_intervals"][0]["start"] == started
    assert await data.pending_live_sessions(1) == []


@pytest.mark.asyncio
async def test_unknown_and_overnight_original_display(tmp_path):
    data, service, schedule = await setup(tmp_path, time=None, end=None)
    text = format_stream(schedule.to_dict()["streams"][0])
    assert "unknown" in text and "调播：" not in text
    payload = {"week_start": schedule.week_start, "streams": [{"date": schedule.streams[0].date,
               "start_time": "23:00", "end_time": "01:00", "title": "深夜"}]}
    night = schedule_from_parser(1, payload)
    assert "23:00–次日 01:00" in format_stream(night.to_dict()["streams"][0])


@pytest.mark.asyncio
async def test_monitoring_mid_stream_without_start_does_not_invent_time(tmp_path):
    data, service, schedule = await setup(tmp_path)
    # 把旧状态重置为未知基线，模拟新订阅。
    data._run(lambda db: db.execute("UPDATE vtubers SET is_live=NULL WHERE uid=1"))
    assert not await data.save_vtuber_state(VtuberState(1, "主播", 10, True))
    recorder = LiveScheduleRecorder(data)
    await recorder.sync(1)
    # 退役而不是永久重扫，也不会凭空写入周表。
    rows = await data.live_sessions_for_uid(1)
    assert rows[0]["start_time"] is None and rows[0]["synced"] == 2
    assert await data.pending_live_sessions(1) == []
    assert not (await service.get_weekly_schedule(1))["streams"][0]["actual_intervals"]
    assert (await recorder.week_summary(1, schedule.week_start))["unknown"] == 1


@pytest.mark.asyncio
async def test_legacy_original_times_recovered_from_audit(tmp_path):
    data, service, original = await setup(tmp_path)
    await service.reschedule_stream(1, original.streams[0].id, original.streams[0].date, "21:00", "改期")
    new_fields = {"original_date", "original_start_time", "original_end_time", "rescheduled_date", "rescheduled_start_time", "actual_intervals"}
    def downgrade(db):
        def strip(raw):
            value = json.loads(raw)
            for item in value["streams"]:
                for key in new_fields:
                    item.pop(key, None)
            return json.dumps(value)
        for table, identity in (("weekly_schedule_archive", "id"), ("weekly_schedules", "uid")):
            for row in list(db.execute(f"SELECT {identity},payload FROM {table}")):
                db.execute(f"UPDATE {table} SET payload=? WHERE {identity}=?", (strip(row["payload"]), row[identity]))
        for row in list(db.execute("SELECT * FROM schedule_revisions")):
            db.execute("UPDATE schedule_revisions SET old_value=?,new_value=? WHERE id=?",
                       (strip(row["old_value"]) if row["old_value"] else None, strip(row["new_value"]), row["id"]))
        db.execute("DELETE FROM plugin_migrations WHERE name='three_times_v1'")
    data._run(downgrade)
    await data.initialize()
    current = (await service.get_weekly_schedule(1))["streams"][0]
    assert current["original_start_time"] == "20:00" and current["rescheduled_start_time"] == "21:00"
    await data.initialize()
    assert (await service.get_weekly_schedule(1))["streams"][0] == current
