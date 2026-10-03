import asyncio
from dataclasses import replace
from datetime import timedelta

import pytest

from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import VtuberState
from astrbot_plugin_vtuber_monitor.core.schedule_models import china_today, schedule_from_parser
from astrbot_plugin_vtuber_monitor.core.schedule_diff import schedule_diff, align_streams
from astrbot_plugin_vtuber_monitor.services.schedule_service import ScheduleService


async def setup_schedule(tmp_path, **options):
    data = DataManager(tmp_path)
    await data.initialize()
    service = ScheduleService(data, **options)
    today = china_today()
    monday = (today - timedelta(days=today.weekday())).isoformat()
    schedule = replace(schedule_from_parser(1, {"week_start": monday, "streams": [
        {"date": monday, "start_time": "20:00", "title": "歌回"}]}), source_dynamic_id="100",
        source_image_url="https://i0.hdslb.com/weekly.png")
    await service.store_parsed_schedule(schedule)
    return data, service, schedule


@pytest.mark.asyncio
async def test_update_stream_info_fills_a_pending_slots_time(tmp_path):
    """动态只给了时间时，用 update_stream_info 给"时间待定"的场次补上，不新增重复场次。"""
    data = DataManager(tmp_path)
    await data.initialize()
    service = ScheduleService(data)
    monday = (china_today() - timedelta(days=china_today().weekday())).isoformat()
    schedule = replace(schedule_from_parser(1, {"week_start": monday, "streams": [
        {"date": monday, "start_time": None, "title": "虚度"}]}), source_dynamic_id="100")
    await service.store_parsed_schedule(schedule)
    stream_id = schedule.streams[0].id

    result = await service.update_stream_info(
        1, stream_id, reason="动态说 17:00 来播", date=monday, start_time="17:00")
    plan = result["after"]["streams"][0]
    assert result["success"] and plan["start_time"] == "17:00" and plan["status"] == "scheduled"
    assert len(result["after"]["streams"]) == 1
    # 已有明确时间的场次要改时间不能走这条（那是改期，得用 reschedule_stream）。
    with pytest.raises(ValueError, match="reschedule_stream"):
        await service.update_stream_info(1, stream_id, reason="改成 18:00", start_time="18:00")
    # 只给 stream_id 不算一次有效更新。
    with pytest.raises(ValueError, match="需要时间或标题"):
        await service.update_stream_info(1, stream_id, reason="什么也没说")
    assert (await service.get_weekly_schedule(1))["streams"][0]["start_time"] == "17:00"


@pytest.mark.asyncio
async def test_all_operations_audit_restart_and_image_dedup(tmp_path):
    data, service, original = await setup_schedule(tmp_path)
    stream_id = original.streams[0].id
    result = await service.reschedule_stream(1, stream_id, original.week_start, "21:00", "推迟一小时",
                                             source_dynamic_id="101", operation_id="dynamic:101")
    assert result["success"] and result["before"]["streams"][0]["start_time"] == "20:00"
    assert result["after"]["streams"][0]["revision"] == 1
    assert result["after"]["revision"] == 2
    assert not await service.replace_weekly_schedule(original)
    assert (await service.get_weekly_schedule(1))["streams"][0]["start_time"] == "21:00"
    await service.update_stream_info(1, stream_id, "新歌回", "改标题")
    await service.cancel_stream(1, stream_id, "取消")
    added = await service.add_stream(1, original.week_start, None, "加播待定", "临时加播",
                                    source_dynamic_id="102", operation_id="dynamic:102")
    assert added["after"]["revision"] == 5
    assert added["after"]["streams"][1]["status"] == "unknown"
    repeated = await service.add_stream(1, original.week_start, None, "加播待定", "临时加播",
                                       source_dynamic_id="102", operation_id="dynamic:102")
    assert not repeated["success"]
    reopened = DataManager(tmp_path)
    await reopened.initialize()
    current = await ScheduleService(reopened).get_weekly_schedule(1)
    assert current["revision"] == 5 and current["streams"][0]["status"] == "cancelled"
    revisions = await reopened.get_schedule_revisions(1)
    assert len(revisions) == 5 and revisions[0]["source_dynamic_id"] == "102"
    assert revisions[0]["old_value"]["revision"] == 4


@pytest.mark.parametrize("kwargs", [
    {"start_time": "25:00"}, {"start_time": None}, {"date": "2099-01-01"},
    {"stream_id": "missing"}, {"reason": ""}, {"date": True},
])
@pytest.mark.asyncio
async def test_invalid_changes_leave_data_unchanged(tmp_path, kwargs):
    data, service, original = await setup_schedule(tmp_path)
    arguments = {"stream_id": original.streams[0].id, "date": original.week_start,
                 "start_time": "21:00", "reason": "推迟"} | kwargs
    before = await service.get_weekly_schedule(1)
    with pytest.raises(ValueError):
        await service.reschedule_stream(1, **arguments)
    assert await service.get_weekly_schedule(1) == before
    assert len(await data.get_schedule_revisions(1)) == 1


@pytest.mark.asyncio
async def test_batch_is_atomic_and_dry_run_never_writes(tmp_path):
    data, service, original = await setup_schedule(tmp_path, adjustment_push=True)
    await data.add_subscription(VtuberState(1, "test"), "group", "special")
    operations = [{"name": "cancel_stream", "arguments": {"stream_id": original.streams[0].id, "reason": "取消"}},
                  {"name": "add_stream", "arguments": {"date": original.week_start, "start_time": "99:00", "title": "加播", "reason": "加播"}}]
    with pytest.raises(ValueError):
        await service.apply_operations(1, operations)
    assert (await service.get_weekly_schedule(1))["revision"] == 1
    result = await service.apply_operations(1, operations[:1], dry_run=True)
    assert result["success"] and result["dry_run"]
    assert (await service.get_weekly_schedule(1))["revision"] == 1
    assert not await data.pending_notifications()


@pytest.mark.asyncio
async def test_concurrent_writers_cannot_lose_update(tmp_path):
    data, service, original = await setup_schedule(tmp_path)
    before = await service.get_weekly_schedule(1)
    a = {**before, "streams": [{**before["streams"][0], "title": "A"}]}
    b = {**before, "streams": [{**before["streams"][0], "title": "B"}]}
    outcomes = await asyncio.gather(*[data.save_weekly_schedule(c, expected=before, source_id="101", reason="test")
                                      for c in (a, b)], return_exceptions=True)
    assert outcomes.count(True) == 1 and sum(isinstance(x, ValueError) for x in outcomes) == 1
    assert len(await data.get_schedule_revisions(1)) == 2


@pytest.mark.asyncio
async def test_diff_aligns_ids_and_history_tracks_revert(tmp_path):
    data, service, original = await setup_schedule(tmp_path)
    revised = replace(original, source_image_url="https://i0.hdslb.com/new.png",
                      streams=(replace(original.streams[0], id="newid", start_time="22:00"),))
    await service.replace_weekly_schedule(revised)
    revisions = await data.get_schedule_revisions(1)
    diff = schedule_diff(revisions[0]["old_value"], revisions[0]["new_value"])
    assert diff[0]["kinds"] == ["time"]
    assert diff[0]["stream_id"] == original.streams[0].id
    await service.update_stream_info(1, original.streams[0].id, "新标题", "改名")
    await service.update_stream_info(1, original.streams[0].id, "歌回", "恢复标题")
    assert (await service.get_weekly_schedule(1))["revision"] == 4
    assert (await service.get_schedule_history(1))[0]["versions"] == 4


@pytest.mark.asyncio
async def test_import_key_prevents_nondeterministic_reparse_undoing_adjustment(tmp_path):
    data, service, original = await setup_schedule(tmp_path)
    await service.store_parsed_schedule(original, import_key="same_input")
    await service.cancel_stream(1, original.streams[0].id, "取消")
    varied = replace(original, streams=(replace(original.streams[0], title="歌 回"),))
    assert await service.store_parsed_schedule(varied, import_key="same_input") == "unchanged"
    assert (await service.get_weekly_schedule(1))["streams"][0]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_alignment_preserves_exact_id_before_fuzzy_matching(tmp_path):
    data, service, original = await setup_schedule(tmp_path)
    original_stream = original.streams[0]
    # 新增场次沿用了旧标题，不能因此抢走后面按 ID 精确匹配的场次。
    incoming = replace(original, streams=(replace(original_stream, id="new", start_time="22:00"),
                                          replace(original_stream, title="重命名")))
    old = await service.get_weekly_schedule(1)
    aligned = align_streams(old, incoming)
    assert len({s.id for s in aligned.streams}) == 2
    diff = schedule_diff(old, aligned.to_dict())
    assert sorted(kind for change in diff for kind in change["kinds"]) == ["added", "title"]


@pytest.mark.asyncio
async def test_multiple_changes_and_removed_scene_diff(tmp_path):
    data, service, original = await setup_schedule(tmp_path)
    await service.add_stream(1, original.week_start, "22:00", "游戏", "加播")
    old = await service.get_weekly_schedule(1)
    replacement = replace(original, source_image_url="https://i0.hdslb.com/update.png",
                          streams=(replace(original.streams[0], title="新歌回", start_time="21:00"),))
    await service.store_parsed_schedule(replacement)
    result = schedule_diff(old, await service.get_weekly_schedule(1))
    assert sorted(kind for change in result for kind in change["kinds"]) == ["removed", "time", "title"]
