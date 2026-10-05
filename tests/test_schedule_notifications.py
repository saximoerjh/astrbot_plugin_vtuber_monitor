from types import SimpleNamespace
from unittest.mock import AsyncMock
from dataclasses import replace
from datetime import date, timedelta

import pytest

from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import VtuberState
from astrbot_plugin_vtuber_monitor.services.dispatcher import Dispatcher
from test_schedule_service import setup_schedule


def test_notice_paths_accepts_single_path_and_json_list():
    """旧数据是一张图的裸路径，新数据可能是一组图的 JSON 数组。"""
    from astrbot_plugin_vtuber_monitor.services.dispatcher import notice_paths

    assert notice_paths("") == [] and notice_paths(None) == []
    assert notice_paths(r"D:\images\a.png") == [r"D:\images\a.png"]
    assert notice_paths('["a.png", "b.png"]') == ["a.png", "b.png"]
    assert notice_paths("[broken") == []


@pytest.mark.asyncio
async def test_notification_queue_retries_failed_destination_only_after_restart(tmp_path):
    data, service, schedule = await setup_schedule(tmp_path, adjustment_push=True)
    for umo, level in [("a", "special"), ("b", "special"), ("c", "normal")]:
        await data.add_subscription(VtuberState(1, "one"), umo, level)
    await service.cancel_stream(1, schedule.streams[0].id, "临时取消", source_dynamic_id="101", operation_id="dynamic:101")
    assert len(await data.pending_notifications()) == 2
    context = SimpleNamespace(send_message=AsyncMock(side_effect=[True, False]))
    dispatcher = Dispatcher(context, message_factory=lambda t: t)
    dispatcher.adjustment_enabled = True
    await dispatcher.flush_schedule_notifications(data)
    assert [j["umo"] for j in await data.pending_notifications()] == ["b"]
    reopened = DataManager(tmp_path)
    await reopened.initialize()
    context.send_message.side_effect = None
    context.send_message.return_value = True
    await dispatcher.flush_schedule_notifications(reopened)
    await dispatcher.flush_schedule_notifications(reopened)
    assert context.send_message.await_count == 3
    assert not await reopened.pending_notifications()
    assert "取消" in context.send_message.await_args.args[1]


@pytest.mark.asyncio
async def test_unsubscribed_and_disabled_destinations_dropped(tmp_path):
    data, service, schedule = await setup_schedule(tmp_path, adjustment_push=True)
    await data.add_subscription(VtuberState(1, "one"), "a", "special")
    await service.cancel_stream(1, schedule.streams[0].id, "取消")
    await data.remove_subscription(1, "a")
    dispatcher = Dispatcher(SimpleNamespace(send_message=AsyncMock()), message_factory=lambda t: t)
    dispatcher.adjustment_enabled = True
    await dispatcher.flush_schedule_notifications(data)
    dispatcher.context.send_message.assert_not_awaited()
    assert not await data.pending_notifications()


@pytest.mark.asyncio
async def test_failed_notification_stops_after_five_rounds(tmp_path):
    data, service, schedule = await setup_schedule(tmp_path, adjustment_push=True)
    await data.add_subscription(VtuberState(1, "one"), "a", "special")
    await service.cancel_stream(1, schedule.streams[0].id, "取消")
    dispatcher = Dispatcher(SimpleNamespace(send_message=AsyncMock(return_value=False)), message_factory=lambda t: t)
    dispatcher.adjustment_enabled = True
    for _ in range(6):
        await dispatcher.flush_schedule_notifications(data)
    assert dispatcher.failed == 5 and not await data.pending_notifications()
    assert (await data.get_work_status())["schedule_outbox"]["failed"] == 1


@pytest.mark.asyncio
async def test_adjustment_notice_sends_screenshot_then_short_text(tmp_path):
    """调播通知分两条：先动态截图，再精简文字；截图一次供所有会话复用。"""
    shots = []

    async def notice_image(uid, dynamic_id):
        shots.append((uid, dynamic_id))
        return await data.save_notice_image(uid, dynamic_id, b"\x89PNGshot")

    data, service, schedule = await setup_schedule(tmp_path, adjustment_push=True,
                                                   notice_image=notice_image)
    for umo in ("a", "b"):
        await data.add_subscription(VtuberState(1, "one"), umo, "special")
    await service.cancel_stream(1, schedule.streams[0].id, "临时取消", source_dynamic_id="101",
                                operation_id="dynamic:101")
    # 截图只做一次，两个会话复用同一份。
    assert shots == [(1, "101")]
    context = SimpleNamespace(send_message=AsyncMock(return_value=True))
    dispatcher = Dispatcher(context, message_factory=lambda text: ("text", text),
                            image_message_factory=lambda path: ("image", path))
    dispatcher.adjustment_enabled = True
    await dispatcher.flush_schedule_notifications(data)
    assert context.send_message.await_count == 4
    first, second = context.send_message.await_args_list[0].args[1], context.send_message.await_args_list[1].args[1]
    assert first[0] == "image" and first[1].endswith(".png")
    assert second[0] == "text"
    body = second[1]
    # 精简后只保留改动与一句依据，不再重复打印变更前后的完整场次。
    assert "调播生效" in body and "取消" in body and "依据：临时取消" in body
    assert "已排期] →" not in body and "（原定" not in body


@pytest.mark.asyncio
async def test_adjustment_notice_carries_dynamic_text_and_all_images(tmp_path):
    """调播通知要能看出触发的动态：附上动态原文，多张配图一起发。"""
    async def notice_images(uid, dynamic_id):
        first = await data.save_notice_image(uid, f"{dynamic_id}-1", b"\x89PNGone")
        second = await data.save_notice_image(uid, f"{dynamic_id}-2", b"\x89PNGtwo")
        return [first, second]

    data, service, schedule = await setup_schedule(tmp_path, adjustment_push=True,
                                                   notice_image=notice_images)
    await data.add_subscription(VtuberState(1, "one"), "a", "special")
    await service.cancel_stream(1, schedule.streams[0].id, "临时取消", source_dynamic_id="101",
                                operation_id="dynamic:101", source_text="今天有直播哦\n等小松绿做下周表")
    job = (await data.pending_notifications())[0]
    assert job["message"].splitlines()[1] == "动态：今天有直播哦 等小松绿做下周表"
    # 多张图存成 JSON 数组，发送时先图后文，共三条。
    context = SimpleNamespace(send_message=AsyncMock(return_value=True))
    dispatcher = Dispatcher(context, message_factory=lambda text: ("text", text),
                            image_message_factory=lambda path: ("image", path))
    dispatcher.adjustment_enabled = True
    await dispatcher.flush_schedule_notifications(data)
    sent = [call.args[1] for call in context.send_message.await_args_list]
    assert [item[0] for item in sent] == ["image", "image", "text"]
    assert sent[0][1].endswith(".png") and sent[1][1].endswith(".png")
    assert sent[0][1] != sent[1][1]


@pytest.mark.asyncio
async def test_adjustment_notice_without_screenshot_sends_text_only(tmp_path):
    data, service, schedule = await setup_schedule(tmp_path, adjustment_push=True)
    await data.add_subscription(VtuberState(1, "one"), "a", "special")
    await service.cancel_stream(1, schedule.streams[0].id, "取消", source_dynamic_id="101",
                                operation_id="dynamic:101")
    context = SimpleNamespace(send_message=AsyncMock(return_value=True))
    dispatcher = Dispatcher(context, message_factory=lambda text: text)
    dispatcher.adjustment_enabled = True
    await dispatcher.flush_schedule_notifications(data)
    assert context.send_message.await_count == 1
    assert "调播生效" in context.send_message.await_args.args[1]


@pytest.mark.asyncio
async def test_screenshot_failure_keeps_text_notice(tmp_path):
    async def broken(uid, dynamic_id):
        raise RuntimeError("browser down")

    data, service, schedule = await setup_schedule(tmp_path, adjustment_push=True,
                                                   notice_image=broken)
    await data.add_subscription(VtuberState(1, "one"), "a", "special")
    await service.cancel_stream(1, schedule.streams[0].id, "取消", source_dynamic_id="101",
                                operation_id="dynamic:101")
    jobs = await data.pending_notifications()
    assert len(jobs) == 1 and jobs[0]["image_path"] == ""
    assert "调播生效" in jobs[0]["message"]


@pytest.mark.asyncio
async def test_new_image_push_dedup_and_no_history_push(tmp_path):
    data, service, schedule = await setup_schedule(tmp_path, schedule_push=True)
    await data.add_subscription(VtuberState(1, "one"), "a", "special")
    updated = replace(schedule, source_image_url="https://i0.hdslb.com/new.png",
                      streams=(replace(schedule.streams[0], title="新标题"),))
    await service.store_parsed_schedule(updated)
    await service.store_parsed_schedule(updated)
    notifications = await data.pending_notifications()
    assert len(notifications) == 1 and notifications[0]["kind"] == "schedule_updated"
    assert "标题" in notifications[0]["message"]
    past = (date.fromisoformat(schedule.week_start) - timedelta(days=7)).isoformat()
    old = replace(schedule, week_start=past, streams=(replace(schedule.streams[0], date=past, original_date=past),))
    await service.store_parsed_schedule(old)
    assert len(await data.pending_notifications()) == 1
