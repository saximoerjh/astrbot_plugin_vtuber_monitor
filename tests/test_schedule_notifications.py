from types import SimpleNamespace
from unittest.mock import AsyncMock
from dataclasses import replace
from datetime import date, timedelta

import pytest

from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import VtuberState
from astrbot_plugin_vtuber_monitor.services.dispatcher import Dispatcher
from test_schedule_service import setup_schedule


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
