import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_vtuber_monitor.services.dynamic_listener import DynamicListener


@pytest.mark.asyncio
async def test_automatic_adjustment_waits_for_schedule_and_provider():
    data = SimpleNamespace(get_special_vtubers=AsyncMock(return_value=[1]),
                           ingest_dynamics=AsyncMock(return_value=[]),
                           pending_adjustments=AsyncMock(return_value=[{
                               "dynamic_id": "100", "payload": json.dumps({"uid": 1, "id": "100",
                               "text": "改到明天", "published_at": 1, "images": [], "is_pinned": False})}]),
                           finish_adjustment=AsyncMock())
    schedules = SimpleNamespace(get_weekly_schedule=AsyncMock(return_value=None))
    agent = SimpleNamespace(provider_id="model", schedules=schedules, process=AsyncMock())
    bili = SimpleNamespace(get_latest_dynamics=AsyncMock(return_value=[]))
    listener = DynamicListener(data, bili, adjustment=agent)
    listener.require_schedule = listener.eligible_only = True
    await listener.poll_once()
    bili.get_latest_dynamics.assert_not_awaited()
    data.finish_adjustment.assert_not_awaited()
    schedules.get_weekly_schedule.return_value = {"week_start": "2026-09-28"}
    agent.provider_id = ""
    await listener.poll_once()
    bili.get_latest_dynamics.assert_not_awaited()
    agent.provider_id = "model"
    await listener.poll_once()
    bili.get_latest_dynamics.assert_awaited_once_with(1)
    agent.process.assert_awaited_once()
    data.finish_adjustment.assert_awaited_once_with(1, "100", True)
    # 进入没有周表的一周后，处理会自动暂停。
    schedules.get_weekly_schedule.return_value = None
    await listener.poll_once()
    assert agent.process.await_count == 1


@pytest.mark.asyncio
async def test_explicit_dynamic_polling_keeps_jobs_until_schedule_exists():
    data = SimpleNamespace(get_special_vtubers=AsyncMock(return_value=[1]),
                           ingest_dynamics=AsyncMock(return_value=[]), pending_adjustments=AsyncMock(),
                           finish_adjustment=AsyncMock())
    agent = SimpleNamespace(provider_id="model", schedules=SimpleNamespace(
        get_weekly_schedule=AsyncMock(return_value=None)), process=AsyncMock())
    bili = SimpleNamespace(get_latest_dynamics=AsyncMock(return_value=[]))
    listener = DynamicListener(data, bili, adjustment=agent)
    listener.require_schedule = True
    await listener.poll_once()
    bili.get_latest_dynamics.assert_awaited_once()
    data.pending_adjustments.assert_not_awaited()
    data.finish_adjustment.assert_not_awaited()
