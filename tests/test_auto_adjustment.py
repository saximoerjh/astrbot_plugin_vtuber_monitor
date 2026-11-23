import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_vtuber_monitor.services.dynamic_listener import DynamicListener


@pytest.mark.asyncio
async def test_automatic_adjustment_waits_for_schedule_and_provider():
    data = SimpleNamespace(
        get_special_vtubers=AsyncMock(return_value=[1]),
        get_dynamic_checkpoint=AsyncMock(return_value=None),
        expire_adjustments=AsyncMock(),
        ingest_dynamics=AsyncMock(return_value=[]),
        pending_adjustments=AsyncMock(
            return_value=[
                {
                    "dynamic_id": "100",
                    "payload": json.dumps(
                        {
                            "uid": 1,
                            "id": "100",
                            "text": "改到明天",
                            "published_at": 1,
                            "images": [],
                            "is_pinned": False,
                        }
                    ),
                }
            ]
        ),
        finish_adjustment=AsyncMock(),
    )
    schedules = SimpleNamespace(get_weekly_schedule=AsyncMock(return_value=None))
    agent = SimpleNamespace(
        provider_available=lambda: False, schedules=schedules, process=AsyncMock()
    )
    bili = SimpleNamespace(get_dynamics_since=AsyncMock(return_value=[]))
    listener = DynamicListener(data, bili, adjustment=agent)
    await listener.poll_once()
    await listener.process_pending()
    bili.get_dynamics_since.assert_awaited_once_with(1, None)
    data.pending_adjustments.assert_not_awaited()
    data.finish_adjustment.assert_not_awaited()
    agent.provider_available = lambda: True
    await listener.process_pending()
    data.pending_adjustments.assert_not_awaited()
    schedules.get_weekly_schedule.return_value = {"week_start": "2026-09-28"}
    await listener.process_pending()
    agent.process.assert_awaited_once()
    data.finish_adjustment.assert_awaited_once_with(
        1, "100", True, payload=data.pending_adjustments.return_value[0]["payload"]
    )
    # 进入没有周表的一周后，处理会自动暂停。
    schedules.get_weekly_schedule.return_value = None
    await listener.process_pending()
    assert agent.process.await_count == 1


@pytest.mark.asyncio
async def test_dynamic_polling_uses_durable_checkpoint():
    data = SimpleNamespace(
        get_special_vtubers=AsyncMock(return_value=[1]),
        get_dynamic_checkpoint=AsyncMock(return_value={"latest_id": "100"}),
        ingest_dynamics=AsyncMock(return_value=[]),
    )
    bili = SimpleNamespace(get_dynamics_since=AsyncMock(return_value=[]))
    listener = DynamicListener(data, bili)
    await listener.poll_once()
    bili.get_dynamics_since.assert_awaited_once_with(1, "100")
    data.ingest_dynamics.assert_awaited_once_with(1, [])
