import asyncio
import json
import time
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_vtuber_monitor.core.models import DynamicPost, VtuberState
from astrbot_plugin_vtuber_monitor.services.adjustment_agent import AdjustmentAgent
from astrbot_plugin_vtuber_monitor.services.dynamic_listener import DynamicListener
from astrbot_plugin_vtuber_monitor.services.dispatcher import Dispatcher
from test_schedule_service import setup_schedule


def post(text="今晚歌回推迟到21:00", id="101"):
    return DynamicPost(1, id, text, int(time.time()))


def agent(service, names=None, args=None):
    context = SimpleNamespace(llm_generate=AsyncMock(return_value=SimpleNamespace(
        tools_call_name=names or [], tools_call_args=args or [])))
    return AdjustmentAgent(context, service, "test", tool_factory=lambda: "PRIVATE_TOOLS")


@pytest.mark.asyncio
async def test_rules_filter_ambiguous_no_tools_and_private_scope(tmp_path):
    data, service, schedule = await setup_schedule(tmp_path)
    instance = agent(service)
    assert not (await instance.process(post("大家晚安")))["success"]
    instance.context.llm_generate.assert_not_awaited()
    assert not (await instance.process(post("可能临时有事，以后再说")))["success"]
    kwargs = instance.context.llm_generate.await_args.kwargs
    assert kwargs["tools"] == "PRIVATE_TOOLS"
    assert json.loads(kwargs["prompt"])["source_dynamic_id"] == "101"
    assert "不能" in kwargs["system_prompt"]
    assert (await service.get_weekly_schedule(1))["revision"] == 1


@pytest.mark.asyncio
async def test_tool_execute_snapshot_and_preview(tmp_path):
    data, service, schedule = await setup_schedule(tmp_path)
    instance = agent(service, ["reschedule_stream"], [{"stream_id": schedule.streams[0].id,
        "date": schedule.week_start, "start_time": "21:00", "reason": "今晚推迟"}])
    preview = await instance.process(post(), dry_run=True)
    assert preview["after"]["revision"] == 2
    assert (await service.get_weekly_schedule(1))["revision"] == 1
    result = await instance.process(post())
    assert result["success"]
    assert not (await instance.process(post()))["success"]
    assert len(await data.get_schedule_revisions(1)) == 2


@pytest.mark.parametrize("names,args", [
    (["delete_database"], [{}]), (["cancel_stream"], [{"stream_id": "x", "reason": "取消", "uid": 2}]),
    (["cancel_stream"], []), (["cancel_stream"], ['{"stream_id":"x"}']),
])
@pytest.mark.asyncio
async def test_malformed_or_unauthorized_calls_rejected(tmp_path, names, args):
    data, service, schedule = await setup_schedule(tmp_path)
    instance = agent(service, names, args)
    with pytest.raises(ValueError):
        await instance.process(post())
    assert (await service.get_weekly_schedule(1))["revision"] == 1


@pytest.mark.asyncio
async def test_stale_post_timeout_and_cancel(tmp_path):
    data, service, schedule = await setup_schedule(tmp_path)
    instance = agent(service)
    assert not (await instance.process(replace(post(), published_at=1)))["success"]
    instance.context.llm_generate.assert_not_awaited()
    instance.context.llm_generate.side_effect = TimeoutError("secret")
    with pytest.raises(ValueError, match="超时") as error:
        await instance.process(post())
    assert "secret" not in str(error.value)
    instance.context.llm_generate.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await instance.process(post())


@pytest.mark.asyncio
async def test_committed_jobs_survive_network_failure_restart_and_bounded_retries(tmp_path):
    data, service, schedule = await setup_schedule(tmp_path)
    await data.add_subscription(VtuberState(1, "one"), "group", "special", initial_dynamics=[])
    await data.ingest_dynamics(1, [post(id="101"), post(id="102")])
    assert (await data.get_dynamic_checkpoint(1))["latest_id"] == "102"
    fake = SimpleNamespace(process=AsyncMock(side_effect=ValueError("model down")))
    client = SimpleNamespace(get_latest_dynamics=AsyncMock(side_effect=TimeoutError()))
    listener = DynamicListener(data, client, adjustment=fake)
    for _ in range(3):
        await listener.poll_once()
    assert [j["dynamic_id"] for j in await data.pending_adjustments(1)] == ["102"]
    assert (await data.get_work_status())["adjustment_jobs"]["failed"] == 1
    fake.process.side_effect = None
    await listener.poll_once()
    assert not await data.pending_adjustments(1)
    assert fake.process.await_count == 4
    assert await service.retry_adjustment(1, "101")
    assert not await service.retry_adjustment(1, "102")
    await listener.poll_once()
    assert not await data.pending_adjustments(1)
    assert fake.process.await_count == 5


@pytest.mark.asyncio
async def test_schedule_changed_during_model_request_rejected(tmp_path):
    data, service, schedule = await setup_schedule(tmp_path)
    instance = agent(service)
    async def response(**kwargs):
        await service.update_stream_info(1, schedule.streams[0].id, "新标题", "并发更新")
        return SimpleNamespace(tools_call_name=["cancel_stream"], tools_call_args=[
            {"stream_id": schedule.streams[0].id, "reason": "取消旧标题的歌回"}])
    instance.context.llm_generate.side_effect = response
    with pytest.raises(ValueError, match="发生变化"):
        await instance.process(post())
    assert (await service.get_weekly_schedule(1))["streams"][0]["status"] == "scheduled"


@pytest.mark.asyncio
async def test_first_page_silent_and_resubscribe_drops_old_jobs(tmp_path):
    data, service, schedule = await setup_schedule(tmp_path)
    await data.add_subscription(VtuberState(1, "one"), "group", "special", initial_dynamics=[post()])
    assert not await data.pending_adjustments(1)
    await data.ingest_dynamics(1, [post(id="102")])
    assert len(await data.pending_adjustments(1)) == 1
    await data.remove_subscription(1, "group")
    await data.add_subscription(VtuberState(1, "one"), "group", "special", initial_dynamics=[post(id="103")])
    assert not await data.pending_adjustments(1)


def test_complex_regex_rejected():
    with pytest.raises(ValueError):
        AdjustmentAgent(None, None, pattern="(a+)+$")


@pytest.mark.asyncio
async def test_full_dynamic_tool_revision_and_notification_pipeline(tmp_path):
    data, service, schedule = await setup_schedule(tmp_path, adjustment_push=True)
    await data.add_subscription(VtuberState(1, "one"), "group", "special", initial_dynamics=[])
    instance = agent(service, ["cancel_stream"], [{"stream_id": schedule.streams[0].id, "reason": "临时取消歌回"}])
    context = SimpleNamespace(send_message=AsyncMock(return_value=True))
    dispatcher = Dispatcher(context, message_factory=lambda t: t)
    dispatcher.adjustment_enabled = True
    client = SimpleNamespace(get_latest_dynamics=AsyncMock(return_value=[post("临时取消歌回")]))
    listener = DynamicListener(data, client, adjustment=instance, dispatcher=dispatcher)
    await listener.poll_once()
    await listener.poll_once()
    instance.context.llm_generate.assert_awaited_once()
    context.send_message.assert_awaited_once()
    assert context.send_message.await_args.args[0] == "group"
    assert "调播生效" in context.send_message.await_args.args[1]
    assert (await service.get_weekly_schedule(1))["revision"] == 2
    assert not await data.pending_adjustments(1)
    assert not await data.pending_notifications()
