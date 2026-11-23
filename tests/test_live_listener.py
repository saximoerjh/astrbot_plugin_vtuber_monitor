import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from astrbot_plugin_vtuber_monitor.bili_client import BiliError, BiliRateLimited
from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import FollowLevel, VtuberState
from astrbot_plugin_vtuber_monitor.services.dispatcher import Dispatcher
from astrbot_plugin_vtuber_monitor.services.live_listener import LiveListener


@pytest.mark.asyncio
async def test_queued_live_observation_recovers_notice_and_late_start(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    await data.add_subscription(
        VtuberState(1, "host", 10, None), "chat", FollowLevel.SPECIAL
    )
    client = AsyncMock()
    dispatch = Dispatcher(AsyncMock(), message_factory=lambda text: text)
    listener = LiveListener(data, client, special_only=True)
    client.get_live_room_info.return_value = VtuberState(1, "host", 10, True)
    await listener.poll_once()
    assert (await data.live_sessions_for_uid(1))[0]["start_time"] is None
    start = "2026-11-22T16:00:00+00:00"
    client.get_live_room_info.return_value = VtuberState(
        1, "host", 10, True, live_started_at=start
    )
    await listener.poll_once()
    assert (await data.live_sessions_for_uid(1))[0]["start_time"] == start
    client.get_live_room_info.return_value = VtuberState(1, "host", 10, False)
    await listener.poll_once()
    client.get_live_room_info.return_value = VtuberState(1, "host", 10, True)
    await listener.poll_once()
    dispatch.context.send_message.assert_not_awaited()
    assert [j["kind"] for j in await data.pending_notifications()] == [
        "live_end",
        "live_start",
    ]
    reopened = DataManager(tmp_path)
    await reopened.initialize()
    dispatch.context.send_message.side_effect = [False, True]
    await dispatch.flush_schedule_notifications(reopened)
    assert [j["kind"] for j in await reopened.pending_notifications()] == ["live_start"]
    await dispatch.flush_schedule_notifications(reopened)
    assert not await reopened.pending_notifications()
    states = (await reopened.get_work_status())["schedule_outbox"]
    assert states == {"done": 1, "skipped": 1}


@pytest.mark.asyncio
async def test_default_special_scope_excludes_normal_destinations(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    await data.add_subscription(
        VtuberState(1, "主播", 10, False), "special", FollowLevel.SPECIAL, []
    )
    await data.add_subscription(VtuberState(1, "主播", 10, False), "normal")
    await data.add_subscription(VtuberState(2, "普通主播", 20, False), "normal")
    bili = AsyncMock()
    bili.get_live_room_info.return_value = VtuberState(1, "主播", 10, True)
    listener = LiveListener(data, bili, special_only=True)
    await listener.poll_once()
    bili.get_live_room_info.assert_awaited_once_with(1)
    assert [job["umo"] for job in await data.pending_notifications()] == ["special"]


@pytest.mark.asyncio
async def test_rate_limit_pauses_whole_round_without_changing_state(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    for uid in (1, 2):
        await data.add_subscription(VtuberState(uid, "主播", uid, False), "a")
    bili = AsyncMock()
    bili.get_live_room_info.side_effect = BiliRateLimited(300)
    listener = LiveListener(data, bili)
    await listener.poll_once()
    await listener.poll_once()
    assert bili.get_live_room_info.await_count == 1
    assert listener.cooldown_remaining > 290
    assert not (await data.get_vtuber_state(1)).is_live
    assert not await data.pending_notifications()
    with patch(
        "astrbot_plugin_vtuber_monitor.services.live_listener.random.uniform",
        return_value=60,
    ):
        assert listener.next_delay() > 350
        listener.cooldown_until = 0
        assert listener.next_delay() == 180
    with patch(
        "astrbot_plugin_vtuber_monitor.services.live_listener.random.uniform",
        return_value=0,
    ):
        assert listener.next_delay() == 120


@pytest.mark.asyncio
async def test_live_cover_title_and_image_failure_fallback(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    await data.add_subscription(VtuberState(1, "主播", 10, False), "a")
    context = AsyncMock()
    context.send_message.side_effect = [False, True, True]
    dispatcher = Dispatcher(
        context,
        message_factory=str,
        live_message_factory=lambda text, cover: (text, cover),
    )
    state = VtuberState(
        1,
        "主播",
        10,
        True,
        live_title="晚间歌回",
        live_cover="https://i0.hdslb.com/a.jpg",
    )
    bili = AsyncMock()
    listener = LiveListener(data, bili)
    bili.get_live_room_info.return_value = state
    await listener.poll_once()
    await dispatcher.flush_schedule_notifications(data)
    image_message = context.send_message.await_args_list[0].args[1]
    assert image_message[1] == state.live_cover
    assert "直播标题：晚间歌回" in image_message[0]
    assert "https://live.bilibili.com/10" in image_message[0]
    assert context.send_message.await_args_list[1].args[1] == image_message[0]
    bili.get_live_room_info.return_value = VtuberState(1, "主播", 10, False)
    await listener.poll_once()
    await dispatcher.flush_schedule_notifications(data)
    ended = context.send_message.await_args_list[2].args[1]
    assert isinstance(ended, str) and "下播了" in ended and "晚间歌回" not in ended


@pytest.mark.asyncio
async def test_transitions_restart_and_multi_session(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    await data.add_subscription(VtuberState(1, "主播", 10, False), "a")
    await data.add_subscription(VtuberState(1, "主播", 10, False), "b")
    context = AsyncMock()
    dispatcher = Dispatcher(context, message_factory=str)
    bili = AsyncMock()
    listener = LiveListener(data, bili)
    for live in (False, True, True, False):
        bili.get_live_room_info.return_value = VtuberState(1, "主播", 10, live)
        await listener.poll_once()
        await dispatcher.flush_schedule_notifications(data)
    assert bili.get_live_room_info.await_count == 4  # 每个 UID 一次，而不是每场直播一次
    assert context.send_message.await_count == 4  # 每场直播两次状态跳变
    messages = [call.args for call in context.send_message.await_args_list]
    assert [umo for umo, _ in messages] == ["a", "b", "a", "b"]
    assert "开播了" in messages[0][1] and "下播了" in messages[2][1]
    state = await data.get_vtuber_state(1)
    assert state.is_live is False and state.last_live_change_at
    reopened = DataManager(tmp_path)
    await reopened.initialize()
    await LiveListener(reopened, bili).poll_once()
    await dispatcher.flush_schedule_notifications(reopened)
    assert context.send_message.await_count == 4
    assert (
        await reopened.get_vtuber_state(1)
    ).last_live_change_at == state.last_live_change_at


@pytest.mark.asyncio
async def test_unknown_baseline_and_resubscribe(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    await data.add_subscription(VtuberState(1, "主播"), "a")
    assert not await data.save_vtuber_state(VtuberState(1, "主播", 10, True))
    await data.remove_subscription(1, "a")
    await data.add_subscription(VtuberState(1, "主播", 10, False), "b")
    assert not await data.save_vtuber_state(VtuberState(1, "主播", 10, False))
    assert await data.get_subscribed_uids() == [1]
    await data.remove_subscription(1, "b")
    assert await data.get_subscribed_uids() == []


@pytest.mark.parametrize(
    "failure",
    [BiliError("timeout"), BiliError("412"), ValueError("missing JSON"), None],
)
@pytest.mark.asyncio
async def test_bad_streamer_does_not_stop_round(tmp_path, failure):
    data = DataManager(tmp_path)
    await data.initialize()
    for uid in (1, 2):
        await data.add_subscription(VtuberState(uid, "主播", uid, False), "a")

    async def fetch(uid):
        if uid == 1:
            if failure is not None:
                raise failure
            return {"unexpected": "data"}
        return VtuberState(uid, "主播", uid, True)

    bili = AsyncMock()
    bili.get_live_room_info.side_effect = fetch
    listener = LiveListener(data, bili)
    await listener.poll_once()
    assert listener.failures == 1 and listener.rounds == 1
    assert (await data.get_vtuber_state(1)).is_live is False
    assert [job["uid"] for job in await data.pending_notifications()] == [2]


@pytest.mark.asyncio
async def test_dispatcher_failures_options_and_special(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    state = VtuberState(1, "主播", 10, False)
    await data.add_subscription(state, "a")
    await data.add_subscription(state, "b", FollowLevel.SPECIAL)
    bili = AsyncMock()
    listener = LiveListener(data, bili)
    bili.get_live_room_info.return_value = VtuberState(1, "主播", 10, True)
    context = AsyncMock()
    context.send_message.side_effect = [RuntimeError("failed"), True]
    dispatcher = Dispatcher(context, message_factory=str)
    await listener.poll_once()
    await dispatcher.flush_schedule_notifications(data)
    assert dispatcher.failed == 1 and dispatcher.sent == 1
    assert context.send_message.await_count == 2
    context.send_message.reset_mock(side_effect=True)
    await dispatcher.flush_schedule_notifications(data)
    assert context.send_message.await_args.args[0] == "a"
    context.send_message.reset_mock()
    dispatcher.normal_end = False
    bili.get_live_room_info.return_value = state
    await listener.poll_once()
    await dispatcher.flush_schedule_notifications(data)
    # 特别关注下播默认关闭：普通关注关掉之后一个都不发。
    assert context.send_message.await_count == 0
    dispatcher.special_end = True
    bili.get_live_room_info.return_value = VtuberState(1, "主播", 10, True)
    await listener.poll_once()
    bili.get_live_room_info.return_value = state
    await listener.poll_once()
    dispatcher.normal_start = dispatcher.special_start = False
    await dispatcher.flush_schedule_notifications(data)
    assert context.send_message.await_args.args[0] == "b"
    assert context.send_message.await_count == 1
    context.send_message.return_value = False
    assert not await dispatcher.push_login_status("a", "t")


@pytest.mark.asyncio
async def test_notification_switches_can_come_from_a_live_reload(tmp_path):
    """发送前重读的开关优先于启动时的值：配置一关立刻不再发，一开立刻就能发。"""
    data = DataManager(tmp_path)
    await data.initialize()
    state = VtuberState(1, "主播", 10, True)
    await data.add_subscription(state, "a", FollowLevel.SPECIAL)
    bili = AsyncMock()
    listener = LiveListener(data, bili, special_only=True)
    context = AsyncMock()
    latest = {"special_end": False}
    dispatcher = Dispatcher(
        context,
        message_factory=str,
        special_end=True,
        flags_provider=AsyncMock(side_effect=lambda: dict(latest)),
    )

    async def observe_end():
        bili.get_live_room_info.return_value = state
        await listener.poll_once()
        bili.get_live_room_info.return_value = VtuberState(1, "主播", 10, False)
        await listener.poll_once()
        await dispatcher.flush_schedule_notifications(data)

    dispatcher.special_start = False
    await observe_end()
    assert context.send_message.await_count == 0
    latest["special_end"] = True
    dispatcher.special_end = False
    await observe_end()
    assert context.send_message.await_count == 1  # 配置现在开着 → 立刻发
    # 上次读到开启，启动时的关闭不能覆盖它。
    dispatcher.flags_provider = AsyncMock(side_effect=RuntimeError("boom"))
    await observe_end()
    assert context.send_message.await_count == 2
    # 上次读到关闭，短暂读取失败也不能恢复启动时的开启。
    dispatcher.special_end = True
    dispatcher.flags_provider = AsyncMock(return_value={"special_end": False})
    await observe_end()
    dispatcher.flags_provider = AsyncMock(return_value=None)
    await observe_end()
    assert context.send_message.await_count == 2
    assert dispatcher.flags_source == "上次有效配置"


@pytest.mark.asyncio
async def test_expired_live_notification_is_skipped_and_fresh_transition_delivered(
    tmp_path,
):
    data = DataManager(tmp_path)
    await data.initialize()
    offline = VtuberState(1, "主播", 10, False)
    old = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    with patch(
        "astrbot_plugin_vtuber_monitor.core.data_manager.utc_now", return_value=old
    ):
        await data.add_subscription(offline, "chat")
        await data.save_vtuber_state(
            VtuberState(1, "主播", 10, True),
            notification=("live_start", "过期开播", "", False),
        )
    await data.save_vtuber_state(
        offline, notification=("live_end", "本次下播", "", False)
    )
    context = AsyncMock()
    await Dispatcher(context, message_factory=str).flush_schedule_notifications(data)
    context.send_message.assert_awaited_once_with("chat", "本次下播")
    assert (await data.get_work_status())["schedule_outbox"] == {
        "expired": 1,
        "done": 1,
    }


@pytest.mark.asyncio
async def test_send_timeout_and_cancellation():
    context = AsyncMock()

    async def hang(*args):
        await asyncio.Event().wait()

    context.send_message.side_effect = hang
    dispatcher = Dispatcher(context, message_factory=str, send_timeout=0.01)
    assert not await dispatcher.push_login_status("a", "t")
    context.send_message.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await dispatcher.push_login_status("a", "t")


@pytest.mark.asyncio
async def test_run_recovers_and_cancels():
    listener = LiveListener(AsyncMock(), AsyncMock())
    listener.data.get_subscribed_uids.side_effect = RuntimeError("db failed")
    task = asyncio.create_task(listener.run())
    # 与第一轮失败同步，不必等待轮询间隔。
    for _ in range(100):
        if listener.failures:
            break
        await asyncio.sleep(0)
    assert listener.failures == 1 and not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_atomic_transition_and_old_database_migration(tmp_path):
    db = sqlite3.connect(tmp_path / "monitor.sqlite3")
    db.execute(
        "CREATE TABLE vtubers (uid INTEGER PRIMARY KEY, name TEXT NOT NULL, room_id INTEGER NOT NULL, is_live INTEGER)"
    )
    db.execute("INSERT INTO vtubers VALUES (1, 'old', 10, 0)")
    db.commit()
    db.close()
    data = DataManager(tmp_path)
    await data.initialize()
    assert (await data.get_vtuber_state(1)).last_live_change_at is None
    changes = await asyncio.gather(
        *(data.save_vtuber_state(VtuberState(1, "new", 10, True)) for _ in range(5))
    )
    assert changes.count(True) == 1


@pytest.mark.asyncio
async def test_send_failure_does_not_repeat_transition(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    await data.add_subscription(VtuberState(1, "主播", 10, False), "a")
    bili = AsyncMock()
    bili.get_live_room_info.return_value = VtuberState(1, "主播", 10, True)
    context = AsyncMock()
    context.send_message.return_value = False
    dispatcher = Dispatcher(context, message_factory=str)
    listener = LiveListener(data, bili)
    await listener.poll_once()
    await listener.poll_once()
    assert len(await data.pending_notifications()) == 1
    await dispatcher.flush_schedule_notifications(data)
    assert dispatcher.failed == 1 and context.send_message.await_count == 1
    assert (await data.pending_notifications())[0]["attempts"] == 1
