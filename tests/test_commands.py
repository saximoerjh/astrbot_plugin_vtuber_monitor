"""使用显式 AstrBot 桩的命令冒烟测试，不是运行时集成测试。"""
import asyncio
import importlib
import logging
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_command_routing_and_lifecycle(monkeypatch, tmp_path):
    class Star:
        def __init__(self, context):
            self.context = context

    def decorator(*args, **kwargs):
        return lambda target: target

    modules = {name: ModuleType(name) for name in [
        "astrbot", "astrbot.api", "astrbot.api.event", "astrbot.api.star",
        "astrbot.core.star.filter.command",
    ]}
    modules["astrbot.api"].AstrBotConfig = dict
    modules["astrbot.core.star.filter.command"].GreedyStr = str
    modules["astrbot.api"].logger = logging.getLogger(__name__)
    modules["astrbot.api.event"].AstrMessageEvent = object
    modules["astrbot.api.event"].filter = SimpleNamespace(command=decorator, permission_type=decorator,
                                                        PermissionType=SimpleNamespace(ADMIN="admin"))
    star = modules["astrbot.api.star"]
    star.Context, star.Star, star.register = object, Star, decorator
    star.StarTools = SimpleNamespace(get_data_dir=lambda name: tmp_path)
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    module_name = "astrbot_plugin_vtuber_monitor.main"
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    main = importlib.import_module(module_name)
    event = SimpleNamespace(unified_msg_origin="qq:GroupMessage:123", plain_result=lambda s: s,
                            image_result=lambda path: ("image", path), get_sender_id=lambda: "user1")
    plugin = main.MyPlugin(object(), {"auto_special_live": False, "auto_adjustment_with_schedule": False,
                                      "schedule_image_enabled": False})
    try:
        await plugin.initialize()
        assert plugin.live_listener_task is None
        assert "未启用" in ([x async for x in plugin.vt_status(event)])[0]
        assert [x async for x in plugin.vt_ping(event)] == ["VTuber Monitor OK"]
        assert "用法" in ([x async for x in plugin.vt_sub(event)])[0]
        event.is_admin = lambda: False
        event.get_group_id = lambda: ""
        assert "管理员" in ([x async for x in plugin.bili_login(event)])[0]
        event.is_admin = lambda: True
        event.get_group_id = lambda: "group"
        assert "私聊" in ([x async for x in plugin.bili_login(event)])[0]
        event.get_group_id = lambda: ""
        plugin.login.start = AsyncMock()
        assert "扫描" in ([x async for x in plugin.bili_login(event)])[0]
        assert plugin.login.start.await_args.args[0] == event.unified_msg_origin
        service = AsyncMock()
        service.subscribe.return_value = SimpleNamespace(name="主播", uid=456)
        service.unsubscribe.return_value = True
        service.list_subscriptions.return_value = []
        plugin.subscriptions = service
        assert "已订阅" in ([x async for x in plugin.vt_sub(event, "456")])[0]
        service.subscribe.assert_awaited_once_with(456, event.unified_msg_origin, "normal", user_id="user1")
        assert [x async for x in plugin.vt_unsub(event, "456")] == ["已取消订阅。"]
        service.unsubscribe.assert_awaited_once_with(456, event.unified_msg_origin)
        assert [x async for x in plugin.vt_list(event)] == ["当前会话暂无订阅。"]
        plugin.schedules = AsyncMock()
        plugin.schedules.get_schedule_history.return_value = [{"week_start": "2026-09-21", "versions": 1}]
        assert "2026-09-21" in ([x async for x in plugin.vt_schedule_history(event, "456")])[0]
        plugin.schedules.get_weekly_schedule.return_value = {
            "week_start": "2026-09-21", "revision": 1,
            "streams": [{"date": "2026-09-24", "start_time": None, "title": "待定联动", "status": "unknown"}]}
        assert "时间待定" in ([x async for x in plugin.vt_schedule(event, "456", "2026-09-21")])[0]
        plugin.schedules.get_weekly_schedule.assert_awaited_once_with(456, "2026-09-21")
        from astrbot_plugin_vtuber_monitor.core.schedule_models import parse_week_override
        for label in ("本周", "这周", "上周", "下周", "本星期", "上星期", "下星期"):
            results = [x async for x in plugin.vt_schedule(event, "456", label)]
            plugin.schedules.get_weekly_schedule.assert_awaited_with(456, parse_week_override(label).isoformat())
            assert "周表起始日" in results[0]
        plugin.schedules.get_weekly_schedule.reset_mock()
        assert "周次" in ([x async for x in plugin.vt_schedule(event, "456", "随便")])[0]
        plugin.schedules.get_weekly_schedule.assert_not_awaited()
        plugin.discovery.scan = AsyncMock(return_value=[{"dynamic_id": "100", "image_index": 4, "status": "needs_date"}])
        assert "周次不明确" in ([x async for x in plugin.vt_parse_schedule(event, "456")])[0]
        await_results = [x async for x in plugin.vt_parse_schedule(event, "456", "2026-09-21")]
        plugin.discovery.scan.assert_awaited_with(456, force=True, week_start="2026-09-21")
        assert "https://t.bilibili.com/100" in await_results[0]
        assert "第 4 张" not in await_results[0]
        from astrbot_plugin_vtuber_monitor.core.schedule_models import parse_week_override
        for label in ("本周", "上周", "下周"):
            expected = parse_week_override(label).isoformat()
            results = [x async for x in plugin.vt_parse_schedule(event, "456", label)]
            plugin.discovery.scan.assert_awaited_with(456, force=True, week_start=expected)
            assert f"指定周次：{expected}" in results[0]
        service.subscribe.side_effect = ValueError("bad uid")
        assert "未找到" in ([x async for x in plugin.vt_sub(event, "bad")])[0]
        from astrbot_plugin_vtuber_monitor.core.models import VtuberState
        await plugin.targets.data.add_subscription(VtuberState(456, "主播"), event.unified_msg_origin, user_id="user1")
        assert "已设置别名" in ([x async for x in plugin.vt_alias(event, "456", "小路")])[0]
        plugin.schedules.get_weekly_schedule.reset_mock()
        assert "时间待定" in ([x async for x in plugin.vt_schedule(event, "小路", "2026-09-21")])[0]
        plugin.schedules.get_weekly_schedule.assert_awaited_once_with(456, "2026-09-21")
        assert "小路" in ([x async for x in plugin.vt_list(event)])[0]
        assert "已有别名保留" in ([x async for x in plugin.vt_alias(event, "小路", "Komichi")])[0]
        assert "小路 / Komichi（UID 456）" in ([x async for x in plugin.vt_list(event)])[0]
        assert "多个别名" in ([x async for x in plugin.vt_alias_del(event, "456")])[0]
        assert "指定别名已删除" in ([x async for x in plugin.vt_alias_del(event, "456", "komichi")])[0]
        assert "小路（UID 456）" in ([x async for x in plugin.vt_list(event)])[0]
        plugin.adjustment.process = AsyncMock(return_value={"success": False, "changes": [], "reason": "无明确调播"})
        assert "未保存" in ([x async for x in plugin.vt_adjust_test(event, "小路", text="今晚 推迟到 21:00")])[0]
        assert plugin.adjustment.process.await_args.args[0].text == "今晚 推迟到 21:00"
        assert plugin.adjustment.process.await_args.args[0].uid == 456
        assert plugin.adjustment.process.await_args.kwargs == {"dry_run": True}
        plugin.schedules.retry_adjustment.return_value = True
        assert "重试队列" in ([x async for x in plugin.vt_retry_adjust(event, "小路", "100")])[0]
        plugin.schedules.retry_adjustment.assert_awaited_once_with(456, "100")
        event.is_admin = lambda: False
        assert "管理员" in ([x async for x in plugin.vt_adjust_test(event, "小路", text="取消")])[0]
        assert "管理员" in ([x async for x in plugin.vt_retry_adjust(event, "小路", "100")])[0]
        event.is_admin = lambda: True
        plugin.schedules.get_weekly_schedule.reset_mock()
        assert "时间待定" in ([x async for x in plugin.vt_schedule(event)])[0]
        plugin.schedules.get_weekly_schedule.assert_awaited_once_with(456, "")
        # 开关打开时只发图片，版面数据来自同一份周表。
        plugin.schedules.get_weekly_schedule.reset_mock()
        plugin.schedule_renderer.render = AsyncMock(return_value="C:/tmp/week.png")
        plugin.profiles.banner = AsyncMock(return_value={"header": "data:image/webp;base64,HEAD"})
        plugin.config["schedule_image_enabled"] = True
        assert [x async for x in plugin.vt_schedule(event, "小路", "2026-09-21")] == [("image", "C:/tmp/week.png")]
        drawn = plugin.schedule_renderer.render.await_args.args[0]
        # 图上是主播原名，不是给命令用的别名。
        assert drawn["name"] == "主播" and len(drawn["days"]) == 7 and drawn["summary_ok"] is True
        assert drawn["banner"]["header"] == "data:image/webp;base64,HEAD"
        plugin.profiles.banner.assert_awaited_once_with(456)
        # 渲染失败不能让查询失败，必须回退到纯文字。
        plugin.schedule_renderer.render.side_effect = RuntimeError("browser down")
        fallback = [x async for x in plugin.vt_schedule(event, "小路", "2026-09-21")]
        assert "周表起始日" in fallback[0] and "实际直播：" in fallback[0]
        plugin.config["schedule_image_enabled"] = False
        plugin.pinned.build = AsyncMock(return_value=["screenshot", "image1", "original text"])
        monkeypatch.setattr(main, "forward_chain", lambda parts, sender: ["forward", parts, sender])
        monkeypatch.setattr(main, "message_parts", lambda parts: [[part] for part in parts])
        event.chain_result = lambda content: content
        event.get_platform_name = lambda: "aiocqhttp"
        event.get_self_id = lambda: "999"
        event.send = AsyncMock()
        assert [x async for x in plugin.vt_pinned(event, "小路")] == []
        plugin.pinned.build.assert_awaited_once_with(456)
        event.send.assert_awaited_once_with(["forward", ["screenshot", "image1", "original text"], "999"])
        event.get_platform_name = lambda: "webchat"
        assert [x async for x in plugin.vt_pinned(event, "小路")] == [["screenshot"], ["image1"], ["original text"]]
        event.get_platform_name = lambda: "aiocqhttp"
        event.send.side_effect = RuntimeError("unsupported")
        fallback = [x async for x in plugin.vt_pinned(event, "小路")]
        assert "合并转发发送失败" in fallback[0] and fallback[1:] == [["screenshot"], ["image1"], ["original text"]]
        plugin.pinned.build.return_value = None
        assert "未发现置顶" in ([x async for x in plugin.vt_pinned(event, "小路")])[0]
        # /vt_latest 返回最新动态截图，截图失败退回文字并保留链接。
        post = SimpleNamespace(id="100", text="周表正文", images=("https://i0.hdslb.com/a.png",),
                               published_at=0, is_pinned=True)
        plugin.bili.get_latest_dynamics = AsyncMock(return_value=[post])
        plugin.screenshot = AsyncMock()
        plugin.screenshot.capture = AsyncMock(return_value=b"\x89PNGshot")
        monkeypatch.setattr(main, "image_then_text", lambda raw, text: ("image", text))
        assert [x async for x in plugin.vt_latest(event, "小路")] == [
            ("image", "动态 100\nhttps://t.bilibili.com/100")]
        plugin.screenshot.capture.side_effect = RuntimeError("browser down")
        fallback = [x async for x in plugin.vt_latest(event, "小路")]
        assert "动态截图未生成" in fallback[0] and "https://t.bilibili.com/100" in fallback[0]
        entered = asyncio.Event()
        async def wait_forever():
            entered.set()
            await asyncio.Event().wait()
        plugin.live_listener.run = wait_forever
        dynamic_entered = asyncio.Event()
        async def wait_dynamic():
            dynamic_entered.set()
            await asyncio.Event().wait()
        plugin.dynamic_listener.run = wait_dynamic
        plugin.config["enable_live_polling"] = True
        plugin.config["auto_adjustment_with_schedule"] = True
        plugin._start_tasks()
        task = plugin.live_listener_task
        dynamic_task = plugin.dynamic_listener_task
        watch_task = plugin.schedule_watch_task
        plugin._start_tasks()
        assert plugin.live_listener_task is task
        assert plugin.dynamic_listener_task is dynamic_task
        assert plugin.schedule_watch_task is watch_task
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(dynamic_entered.wait(), 1)
        await plugin.terminate()
        assert task.cancelled()
        assert dynamic_task.cancelled()
        assert watch_task.cancelled()
    finally:
        await plugin.terminate()
        await plugin.terminate()
        sys.modules.pop(module_name, None)
    assert plugin.bili.http.is_closed
