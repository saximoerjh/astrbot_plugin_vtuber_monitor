"""需用 AstrBot 自带的 Python 手动执行；不联网、不发消息。"""
import asyncio
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch


def main():
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    original_cwd = Path.cwd()
    with tempfile.TemporaryDirectory(prefix="vt-runtime-smoke-") as directory:
        try:
            os.chdir(directory)
            from astrbot.api.star import StarTools
            from astrbot_plugin_vtuber_monitor.main import MyPlugin
            from astrbot_plugin_vtuber_monitor.tools.schedule_tools import build_toolset
            from astrbot.core.star.filter.command import CommandFilter, GreedyStr
            from types import SimpleNamespace
            from astrbot_plugin_vtuber_monitor.services.pinned_service import PinnedPart, forward_chain
            from astrbot_plugin_vtuber_monitor.services.dispatcher import make_live_message

            async def check():
                import json
                from astrbot.api import AstrBotConfig
                from astrbot_plugin_vtuber_monitor.core.plugin_config import prepare_config
                import astrbot_plugin_vtuber_monitor.core.plugin_config as config_module
                schema = json.loads((Path(config_module.__file__).resolve().parents[1] / "_conf_schema.json").read_text(encoding="utf-8"))
                config_path = Path(directory) / "legacy_config.json"
                config_path.write_text(json.dumps({"live_poll_interval": 240, "auto_special_live": False,
                                                   "schedule_provider_id": "test/provider"}), encoding="utf-8")
                migrated = AstrBotConfig(str(config_path), schema=schema)
                flat = prepare_config(migrated)
                assert flat["live_poll_interval"] == 240 and not flat["auto_special_live"]
                assert flat["schedule_provider_id"] == "test/provider"
                migrated["live"]["live_poll_interval"] = 150
                migrated.save_config()
                assert prepare_config(AstrBotConfig(str(config_path), schema=schema))["live_poll_interval"] == 150
                with patch.object(StarTools, "get_data_dir", return_value=Path(directory) / "plugin_data"):
                    plugin = MyPlugin(object(), {})
                    try:
                        await plugin.initialize()
                        class Event:
                            def plain_result(self, text):
                                return text
                        assert [s async for s in plugin.vt_ping(Event())] == ["VTuber Monitor OK"]
                        assert plugin.live_listener_task is not None
                        assert plugin.live_listener.special_only
                        assert plugin.dynamic_listener_task is not None
                        assert plugin.dynamic_listener.eligible_only
                        assert plugin.dynamic_listener.require_schedule
                        live_task, dynamic_task = plugin.live_listener_task, plugin.dynamic_listener_task
                        from astrbot.core.message.components import Plain, Image
                        live_message = make_live_message("live title and link", "https://i0.hdslb.com/cover.jpg")
                        assert isinstance(live_message.chain[0], Plain)
                        assert isinstance(live_message.chain[1], Image)
                        watch_task = plugin.schedule_watch_task
                        assert watch_task is not None
                        profile_task = plugin.profile_task
                        assert profile_task is not None and not profile_task.done()
                        await asyncio.sleep(0)
                        plugin._start_tasks()
                        assert plugin.schedule_watch_task is watch_task
                        assert plugin.live_listener_task is live_task
                        assert plugin.dynamic_listener_task is dynamic_task
                        assert plugin.profile_task is profile_task
                        assert "未配置" in ([s async for s in plugin.vt_status(Event())])[0]
                        toolset = build_toolset()
                        assert {tool.name for tool in toolset.tools} == {
                            "reschedule_stream", "cancel_stream", "add_stream", "update_stream_info"}
                        command = CommandFilter("vt_adjust_test", handler_md=SimpleNamespace(handler=MyPlugin.vt_adjust_test))
                        assert command.handler_params["text"] is GreedyStr
                        params = command.validate_and_convert_params(["小路", "今晚", "推迟到", "21:00"], command.handler_params)
                        assert params == {"uid": "小路", "text": "今晚 推迟到 21:00"}
                        import base64
                        pixel = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl6lGkAAAAASUVORK5CYII=")
                        nodes = forward_chain([PinnedPart(image=pixel), PinnedPart(image=pixel),
                                               PinnedPart(text="original text")], "123456")[0]
                        wire = await nodes.to_dict()
                        assert [n["data"]["content"][0]["type"] for n in wire["messages"]] == ["image", "image", "text"]
                        # 周表图片：本机渲染的 PNG 必须能被真实图片组件引用。
                        from datetime import date
                        from astrbot.core.message.message_event_result import MessageEventResult
                        from astrbot_plugin_vtuber_monitor.services.schedule_renderer import build_schedule_view
                        plan = {"id": "s", "date": "2026-09-21", "start_time": "20:00", "title": "歌回",
                                "status": "scheduled", "source": "weekly_image", "revision": 1,
                                "original_date": "2026-09-21", "original_start_time": "20:00",
                                "original_end_time": "22:00"}
                        view = build_schedule_view({"uid": 1, "week_start": "2026-09-21", "revision": 1,
                                                    "streams": [plan]}, uid=1, display_name="预览",
                                                   summary={"recorded": 0, "extra": 0, "pending": 0, "unknown": 0},
                                                   today=date(2026, 9, 21))
                        rendered = await plugin.schedule_renderer.render(view)
                        component = MessageEventResult().file_image(rendered).chain[0]
                        assert isinstance(component, Image) and Path(component.path).is_file()
                    finally:
                        await plugin.terminate()
                    assert plugin.bili.http.is_closed
                    assert watch_task.cancelled()
                    assert live_task.cancelled() and dynamic_task.cancelled()
                    assert profile_task.cancelled()
                    print("PASS: real AstrBot lifecycle, live notification Plain/Image, ToolSet, GreedyStr and forward Nodes serialization; no network/messages")
            asyncio.run(check())
        finally:
    # Windows 无法删除进程当前所在的工作目录。
            os.chdir(original_cwd)


if __name__ == "__main__":
    main()
