"""需手动开启的真实模型检查，使用临时数据库，绝不发送平台消息。

用 AstrBot 自带的 Python 加 --run 执行；只在内存中读取已配置的模型凭据。
"""
import argparse
import asyncio
import json
import logging
import os
import sys
import tempfile
import time
from dataclasses import replace
from datetime import timedelta
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true", required=True)
    parser.parse_args()
    root = Path(__file__).resolve().parents[4]
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    cfg = json.loads((root / "data/cmd_config.json").read_text(encoding="utf-8-sig"))
    plugin_cfg = json.loads((root / "data/config/astrbot_plugin_vtuber_monitor_config.json").read_text(encoding="utf-8-sig"))
    provider_id = plugin_cfg.get("adjustment_provider_id") or plugin_cfg.get("schedule_provider_id")
    provider_config = next(p for p in cfg["provider"] if p["id"] == provider_id)
    source = next(s for s in cfg["provider_sources"] if s["id"] == provider_config["provider_source_id"])
    previous = Path.cwd()
    logging.disable(logging.CRITICAL)
    with tempfile.TemporaryDirectory(prefix="vt-live-adjustment-") as directory:
        try:
            os.chdir(directory)
            from astrbot.core.provider.sources.openai_source import ProviderOpenAIOfficial
            from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
            from astrbot_plugin_vtuber_monitor.core.models import DynamicPost
            from astrbot_plugin_vtuber_monitor.core.schedule_models import china_today, schedule_from_parser
            from astrbot_plugin_vtuber_monitor.services.schedule_service import ScheduleService
            from astrbot_plugin_vtuber_monitor.services.adjustment_agent import AdjustmentAgent

            async def check():
                provider = ProviderOpenAIOfficial({**source, **provider_config}, cfg.get("provider_settings", {}))
                class Context:
                    async def llm_generate(self, **kwargs):
                        kwargs.pop("chat_provider_id")
                        kwargs["func_tool"] = kwargs.pop("tools")
                        return await provider.text_chat(**kwargs)
                try:
                    data = DataManager(Path(directory) / "isolated_data")
                    await data.initialize()
                    service = ScheduleService(data)
                    today = china_today()
                    monday = today - timedelta(days=today.weekday())
                    schedule = replace(schedule_from_parser(1, {"week_start": monday.isoformat(), "streams": [
                        {"date": today.isoformat(), "start_time": "20:00", "title": "测试歌回"}]}), source_dynamic_id="100")
                    await service.store_parsed_schedule(schedule)
                    agent = AdjustmentAgent(Context(), service, provider_id)
                    text = f"通知：{today.isoformat()} 20:00 的测试歌回推迟到当天 21:00，其他安排不变。"
                    result = await agent.process(DynamicPost(1, "101", text, int(time.time())))
                    assert result["success"] and result["after"]["streams"][0]["start_time"] == "21:00"
                    assert (await service.get_weekly_schedule(1))["revision"] == 2
                    print("PASS: real model selected reschedule tool; isolated service/DB revision 2", flush=True)
                    result = await agent.process(DynamicPost(1, "102", "我取消了外卖订单，不是取消直播，今晚测试歌回照常。", int(time.time())))
                    assert not result["success"] and (await service.get_weekly_schedule(1))["revision"] == 2
                    print("PASS: negated/non-schedule cancellation produced no mutation; no real user data/messages touched", flush=True)
                finally:
                    await provider.terminate()
            asyncio.run(check())
        finally:
            os.chdir(previous)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL: {type(error).__name__}; provider details suppressed to protect credentials")
        sys.exit(1)
