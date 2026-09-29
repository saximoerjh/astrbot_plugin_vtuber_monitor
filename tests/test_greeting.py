from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from astrbot_plugin_vtuber_monitor.core.countries import COUNTRIES, WINDOW_END, WINDOW_START
from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.services import greeting_service
from astrbot_plugin_vtuber_monitor.services.greeting_service import (
    EXAMPLES, GreetingService, at_four, clean_sentence, greeting_for, nearest_to_four)

CHINA = timezone(timedelta(hours=8))


def build_context(text="现在是马里凌晨四点半左右"):
    return SimpleNamespace(llm_generate=AsyncMock(return_value=SimpleNamespace(completion_text=text)))


def pick(label):
    """让随机抽选在断言中可复现。"""
    return patch.object(greeting_service.random, "choice",
                        side_effect=lambda items: next(item for item in items if item[0] == label))


async def build_service(tmp_path, context=None, provider="p1"):
    data = DataManager(tmp_path)
    await data.initialize()
    return GreetingService(context or build_context(), data, provider), data


def test_greeting_bands_wrap_past_midnight():
    assert [greeting_for(hour) for hour in (5, 10, 11, 12, 13, 17)] == \
        ["早上好", "早上好", "中午好", "中午好", "下午好", "下午好"]
    assert [greeting_for(hour) for hour in (18, 20, 21, 23, 0, 4)] == \
        ["晚上好", "晚上好", "晚安", "晚安", "晚安", "晚安"]


def test_clean_sentence_keeps_one_line_without_quotes():
    assert clean_sentence("“现在是英国凌晨的四点多哦”") == "现在是英国凌晨的四点多哦"
    assert clean_sentence("  现在是马里凌晨四点半左右\n多余的一行 ") == "现在是马里凌晨四点半左右"
    assert clean_sentence("") == "" and clean_sentence("x" * 61) == ""


def test_table_covers_every_hour_of_the_year():
    """任何月份里，一小时宽的窗口都不能为空。"""
    for month in range(1, 13):
        anchor = datetime(2026, month, 15, 12, 0, tzinfo=timezone.utc)
        offsets = {int(anchor.astimezone(ZoneInfo(zone)).utcoffset().total_seconds() // 60) % 1440
                   for _, zone in COUNTRIES}
        for step in range(0, 24 * 60, 5):
            required = (WINDOW_START * 60 - step) % 1440
            assert any((offset - required) % 1440 < (WINDOW_END - WINDOW_START) * 60 for offset in offsets), \
                f"empty window month={month} utc_minute={step}"


def test_at_four_uses_each_zone_local_clock():
    # 北京 12:10 相当于马里（UTC+0）04:10、英国（夏令时 UTC+1）05:10。
    moment = datetime(2026, 9, 29, 12, 10, tzinfo=CHINA)
    labels = [label for label, _ in at_four(moment)]
    assert "马里" in labels and "英国" not in labels
    assert all(local.hour == 4 for _, local in at_four(moment))


def test_nearest_to_four_is_the_safety_net():
    moment = datetime(2026, 9, 29, 12, 10, tzinfo=CHINA)
    assert nearest_to_four(moment) and len(nearest_to_four(moment)) < len(COUNTRIES)


@pytest.mark.asyncio
async def test_message_prefix_uses_beijing_time_and_a_plus(tmp_path):
    service, _ = await build_service(tmp_path, build_context("现在是马里凌晨四点半左右"))
    with pick("马里"):
        noon = datetime(2026, 9, 29, 4, 10, tzinfo=timezone.utc)  # 北京 12:10
        assert await service.build(noon) == "小路泥中午好+\n现在是马里凌晨四点半左右"
    midnight = datetime(2026, 9, 29, 17, 10, tzinfo=timezone.utc)  # 北京 01:10
    assert (await service.build(midnight)).startswith("小路泥晚安+\n")


@pytest.mark.asyncio
async def test_prompt_carries_the_few_shot_examples(tmp_path):
    context = build_context("现在是安提瓜和巴布达的凌晨四点")
    service, _ = await build_service(tmp_path, context)
    with pick("马里"):
        text = await service.build(datetime(2026, 9, 29, 12, 10, tzinfo=CHINA))
    prompt = context.llm_generate.await_args.kwargs["prompt"]
    assert all(example in prompt for example in EXAMPLES)
    assert "马里" in prompt and '"当地时间": "04:10"' in prompt
    assert "凌晨四点" in context.llm_generate.await_args.kwargs["system_prompt"]
    assert text.endswith("现在是安提瓜和巴布达的凌晨四点")


@pytest.mark.asyncio
async def test_recent_picks_are_not_repeated(tmp_path):
    service, data = await build_service(tmp_path)
    pool = [("甲国", datetime(2026, 9, 29, 4, 10, tzinfo=CHINA)),
            ("乙国", datetime(2026, 9, 29, 4, 20, tzinfo=CHINA))]
    with patch.object(greeting_service, "at_four", return_value=pool), \
            patch.object(greeting_service.random, "choice", side_effect=lambda items: items[0]):
        await service.build()
        first = (await data.recent_greetings(1))[0]
        await service.build()
        second = (await data.recent_greetings(1))[0]
    assert first != second
    assert await data.recent_greetings(5) == ["乙国", "甲国"]


@pytest.mark.asyncio
async def test_model_failure_and_missing_provider_use_the_template(tmp_path):
    failing = SimpleNamespace(llm_generate=AsyncMock(side_effect=RuntimeError("boom")))
    service, _ = await build_service(tmp_path, failing)
    with pick("马里"):
        text = await service.build(datetime(2026, 9, 29, 12, 10, tzinfo=CHINA))
    assert text == "小路泥中午好+\n现在是马里凌晨四点多"
    plain, _ = await build_service(tmp_path / "b", build_context(), provider="")
    assert (await plain.build()).splitlines()[1].endswith("凌晨四点多")
