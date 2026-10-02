import json
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from astrbot_plugin_vtuber_monitor.bili_client import BiliClient
from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import DynamicPost
from astrbot_plugin_vtuber_monitor.core.schedule_models import china_today, explicit_week_hint, schedule_from_parser, parse_week_override
from astrbot_plugin_vtuber_monitor.services.dynamic_listener import DynamicListener
from astrbot_plugin_vtuber_monitor.services.schedule_discovery import ScheduleDiscovery
from astrbot_plugin_vtuber_monitor.services.schedule_parser import ScheduleParser, ScheduleParseError
from astrbot_plugin_vtuber_monitor.services.schedule_service import ScheduleService


def current_payload():
    today = china_today()
    monday = today - timedelta(days=today.weekday())
    return {"week_start": monday.isoformat(), "streams": [
        {"date": monday.isoformat(), "start_time": "20:00", "title": "歌回"}]}


@pytest.mark.parametrize("today,label,expected", [
    (date(2026, 9, 28), "本周", date(2026, 9, 28)),
    (date(2026, 9, 28), "上周", date(2026, 9, 21)),
    (date(2026, 9, 28), "下周", date(2026, 10, 5)),
    (date(2026, 10, 4), "这周", date(2026, 9, 28)),
    (date(2027, 1, 1), "本周", date(2026, 12, 28)),
    (date(2027, 1, 1), "下星期", date(2027, 1, 4)),
    (date(2026, 9, 28), "  上周  ", date(2026, 9, 21)),
])
def test_relative_week_uses_monday_across_month_and_year(today, label, expected):
    assert parse_week_override(label, today) == expected


def test_week_input_preserves_date_and_rejects_ambiguous_values():
    assert parse_week_override("", date(2026, 9, 28)) is None
    assert parse_week_override("2026-09-21", date(2026, 9, 28)) == date(2026, 9, 21)
    for value in ("最近", "2026-09-29", "2026-02-30"):
        with pytest.raises(ValueError):
            parse_week_override(value, date(2026, 9, 28))


def schedule(id="100", url="https://i0.hdslb.com/a.png"):
    return replace(schedule_from_parser(1, current_payload()), source_dynamic_id=id, source_image_url=url)


@pytest.mark.asyncio
async def test_observed_opus_format_and_string_timestamp():
    raw = {"code": 0, "data": {"items": [{"id_str": "1234580674174779395", "modules": {
        "module_author": {"mid": 1512246445, "pub_ts": "1786286613"},
        "module_tag": {"text": "置顶"},
        "module_dynamic": {"desc": None, "major": {"type": "MAJOR_TYPE_OPUS", "draw": None,
            "opus": {"title": "本周安排", "summary": {"text": "目前是9.21-9.27的周表"},
                     "pics": [{"url": "http://i0.hdslb.com/a.png"}]}}}}}]}}
    def handler(request):
        assert request.url.params["features"] == "itemOpusStyle"
        return httpx.Response(200, json=raw)
    client = BiliClient(transport=httpx.MockTransport(handler))
    try:
        post = (await client.get_latest_dynamics(1512246445))[0]
        assert post.is_pinned and post.published_at == 1786286613
        assert post.text == "本周安排\n目前是9.21-9.27的周表"
        assert post.images == ("https://i0.hdslb.com/a.png",)
    finally:
        await client.close()


def test_real_vision_fixture_is_old_not_current():
    payload = json.loads((Path(__file__).parent / "fixtures/komichi_week_2026_09_21.json").read_text(encoding="utf-8"))
    parsed = schedule_from_parser(1512246445, payload, today=date(2026, 9, 21))
    assert len(parsed.streams) == 6
    assert parsed.streams[3].start_time is None
    assert parsed.streams[3].status == "unknown"
    with pytest.raises(ValueError, match="本周或下周"):
        schedule_from_parser(1512246445, payload, today=date(2026, 9, 28))
    assert explicit_week_hint("目前是9.21-9.27的周表", date(2026, 9, 28)) == date(2026, 9, 21)
    assert explicit_week_hint("本周 DAY1", date(2026, 9, 28)) is None


@pytest.mark.asyncio
async def test_parser_sends_image_and_source_no_tools(tmp_path):
    image = tmp_path / "test.png"
    image.write_bytes(b"test")
    context = SimpleNamespace(llm_generate=AsyncMock(return_value=SimpleNamespace(completion_text=json.dumps(current_payload()))))
    parser = ScheduleParser(context, "vision")
    result = await parser.parse(1, str(image), source_text="明确周表日期")
    kwargs = context.llm_generate.await_args.kwargs
    assert kwargs["image_urls"] == [str(image)] and "明确周表日期" in kwargs["prompt"]
    assert "tools" not in kwargs and len(result.streams) == 1
    context.llm_generate.return_value.completion_text = "```json\n{}\n```"
    with pytest.raises(ValueError):
        await parser.parse(1, str(image))


@pytest.mark.asyncio
async def test_replacement_revision_same_pinned_id_and_empty_protection(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    service = ScheduleService(data)
    first = schedule()
    assert await service.replace_weekly_schedule(first)
    assert not await service.replace_weekly_schedule(first)
    edited = replace(first, source_image_url="https://i0.hdslb.com/new.png",
                     streams=(replace(first.streams[0], title="新歌回"),))
    assert await service.replace_weekly_schedule(edited)
    assert (await service.get_weekly_schedule(1))["revision"] == 2
    with pytest.raises(ValueError):
        await service.replace_weekly_schedule(replace(edited, streams=()))
    assert (await service.get_weekly_schedule(1))["streams"][0]["title"] == "新歌回"
    assert not await service.replace_weekly_schedule(replace(first, source_dynamic_id="99"))


@pytest.mark.asyncio
async def test_discovery_pending_parse_failure_cache_and_retry(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    client = SimpleNamespace(download_image=AsyncMock(return_value=b"\x89PNGtest"))
    parser = SimpleNamespace(provider_id="", parse=AsyncMock(return_value=schedule()))
    discovery = ScheduleDiscovery(data, client, parser, ScheduleService(data))
    post = DynamicPost(1, "100", "周表", 0, ("https://i0.hdslb.com/a.png",), True)
    assert (await discovery.scan(1, [post]))[0]["status"] == "pending"
    parser.parse.assert_not_awaited()
    parser.provider_id = "vision"
    assert (await discovery.scan(1, [post]))[0]["status"] == "parsed"
    await discovery.scan(1, [post])
    assert parser.parse.await_count == 1 and client.download_image.await_count == 1
    parser.parse.side_effect = ValueError("bad model response")
    changed = replace(post, images=("https://i0.hdslb.com/new.png",))
    assert (await discovery.scan(1, [changed]))[0]["status"] == "failed"
    assert (await data.get_weekly_schedule(1))["source_image_url"] == post.images[0]
    parser.parse.side_effect = None
    assert (await discovery.scan(1, [changed], force=True))[0]["status"] == "parsed"
    assert (await data.get_weekly_schedule(1))["revision"] == 2


@pytest.mark.asyncio
async def test_not_schedule_candidate_is_not_downloaded_again(tmp_path):
    """判定过"不是周表"的配图：原图被清理掉之后也不重新下载、不重新调模型。"""
    data = DataManager(tmp_path)
    await data.initialize()
    client = SimpleNamespace(download_image=AsyncMock(return_value=b"\x89PNGart"))
    parser = SimpleNamespace(provider_id="vision",
                             parse=AsyncMock(side_effect=ScheduleParseError("not_schedule")))
    discovery = ScheduleDiscovery(data, client, parser, ScheduleService(data))
    post = DynamicPost(1, "100", "周表", 0, ("https://i0.hdslb.com/a.png",), True)
    assert (await discovery.scan(1, [post]))[0]["status"] == "skipped"
    assert client.download_image.await_count == 1 and parser.parse.await_count == 1
    for path in (tmp_path / "schedule_images").glob("*"):
        path.unlink()                       # 模拟保留策略把这张判定过的原图清掉
    assert await discovery.scan(1, [post]) == []
    assert client.download_image.await_count == 1 and parser.parse.await_count == 1
    # 正文变了就要重新判定，不能一直沿用旧结论。
    changed = replace(post, text="周表（补充说明）")
    assert (await discovery.scan(1, [changed]))[0]["status"] == "skipped"
    assert parser.parse.await_count == 2


@pytest.mark.asyncio
async def test_old_pinned_without_model_is_kept_for_later(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    today = china_today()
    last_monday = today - timedelta(days=today.weekday() + 7)
    end = last_monday + timedelta(days=6)
    post = DynamicPost(1, "100", f"周表 {last_monday.month}.{last_monday.day}-{end.month}.{end.day}",
                       0, ("https://i0.hdslb.com/a.png",), True)
    client = SimpleNamespace(download_image=AsyncMock(return_value=b"\x89PNGtest"))
    parser = SimpleNamespace(provider_id="", parse=AsyncMock())
    records = await ScheduleDiscovery(data, client, parser, ScheduleService(data)).scan(1, [post])
    assert records[0]["status"] == "stale"
    assert Path(records[0]["local_image_path"]).is_file()
    parser.parse.assert_not_awaited()
    assert await data.get_weekly_schedule(1) is None


@pytest.mark.asyncio
async def test_history_archive_versions_query_and_current_isolation(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    service = ScheduleService(data)
    current = schedule()
    await service.store_parsed_schedule(current)
    start = date.fromisoformat(current.week_start) - timedelta(days=7)
    old = replace(current, week_start=start.isoformat(),
                  streams=(replace(current.streams[0], date=start.isoformat(), original_date=start.isoformat()),))
    assert await service.store_parsed_schedule(old) == "archived"
    assert await service.store_parsed_schedule(old) == "archived"
    assert (await service.get_weekly_schedule(1))["week_start"] == current.week_start
    assert (await service.get_weekly_schedule(1, start.isoformat()))["revision"] == 1
    edit = replace(old, streams=(replace(old.streams[0], title="修订歌回"),))
    await service.store_parsed_schedule(edit)
    cached = await service.get_weekly_schedule(1, start.isoformat())
    assert cached["revision"] == 2 and cached["streams"][0]["title"] == "修订歌回"
    assert len(await service.get_schedule_history(1)) == 2
    other_data = DataManager(tmp_path / "history_only")
    await other_data.initialize()
    history_only = ScheduleService(other_data)
    await history_only.store_parsed_schedule(old)
    assert await history_only.get_weekly_schedule(1) is None
    assert (await history_only.get_weekly_schedule(1, start.isoformat()))["week_start"] == start.isoformat()
    with pytest.raises(ValueError):
        await service.get_weekly_schedule(1, "not-a-date")


@pytest.mark.asyncio
async def test_listener_passes_pinned_page_even_when_no_new_dynamics():
    post = DynamicPost(1, "1", "周表", 0, ("https://i0.hdslb.com/a.png",), True)
    data = SimpleNamespace(get_special_vtubers=AsyncMock(return_value=[1]), ingest_dynamics=AsyncMock(return_value=[]))
    discovery = SimpleNamespace(scan=AsyncMock())
    listener = DynamicListener(data, SimpleNamespace(get_latest_dynamics=AsyncMock(return_value=[post])), discovery=discovery)
    await listener.poll_once()
    discovery.scan.assert_awaited_once_with(1, [post])


@pytest.mark.parametrize("payload,code", [({"status": "not_schedule"}, "not_schedule"),
    ({"status": "needs_date"}, "needs_date"), ({}, "unresolved")])
@pytest.mark.asyncio
async def test_parser_explains_non_schedule_and_missing_dates(tmp_path, payload, code):
    path = tmp_path / "image.png"
    path.write_bytes(b"test")
    context = SimpleNamespace(llm_generate=AsyncMock(return_value=SimpleNamespace(completion_text=json.dumps(payload))))
    with pytest.raises(ScheduleParseError) as error:
        await ScheduleParser(context, "vision").parse(1, str(path))
    assert error.value.code == code


@pytest.mark.asyncio
async def test_explicit_week_is_passed_and_conflicts_rejected(tmp_path):
    path = tmp_path / "image.png"
    path.write_bytes(b"test")
    payload = current_payload()
    context = SimpleNamespace(llm_generate=AsyncMock(return_value=SimpleNamespace(completion_text=json.dumps(payload))))
    parser = ScheduleParser(context, "vision")
    assert (await parser.parse(1, str(path), week_start=payload["week_start"])).week_start == payload["week_start"]
    assert f"用户明确指定的周一起点：{payload['week_start']}" in context.llm_generate.await_args.kwargs["prompt"]
    next_week = (date.fromisoformat(payload["week_start"]) + timedelta(days=7)).isoformat()
    with pytest.raises(ScheduleParseError) as error:
        await parser.parse(1, str(path), week_start=next_week)
    assert error.value.code == "date_conflict"
    before = context.llm_generate.await_count
    with pytest.raises(ValueError):
        await parser.parse(1, str(path), week_start="not-a-date")
    assert context.llm_generate.await_count == before


@pytest.mark.asyncio
async def test_discovery_skips_art_reports_date_and_scans_beyond_eight(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    client = SimpleNamespace(download_image=AsyncMock(return_value=b"\x89PNGtest"))
    parser = SimpleNamespace(provider_id="vision", parse=AsyncMock(side_effect=[
        *[ScheduleParseError("not_schedule") for _ in range(8)], ScheduleParseError("needs_date")]))
    discovery = ScheduleDiscovery(data, client, parser, ScheduleService(data))
    post = DynamicPost(1, "100", "置顶个人介绍", 0, tuple(f"https://i0.hdslb.com/{i}.png" for i in range(9)), True)
    records = await discovery.scan(1, [post])
    assert len(records) == 9
    assert records[0]["status"] == "skipped"
    assert records[-1]["status"] == "needs_date" and records[-1]["image_index"] == 9
    assert await data.get_weekly_schedule(1) is None
    parser.parse.side_effect = None
    parser.parse.return_value = schedule()
    await discovery.scan(1, [post], force=True, week_start=current_payload()["week_start"])
    assert parser.parse.await_args.kwargs["week_start"] == current_payload()["week_start"]


@pytest.mark.asyncio
async def test_model_failure_is_safe_and_distinct(tmp_path):
    path = tmp_path / "image.png"
    path.write_bytes(b"test")
    context = SimpleNamespace(llm_generate=AsyncMock(side_effect=RuntimeError("secret provider credentials")))
    with pytest.raises(ScheduleParseError) as error:
        await ScheduleParser(context, "vision").parse(1, str(path))
    assert error.value.code == "model_error" and "secret" not in str(error.value)
