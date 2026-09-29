import asyncio
from dataclasses import replace
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import DynamicPost
from astrbot_plugin_vtuber_monitor.core.schedule_models import china_today, schedule_from_parser
from astrbot_plugin_vtuber_monitor.services.schedule_service import ScheduleService
from astrbot_plugin_vtuber_monitor.services.schedule_watch import ScheduleWatch, monday, snapshot


async def setup(tmp_path, offset=-1):
    data = DataManager(tmp_path)
    await data.initialize()
    current = monday(china_today())
    start = current + timedelta(weeks=offset)
    url = "https://i0.hdslb.com/week.png"
    post = DynamicPost(1, "100", "周表", 1, (url,), True)
    def parsed(week, title="直播"):
        return replace(schedule_from_parser(1, {"week_start": week.isoformat(), "streams": [
            {"date": week.isoformat(), "start_time": "20:00", "title": title}]}, allow_history=True),
            source_dynamic_id="100", source_image_url=url)
    parser = SimpleNamespace(parse=AsyncMock(return_value=parsed(start + timedelta(days=7))),
                             is_schedule_image=AsyncMock(return_value=True))
    bili = SimpleNamespace(get_dynamic=AsyncMock(return_value=post),
                           download_image=AsyncMock(return_value=b"\x89PNGnew"))
    service = ScheduleService(data)
    discovery = SimpleNamespace(data=data, parser=parser, bili=bili, schedules=service, _lock=asyncio.Lock())
    watch = ScheduleWatch(discovery)
    initial = parsed(start)
    await service.store_parsed_schedule(initial)
    await watch.remember(post, initial, b"\x89PNGold")
    return watch, discovery, current, parsed


@pytest.mark.asyncio
@pytest.mark.parametrize("offset", [-1, 0])
async def test_anchor_advances_exactly_one_week_and_restart_deduplicates(tmp_path, offset):
    watch, d, day, parsed = await setup(tmp_path, offset)
    await watch.check(1, day)
    expected = (day + timedelta(weeks=offset + 1)).isoformat()
    assert d.parser.parse.call_args.kwargs["week_start"] == expected
    state = await d.data.get_schedule_tracking(1)
    assert state["week"] == expected
    assert state["anchor_week"] == (day + timedelta(weeks=offset)).isoformat()
    assert await d.data.get_historical_schedule(1, expected)
    await ScheduleWatch(d).check(1, day)
    assert d.bili.get_dynamic.await_count == 1
    await watch.check(1, day + timedelta(days=1))
    assert d.parser.parse.await_count == 1


@pytest.mark.asyncio
async def test_second_update_uses_llm_and_can_keep_same_week(tmp_path):
    watch, d, day, parsed = await setup(tmp_path)
    await watch.check(1, day)
    d.bili.download_image.return_value = b"\x89PNGsecond"
    d.parser.parse.return_value = parsed(day, "修订")
    await watch.check(1, day + timedelta(days=1))
    options = d.parser.parse.call_args.kwargs
    assert "week_start" not in options
    assert options["update_context"]["updates_this_week"] == 2
    assert (await d.data.get_schedule_tracking(1))["week"] == day.isoformat()
    assert (await d.data.get_historical_schedule(1, day.isoformat()))["streams"][0]["title"] == "修订"


@pytest.mark.asyncio
async def test_failure_retains_anchor_and_retries_same_observation(tmp_path):
    watch, d, day, parsed = await setup(tmp_path)
    d.parser.parse.side_effect = ValueError("ambiguous")
    await watch.check(1, day)
    state = await d.data.get_schedule_tracking(1)
    assert state["pending"] and state["week"] == state["anchor_week"]
    d.parser.parse.side_effect = None
    await ScheduleWatch(d).check(1, day + timedelta(days=1))
    state = await d.data.get_schedule_tracking(1)
    assert state["pending"] is None and len(state["observations"]) == 1
    assert state["week"] == day.isoformat()


@pytest.mark.asyncio
async def test_crash_after_import_reuses_parsed_result(tmp_path):
    watch, d, day, parsed = await setup(tmp_path)
    original = d.schedules.store_parsed_schedule
    async def crash(*args, **kwargs):
        await original(*args, **kwargs)
        raise asyncio.CancelledError()
    d.schedules.store_parsed_schedule = crash
    with pytest.raises(asyncio.CancelledError):
        await watch.check(1, day)
    assert (await d.data.get_schedule_tracking(1))["pending"]["parsed"]
    d.schedules.store_parsed_schedule = original
    await ScheduleWatch(d).check(1, day)
    assert d.parser.parse.await_count == 1
    assert (await d.data.get_schedule_tracking(1))["week"] == day.isoformat()
    history = await d.data.get_schedule_history(1)
    assert next(r for r in history if r["week_start"] == day.isoformat())["versions"] == 1


@pytest.mark.asyncio
async def test_no_anchor_no_request_and_unsubscribed_not_polled(tmp_path):
    watch, d, day, parsed = await setup(tmp_path)
    await watch.check(2, day)
    await watch.run_once()
    d.bili.get_dynamic.assert_not_awaited()


def test_fingerprint_ignores_prose_but_detects_dates_and_image_bytes():
    assert snapshot(b"img", "周表9.21-9.27") == snapshot(b"img", "周表9.21-9.27 谢谢")
    assert snapshot(b"img", "9.21-9.27") != snapshot(b"img", "9.28-10.4")
    assert snapshot(b"old", "周表") != snapshot(b"new", "周表")
    assert monday(date(2027, 1, 1)) == date(2026, 12, 28)


@pytest.mark.asyncio
async def test_images_are_immutable_between_versions(tmp_path):
    from pathlib import Path
    data = DataManager(tmp_path)
    await data.initialize()
    first = await data.save_schedule_image(1, "100", "same-url", b"\x89PNGold")
    second = await data.save_schedule_image(1, "100", "same-url", b"\x89PNGnew")
    assert first != second
    assert Path(first).read_bytes() == b"\x89PNGold"


@pytest.mark.asyncio
async def test_text_date_changes_trigger_import_even_if_bytes_unchanged(tmp_path):
    watch, d, day, parsed = await setup(tmp_path)
    d.bili.download_image.return_value = b"\x89PNGold"
    d.bili.get_dynamic.return_value = replace(d.bili.get_dynamic.return_value, text="周表9.28-10.4")
    await watch.check(1, day)
    d.parser.parse.assert_awaited_once()


@pytest.mark.asyncio
async def test_new_calendar_week_resets_multiple_update_rule(tmp_path):
    watch, d, day, parsed = await setup(tmp_path)
    await watch.check(1, day)
    d.bili.download_image.return_value = b"\x89PNGnext-week"
    d.parser.parse.return_value = parsed(day + timedelta(days=7))
    await watch.check(1, day + timedelta(days=7))
    assert d.parser.parse.call_args.kwargs["week_start"] == (day + timedelta(days=7)).isoformat()
    assert len((await d.data.get_schedule_tracking(1))["observations"]) == 1


@pytest.mark.asyncio
async def test_multiple_update_ambiguous_keeps_saved_schedule(tmp_path):
    watch, d, day, parsed = await setup(tmp_path)
    await watch.check(1, day)
    previous = await d.data.get_historical_schedule(1, day.isoformat())
    d.bili.download_image.return_value = b"\x89PNGsecond"
    d.parser.parse.side_effect = ValueError("needs_date")
    await watch.check(1, day + timedelta(days=1))
    assert await d.data.get_historical_schedule(1, day.isoformat()) == previous
    assert (await d.data.get_schedule_tracking(1))["pending"]["multiple"]
