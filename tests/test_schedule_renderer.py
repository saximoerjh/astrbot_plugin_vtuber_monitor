"""周表图片：纯数据整理、HTML 转义与渲染缓存。"""
from datetime import date
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_vtuber_monitor.services.schedule_renderer import (
    BOARD_COLUMNS, BOARD_WIDTH, FONT_SCALE,
    ScheduleRenderError, ScheduleRenderer, build_schedule_html, build_schedule_view)

WEEK = "2026-09-21"


def plan(**overrides):
    item = {"id": "s1", "date": WEEK, "start_time": "20:00", "title": "歌回",
            "status": "scheduled", "source": "weekly_image", "revision": 1,
            "original_date": WEEK, "original_start_time": "20:00", "original_end_time": "22:00",
            "rescheduled_date": None, "rescheduled_start_time": None, "actual_intervals": []}
    item.update(overrides)
    return item


def schedule(streams, *, week_start=WEEK, revision=1):
    return {"uid": 1, "week_start": week_start, "revision": revision, "streams": list(streams)}


def interval(start, end, *, session="a", start_basis="api"):
    return {"session_id": session, "start": start, "end": end, "start_basis": start_basis,
            "end_basis": "observed" if end else None}


def view(streams, **kwargs):
    return build_schedule_view(schedule(streams), uid=1, display_name="小路",
                               today=date(2026, 9, 23), **kwargs)


def test_timeline_has_seven_days_and_places_stream_by_effective_date():
    built = view([plan()])
    assert [day["weekday"] for day in built["days"]] == list("一二三四五六日")
    assert [day["date"] for day in built["days"]] == [
        "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24",
        "2026-09-25", "2026-09-26", "2026-09-27"]
    assert built["week_range"] == "09-21 ~ 09-27"
    assert built["name"] == "小路"
    assert built["total"] == 1
    monday, tuesday = built["days"][0], built["days"][1]
    assert [item["title"] for item in monday["streams"]] == ["歌回"]
    assert tuesday["streams"] == []
    assert built["days"][2]["is_today"] and not monday["is_today"]


def test_rescheduled_stream_moves_to_new_day_and_keeps_original():
    built = view([plan(date="2026-09-22", start_time="21:30", status="postponed",
                       rescheduled_date="2026-09-22", rescheduled_start_time="21:30")])
    item = built["days"][1]["streams"][0]
    assert item["time_label"] == "21:30"
    assert item["original_label"] == "原定 09-21（周一）20:00–22:00"
    assert built["days"][0]["streams"] == []
    # 卡片所在列就是调播后的日期，“调播至 …”整行都属于重复信息。
    assert "调播至" not in build_schedule_html(built)


def test_unchanged_slot_shows_range_inline_without_repeating_the_original():
    normal = view([plan()])["days"][0]["streams"][0]
    assert normal["time_label"] == "20:00–22:00"
    assert normal["original_label"] == "" and normal["actual_lines"] == []
    assert "原定" not in build_schedule_html(view([plan()]))


def test_overnight_range_and_unknown_time_are_readable():
    overnight = view([plan(original_end_time="01:00")])["days"][0]["streams"][0]
    assert overnight["time_label"] == "20:00–次日 01:00"
    unknown = view([plan(start_time=None, original_start_time=None, original_end_time=None,
                         status="unknown")])["days"][0]["streams"][0]
    assert unknown["time_label"] == "待定"
    assert unknown["original_label"] == ""
    assert unknown["status_label"] == "待定"


def test_extra_live_observation_is_marked_and_ordered_last():
    built = view([plan(id="extra", source="live_observation", original_date=None,
                       start_time="22:00", title="深夜杂谈", status="completed",
                       original_start_time=None, original_end_time=None),
                  plan(title="歌回")])
    titles = [item["title"] for item in built["days"][0]["streams"]]
    assert titles == ["歌回", "深夜杂谈"]
    extra = built["days"][0]["streams"][1]
    assert extra["extra"] and extra["status_label"] == "突击直播"
    assert extra["original_label"] == "周表外记录"


def test_actual_intervals_and_status_labels():
    built = view([plan(status="cancelled", actual_intervals=[
        {"session_id": "x", "start": "2026-09-21T20:05:00+08:00", "end": "2026-09-21T23:00:00+08:00",
         "start_basis": "api", "end_basis": "observed"}])])
    item = built["days"][0]["streams"][0]
    assert item["status_label"] == "已取消"
    assert item["actual_lines"] == ["20:05–23:00 · 2小时55分"]


def test_actual_line_keeps_only_times_and_duration():
    built = view([plan(actual_intervals=[
        {"session_id": "c", "start": "2026-09-21T14:07:00+08:00", "end": "2026-09-21T16:20:00+08:00",
         "start_basis": "observed", "end_basis": "observed"},
        {"session_id": "d", "start": "2026-09-21T21:04:00+08:00", "end": "2026-09-22T00:40:00+08:00",
         "start_basis": "api", "end_basis": "observed"}])])
    assert built["days"][0]["streams"][0]["actual_lines"] == [
        "14:07–16:20 · 2小时13分",
        "21:04–次日 00:40 · 3小时36分"]


def test_actual_line_marks_ongoing_and_keeps_off_column_dates():
    built = view([plan(actual_intervals=[
        {"session_id": "e", "start": "2026-09-21T21:02:00+08:00", "end": None,
         "start_basis": "api", "end_basis": None},
        {"session_id": "f", "start": "2026-09-20T19:00:00+08:00", "end": "2026-09-20T20:30:00+08:00",
         "start_basis": "observed", "end_basis": "observed"}])])
    assert built["days"][0]["streams"][0]["actual_lines"] == [
        "21:02 起 · 直播中",
        "09-20 19:00–20:30 · 1小时30分"]


def test_duration_formatting_covers_short_and_whole_hours():
    def durations(pairs):
        return view([plan(actual_intervals=[
            {"session_id": str(index), "start": start, "end": end,
             "start_basis": "api", "end_basis": "observed"}
            for index, (start, end) in enumerate(pairs)])])["days"][0]["streams"][0]["actual_lines"]

    assert durations([("2026-09-21T20:00:00+08:00", "2026-09-21T20:45:00+08:00")]) == ["20:00–20:45 · 45分"]
    assert durations([("2026-09-21T20:00:00+08:00", "2026-09-21T23:00:00+08:00")]) == ["20:00–23:00 · 3小时"]
    assert durations([("2026-09-21T20:00:00+08:00", "2026-09-21T20:00:30+08:00")]) == ["20:00–20:00 · 不足1分"]


def test_summary_failure_is_labelled_instead_of_showing_zero():
    failed = view([plan()], summary=None)
    assert failed["summary_ok"] is False
    assert failed["summary_text"] == "实际直播：统计失败。"
    counted = view([plan()], summary={"recorded": 3, "extra": 1, "pending": 2, "unknown": 4})
    assert counted["summary_ok"] is True
    assert "已记录 3 场（突击 1 场）" in counted["summary_text"]
    assert "未记录 4 场" in counted["summary_text"]


def stats_of(streams):
    return {item["label"]: item["value"] for item in view(streams)["stats"]}


def test_stats_sum_durations_and_average_across_recorded_streams():
    built = stats_of([
        plan(actual_intervals=[interval("2026-09-21T20:00:00+08:00", "2026-09-21T22:00:00+08:00")]),
        plan(id="b", date="2026-09-22", start_time="21:00", original_date="2026-09-22",
             original_start_time="21:00", actual_intervals=[
                 interval("2026-09-22T21:00:00+08:00", "2026-09-22T23:00:00+08:00", session="b1"),
                 interval("2026-09-22T23:30:00+08:00", "2026-09-23T00:00:00+08:00", session="b2")]),
    ])
    # 第一场 2 小时；第二场中途重开，两段相加 2 小时 30 分。合计 4 小时 30 分，
    # 本周已过 3 天（周一~周三，today=09-23）→ 日均 1 小时 30 分。
    assert built["直播总时长"] == "4小时30分"
    assert built["日均直播时长"] == "1小时30分"
    assert built["迟到次数"] == "0 次" and built["平均迟到"] == "—"


def test_daily_average_spreads_the_total_over_elapsed_days():
    """日均 = 总时长 ÷ 本周已过天数：同一天播两场不会把日均算小。"""
    same_day = stats_of([
        plan(actual_intervals=[interval("2026-09-21T10:00:00+08:00", "2026-09-21T12:00:00+08:00")]),
        plan(id="b", actual_intervals=[
            interval("2026-09-21T20:00:00+08:00", "2026-09-21T22:00:00+08:00", session="b1")]),
    ])
    # today=09-23 时已过 3 天：4 小时 ÷ 3 = 1 小时 20 分（按场次平均会是 2 小时）。
    assert same_day["直播总时长"] == "4小时"
    assert same_day["日均直播时长"] == "1小时20分"
    # 周一只过去 1 天时，同样两场就是 4 小时。
    monday_only = {item["label"]: item["value"] for item in build_schedule_view(
        schedule([plan(actual_intervals=[
            interval("2026-09-21T10:00:00+08:00", "2026-09-21T12:00:00+08:00")])]),
        uid=1, today=date(2026, 9, 21))["stats"]}
    assert monday_only["日均直播时长"] == "2小时"
    # 整周结束后按 7 天摊。
    finished = {item["label"]: item["value"] for item in build_schedule_view(
        schedule([plan(actual_intervals=[
            interval("2026-09-21T10:00:00+08:00", "2026-09-21T17:00:00+08:00")])]),
        uid=1, today=date(2026, 9, 30))["stats"]}
    assert finished["直播总时长"] == "7小时" and finished["日均直播时长"] == "1小时"


def test_stats_require_more_than_five_minutes_to_count_as_late():
    close = stats_of([plan(actual_intervals=[
        interval("2026-09-21T20:05:00+08:00", "2026-09-21T22:00:00+08:00")])])
    assert close["迟到次数"] == "0 次"
    late = stats_of([plan(actual_intervals=[
        interval("2026-09-21T20:05:01+08:00", "2026-09-21T22:00:00+08:00")])])
    assert late["迟到次数"] == "1 次"


def test_stats_average_lateness_uses_only_late_streams():
    built = stats_of([
        plan(actual_intervals=[interval("2026-09-21T20:10:00+08:00", "2026-09-21T22:00:00+08:00")]),
        plan(id="b", date="2026-09-22", start_time="21:00", original_date="2026-09-22",
             original_start_time="21:00", actual_intervals=[
                 interval("2026-09-22T21:50:00+08:00", "2026-09-22T23:00:00+08:00")]),
        plan(id="c", date="2026-09-23", start_time="21:00", original_date="2026-09-23",
             original_start_time="21:00", actual_intervals=[
                 interval("2026-09-23T21:01:00+08:00", "2026-09-23T23:00:00+08:00")]),
    ])
    # 迟到 10 分与 50 分，准点的那场不参与平均。
    assert built["迟到次数"] == "2 次"
    assert built["平均迟到"] == "30分"


def test_stats_compare_against_the_rescheduled_time_and_skip_unpromised_streams():
    built = stats_of([
        # 原定 09-24 21:00 调播到 09-23 22:30，实际 22:32 开播：迟到 2 分，不算迟到。
        plan(date="2026-09-23", start_time="22:30", status="postponed",
             original_date="2026-09-24", original_start_time="21:00",
             rescheduled_date="2026-09-23", rescheduled_start_time="22:30",
             actual_intervals=[interval("2026-09-23T22:32:00+08:00", "2026-09-24T00:30:00+08:00")]),
        # 待定时间的场次没有承诺，不参与迟到统计。
        plan(id="b", date="2026-09-22", start_time=None, status="unknown",
             original_start_time=None, original_end_time=None,
             actual_intervals=[interval("2026-09-22T14:00:00+08:00", "2026-09-22T16:00:00+08:00")]),
        # 突击直播不在周表里，同样不参与迟到统计。
        plan(id="c", date="2026-09-25", start_time="20:00", source="live_observation",
             original_date=None, original_start_time=None,
             actual_intervals=[interval("2026-09-25T20:00:00+08:00", "2026-09-25T21:00:00+08:00")]),
    ])
    assert built["迟到次数"] == "0 次"
    assert built["平均迟到"] == "—"
    # 1 小时 58 分 + 2 小时 + 1 小时 = 4 小时 58 分，已过 3 天 → 日均 1 小时 39 分。
    assert built["直播总时长"] == "4小时58分"
    assert built["日均直播时长"] == "1小时39分"


def test_stats_ignore_ongoing_streams_for_duration_but_count_lateness():
    built = stats_of([
        plan(actual_intervals=[interval("2026-09-21T21:30:00+08:00", None)]),
    ])
    assert built["直播总时长"] == "—" and built["日均直播时长"] == "—"
    assert built["迟到次数"] == "1 次" and built["平均迟到"] == "1小时30分"


def test_stats_show_dashes_when_nothing_is_comparable():
    built = stats_of([plan(start_time=None, status="unknown", original_start_time=None,
                           original_end_time=None)])
    assert set(built.values()) == {"—"}


def test_html_escapes_schedule_text():
    built = view([plan(title='<script>alert("x")</script>&')])
    page = build_schedule_html(built)
    assert "<script>" not in page
    assert "&lt;script&gt;" in page
    assert "&amp;" in page


def test_unfulfilled_card_uses_the_red_style():
    built = view([plan(status="unfulfilled")])
    item = built["days"][0]["streams"][0]
    assert item["status_label"] == "未兑现" and item["status_class"] == "s-unfulfilled"
    page = build_schedule_html(built)
    assert "未兑现" in page and "s-unfulfilled" in page


def test_footer_counts_unfulfilled_from_the_rendered_schedule():
    """新周周表还没出来时的空白容器也要能出图：7 天空板 + 页脚统计。"""
    empty = build_schedule_view({"uid": 1, "week_start": "2026-10-05", "streams": []}, uid=1,
                                display_name="主播", summary={"recorded": 0, "extra": 0,
                                                              "pending": 0, "unknown": 0,
                                                              "unfulfilled": 0})
    assert len(empty["days"]) == 7 and all(day["streams"] == [] for day in empty["days"])
    assert "周表未发布" in empty["summary_text"]      # 空板要有提示，免得以为坏了
    page = build_schedule_html(empty)
    assert isinstance(page, str) and "主播" in page
    summary = {"recorded": 0, "extra": 0, "pending": 0, "unknown": 0}
    built = view([plan(status="unfulfilled"), plan(id="b", status="unfulfilled")], summary=summary)
    assert "未兑现 2 场" in built["summary_text"]
    # 调用方没带未兑现计数时（例如外部拼接的 summary）按图上的数据算，不能显示成 0。
    assert "未兑现 1 场" in view([plan(status="unfulfilled")], summary=dict(summary))["summary_text"]


def test_phone_friendly_layout_is_the_default():
    """手机可读性靠字号占图片宽度的比例；整周并排一行会把这个比例压到 1% 出头。"""
    assert (BOARD_WIDTH, BOARD_COLUMNS, FONT_SCALE) == (840, 4, 1.13)
    assert FONT_SCALE * 15 / BOARD_WIDTH > 0.019
    page = build_schedule_html(view([plan()]))
    assert f"width: {BOARD_WIDTH}px" in page
    assert f"repeat({BOARD_COLUMNS}, 1fr)" in page
    assert "repeat(7, 1fr)" not in page
    # 非默认列数必须真的改变版面。
    assert "repeat(7, 1fr)" in build_schedule_html(view([plan()]), columns=7)


def test_hero_uses_space_banner_and_avatar_with_identity_block():
    built = view([plan()], banner={"header": "data:image/webp;base64,HEADER",
                                   "avatar": "data:image/png;base64,AVATAR"})
    page = build_schedule_html(built)
    assert 'class="hero hero-image"' in page
    assert "background-image:url('data:image/webp;base64,HEADER')" in page
    assert '<img class="avatar" src="data:image/png;base64,AVATAR"' in page
    assert '<div class="hero-name">小路</div>' in page
    assert "UID 1 · 周表 2026-09-21 ~ 09-27 · 共 1 场 · 修订 1" in page
    # 头部信息不再重复周次起始日。
    assert "2026-09-21 ~ 09-21" not in page


def test_hero_falls_back_to_plain_header_without_assets():
    page = build_schedule_html(view([plan()]))
    assert 'class="hero' not in page
    assert '<div class="name">小路</div>' in page
    assert "生成于" in page and "VTuber Monitor" in page


def test_hero_keeps_avatar_when_only_the_avatar_is_available():
    page = build_schedule_html(view([plan()], banner={"avatar": "data:image/png;base64,AVATAR"}))
    assert 'class="hero hero-plain"' in page
    assert '<img class="avatar" src="data:image/png;base64,AVATAR"' in page
    assert "background-image" not in page


@pytest.mark.asyncio
async def test_renderer_caches_by_content(tmp_path, monkeypatch):
    renderer = ScheduleRenderer(tmp_path)
    monkeypatch.setattr(renderer, "_capture", AsyncMock(return_value=b"\x89PNG-test"))
    first = await renderer.render(view([plan()]))
    second = await renderer.render(view([plan()]))
    assert first == second
    assert renderer._capture.await_count == 1
    assert renderer.cache_path(view([plan()])).is_file()
    # 内容变化（场次或修订不同）必须重新渲染。
    await renderer.render(view([plan(title="新标题")]))
    assert renderer._capture.await_count == 2
    assert renderer.directory.parent == tmp_path


@pytest.mark.asyncio
async def test_renderer_reports_safe_error_on_browser_failure(tmp_path, monkeypatch):
    renderer = ScheduleRenderer(tmp_path)
    monkeypatch.setattr(renderer, "_screenshot", AsyncMock(side_effect=RuntimeError("browser log with secret")))
    with pytest.raises(ScheduleRenderError) as error:
        await renderer.render(view([plan()]))
    assert "secret" not in str(error.value)
