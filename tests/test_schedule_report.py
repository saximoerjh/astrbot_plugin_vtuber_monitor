from astrbot_plugin_vtuber_monitor.services.schedule_report import format_schedule_report


def test_many_non_schedules_and_unknown_date_are_one_summary():
    records = [{"dynamic_id": "123", "status": "skipped"} for _ in range(11)]
    records.append({"dynamic_id": "123", "status": "needs_date", "error_message": "verbose details"})
    text = format_schedule_report(records, 1, target="满区")
    assert text.count("https://t.bilibili.com/123") == 1
    assert "周次不明确" in text and "/vt_parse_schedule 满区 本周" in text
    assert "verbose" not in text and "跳过" not in text and len(text) < 160


def test_failure_reasons_deduplicated_and_success_not_obscured():
    records = [{"dynamic_id": "123", "status": "failed", "error_code": "model_error"} for _ in range(8)]
    text = format_schedule_report(records, 1)
    assert text.count("模型调用失败") == 1 and text.count("https://") == 1
    records.append({"dynamic_id": "123", "status": "archived", "week_start": "2026-09-21"})
    text = format_schedule_report(records, 1)
    assert "历史周表已缓存" in text and "模型调用失败" not in text


def test_each_dynamic_has_own_link_and_empty_scan_links_to_profile():
    text = format_schedule_report([{"dynamic_id": "123", "status": "skipped"},
                                   {"dynamic_id": "456", "status": "failed", "error_stage": "download"}], 1)
    assert "https://t.bilibili.com/123" in text and "https://t.bilibili.com/456" in text
    assert "图片下载失败" in text
    text = format_schedule_report([], 1, week_start="2026-09-28")
    assert "指定周次：2026-09-28" in text and "https://space.bilibili.com/1/dynamic" in text
