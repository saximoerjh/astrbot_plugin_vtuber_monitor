from datetime import date, datetime, timedelta, timezone

from ..core.schedule_models import UNFULFILLED_STATUS

CHINA = timezone(timedelta(hours=8))


def format_stream(plan):
    day = plan.get("original_date") or plan["date"]
    start = plan.get("original_start_time", plan.get("start_time"))
    end = plan.get("original_end_time")
    original = start or "unknown（时间待定）"
    if start and end:
        original += f"–{'次日 ' if end < start else ''}{end}"
    weekday = "一二三四五六日"[date.fromisoformat(day).weekday()]
    status = {"scheduled": "已排期", "postponed": "已调播", "completed": "已结束", "cancelled": "已取消",
              "unknown": "待定", UNFULFILLED_STATUS: "未兑现"}.get(plan["status"], plan["status"])
    if plan.get("source") == "live_observation":
        # 周表里没有对应场次，这条只承载实际观测。
        lines = [f"{plan['title']} [突击直播]"]
    else:
        lines = [f"{plan['title']} [{status}]", f"原定：{day}（周{weekday}）{original}"]
    if plan.get("rescheduled_start_time"):
        lines.append(f"调播：{plan['rescheduled_date']} {plan['rescheduled_start_time']}")
    intervals = plan.get("actual_intervals", [])
    for item in intervals:
        def stamp(value):
            return datetime.fromisoformat(value).astimezone(CHINA).strftime("%Y-%m-%d %H:%M:%S")
        start_label = "接口" if item["start_basis"] == "api" else "观测"
        end_label = f"{stamp(item['end'])}（观测）" if item["end"] else "直播中"
        lines.append(f"实际：{stamp(item['start'])}（{start_label}）–{end_label}")
    if not intervals:
        lines.append("实际：未记录")
    return "\n".join(lines)


def format_live_summary(summary):
    """图片与文字共用同一份统计口径；统计失败时明确说明而不是显示为 0。"""
    if summary is None:
        return "实际直播：统计失败。"
    return (f"实际直播：已记录 {summary['recorded']} 场（突击 {summary['extra']} 场）"
            f" · 待落位 {summary['pending']} 场"
            f" · 未记录 {summary['unknown']} 场（起点未知）"
            f" · 未兑现 {summary.get('unfulfilled', 0)} 场")
