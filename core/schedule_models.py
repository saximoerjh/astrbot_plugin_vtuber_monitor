"""解析器与持久化服务共用的严格周表校验。"""
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import re
import uuid

from .models import utc_now, validate_uid


def china_today():
    return datetime.now(timezone(timedelta(hours=8))).date()


def parse_week_override(value, today=None, *, limit_range=True):
    if not value:
        return None
    today = today or china_today()
    monday = today - timedelta(days=today.weekday())
    if not isinstance(value, str):
        raise ValueError("周次请填写本周、上周、下周或周一日期 YYYY-MM-DD。")
    value = value.strip()
    offsets = {"本周": 0, "这周": 0, "上周": -1, "下周": 1,
               "本星期": 0, "上星期": -1, "下星期": 1}
    if value in offsets:
        return monday + timedelta(weeks=offsets[value])
    try:
        day = date.fromisoformat(value)
    except (TypeError, ValueError):
        raise ValueError("周次请填写本周、上周、下周或周一日期 YYYY-MM-DD。") from None
    if day.isoformat() != value or day.weekday() != 0:
        raise ValueError("请指定该周周一的日期（YYYY-MM-DD）。")
    if limit_range and not monday - timedelta(days=371) <= day <= monday + timedelta(days=7):
        raise ValueError("请指定过去一年至下周范围内的周一日期。")
    return day


@dataclass(frozen=True)
class StreamPlan:
    id: str
    date: str
    start_time: str | None
    title: str
    status: str = "scheduled"
    source: str = "weekly_image"
    revision: int = 0
    original_date: str | None = None
    original_start_time: str | None = None
    original_end_time: str | None = None
    rescheduled_date: str | None = None
    rescheduled_start_time: str | None = None
    actual_intervals: tuple = ()

    def __post_init__(self):
        # date/start_time 仍是现有工具使用的有效排期。
        if self.original_date is None:
            object.__setattr__(self, "original_date", self.date)
            object.__setattr__(self, "original_start_time", self.start_time)


@dataclass(frozen=True)
class WeeklySchedule:
    uid: int
    week_start: str
    streams: tuple[StreamPlan, ...]
    source_dynamic_id: str = ""
    source_image_url: str = ""
    local_image_path: str = ""
    parsed_at: str = ""
    updated_at: str = ""
    revision: int = 0

    def to_dict(self):
        return asdict(self)


def validate_schedule(schedule, today=None, allow_history=False):
    if not isinstance(schedule, WeeklySchedule):
        raise ValueError("周表模型无效。")
    validate_uid(schedule.uid)
    if not isinstance(schedule.week_start, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", schedule.week_start):
        raise ValueError("周表起始日必须是 YYYY-MM-DD。")
    start = date.fromisoformat(schedule.week_start)
    if type(schedule.revision) is not int or schedule.revision < 0:
        raise ValueError("周表修订号无效。")
    today = today or china_today()
    monday = today - timedelta(days=today.weekday())
    valid_week = (monday - timedelta(days=371) <= start <= monday + timedelta(days=7)
                  if allow_history else start in (monday, monday + timedelta(days=7)))
    if start.weekday() != 0 or not valid_week:
        raise ValueError("只接受本周或下周且以周一开始的周表，旧周表不会覆盖现有日程。")
    if not schedule.streams or len(schedule.streams) > 100:
        raise ValueError("周表为空或场次数量异常，不覆盖已有周表。")
    ids = set()
    for stream in schedule.streams:
        if not isinstance(stream, StreamPlan):
            raise ValueError("直播场次模型无效。")
        if not isinstance(stream.id, str) or not stream.id or stream.id in ids:
            raise ValueError("直播 ID 为空或重复。")
        ids.add(stream.id)
        if not isinstance(stream.date, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", stream.date):
            raise ValueError("直播日期必须是 YYYY-MM-DD。")
        day = date.fromisoformat(stream.date)
        if not start <= day < start + timedelta(days=7):
            raise ValueError("直播日期不在周表范围内。")
        if stream.start_time is not None and (not isinstance(stream.start_time, str) or not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", stream.start_time)):
            raise ValueError("直播时间必须是 HH:MM，无法确定时应为 null。")
        for value in (stream.original_start_time, stream.original_end_time, stream.rescheduled_start_time):
            if value is not None and (not isinstance(value, str) or not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", value)):
                raise ValueError("活动时间必须为 HH:MM 或 null。")
        if stream.original_end_time and (not stream.original_start_time or stream.original_end_time == stream.original_start_time):
            raise ValueError("原定时间区间需要明确且不同的起止时间。")
        if not start <= date.fromisoformat(stream.original_date) < start + timedelta(days=7):
            raise ValueError("原定日期不在所属周内。")
        if bool(stream.rescheduled_date) != bool(stream.rescheduled_start_time):
            raise ValueError("调播时间必须同时包含日期和开始时间。")
        if stream.rescheduled_date and (stream.date, stream.start_time) != (stream.rescheduled_date, stream.rescheduled_start_time):
            raise ValueError("有效排期与调播时间不一致。")
        if not isinstance(stream.actual_intervals, (list, tuple)):
            raise ValueError("实际直播时间区间无效。")
        for interval in stream.actual_intervals:
            if not isinstance(interval, dict) or set(interval) != {"session_id", "start", "end", "start_basis", "end_basis"}:
                raise ValueError("实际直播记录无效。")
            timestamps = [datetime.fromisoformat(interval[k]) if interval[k] else None for k in ("start", "end")]
            if not timestamps[0] or any(t and t.tzinfo is None for t in timestamps):
                raise ValueError("实际直播记录必须含带时区的开播时间。")
            if timestamps[1] and timestamps[1] < timestamps[0]:
                raise ValueError("下播时间不能早于开播时间。")
            if interval["start_basis"] not in ("api", "observed") or interval["end_basis"] not in (None, "observed"):
                raise ValueError("实际直播时间来源无效。")
        if not isinstance(stream.title, str) or not stream.title.strip() or len(stream.title) > 300:
            raise ValueError("直播标题无效。")
        if stream.status not in ("scheduled", "postponed", "cancelled", "completed", "unknown"):
            raise ValueError("直播状态无效。")
        if stream.source not in ("weekly_image", "dynamic_adjustment", "manual", "live_observation"):
            raise ValueError("直播来源无效。")
        if type(stream.revision) is not int or stream.revision < 0:
            raise ValueError("修订号无效。")


def schedule_from_parser(uid, payload, today=None, allow_history=False):
    if not isinstance(payload, dict) or set(payload) != {"week_start", "streams"}:
        raise ValueError("解析结果必须仅包含 week_start 和 streams。")
    if not isinstance(payload["streams"], list):
        raise ValueError("streams 必须是列表。")
    plans = []
    for item in payload["streams"]:
        if not isinstance(item, dict) or set(item) not in ({"date", "start_time", "title"}, {"date", "start_time", "end_time", "title"}):
            raise ValueError("场次必须包含 date、start_time、title，可额外包含 end_time。")
        item = dict(item)
        end_time = item.pop("end_time", None)
        key = f"{uid}:{item['date']}:{item['start_time']}:{item['title']}"
        plans.append(StreamPlan(str(uuid.uuid5(uuid.NAMESPACE_URL, key)), **item,
                                original_end_time=end_time,
                                status="unknown" if item["start_time"] is None else "scheduled"))
    result = WeeklySchedule(validate_uid(uid), payload["week_start"], tuple(plans),
                            parsed_at=utc_now(), updated_at=utc_now())
    validate_schedule(result, today, allow_history=allow_history)
    return result


def explicit_week_hint(text, today=None):
    """解析正文里明确写出的七天月日范围；绝不依据动态发布时间推算。

    置顶动态可能在发布数月后才被编辑，因此只在距今天 45 天内、
    且完整覆盖周一到周日时，才采用最接近的那个年份。
    """
    today = today or china_today()
    matches = re.findall(r"(?<![0-9])(\d{1,2})[./月](\d{1,2})日?\s*[-—~～至]\s*(\d{1,2})[./月](\d{1,2})日?", text)
    candidates = set()
    for m1, d1, m2, d2 in matches:
        for year in (today.year - 1, today.year, today.year + 1):
            try:
                start = date(year, int(m1), int(d1))
                end = date(year + (int(m2) < int(m1)), int(m2), int(d2))
            except ValueError:
                continue
            if start.weekday() == 0 and (end - start).days == 6 and abs((start - today).days) <= 45:
                candidates.add(start)
    return next(iter(candidates)) if len(candidates) == 1 else None
