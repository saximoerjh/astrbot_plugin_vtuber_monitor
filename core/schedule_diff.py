"""保守匹配：存在歧义的场次一律保留为新增或删除。"""
from dataclasses import replace
from .schedule_models import UNFULFILLED_STATUS, StreamPlan, is_pending_title


def align_streams(old, incoming):
    if not old or old["week_start"] != incoming.week_start:
        return incoming
    remaining = {s["id"]: s for s in old["streams"]}
    reserved = {s.id for s in incoming.streams} & remaining.keys()
    plans = []
    for stream in incoming.streams:
        match = remaining.get(stream.id)
        if match is None:
            candidates = [s for s in remaining.values() if s["id"] not in reserved and s["title"] == stream.title]
            incoming_matches = [s for s in incoming.streams if s.title == stream.title]
            if len(candidates) == len(incoming_matches) == 1:
                match = candidates[0]
        if match is None:
            candidates = [s for s in remaining.values() if s["id"] not in reserved
                          and (s["date"], s["start_time"]) == (stream.date, stream.start_time)]
            incoming_matches = [s for s in incoming.streams
                                if (s.date, s.start_time) == (stream.date, stream.start_time)]
            if len(candidates) == len(incoming_matches) == 1:
                match = candidates[0]
        if match:
            previous = StreamPlan(**match)
            if previous.rescheduled_date or previous.actual_intervals:
                stream = replace(stream, original_date=previous.original_date,
                                 original_start_time=previous.original_start_time,
                                 original_end_time=previous.original_end_time,
                                 actual_intervals=previous.actual_intervals)
            if previous.rescheduled_date:
                stream = replace(stream, date=previous.date, start_time=previous.start_time,
                                 rescheduled_date=previous.rescheduled_date,
                                 rescheduled_start_time=previous.rescheduled_start_time,
                                 status=previous.status)
            if previous.actual_intervals and previous.actual_intervals[-1].get("end"):
                stream = replace(stream, status="completed")
            if previous.status == UNFULFILLED_STATUS and stream.status == "scheduled":
                # 未兑现是判定出来的状态，重新解析仍是“已排期”时不能被抹掉，
                # 否则每次重新解析都会重标一次并产生假的状态变更。
                stream = replace(stream, status=UNFULFILLED_STATUS)
            if is_pending_title(stream.title) and not is_pending_title(previous.title):
                # 上一次已用实测直播标题补全过，重新解析仍是“内容待定”时保留补全结果。
                stream = replace(stream, title=previous.title)
            changed = any(getattr(stream, key) != match[key]
                          for key in ("date", "start_time", "title", "status")) or stream.original_end_time != match.get("original_end_time")
            stream = replace(stream, id=match["id"], revision=match.get("revision", 0) + int(changed))
            remaining.pop(match["id"])
        plans.append(stream)
    plans.extend(StreamPlan(**s) for s in remaining.values() if s.get("actual_intervals"))
    return replace(incoming, streams=tuple(plans))


def schedule_diff(before, after):
    old = {s["id"]: s for s in before["streams"]} if before and before["week_start"] == after["week_start"] else {}
    new = {s["id"]: s for s in after["streams"]}
    changes = []
    def value(item, field):
        return list(item.get(field) or ()) if field == "actual_intervals" else item.get(field)
    for key in old.keys() | new.keys():
        a, b = old.get(key), new.get(key)
        kinds = (["added"] if a is None else ["removed"] if b is None else
                 [name for name, fields in (("time", ("date", "start_time", "original_start_time", "original_end_time", "rescheduled_date", "rescheduled_start_time")),
                                             ("actual", ("actual_intervals",)),
                                             ("title", ("title",)), ("status", ("status",)))
                  if any(value(a, field) != value(b, field) for field in fields)])
        if kinds:
            changes.append({"stream_id": key, "kinds": kinds, "before": a, "after": b})
    return sorted(changes, key=lambda c: ((c["after"] or c["before"])["date"], c["stream_id"]))


def format_diff(changes):
    labels = {"added": "新增", "removed": "删除", "time": "时间", "actual": "实际直播", "title": "标题", "status": "状态"}
    statuses = {"scheduled": "已排期", "postponed": "改期", "cancelled": "取消", "unknown": "待定",
                "completed": "完成", UNFULFILLED_STATUS: "未兑现"}
    def describe(s):
        if s is None:
            return "无"
        original = s.get('original_start_time', s['start_time']) or 'unknown'
        if s.get('original_end_time'):
            original += '–' + s['original_end_time']
        return f"{s['date']} {s['start_time'] or '时间待定'} {s['title']}（原定 {original}） [{statuses.get(s['status'], s['status'])}]"
    return "\n".join(f"{'/'.join(labels[k] for k in c['kinds'])}：{describe(c['before'])} → {describe(c['after'])}"
                     for c in changes[:20]) + ("\n（仅展示前 20 项）" if len(changes) > 20 else "")
