"""把观测到的直播落位到周表场次上。

场次按“同一天、开播时间落在 ``tolerance`` 容差内”就近匹配；匹配不上的
观测会新增一条“突击直播”条目，不会被丢弃。真实场次的落位是粘滞的，
只有突击条目会重新评估，因此直播之后才保存的周表也能通过
``reconcile_week`` 把这场直播补记进去。
"""
import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone

from ..core.data_manager import (LIVE_SESSION_PENDING, LIVE_SESSION_RECORDED,
                                 LIVE_SESSION_SKIPPED)
from ..core.schedule_models import StreamPlan, WeeklySchedule, validate_schedule

logger = logging.getLogger(__name__)

CHINA = timezone(timedelta(hours=8))
MATCH_TOLERANCE = timedelta(hours=1)
EXTRA_SOURCE = "live_observation"
EXTRA_TITLE = "突击直播"
NO_DELTA = timedelta(0)


def local(moment):
    return datetime.fromisoformat(moment).astimezone(CHINA)


def week_start_of(day):
    return (day - timedelta(days=day.weekday())).isoformat()


def week_of_session(session):
    """一场直播属于包含它本地开播日期的那一周周表。"""
    return week_start_of(local(session["start_time"]).date())


def extra_plan_id(uid, session_id):
    return uuid.uuid5(uuid.NAMESPACE_URL, f"live-observation:{uid}:{session_id}").hex


def interval_of(session):
    return {"session_id": session["id"], "start": session["start_time"], "end": session["end_time"],
            "start_basis": session["start_basis"],
            "end_basis": "observed" if session["end_time"] else None}


class LiveScheduleRecorder:
    def __init__(self, data, tolerance=MATCH_TOLERANCE):
        self.data = data
        self.tolerance = tolerance
        self._lock = asyncio.Lock()

    async def sync(self, uid):
        """轮询入口：先退役无法落位的记录，再处理待落位的场次。"""
        async with self._lock:
            await self._retire_unknown_starts(uid)
            for week in await self._pending_weeks(uid):
                await self._place_week(uid, week)

    async def reconcile_week(self, uid, week_start):
        """导入入口：为刚刚保存的周表补记此前观测到的场次。"""
        async with self._lock:
            await self._retire_unknown_starts(uid)
            await self._place_week(uid, week_start)

    async def week_summary(self, uid, week_start):
        """供 /vt_schedule 使用的计数，让未落位的观测保持可见。"""
        rows = await self.data.live_sessions_for_uid(uid)
        known, unknown = [], []
        for row in rows:
            if row["start_time"]:
                if week_start_of(local(row["start_time"]).date()) == week_start:
                    known.append(row)
            elif row["synced"] == LIVE_SESSION_SKIPPED and row["observed_at"] \
                    and week_start_of(local(row["observed_at"]).date()) == week_start:
                unknown.append(row)
        schedule = await self.data.get_historical_schedule(uid, week_start)
        extras = {item["session_id"] for plan in (schedule or {"streams": ()})["streams"]
                  if plan.get("source") == EXTRA_SOURCE
                  for item in plan.get("actual_intervals") or ()}
        return {"recorded": sum(1 for row in known if row["synced"] == LIVE_SESSION_RECORDED),
                "pending": sum(1 for row in known if row["synced"] == LIVE_SESSION_PENDING),
                "extra": sum(1 for row in known if row["id"] in extras),
                "unknown": len(unknown)}

    async def _retire_unknown_starts(self, uid):
        for session in await self.data.live_sessions_for_uid(uid):
            if not session["start_time"] and session["synced"] == LIVE_SESSION_PENDING:
                # 接口没有返回开播时间，也没有观测到状态跳变。
                await self.data.retire_live_session(session["id"])

    async def _pending_weeks(self, uid):
        weeks = set()
        for session in await self.data.live_sessions_for_uid(uid):
            if session["synced"] == LIVE_SESSION_PENDING and session["start_time"]:
                weeks.add(week_of_session(session))
        return sorted(weeks)

    async def _week_sessions(self, uid, week_start):
        sessions = []
        for session in await self.data.live_sessions_for_uid(uid):
            if session["start_time"] and week_of_session(session) == week_start:
                sessions.append(session)
        return sorted(sessions, key=lambda row: (row["start_time"], row["id"]))

    @staticmethod
    def _planned_start(plan):
        if not plan.get("start_time"):
            return None
        return datetime.fromisoformat(f"{plan['date']}T{plan['start_time']}:00").replace(tzinfo=CHINA)

    def _nearest(self, streams, session):
        """容差内同一天最近的场次；有具体时间的场次优先于待定场次。"""
        started = local(session["start_time"])
        best = None
        for plan in streams:
            if plan.get("source") == EXTRA_SOURCE or plan["status"] == "cancelled":
                continue
            if plan["date"] != started.date().isoformat():
                continue
            planned = self._planned_start(plan)
            delta = NO_DELTA if planned is None else abs(started - planned)
            if planned is not None and delta > self.tolerance:
                continue
            key = (planned is None, delta, plan.get("start_time") or "", plan["id"])
            if best is None or key < best[0]:
                best = (key, plan["id"])
        return best[1] if best else None

    def _extra_plan(self, uid, streams, session):
        plan_id = extra_plan_id(uid, session["id"])
        if any(plan["id"] == plan_id for plan in streams):
            return plan_id
        started = local(session["start_time"])
        title = (session.get("title") or "").strip()[:300] or EXTRA_TITLE
        streams.append({"id": plan_id, "date": started.date().isoformat(),
                        "start_time": started.strftime("%H:%M"), "title": title,
                        "status": "completed" if session["end_time"] else "scheduled",
                        "source": EXTRA_SOURCE, "revision": 0, "original_date": None,
                        "original_start_time": None, "original_end_time": None,
                        "rescheduled_date": None, "rescheduled_start_time": None,
                        "actual_intervals": []})
        return plan_id

    @staticmethod
    def _apply(plan, session):
        intervals = [item for item in plan.get("actual_intervals") or ()
                     if item["session_id"] != session["id"]]
        intervals.append(interval_of(session))
        intervals.sort(key=lambda item: item["start"])
        changed = intervals != list(plan.get("actual_intervals") or ())
        if changed:
            plan["actual_intervals"] = intervals
            plan["revision"] = plan.get("revision", 0) + 1
        if session["end_time"] and plan["status"] != "completed":
            # 已记录的下播时间覆盖周表里的待定状态。
            plan["status"] = "completed"
            changed = True
        return changed

    async def _place_week(self, uid, week_start):
        sessions = await self._week_sessions(uid, week_start)
        if not sessions:
            return
        schedule = await self.data.get_historical_schedule(uid, week_start)
        if schedule is None:
            # 没有本周周表就没有可挂靠的位置，记录保持待落位，
            # 并在 /vt_status 与 /vt_schedule 中保持可见。
            return
        streams = [dict(plan) for plan in schedule["streams"]]
        by_id = {plan["id"]: plan for plan in streams}
        placed = {}
        for plan in streams:
            if plan.get("source") == EXTRA_SOURCE:
                continue
            for item in plan.get("actual_intervals") or ():
                # 粘滞：真实场次保留它已经记下的落位。
                placed.setdefault(item["session_id"], plan["id"])
        changed = False
        placement = {}
        for session in sessions:
            target = placed.get(session["id"])
            if target is None:
                target = self._nearest(streams, session)
                if target is None:
                    target = self._extra_plan(uid, streams, session)
                    by_id[target] = streams[-1]
                placed[session["id"]] = target
            placement[session["id"]] = target
            changed |= self._apply(by_id[target], session)
        streams = [plan for plan in streams
                   if plan.get("source") != EXTRA_SOURCE or plan.get("actual_intervals")]
        if changed or len(streams) != len(schedule["streams"]):
            if not await self._save(uid, week_start, schedule, streams):
                return
        await self._finish(sessions, placement, week_start)

    async def _save(self, uid, week_start, schedule, streams):
        current = await self.data.get_historical_schedule(uid, week_start)
        if current != schedule:
            # 周表已被其他写入方改动，下一轮重新读取后再落位。
            logger.warning("Live schedule placement deferred uid=%s week=%s", uid, week_start)
            return False
        candidate = WeeklySchedule(**{**schedule, "streams": tuple(StreamPlan(**plan) for plan in streams)})
        validate_schedule(candidate, allow_history=True)
        # 内容没有变化说明区间已经写入，同样算成功。
        await self.data.save_weekly_schedule(
            json.loads(json.dumps(candidate.to_dict())), expected=schedule,
            source_id="", reason="live_observation")
        return True

    async def _finish(self, sessions, placement, week_start):
        for session in sessions:
            plan_id = placement.get(session["id"])
            if plan_id is None:
                continue
            await self.data.bind_live_session(session["id"], week_start, plan_id)
            if session["synced"] == LIVE_SESSION_PENDING:
                await self.data.finish_live_session(session)
