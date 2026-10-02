"""把观测到的直播落位到周表场次上。

场次按“同一天、开播时间落在 ``tolerance`` 容差内”就近匹配；匹配不上的
观测会新增一条“突击直播”条目，不会被丢弃。真实场次的落位是粘滞的，
只有突击条目会重新评估，因此直播之后才保存的周表也能通过
``reconcile_week`` 把这场直播补记进去。
"""
import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

from astrbot.api import logger

from ..core.data_manager import (LIVE_SESSION_PENDING, LIVE_SESSION_RECORDED,
                                 LIVE_SESSION_SKIPPED)
from ..core.schedule_models import (UNFULFILLED_STATUS, StreamPlan, WeeklySchedule,
                                    is_pending_title, validate_schedule)

CHINA = timezone(timedelta(hours=8))
MATCH_TOLERANCE = timedelta(hours=1)
UNFULFILLED_AFTER = timedelta(hours=2)
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
    def __init__(self, data, tolerance=MATCH_TOLERANCE, *, unfulfilled_after=UNFULFILLED_AFTER):
        self.data = data
        self.tolerance = tolerance
        self.unfulfilled_after = unfulfilled_after
        self._lock = asyncio.Lock()

    async def sync(self, uid, *, now=None):
        """轮询入口：先退役无法落位的记录，再处理待落位的场次，最后判未兑现。"""
        async with self._lock:
            await self._retire_unknown_starts(uid)
            for week in await self._pending_weeks(uid):
                await self._place_week(uid, week)
            await self.mark_unfulfilled(uid, now)

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
                "unknown": len(unknown),
                "unfulfilled": sum(1 for plan in (schedule or {"streams": ()})["streams"]
                                   if plan.get("status") == UNFULFILLED_STATUS)}

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

    @staticmethod
    def awaiting_start(plan):
        """还没开播、也没被调播或取消的排期场次：未兑现的候选。"""
        return (plan.get("status") == "scheduled"
                and plan.get("source") != EXTRA_SOURCE
                and not plan.get("rescheduled_start_time")
                and bool(plan.get("start_time"))
                and not plan.get("actual_intervals"))

    async def repair_placements(self, uid, weeks):
        """兜底修复：同一场观测只保留一个持有者。

        正常写入路径已经保证这一点；这里只为旧版本写坏的数据收尾——
        否则同一条直播会一边显示“已结束”、一边永远显示“直播中”。
        只删重复，不重新匹配、不改动其他字段。
        """
        repaired = 0
        for week in weeks:
            schedule = await self.data.get_historical_schedule(uid, week)
            if schedule is None:
                continue
            streams = [dict(plan) for plan in schedule["streams"]]
            owners = {}
            for plan in streams:
                if plan.get("source") == EXTRA_SOURCE:
                    continue
                for item in plan.get("actual_intervals") or ():
                    owners.setdefault(item["session_id"], plan["id"])
            changed = False
            for plan in streams:
                intervals = plan.get("actual_intervals") or ()
                kept = [item for item in intervals
                        if owners.get(item["session_id"], plan["id"]) == plan["id"]]
                if len(kept) != len(intervals):
                    plan["actual_intervals"] = kept
                    plan["revision"] = plan.get("revision", 0) + 1
                    changed = True
            trimmed = [plan for plan in streams
                       if plan.get("source") != EXTRA_SOURCE or plan.get("actual_intervals")]
            if not changed and len(trimmed) == len(streams):
                continue
            if await self._save(uid, week, schedule, trimmed):
                repaired += 1
        return repaired

    async def mark_unfulfilled(self, uid, now=None):
        """排期到点后仍没开播的场次记为未兑现。

        只处理「场次自己所在的周」，因为周日 23:00 的场次到点两小时已经跨周，
        那时它属于上一周的周表。已经在直播轮询里，所以不额外起任务。
        另外只在周表**早于该场次开播**就解析到的情况下才判：解析之前的日子插件
        根本不知道有这个排期，也没有观测可以回补，判成未兑现就是误伤。
        """
        now = now or datetime.now(CHINA)
        today = now.date()
        marked = 0
        for week in sorted({week_start_of(today), week_start_of(today - timedelta(days=1))}):
            schedule = await self.data.get_historical_schedule(uid, week)
            if schedule is None:
                continue
            imported = self._imported_at(await self.data.schedule_imported_at(uid, week))
            streams = [dict(plan) for plan in schedule["streams"]]
            changed = False
            count = 0
            for plan in streams:
                planned = self._planned_start(plan)
                if plan.get("status") == UNFULFILLED_STATUS:
                    # 旧版本按“没观测到就判”写下的错标记：周表其实是这场之后才
                    # 解析到的，同一轮里撤销，免得永久留红。
                    if imported is not None and planned is not None and planned < imported:
                        plan["status"] = "scheduled"
                        plan["revision"] = plan.get("revision", 0) + 1
                        changed = True
                    continue
                if not self.awaiting_start(plan):
                    continue
                if planned is None or now < planned + self.unfulfilled_after:
                    continue
                if imported is None or planned < imported:
                    # 周表是这场排期结束之后才解析到的：插件当时没在看着这一场，
                    # 事后也无法从接口回补，保持原状不判。
                    continue
                plan["status"] = UNFULFILLED_STATUS
                plan["revision"] = plan.get("revision", 0) + 1
                changed = True
                count += 1
            if not changed:
                continue
            if await self._save(uid, week, schedule, streams):
                marked += count
            # 保存失败说明周表被其他写入方改动，下一轮重新读取后再判。
        return marked

    def _unfulfilled_candidate(self, streams, session):
        """当天已被判未兑现、又被迟到的直播回填的场次；取原定时间最近的。"""
        day = local(session["start_time"]).date().isoformat()
        started = local(session["start_time"])
        best = None
        for plan in streams:
            if plan.get("status") != UNFULFILLED_STATUS or plan.get("source") == EXTRA_SOURCE:
                continue
            if plan["date"] != day:
                continue
            planned = self._planned_start(plan)
            key = (abs(started - planned) if planned else NO_DELTA, plan["id"])
            if best is None or key < best[0]:
                best = (key, plan["id"])
        return best[1] if best else None

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

    @staticmethod
    def _imported_at(value):
        """把周表首次导入时间解析成带时区的 datetime；取不到或格式不对按未知处理。"""
        try:
            moment = datetime.fromisoformat(value)
        except (TypeError, ValueError):
            return None
        return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)

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
        if plan.get("status") == UNFULFILLED_STATUS:
            # 当天迟到的直播回填了这条：撤销未兑现标记，按正常结果记录。
            plan["status"] = "completed" if session["end_time"] else "scheduled"
            changed = True
        observed = (session.get("title") or "").strip()[:300]
        if observed and is_pending_title(plan.get("title")) and plan.get("title") != observed:
            # 周表只写了“内容待定”时，用实测到的直播间标题补全；
            # 周表原本写了真实节目名时不覆盖。
            plan["title"] = observed
            if not changed:
                plan["revision"] = plan.get("revision", 0) + 1
            changed = True
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
                    # 当天已判未兑现的场次优先被迟到的直播回填，避免同时出现
                    # 一条“未兑现”和一条“突击直播”。
                    target = self._unfulfilled_candidate(streams, session)
                if target is None:
                    target = self._extra_plan(uid, streams, session)
                    by_id[target] = streams[-1]
                placed[session["id"]] = target
            placement[session["id"]] = target
            for plan in streams:
                # 一次观测只能挂在一个场次上：回填周表条目后，先前为同一次
                # 直播建的突击条目必须交出这条区间，否则同一场直播会同时
                # 出现「已结束」和「直播中」两条。
                if plan["id"] == target:
                    continue
                intervals = plan.get("actual_intervals") or ()
                if not any(item["session_id"] == session["id"] for item in intervals):
                    continue
                plan["actual_intervals"] = [item for item in intervals
                                            if item["session_id"] != session["id"]]
                plan["revision"] = plan.get("revision", 0) + 1
                changed = True
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
