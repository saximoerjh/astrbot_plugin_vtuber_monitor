"""所有被接受的周表写入都必须经过这一层服务。"""
import re
import hashlib
import json
import uuid
from dataclasses import replace
from datetime import date, timedelta

from astrbot.api import logger

from ..core.schedule_models import validate_schedule, china_today, WeeklySchedule, StreamPlan
from ..core.models import validate_uid, utc_now
from ..core.schedule_diff import (align_streams, format_adjustment_notice, format_diff,
                                  schedule_diff)


def _info_updates(args):
    """把 update_stream_info 的参数整理成待写入字段：只处理真正给出的部分。"""
    updates = {}
    title = args.get("title")
    if isinstance(title, str) and title.strip():
        updates["title"] = title.strip()
    day = args.get("date")
    if isinstance(day, str) and day.strip():
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", day.strip()):
            raise ValueError("补时间的日期必须是 YYYY-MM-DD。")
        updates["date"] = day.strip()
    started = args.get("start_time")
    if isinstance(started, str) and started.strip():
        if not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", started.strip()):
            raise ValueError("补时间必须是 HH:MM。")
        updates["start_time"] = started.strip()
    return updates


class ScheduleService:
    def __init__(self, data, *, schedule_push=False, adjustment_push=False, reconciler=None,
                 notice_image=None):
        self.data = data
        self.schedule_push = schedule_push
        self.adjustment_push = adjustment_push
        # 可选的直播落位器：刚保存的周表会回填
        # 在该周周表发布之前发生的观测。
        self.reconciler = reconciler
        # 可选：为调播通知提供触发它的动态截图，返回本地路径。
        self.notice_image = notice_image

    async def replace_weekly_schedule(self, schedule, *, import_key=""):
        validate_schedule(schedule)
        if not re.fullmatch(r"[0-9]{1,30}", schedule.source_dynamic_id):
            raise ValueError("周表来源动态 ID 无效。")
        return await self._import(schedule, import_key=import_key)

    async def _import(self, schedule, *, import_key=""):
        old = await self.data.get_historical_schedule(schedule.uid, schedule.week_start)
        if old is None:
            current = await self.data.get_weekly_schedule(schedule.uid)
            old = current if current and current["week_start"] == schedule.week_start else None
        # 空白容器没有来源动态（空字符串），不能用序号比较，也不能被它挡住导入。
        old_source = str(old.get("source_dynamic_id") or "") if old else ""
        if old_source.isdigit() and str(schedule.source_dynamic_id).isdigit() \
                and int(old_source) > int(schedule.source_dynamic_id):
            return False
        stable = {"week_start": schedule.week_start, "source_dynamic_id": schedule.source_dynamic_id,
                  "source_image_url": schedule.source_image_url,
                  "streams": [{k: getattr(s, k) for k in ("date", "start_time", "original_end_time", "title", "status")} for s in schedule.streams]}
        fingerprint = hashlib.sha256(json.dumps(stable, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        if import_key:
            fingerprint = hashlib.sha256(f"{schedule.week_start}:{import_key}".encode()).hexdigest()
        schedule = align_streams(old, schedule)
        validate_schedule(schedule, allow_history=True)
        content = json.loads(json.dumps(schedule.to_dict()))
        changes = schedule_diff(old, content)
        notification = None
        today = china_today()
        if self.schedule_push and changes and date.fromisoformat(schedule.week_start) >= today - timedelta(days=today.weekday()):
            notification = ("schedule_updated", f"UID {schedule.uid} 周表更新（{schedule.week_start}）\n{format_diff(changes)}")
        saved = await self.data.save_weekly_schedule(content, expected=old, source_id=schedule.source_dynamic_id,
                                              reason="weekly_image_replacement", fingerprint=fingerprint,
                                              notification=notification)
        if saved and self.reconciler is not None:
            try:
                await self.reconciler.reconcile_week(schedule.uid, schedule.week_start)
            except Exception:
                # 周表已经存好了，回填失败不能让导入命令失败。
                logger.warning("Live schedule backfill failed uid=%s week=%s",
                               schedule.uid, schedule.week_start)
        return saved

    async def store_parsed_schedule(self, schedule, *, import_key=""):
        validate_schedule(schedule, allow_history=True)
        if not re.fullmatch(r"[0-9]{1,30}", schedule.source_dynamic_id):
            raise ValueError("周表来源动态 ID 无效。")
        today = china_today()
        monday = today - timedelta(days=today.weekday())
        if date.fromisoformat(schedule.week_start) < monday:
            await self._import(schedule, import_key=import_key)
            return "archived"
        return "parsed" if await self.replace_weekly_schedule(schedule, import_key=import_key) else "unchanged"

    async def ensure_placeholder(self, uid, week_start=""):
        """本周还没有周表时，先放一张空白周表，让直播有地方落位。

        这里故意绕过 `validate_schedule` 的"周表不能为空"：那条规则是为了防止模型
        解析失败后把已有周表清空，而这里创建的是一张本来就不存在任何排期的容器。
        真正的周表到了以后会照常导入，并把之前落在这里的直播按新周表重新匹配。
        """
        uid = validate_uid(uid)
        if week_start:
            week = date.fromisoformat(week_start).isoformat()
        else:
            today = china_today()
            week = (today - timedelta(days=today.weekday())).isoformat()
        if await self.data.get_historical_schedule(uid, week) is not None:
            return False
        return await self.data.save_weekly_schedule(
            {"uid": uid, "week_start": week, "streams": [], "source_dynamic_id": "",
             "source_image_url": "", "local_image_path": "", "parsed_at": "",
             "updated_at": utc_now(), "revision": 0},
            expected=None, source_id="", reason="placeholder")

    async def apply_operations(self, uid, operations, *, source_dynamic_id="", operation_id="",
                               expected=None, dry_run=False, source_text=""):
        uid = validate_uid(uid)
        if source_dynamic_id and not re.fullmatch(r"[0-9]{1,30}", source_dynamic_id):
            raise ValueError("调播来源动态 ID 无效。")
        old = await self.get_weekly_schedule(uid)
        if operation_id and await self.data.has_schedule_operation(uid, operation_id):
            return {"success": False, "before": old, "after": old, "changes": [], "reason": "该动态已处理"}
        if not old:
            raise ValueError("尚无本周周表，不能进行调播；可先解析周表。")
        if expected is not None and old != expected:
            raise ValueError("模型分析期间周表发生变化，请重试。")
        if not isinstance(operations, list) or not 1 <= len(operations) <= 10:
            raise ValueError("调播操作数量须为 1–10。")
        plans = [StreamPlan(**p) for p in old["streams"]]
        reasons = []
        source = "dynamic_adjustment" if source_dynamic_id else "manual"
        for operation in operations:
            if not isinstance(operation, dict) or set(operation) != {"name", "arguments"}:
                raise ValueError("调播操作格式无效。")
            name, args = operation["name"], operation["arguments"]
            # update_stream_info 的 title/date/start_time 可选：动态只给了时间时，
            # 就只补时间，不必把没改的字段一起传进来。
            allowed = {"reschedule_stream": {"stream_id", "date", "start_time", "reason"},
                       "cancel_stream": {"stream_id", "reason"},
                       "add_stream": {"date", "start_time", "title", "reason"},
                       "update_stream_info": {"stream_id", "title", "date", "start_time", "reason"}}
            required = {"reschedule_stream": allowed["reschedule_stream"],
                        "cancel_stream": allowed["cancel_stream"],
                        "add_stream": allowed["add_stream"],
                        "update_stream_info": {"stream_id", "reason"}}
            if (not isinstance(name, str) or name not in allowed or not isinstance(args, dict)
                    or not required[name] <= set(args) <= allowed[name]):
                raise ValueError("调播工具或参数不在允许范围内。")
            reason = args["reason"]
            if not isinstance(reason, str) or not reason.strip() or len(reason) > 500:
                raise ValueError("必须提供不超过 500 字的调播原因。")
            reasons.append(reason)
            if name == "add_stream":
                if any((s.date, s.start_time, s.title) == (args["date"], args["start_time"], args["title"]) for s in plans):
                    raise ValueError("同一场直播已存在，不能重复新增。")
                plans.append(StreamPlan(uuid.uuid4().hex, args["date"], args["start_time"], args["title"],
                                        status="unknown" if args["start_time"] is None else "scheduled", source=source, revision=1))
                continue
            matches = [i for i, s in enumerate(plans) if s.id == args["stream_id"]]
            if len(matches) != 1:
                raise ValueError("找不到唯一的目标直播场次。")
            i = matches[0]
            plan = plans[i]
            updates = {"status": "cancelled"}
            if name == "update_stream_info":
                updates = _info_updates(args)
                if not updates:
                    raise ValueError("update_stream_info 需要时间或标题，不能只给 stream_id。")
                if {"date", "start_time"} & set(updates):
                    if plan.status in ("cancelled", "completed"):
                        raise ValueError("已取消或已结束的直播不能补时间。")
                    if plan.start_time and updates.get("start_time", plan.start_time) != plan.start_time:
                        raise ValueError("已有明确时间的场次请改用 reschedule_stream。")
                    if "start_time" in updates and plan.status == "unknown":
                        updates["status"] = "scheduled"
            if name == "reschedule_stream":
                if plan.status in ("cancelled", "completed"):
                    raise ValueError("已取消或完成的直播不能直接改期。")
                if args["start_time"] is None:
                    raise ValueError("改期需要明确时间，不能猜测或清空。")
                updates = {"date": args["date"], "start_time": args["start_time"], "status": "postponed",
                           "rescheduled_date": args["date"], "rescheduled_start_time": args["start_time"]}
            if any(getattr(plan, k) != v for k, v in updates.items()):
                plans[i] = replace(plan, **updates, revision=plan.revision + 1, source=source)
        new = WeeklySchedule(**{**old, "streams": tuple(plans), "updated_at": utc_now()})
        validate_schedule(new)
        content = json.loads(json.dumps(new.to_dict()))
        changes = schedule_diff(old, content)
        reason = "；".join(reasons)
        if not changes:
            return {"success": False, "before": old, "after": old, "changes": [], "reason": "无需修改"}
        content["revision"] = old.get("revision", 0) + 1
        notification = None
        if self.adjustment_push:
            # 附上触发调播的那条动态：优先它的原图（多张就都发），取不到再退回整页截图。
            image_path = ""
            if self.notice_image is not None and source_dynamic_id and not dry_run:
                try:
                    shots = await self.notice_image(uid, source_dynamic_id) or []
                except Exception:
                    logger.warning("Adjustment notice screenshot failed uid=%s", uid)
                    shots = []
                if isinstance(shots, str):
                    shots = [shots] if shots else []
                paths = [str(path) for path in shots if path]
                image_path = (json.dumps(paths, ensure_ascii=False) if len(paths) > 1
                              else (paths[0] if paths else ""))
            notification = ("schedule_adjusted",
                            format_adjustment_notice(uid, changes, reason, source_text=source_text),
                            image_path)
        success = dry_run or await self.data.save_weekly_schedule(
            content, expected=old, source_id=source_dynamic_id, reason=reason,
            operation_id=operation_id, notification=notification)
        return {"success": success, "dry_run": dry_run, "before": old, "after": content if success else old,
                "changes": changes if success else [], "reason": reason if success else "该动态已处理"}

    async def reschedule_stream(self, uid, stream_id, date, start_time, reason, **options):
        return await self.apply_operations(uid, [{"name": "reschedule_stream", "arguments": {
            "stream_id": stream_id, "date": date, "start_time": start_time, "reason": reason}}], **options)

    async def cancel_stream(self, uid, stream_id, reason, **options):
        return await self.apply_operations(uid, [{"name": "cancel_stream", "arguments": {
            "stream_id": stream_id, "reason": reason}}], **options)

    async def add_stream(self, uid, date, start_time, title, reason, **options):
        return await self.apply_operations(uid, [{"name": "add_stream", "arguments": {
            "date": date, "start_time": start_time, "title": title, "reason": reason}}], **options)

    async def update_stream_info(self, uid, stream_id, title="", reason="", *,
                                 date=None, start_time=None, **options):
        """改标题，或给"时间待定"的场次补上日期/开始时间（动态只给时间时用）。"""
        arguments = {"stream_id": stream_id, "reason": reason}
        if isinstance(title, str) and title.strip():
            arguments["title"] = title
        if isinstance(date, str) and date.strip():
            arguments["date"] = date
        if isinstance(start_time, str) and start_time.strip():
            arguments["start_time"] = start_time
        return await self.apply_operations(
            uid, [{"name": "update_stream_info", "arguments": arguments}], **options)

    async def get_weekly_schedule(self, uid, week_start=""):
        if week_start:
            try:
                day = date.fromisoformat(week_start)
            except ValueError:
                raise ValueError("周起始日期请使用 YYYY-MM-DD。") from None
            if day.isoformat() != week_start or day.weekday() != 0:
                raise ValueError("请指定该周周一的日期（YYYY-MM-DD）。")
            return await self.data.get_historical_schedule(uid, week_start)
        today = china_today()
        monday = (today - timedelta(days=today.weekday())).isoformat()
        archived = await self.data.get_historical_schedule(uid, monday)
        if archived:
            return archived
        current = await self.data.get_weekly_schedule(uid)
        return current if current and current["week_start"] == monday else None

    async def get_schedule_history(self, uid):
        return await self.data.get_schedule_history(uid)

    async def retry_adjustment(self, uid, dynamic_id):
        if not isinstance(dynamic_id, str) or not re.fullmatch(r"[0-9]{1,30}", dynamic_id):
            raise ValueError("请提供失败动态的数字 ID。")
        return await self.data.retry_adjustment(uid, dynamic_id)
