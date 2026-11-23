"""按配置的时间点扫描周表：看当前置顶与最近几条动态。

扫描不再锚定"当初导入的那条动态"——主播换一条新置顶、把周表发成新动态都能
被发现；同一张图靠内容指纹去重，稳态下不会重复调用模型。
"""

import asyncio
import hashlib
import re
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone

from astrbot.api import logger

from ..bili_client import BiliError, BiliRateLimited
from ..core.schedule_models import (
    StreamPlan,
    WeeklySchedule,
    china_today,
    explicit_week_hint,
)
from .schedule_parser import ScheduleParseError

CHINA = timezone(timedelta(hours=8))

DEFAULT_SCAN_TIMES = ("00:30", "12:30", "20:30")
# 每轮最多看几条候选动态（置顶另算）。
RECENT_POSTS = 5
# 同一个"待确认周次"的任务连续失败几次就放弃这张图，避免卡住整个扫描。
PENDING_ATTEMPTS = 3
_CLOCK = re.compile(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]")


def parse_scan_times(values):
    """把 HH:MM 列表解析成去重、排序后的 time；非法值抛 ValueError。"""
    if values is None:
        values = DEFAULT_SCAN_TIMES
    if not isinstance(values, (list, tuple)):
        raise ValueError("周表扫描时间必须是 HH:MM 列表。")
    times = set()
    for value in values:
        text = str(value).strip()
        if not _CLOCK.fullmatch(text):
            raise ValueError(f"周表扫描时间 {value!r} 不是 HH:MM。")
        times.add(time(int(text[:2]), int(text[3:])))
    return tuple(sorted(times))


def latest_due_slot(now, times):
    """今天已经到点的最晚一个时间点；没有则为 None。"""
    passed = [item for item in times if item <= now.time()]
    return (
        f"{now.date().isoformat()}T{passed[-1].strftime('%H:%M')}" if passed else None
    )


def next_scan_at(now, times):
    """下一次扫描时刻；没有配置时间点时返回 None。"""
    for item in times:
        if item > now.time():
            return datetime.combine(now.date(), item, CHINA)
    return (
        datetime.combine(now.date() + timedelta(days=1), times[0], CHINA)
        if times
        else None
    )


def monday(day):
    return day - timedelta(days=day.weekday())


def snapshot(raw, text):
    # 图片字节或随附正文里出现日期，都会触发解析。
    dates = re.findall(
        r"\d{4}-\d{2}-\d{2}|(?:\d{4}年)?\d{1,2}[./月-]\d{1,2}日?|"
        r"[零〇一二三四五六七八九十廿\d]+[年月日号]|本周|上周|下周|这周",
        text,
    )
    return hashlib.sha256(raw + "\n".join(dates).encode()).hexdigest()


async def remember_schedule(data, post, schedule, raw):
    old = await data.get_schedule_tracking(post.uid)
    state = old or {}
    state.setdefault("anchor_week", schedule.week_start)
    state.setdefault("observations", [])
    state.update(
        week=schedule.week_start,
        dynamic_id=post.id,
        image_url=schedule.source_image_url,
        fingerprint=snapshot(raw, post.text),
        schedule=schedule.to_dict(),
        checked_slot="",
        pending=None,
        error="",
    )
    await data.save_schedule_tracking(post.uid, state)


class ScheduleWatch:
    def __init__(
        self,
        data,
        bili,
        parser,
        schedules,
        *,
        bootstrap,
        is_candidate,
        lock_for,
        scan_times=DEFAULT_SCAN_TIMES,
        recent_posts=RECENT_POSTS,
        auto_parse_normal=False,
    ):
        self.data, self.bili, self.parser, self.schedules = (
            data,
            bili,
            parser,
            schedules,
        )
        self.bootstrap, self.is_candidate, self.lock_for = (
            bootstrap,
            is_candidate,
            lock_for,
        )
        self.scan_times = parse_scan_times(scan_times)
        self.recent_posts = max(1, int(recent_posts))
        self.auto_parse_normal = bool(auto_parse_normal)

    def select_posts(self, posts):
        """范围：置顶动态 + 最近几条候选动态，最多 recent_posts 条。"""
        candidates = [post for post in posts if self.is_candidate(post)]
        pinned = [post for post in candidates if post.is_pinned]
        others = sorted(
            (post for post in candidates if not post.is_pinned),
            key=lambda post: int(post.id),
            reverse=True,
        )
        return (pinned + others)[: self.recent_posts]

    def _known(self, previous, post, url):
        """已判定过、正文也没变的图片不必重复下载与分类。"""
        record = previous.get((post.id, url))
        if not record:
            return False
        if (
            record.get("text_fingerprint")
            != hashlib.sha256(post.text.encode()).hexdigest()
        ):
            return False
        return record.get("status") in ("parsed", "unchanged", "archived", "skipped")

    @staticmethod
    def _given_up(state, post, url, fingerprint=None):
        """确认不了周次、已经放弃过的图不再重复处理；换了图或正文才会重新看。"""
        for item in state.get("ignored_images") or []:
            if item.get("dynamic_id") != post.id or item.get("image_url") != url:
                continue
            if (
                fingerprint is None
                or not item.get("fingerprint")
                or item["fingerprint"] == fingerprint
            ):
                return True
        return False

    async def check(self, uid, *, now=None, special=False):
        """到点后扫描一次；同一时间点只处理一次。"""
        now = now or datetime.now(CHINA)
        slot = latest_due_slot(now, self.scan_times)
        if slot is None:
            return False
        state = await self.data.get_schedule_tracking(uid) or {}
        if state.get("checked_slot") == slot:
            return False
        if state.get("retry_slot") == slot and now.timestamp() < state.get(
            "retry_at", 0
        ):
            return True
        if not state.get("week"):
            if not (special or self.auto_parse_normal):
                return False
            try:
                results = await self.bootstrap(uid)
                if (
                    isinstance(results, list)
                    and any(row["status"] == "failed" for row in results)
                    and not any(
                        row["status"] in ("parsed", "archived", "unchanged")
                        for row in results
                    )
                ):
                    state = await self.data.get_schedule_tracking(uid) or state
                    return await self._retry(
                        uid, state, slot, now, "初次周表扫描遇到图片或识别错误"
                    )
            except Exception as exc:
                return await self._retry(
                    uid, state, slot, now, self._error_text(exc), exc
                )
            state = await self.data.get_schedule_tracking(uid) or state
            state["checked_slot"] = slot
            await self.data.save_schedule_tracking(uid, state)
            return False
        async with self.lock_for(uid):
            state = await self.data.get_schedule_tracking(uid)
            if state is None or state.get("checked_slot") == slot:
                return False
            try:
                await self._scan_once(uid, state, now, slot)
            except Exception as exc:
                return await self._retry(
                    uid, state, slot, now, self._error_text(exc), exc
                )
            for key in ("retry_slot", "retry_at", "scan_attempts"):
                state.pop(key, None)
            await self.data.save_schedule_tracking(uid, state)
            return False

    @staticmethod
    def _error_text(exc):
        if isinstance(exc, (BiliError, ScheduleParseError)):
            return str(exc)
        if isinstance(exc, OSError):
            return "周表图片读写失败，请检查数据目录"
        return f"周表扫描失败（{type(exc).__name__}）"

    async def _retry(self, uid, state, slot, now, error, exc=None):
        attempts = (
            state.get("scan_attempts", 0) + 1 if state.get("retry_slot") == slot else 1
        )
        delay = max(
            60 * attempts, exc.retry_after if isinstance(exc, BiliRateLimited) else 0
        )
        state.update(
            retry_slot=slot,
            retry_at=now.timestamp() + delay,
            scan_attempts=attempts,
            error=f"{error}。保留原周表。",
        )
        if attempts >= PENDING_ATTEMPTS:
            state["checked_slot"] = slot
        await self.data.save_schedule_tracking(uid, state)
        logger.warning(
            "Schedule scan uid=%s slot=%s attempt=%s: %s", uid, slot, attempts, error
        )
        return attempts < PENDING_ATTEMPTS

    async def _scan_once(self, uid, state, now, slot):
        today = now.date()
        # 先续做上次中断的任务，再比对新的快照。
        dropped = False
        if state.get("pending"):
            try:
                await self._complete(uid, state)
            except ValueError as exc:
                dropped = await self._postpone_or_drop(uid, state, exc)
        previous = {
            (row["dynamic_id"], row["image_url"]): row
            for row in await self.data.get_schedule_candidates(uid)
        }
        chosen = None
        unchanged = False
        errors = []
        for post in self.select_posts(await self.bili.get_latest_dynamics(uid)):
            for index, url in enumerate(post.images, 1):
                current = url == state.get("image_url")
                if not current and self._known(previous, post, url):
                    continue
                if not current and self._given_up(state, post, url):
                    continue
                try:
                    raw = await self.bili.download_image(url)
                    fingerprint = snapshot(raw, post.text)
                    if fingerprint == state.get("fingerprint"):
                        unchanged = True
                        continue
                    if self._given_up(state, post, url, fingerprint):
                        continue
                    async with self.data.classification_image(raw) as path:
                        is_schedule = await self.parser.is_schedule_image(path)
                    if is_schedule:
                        path = await self.data.save_schedule_image(
                            uid, post.id, url, raw
                        )
                        chosen = (post, url, path, fingerprint, index)
                        break
                    await self._remember_skip(uid, post, url, index, raw)
                except BiliRateLimited:
                    raise
                except (BiliError, ScheduleParseError, OSError) as exc:
                    errors.append(exc)
                    continue
            if chosen:
                break
        if chosen:
            post, url, path, fingerprint, index = chosen
            pending = state.get("pending")
            if not pending or pending["fingerprint"] != fingerprint:
                week = monday(today).isoformat()
                observations = [
                    o for o in state.get("observations", []) if o["week"] == week
                ]
                if not any(o["fingerprint"] == fingerprint for o in observations):
                    observations.append({"week": week, "fingerprint": fingerprint})
                state["observations"] = observations
                state["pending"] = {
                    "fingerprint": fingerprint,
                    "observed_on": today.isoformat(),
                    "multiple": len(observations) >= 2,
                    "count": len(observations),
                    "dynamic_id": post.id,
                    "image_url": url,
                    "image_index": index,
                    "path": path,
                    "text": post.text,
                }
                await self.data.save_schedule_tracking(uid, state)
                try:
                    await self._complete(uid, state)
                except ValueError as exc:
                    dropped = await self._postpone_or_drop(uid, state, exc)
        elif errors:
            raise errors[0]
        elif unchanged and not state.get("pending"):
            state["error"] = ""
        elif not dropped and not state.get("pending"):
            state["error"] = "置顶与最近动态里没有找到可确认的周表图，请手动检查"
        state["checked_slot"] = slot

    async def _remember_skip(self, uid, post, url, index, raw):
        """记住"这张图不是周表"，避免每轮重复分类。"""
        await self.data.save_schedule_candidate(
            {
                "uid": uid,
                "dynamic_id": post.id,
                "image_url": url,
                "image_index": index,
                "is_pinned": post.is_pinned,
                "published_at": post.published_at,
                "text_fingerprint": hashlib.sha256(post.text.encode()).hexdigest(),
                "evaluated_on": china_today().isoformat(),
                "local_image_path": "",
                "status": "skipped",
                "checked_at": datetime.now(CHINA).isoformat(timespec="seconds"),
            }
        )

    async def _postpone_or_drop(self, uid, state, error):
        """待确认周次的任务失败：记次数，连续失败到上限就放弃这张图。

        一张"确实是周表、但模型始终判不出周次"的图（例如主播把本周周表换成另一种
        排版）原来会让这个任务无限重试：每个时间点都重新下载、重新分类同一张图，
        却永远确认不了。放弃时把它记成已处理的候选，扫描就能继续往前看。

        Returns:
            是否已经放弃这张图（True 表示本轮不再重复处理它）。
        """
        code = getattr(error, "code", "needs_date")
        if code in (
            "model_error",
            "model_timeout",
            "model_unconfigured",
            "schedule_conflict",
        ):
            raise error
        if code == "image_missing":
            state["pending"] = None
            raise error
        job = dict(state.get("pending") or {})
        attempts = int(job.get("attempts", 0)) + 1
        if attempts < PENDING_ATTEMPTS:
            job["attempts"] = attempts
            state["pending"] = job
            state["error"] = (
                str(error)
                if isinstance(error, ScheduleParseError)
                else "上次更新尚不能确认周次或解析失败"
            )
            return False
        state["pending"] = None
        state["error"] = (
            "有一张新周表图无法确认周次，已跳过；需要的话用 /vt_parse_schedule 指定周次"
        )
        logger.warning(
            "Schedule image left unconfirmed uid=%s dynamic=%s",
            uid,
            job.get("dynamic_id"),
        )
        ignored = [
            item
            for item in (state.get("ignored_images") or [])
            if item.get("image_url") != job.get("image_url")
        ]
        ignored.append(
            {
                "dynamic_id": str(job.get("dynamic_id", "")),
                "image_url": job.get("image_url", ""),
                "fingerprint": job.get("fingerprint", ""),
            }
        )
        state["ignored_images"] = ignored[-10:]
        if job.get("dynamic_id") and job.get("image_url"):
            await self.data.save_schedule_candidate(
                {
                    "uid": uid,
                    "dynamic_id": str(job["dynamic_id"]),
                    "image_url": job["image_url"],
                    "image_index": int(job.get("image_index", 1) or 1),
                    "text_fingerprint": job.get("fingerprint", ""),
                    "evaluated_on": china_today().isoformat(),
                    "local_image_path": job.get("path", ""),
                    "status": "skipped",
                    "error_code": code,
                    "error_message": str(error)
                    if isinstance(error, ScheduleParseError)
                    else "周表图无法确认周次，已跳过",
                    "checked_at": datetime.now(CHINA).isoformat(timespec="seconds"),
                }
            )
        return True

    async def _complete(self, uid, state):
        job = state["pending"]
        options = {"source_text": job["text"]}
        observed = date.fromisoformat(job["observed_on"])
        week = monday(observed)
        allowed = {week.isoformat(), (week + timedelta(days=7)).isoformat()}
        if (
            state["week"] in allowed
            or state["week"] == (week - timedelta(days=7)).isoformat()
        ):
            allowed.add(state["week"])
        hint = explicit_week_hint(job["text"], observed)
        if hint is not None and hint.isoformat() in allowed:
            options["week_start"] = hint.isoformat()
        else:
            options["update_context"] = {
                "initial_anchor_week": state["anchor_week"],
                "previous_week": state["week"],
                "observed_on": job["observed_on"],
                "previous_schedule": state["schedule"],
                "updates_this_week": job["count"],
                "allowed_weeks": sorted(allowed),
            }
        if job.get("parsed"):
            content = dict(job["parsed"])
            content["streams"] = tuple(StreamPlan(**s) for s in content["streams"])
            schedule = WeeklySchedule(**content)
        else:
            schedule = await self.parser.parse(uid, job["path"], **options)
        if schedule.week_start not in allowed:
            raise ValueError("自动识别的周次超出扫描日期允许范围")
        schedule = replace(
            schedule,
            source_dynamic_id=job["dynamic_id"],
            source_image_url=job["image_url"],
            local_image_path=job["path"],
        )
        job["parsed"] = schedule.to_dict()
        await self.data.save_schedule_tracking(uid, state)
        stored = await self.data.get_historical_schedule(uid, schedule.week_start)
        stored_source = str(stored.get("source_dynamic_id") or "") if stored else ""
        if stored_source.isdigit() and int(stored_source) > int(job["dynamic_id"]):
            raise ValueError("已有更新来源的周表，需要手动确认基准")
        # 可恢复的任务在导入前就固定了目标周；导入后崩溃只会重试
        # 这次幂等导入，而不会把锚点再往后推一周。
        status = await self.schedules.store_parsed_schedule(
            schedule, import_key=f"scan:{job['observed_on']}:{job['fingerprint']}"
        )
        await self.data.save_schedule_candidate(
            {
                "uid": uid,
                "dynamic_id": str(job["dynamic_id"]),
                "image_url": job["image_url"],
                "image_index": job.get("image_index", 1),
                "text_fingerprint": hashlib.sha256(job["text"].encode()).hexdigest(),
                "evaluated_on": china_today().isoformat(),
                "local_image_path": job["path"],
                "status": status,
                "week_start": schedule.week_start,
                "checked_at": datetime.now(CHINA).isoformat(timespec="seconds"),
            }
        )
        state.update(
            week=schedule.week_start,
            schedule=schedule.to_dict(),
            fingerprint=job["fingerprint"],
            dynamic_id=job["dynamic_id"],
            image_url=job["image_url"],
            pending=None,
            error="",
        )
        await self.data.save_schedule_tracking(uid, state)

    async def run_once(self, now=None):
        now = now or datetime.now(CHINA)
        if latest_due_slot(now, self.scan_times) is None:
            return False
        specials = set(await self.data.get_special_vtubers())
        uids = await self.data.get_subscribed_uids()
        limit = asyncio.Semaphore(2)

        async def scan_uid(uid):
            try:
                async with limit:
                    if uid in specials:
                        await self.ensure_placeholder(uid)
                    return await self.check(uid, now=now, special=uid in specials)
            except Exception as exc:
                logger.warning(
                    "Unable to scan schedule uid=%s type=%s", uid, type(exc).__name__
                )
                return True

        return any(await asyncio.gather(*(scan_uid(uid) for uid in uids)))

    async def ensure_placeholder(self, uid):
        """确保本周有周表容器；没有周表服务时静默跳过。"""
        return await self.schedules.ensure_placeholder(uid)

    async def run(self):
        while True:
            retry = True
            try:
                # 启动与每个时间点都跑一轮；同一时间点由 checked_slot 去重。
                retry = await self.run_once()
            except Exception:
                logger.warning("Unable to enumerate schedule subscriptions")
            target = next_scan_at(datetime.now(CHINA), self.scan_times)
            if target is None:
                return
            delay = (target - datetime.now(CHINA)).total_seconds()
            if retry:
                delay = min(delay, 60)
            await asyncio.sleep(max(1, delay))
