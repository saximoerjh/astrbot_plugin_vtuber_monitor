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

from ..core.schedule_models import StreamPlan, WeeklySchedule, china_today

CHINA = timezone(timedelta(hours=8))

DEFAULT_SCAN_TIMES = ("00:30", "12:30", "20:30")
# 每轮最多看几条候选动态（置顶另算）。
RECENT_POSTS = 5
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
    return f"{now.date().isoformat()}T{passed[-1].strftime('%H:%M')}" if passed else None


def next_scan_at(now, times):
    """下一次扫描时刻；没有配置时间点时返回 None。"""
    for item in times:
        if item > now.time():
            return datetime.combine(now.date(), item, CHINA)
    return datetime.combine(now.date() + timedelta(days=1), times[0], CHINA) if times else None


def monday(day):
    return day - timedelta(days=day.weekday())


def snapshot(raw, text):
    # 图片字节或随附正文里出现日期，都会触发解析。
    dates = re.findall(r"\d{4}-\d{2}-\d{2}|(?:\d{4}年)?\d{1,2}[./月-]\d{1,2}日?|"
                       r"[零〇一二三四五六七八九十廿\d]+[年月日号]|本周|上周|下周|这周", text)
    return hashlib.sha256(raw + "\n".join(dates).encode()).hexdigest()


class ScheduleWatch:
    def __init__(self, discovery, *, scan_times=DEFAULT_SCAN_TIMES, recent_posts=RECENT_POSTS,
                 auto_parse_normal=False):
        self.discovery = discovery
        self.data = discovery.data
        self.scan_times = parse_scan_times(scan_times)
        self.recent_posts = max(1, int(recent_posts))
        self.auto_parse_normal = bool(auto_parse_normal)

    async def remember(self, post, schedule, raw):
        """记录/更新定时检查的基准；解析成功后由发现流程调用。"""
        old = await self.data.get_schedule_tracking(post.uid)
        state = old or {"anchor_week": schedule.week_start, "observations": []}
        state.update(week=schedule.week_start, dynamic_id=post.id,
                     image_url=schedule.source_image_url, fingerprint=snapshot(raw, post.text),
                     schedule=schedule.to_dict(), checked_slot="", pending=None, error="")
        await self.data.save_schedule_tracking(post.uid, state)

    def select_posts(self, posts):
        """范围：置顶动态 + 最近几条候选动态，最多 recent_posts 条。"""
        candidates = [post for post in posts if self.discovery.is_candidate(post)]
        pinned = [post for post in candidates if post.is_pinned]
        others = sorted((post for post in candidates if not post.is_pinned),
                        key=lambda post: int(post.id), reverse=True)
        return (pinned + others)[:self.recent_posts]

    def _known(self, previous, post, url):
        """已判定过、正文也没变的图片不必重复下载与分类。"""
        record = previous.get((post.id, url))
        if not record:
            return False
        if record.get("text_fingerprint") != hashlib.sha256(post.text.encode()).hexdigest():
            return False
        return record.get("status") in ("parsed", "unchanged", "archived", "skipped")

    async def check(self, uid, *, now=None, special=False):
        """到点后扫描一次；同一时间点只处理一次。"""
        now = now or datetime.now(CHINA)
        state = await self.data.get_schedule_tracking(uid)
        if state is None:
            # 还没有基准：特别关注（或开了普通关注自动解析）先扫一页建立基准。
            if special or self.auto_parse_normal:
                await self.discovery.scan(uid)
            return
        slot = latest_due_slot(now, self.scan_times)
        if slot is None or state.get("checked_slot") == slot:
            return
        async with self.discovery._lock:
            state = await self.data.get_schedule_tracking(uid)
            if state is None or state.get("checked_slot") == slot:
                return
            try:
                await self._scan_once(uid, state, now, slot)
            except Exception:
                # 不暴露模型错误与 Cookie；保留已采用的周表和任务。
                state.update(checked_slot=slot, error="检查或解析失败，保留原周表，下次检查重试")
                logger.warning("Schedule scan failed uid=%s", uid)
            await self.data.save_schedule_tracking(uid, state)
            await self.discovery.prune_images()

    async def _scan_once(self, uid, state, now, slot):
        today = now.date()
        # 先续做上次中断的任务，再比对新的快照。
        if state.get("pending"):
            try:
                await self._complete(uid, state)
            except Exception:
                state["error"] = "上次更新尚不能确认周次或解析失败"
        previous = {(row["dynamic_id"], row["image_url"]): row
                    for row in await self.data.get_schedule_candidates(uid)}
        chosen = None
        unchanged = False
        for post in self.select_posts(await self.discovery.bili.get_latest_dynamics(uid)):
            for index, url in enumerate(post.images, 1):
                current = url == state.get("image_url")
                if not current and self._known(previous, post, url):
                    continue
                raw = await self.discovery.bili.download_image(url)
                fingerprint = snapshot(raw, post.text)
                if fingerprint == state.get("fingerprint"):
                    # 已采用的周表图仍然存在且没变。
                    unchanged = True
                    continue
                # 先用临时图判定；只有确实是周表才落成候选原图，
                # 免得每个不是周表的配图都在 schedule_images/ 留下孤儿文件。
                path = await self.data.save_classification_image(raw)
                if await self.discovery.parser.is_schedule_image(path):
                    path = await self.data.save_schedule_image(uid, post.id, url, raw)
                    chosen = (post, url, path, fingerprint)
                    break
                await self._remember_skip(uid, post, url, index, raw)
            if chosen:
                break
        if chosen:
            post, url, path, fingerprint = chosen
            pending = state.get("pending")
            if not pending or pending["fingerprint"] != fingerprint:
                week = monday(today).isoformat()
                observations = [o for o in state.get("observations", []) if o["week"] == week]
                if not any(o["fingerprint"] == fingerprint for o in observations):
                    observations.append({"week": week, "fingerprint": fingerprint})
                state["observations"] = observations
                state["pending"] = {
                    "fingerprint": fingerprint, "observed_on": today.isoformat(),
                    "target_week": (date.fromisoformat(state["week"]) + timedelta(days=7)).isoformat(),
                    "multiple": len(observations) >= 2, "count": len(observations),
                    "dynamic_id": post.id, "image_url": url, "path": path, "text": post.text,
                }
                await self.data.save_schedule_tracking(uid, state)
                await self._complete(uid, state)
        elif unchanged:
            state.update(pending=None, error="")
        else:
            state["error"] = "置顶与最近动态里没有找到可确认的周表图，请手动检查"
        state["checked_slot"] = slot

    async def _remember_skip(self, uid, post, url, index, raw):
        """记住"这张图不是周表"，避免每轮重复分类。"""
        await self.data.save_schedule_candidate({
            "uid": uid, "dynamic_id": post.id, "image_url": url, "image_index": index,
            "is_pinned": post.is_pinned, "published_at": post.published_at,
            "text_fingerprint": hashlib.sha256(post.text.encode()).hexdigest(),
            "evaluated_on": china_today().isoformat(), "local_image_path": "",
            "status": "skipped", "checked_at": datetime.now(CHINA).isoformat(timespec="seconds"),
        })

    async def _complete(self, uid, state):
        job = state["pending"]
        options = {"source_text": job["text"]}
        if job["multiple"]:
            options["update_context"] = {
                "initial_anchor_week": state["anchor_week"], "previous_week": state["week"],
                "observed_on": job["observed_on"], "previous_schedule": state["schedule"],
                "updates_this_week": job["count"],
            }
        else:
            options["week_start"] = job["target_week"]
        if job.get("parsed"):
            content = dict(job["parsed"])
            content["streams"] = tuple(StreamPlan(**s) for s in content["streams"])
            schedule = WeeklySchedule(**content)
        else:
            schedule = await self.discovery.parser.parse(uid, job["path"], **options)
        # 兜底只允许保持原周或顺延一周，绝不向前跳。
        if schedule.week_start not in (state["week"], job["target_week"]):
            raise ValueError("自动周次判断超出相邻周范围")
        schedule = replace(schedule, source_dynamic_id=job["dynamic_id"],
                           source_image_url=job["image_url"], local_image_path=job["path"])
        job["parsed"] = schedule.to_dict()
        await self.data.save_schedule_tracking(uid, state)
        stored = await self.data.get_historical_schedule(uid, schedule.week_start)
        if stored and int(stored["source_dynamic_id"]) > int(job["dynamic_id"]):
            raise ValueError("已有更新来源的周表，需要手动确认基准")
        # 可恢复的任务在导入前就固定了目标周；导入后崩溃只会重试
        # 这次幂等导入，而不会把锚点再往后推一周。
        await self.discovery.schedules.store_parsed_schedule(
            schedule, import_key=f"scan:{job['observed_on']}:{job['fingerprint']}")
        state.update(week=schedule.week_start, schedule=schedule.to_dict(),
                     fingerprint=job["fingerprint"], dynamic_id=job["dynamic_id"],
                     image_url=job["image_url"], pending=None, error="")
        await self.data.save_schedule_tracking(uid, state)

    async def run_once(self, now=None):
        now = now or datetime.now(CHINA)
        if latest_due_slot(now, self.scan_times) is None:
            return
        specials = set(await self.data.get_special_vtubers())
        for uid in await self.data.get_subscribed_uids():
            try:
                await self.check(uid, now=now, special=uid in specials)
            except Exception:
                logger.warning("Unable to scan schedule uid=%s", uid)

    async def run(self):
        while True:
            try:
                # 启动与每个时间点都跑一轮；同一时间点由 checked_slot 去重。
                await self.run_once()
            except Exception:
                logger.warning("Unable to enumerate schedule subscriptions")
            target = next_scan_at(datetime.now(CHINA), self.scan_times)
            if target is None:
                return
            delay = (target - datetime.now(CHINA)).total_seconds()
            await asyncio.sleep(max(1, delay))
