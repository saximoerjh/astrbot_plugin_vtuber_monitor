"""可恢复的零点检查；与手动导入共用周表发现的锁。"""
import asyncio
import hashlib
import logging
import re
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from ..core.schedule_models import WeeklySchedule, StreamPlan

CHINA = timezone(timedelta(hours=8))
log = logging.getLogger(__name__)


def monday(day):
    return day - timedelta(days=day.weekday())


def snapshot(raw, text):
    # 图片字节或随附正文里出现日期，都会触发解析。
    dates = re.findall(r"\d{4}-\d{2}-\d{2}|(?:\d{4}年)?\d{1,2}[./月-]\d{1,2}日?|"
                       r"[零〇一二三四五六七八九十廿\d]+[年月日号]|本周|上周|下周|这周", text)
    return hashlib.sha256(raw + "\n".join(dates).encode()).hexdigest()


class ScheduleWatch:
    def __init__(self, discovery):
        self.discovery = discovery
        self.data = discovery.data

    async def remember(self, post, schedule, raw):
        """仅在手动导入成功后、且持有周表发现锁的情况下调用。"""
        old = await self.data.get_schedule_tracking(post.uid)
        state = old or {"anchor_week": schedule.week_start, "observations": []}
        state.update(week=schedule.week_start, dynamic_id=post.id,
                     image_url=schedule.source_image_url, fingerprint=snapshot(raw, post.text),
                     schedule=schedule.to_dict(), checked_on="", pending=None, error="")
        await self.data.save_schedule_tracking(post.uid, state)

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
            schedule, import_key=f"midnight:{job['observed_on']}:{job['fingerprint']}")
        state.update(week=schedule.week_start, schedule=schedule.to_dict(),
                     fingerprint=job["fingerprint"], dynamic_id=job["dynamic_id"],
                     image_url=job["image_url"], pending=None, error="")
        await self.data.save_schedule_tracking(uid, state)

    async def check(self, uid, today=None):
        today = today or datetime.now(CHINA).date()
        async with self.discovery._lock:
            state = await self.data.get_schedule_tracking(uid)
            if not state or state.get("checked_on") == today.isoformat():
                return
            try:
                # 先续做上次中断的任务，再比对新的快照。
                if state.get("pending"):
                    try:
                        await self._complete(uid, state)
                    except Exception:
                        state["error"] = "上次更新尚不能确认周次或解析失败"
                post = await self.discovery.bili.get_dynamic(uid, state["dynamic_id"])
                chosen = None
                unchanged = False
                urls = list(post.images)
                if state["image_url"] in urls:
                    urls.remove(state["image_url"])
                    urls.insert(0, state["image_url"])
                for url in urls:
                    raw = await self.discovery.bili.download_image(url)
                    fingerprint = snapshot(raw, post.text)
                    if fingerprint == state["fingerprint"]:
                        # 已采用的图片仍然存在且没有变化。
                        chosen = None
                        unchanged = True
                        state.update(pending=None, error="")
                        break
                    path = await self.data.save_schedule_image(uid, post.id, url, raw)
                    if await self.discovery.parser.is_schedule_image(path):
                        chosen = (url, path, fingerprint)
                        break
                if chosen:
                    url, path, fingerprint = chosen
                    pending = state.get("pending")
                    if not pending or pending["fingerprint"] != fingerprint:
                        week = monday(today).isoformat()
                        observations = [o for o in state["observations"] if o["week"] == week]
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
                elif not unchanged:
                    state["error"] = "来源动态未找到可确认的周表图，请手动检查"
                state["checked_on"] = today.isoformat()
            except Exception:
                # 不暴露模型错误与 Cookie；保留已采用的周表和任务。
                state.update(checked_on=today.isoformat(), error="检查或解析失败，保留原周表，下次检查重试")
                log.warning("Midnight schedule check failed uid=%s", uid)
            await self.data.save_schedule_tracking(uid, state)

    async def run_once(self):
        for uid in await self.data.get_subscribed_uids():
            try:
                await self.check(uid)
            except Exception:
                log.warning("Unable to check schedule uid=%s", uid)

    async def run(self):
        while True:
            try:
                # 启动时补查一次；已持久化的日期避免同一天重复检查。
                await self.run_once()
            except Exception:
                log.warning("Unable to enumerate schedule subscriptions")
            now = datetime.now(CHINA)
            midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time(), CHINA)
            await asyncio.sleep(max(1, (midnight - now).total_seconds()))
