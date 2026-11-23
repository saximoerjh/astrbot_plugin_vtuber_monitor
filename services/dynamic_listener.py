"""先持久化；把可恢复的调播任务与 Bilibili 读取分开处理。"""

import asyncio
import json
import math

from astrbot.api import logger

from ..bili_client import BiliError
from ..core.models import DynamicPost, utc_now
from .schedule_discovery import ScheduleRouteError


class DynamicListener:
    def __init__(self, data, bili, interval=300, *, adjustment=None, discovery=None):
        if not math.isfinite(interval) or not 30 <= interval <= 86400:
            raise ValueError("dynamic_poll_interval 必须在 30 到 86400 秒之间。")
        self.data = data
        self.bili = bili
        self.adjustment = adjustment
        self.discovery = discovery
        self.interval = interval
        self._lock = asyncio.Lock()
        self.rounds = 0
        self.failures = 0
        self.received = 0
        self.last_poll_at = None
        self.last_success_at = None
        self.last_error = ""

    async def poll_once(self):
        async with self._lock:
            self.last_poll_at = utc_now()
            for uid in await self.data.get_special_vtubers():
                try:
                    checkpoint = await self.data.get_dynamic_checkpoint(uid)
                    async with asyncio.timeout(120):
                        posts = await self.bili.get_dynamics_since(
                            uid, checkpoint["latest_id"] if checkpoint else None
                        )
                    accepted = await self.data.ingest_dynamics(uid, posts)
                    self.received += len(accepted)
                    self.last_success_at = utc_now()
                except Exception as exc:
                    self.failures += 1
                    self.last_error = (
                        str(exc)
                        if isinstance(exc, BiliError)
                        else f"动态采集失败（{type(exc).__name__}）"
                    )
                    logger.warning(
                        "Dynamic fetch failed uid=%s: %s", uid, self.last_error
                    )
            self.rounds += 1

    async def _process_uid(self, uid):
        try:
            has_schedule = bool(
                await self.adjustment.schedules.get_weekly_schedule(uid)
            )
            if not has_schedule and self.discovery is None:
                return
            jobs = await self.data.pending_adjustments(
                uid, unchecked_images_only=not has_schedule
            )
            for job in jobs:
                try:
                    payload = json.loads(job["payload"])
                    payload["images"] = tuple(payload["images"])
                    revision = payload.pop("_revision", "")
                    post = DynamicPost(**payload)
                    if post.images and not job.get("images_checked"):
                        if self.discovery is None:
                            raise ScheduleRouteError(
                                "周表识别服务未就绪，暂不执行自动调播。"
                            )
                        if await self.discovery.process_dynamic(post):
                            await self.data.finish_adjustment(
                                uid, post.id, True, payload=job["payload"]
                            )
                            if not has_schedule:
                                has_schedule = bool(
                                    await self.adjustment.schedules.get_weekly_schedule(
                                        uid
                                    )
                                )
                            continue
                        if not await self.data.mark_adjustment_images_checked(
                            uid, post.id, job["payload"]
                        ):
                            continue
                    if not has_schedule:
                        continue
                    await self.adjustment.process(post, revision=revision)
                except Exception as exc:
                    self.failures += 1
                    error = f"调播任务未完成（{type(exc).__name__}）"
                    if getattr(exc, "code", None) in (
                        "model_timeout",
                        "model_error",
                        "schedule_conflict",
                        "schedule_route_unresolved",
                    ):
                        error = str(exc)
                    await self.data.finish_adjustment(
                        uid,
                        job["dynamic_id"],
                        False,
                        payload=job["payload"],
                        error=error,
                    )
                    logger.warning(
                        "Adjustment failed uid=%s dynamic=%s: %s",
                        uid,
                        job["dynamic_id"],
                        error,
                    )
                    break
                else:
                    await self.data.finish_adjustment(
                        uid, job["dynamic_id"], True, payload=job["payload"]
                    )
        except Exception as exc:
            self.failures += 1
            logger.warning(
                "Adjustment queue failed uid=%s type=%s", uid, type(exc).__name__
            )

    async def process_pending(self):
        if self.adjustment is None or not self.adjustment.provider_available():
            return
        await self.data.expire_adjustments()
        uids = await self.data.get_special_vtubers()
        for start in range(0, len(uids), 2):
            await asyncio.gather(
                *(self._process_uid(uid) for uid in uids[start : start + 2])
            )

    async def run_adjustments(self):
        while True:
            try:
                await self.process_pending()
            except Exception as exc:
                self.failures += 1
                logger.warning("Adjustment worker failed type=%s", type(exc).__name__)
            await asyncio.sleep(30)

    async def run(self):
        while True:
            try:
                await self.poll_once()
            except Exception:
                self.failures += 1
                logger.warning("Dynamic poll round failed")
            await asyncio.sleep(self.interval)
