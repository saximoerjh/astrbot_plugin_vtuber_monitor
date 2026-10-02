"""先持久化；把可恢复的调播任务与 Bilibili 读取分开处理。"""
import asyncio
import json
import math

from astrbot.api import logger

from ..core.models import DynamicPost, utc_now


class DynamicListener:
    def __init__(self, data, bili, interval=300, discovery=None, adjustment=None, dispatcher=None):
        if not math.isfinite(interval) or not 30 <= interval <= 86400:
            raise ValueError("dynamic_poll_interval 必须在 30 到 86400 秒之间。")
        self.data = data
        self.bili = bili
        self.discovery = discovery
        self.adjustment = adjustment
        self.dispatcher = dispatcher
        self.interval = interval
        self._lock = asyncio.Lock()
        self.rounds = 0
        self.failures = 0
        self.received = 0
        self.last_poll_at = None
        self.require_schedule = False
        self.eligible_only = False

    async def poll_once(self):
        async with self._lock:
            self.last_poll_at = utc_now()
            for uid in await self.data.get_special_vtubers():
                ready = self.adjustment is not None
                if ready and self.require_schedule:
                    try:
                        ready = bool(self.adjustment.provider_id and
                                     await self.adjustment.schedules.get_weekly_schedule(uid))
                    except Exception:
                        self.failures += 1
                        logger.warning("Unable to check adjustment eligibility uid=%s", uid)
                        continue
                if self.eligible_only and not ready:
                    continue
                try:
                    posts = await self.bili.get_latest_dynamics(uid)
                    accepted = await self.data.ingest_dynamics(uid, posts)
                    self.received += len(accepted)
                    if self.discovery is not None:
                        # 置顶动态会在不产生新动态 ID 的情况下被编辑。
                        # 周表发现有自己的图片与正文缓存，必须看到完整一页。
                        await self.discovery.scan(uid, posts)
                except Exception:
                    self.failures += 1
                    # 绝不打印上游响应体或 Cookie。
                    logger.warning("Dynamic fetch/discovery failed uid=%s; committed jobs remain available", uid)
                if ready:
                    try:
                        for job in await self.data.pending_adjustments(uid):
                            try:
                                payload = json.loads(job["payload"])
                                payload["images"] = tuple(payload["images"])
                                await self.adjustment.process(DynamicPost(**payload))
                            except Exception:
                                self.failures += 1
                                await self.data.finish_adjustment(uid, job["dynamic_id"], False)
                                logger.warning("Adjustment failed uid=%s; bounded retry queued", uid)
                                # 保持时间顺序：后面的改动不得越过正在重试的那一条。
                                break
                            else:
                                await self.data.finish_adjustment(uid, job["dynamic_id"], True)
                    except Exception:
                        self.failures += 1
                        logger.warning("Adjustment queue failed uid=%s", uid)
            if self.dispatcher is not None:
                await self.dispatcher.flush_schedule_notifications(self.data)
            self.rounds += 1

    async def run(self):
        while True:
            try:
                await self.poll_once()
            except Exception:
                self.failures += 1
                logger.warning("Dynamic poll round failed")
            await asyncio.sleep(self.interval)
