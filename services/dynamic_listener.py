"""先持久化；把可恢复的调播任务与 Bilibili 读取分开处理。"""
import asyncio
import json
import math

from astrbot.api import logger

from ..bili_client import BiliError
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
        self.last_success_at = None
        self.last_error = ""
        self.require_schedule = False
        self.eligible_only = False

    async def poll_once(self, *, process=True):
        async with self._lock:
            self.last_poll_at = utc_now()
            for uid in await self.data.get_special_vtubers():
                try:
                    if self.eligible_only and not await self._ready(uid):
                        continue
                    if callable(getattr(type(self.bili), "get_dynamics_since", None)):
                        checkpoint = await self.data.get_dynamic_checkpoint(uid)
                        async with asyncio.timeout(120):
                            posts = await self.bili.get_dynamics_since(uid, checkpoint["latest_id"] if checkpoint else None)
                    else:
                        posts = await self.bili.get_latest_dynamics(uid)
                    accepted = await self.data.ingest_dynamics(uid, posts)
                    self.received += len(accepted)
                    self.last_success_at = utc_now()
                    if self.discovery is not None:
                        # 置顶动态会在不产生新动态 ID 的情况下被编辑。
                        # 周表发现有自己的图片与正文缓存，必须看到完整一页。
                        await self.discovery.scan(uid, posts)
                except Exception as exc:
                    self.failures += 1
                    self.last_error = str(exc) if isinstance(exc, BiliError) else f"动态采集失败（{type(exc).__name__}）"
                    logger.warning("Dynamic fetch/discovery failed uid=%s: %s", uid, self.last_error)
                if process:
                    await self._process_uid(uid)
            if self.dispatcher is not None:
                await self.dispatcher.flush_schedule_notifications(self.data)
            self.rounds += 1

    async def _ready(self, uid):
        return self.adjustment is not None and (
            not self.require_schedule or bool(self.adjustment.provider_id and
                                              await self.adjustment.schedules.get_weekly_schedule(uid)))

    async def _process_uid(self, uid):
        try:
            if not await self._ready(uid):
                return
            for job in await self.data.pending_adjustments(uid):
                try:
                    payload = json.loads(job["payload"])
                    payload["images"] = tuple(payload["images"])
                    revision = payload.pop("_revision", "")
                    await self.adjustment.process(DynamicPost(**payload), **({"revision": revision} if revision else {}))
                except Exception as exc:
                    self.failures += 1
                    error = f"调播任务未完成（{type(exc).__name__}）"
                    if getattr(exc, "code", None) in ("model_timeout", "model_error", "schedule_conflict"):
                        error = str(exc)
                    await self.data.finish_adjustment(uid, job["dynamic_id"], False, payload=job["payload"], error=error)
                    logger.warning("Adjustment failed uid=%s dynamic=%s: %s", uid, job["dynamic_id"], error)
                    break
                else:
                    await self.data.finish_adjustment(uid, job["dynamic_id"], True, payload=job["payload"])
        except Exception as exc:
            self.failures += 1
            logger.warning("Adjustment queue failed uid=%s type=%s", uid, type(exc).__name__)

    async def run_adjustments(self):
        while True:
            try:
                uids = await self.data.get_special_vtubers()
                for start in range(0, len(uids), 2):
                    await asyncio.gather(*(self._process_uid(uid) for uid in uids[start:start + 2]))
            except Exception as exc:
                self.failures += 1
                logger.warning("Adjustment worker failed type=%s", type(exc).__name__)
            await asyncio.sleep(30)

    async def run(self):
        while True:
            try:
                await self.poll_once(process=False)
            except Exception:
                self.failures += 1
                logger.warning("Dynamic poll round failed")
            await asyncio.sleep(self.interval)
