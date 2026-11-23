"""每轮轮询每个订阅主播一次；只落库观测到的状态变化。"""

import asyncio
import math
import random
import time

from astrbot.api import logger

from ..bili_client import BiliRateLimited

from ..core.models import VtuberState, utc_now


def live_notification(state):
    kind = "live_start" if state.is_live else "live_end"
    action = "开播了" if state.is_live else "下播了"
    text = f"{state.name}（UID {state.uid}）{action}。"
    if state.is_live and state.live_title:
        text += f"\n直播标题：{state.live_title}"
    if state.room_id:
        text += f"\nhttps://live.bilibili.com/{state.room_id}"
    return kind, text, state.live_cover if state.is_live else ""


class LiveListener:
    def __init__(
        self,
        data,
        bili,
        interval=120,
        jitter=60,
        *,
        special_only=False,
        schedule_recorder=None,
    ):
        if not math.isfinite(interval) or not 10 <= interval <= 86400:
            raise ValueError("live_poll_interval 必须在 10 到 86400 秒之间。")
        if not math.isfinite(jitter) or not 0 <= jitter <= 3600:
            raise ValueError("live_poll_jitter 必须在 0 到 3600 秒之间。")
        self.data = data
        self.bili = bili
        self.interval = interval
        self.jitter = jitter
        self.special_only = special_only
        self.cooldown_until = 0
        self._poll_lock = asyncio.Lock()
        self.last_poll_at = None
        self.last_success_at = None
        self.failures = 0
        self.rounds = 0
        self.schedule_recorder = schedule_recorder
        self._pending_sync = set()
        self._sync_ready = asyncio.Event()
        self._resume_uid = None

    async def poll_once(self):
        async with self._poll_lock:
            if self.cooldown_remaining > 0:
                return
            self.last_poll_at = utc_now()
            uids = await (
                self.data.get_special_vtubers()
                if self.special_only
                else self.data.get_subscribed_uids()
            )
            if self._resume_uid in uids:
                start = uids.index(self._resume_uid)
                uids = uids[start:] + uids[:start]
            self._resume_uid = None
            for index, uid in enumerate(uids):
                try:
                    state = await self.bili.get_live_room_info(uid)
                    if (
                        not isinstance(state, VtuberState)
                        or state.uid != uid
                        or state.is_live is None
                    ):
                        raise ValueError("Invalid live observation")
                    await self.data.save_vtuber_state(
                        state,
                        notification=(*live_notification(state), self.special_only),
                    )
                    self.last_success_at = utc_now()
                    if self.schedule_recorder is not None:
                        self._pending_sync.add(uid)
                        self._sync_ready.set()
                except BiliRateLimited as exc:
                    self.failures += 1
                    self.cooldown_until = time.monotonic() + exc.retry_after
                    self._resume_uid = uids[(index + 1) % len(uids)]
                    logger.warning(
                        "Live polling paused for %.0f seconds after rate limit",
                        exc.retry_after,
                    )
                    break
                except Exception:
                    self.failures += 1
                    logger.exception("VTuber live poll failed uid=%s", uid)
            self.rounds += 1

    @property
    def cooldown_remaining(self):
        return max(0, self.cooldown_until - time.monotonic())

    def next_delay(self):
        return max(self.interval, self.cooldown_remaining) + random.uniform(
            0, self.jitter
        )

    async def run_reconciliation(self):
        while True:
            await self._sync_ready.wait()
            pending, self._pending_sync = self._pending_sync, set()
            self._sync_ready.clear()
            for uid in sorted(pending):
                try:
                    await self.schedule_recorder.sync(uid)
                except Exception as exc:
                    self.failures += 1
                    logger.warning(
                        "Live schedule recording deferred uid=%s type=%s",
                        uid,
                        type(exc).__name__,
                    )

    async def run(self):
        while True:
            started = time.monotonic()
            try:
                await self.poll_once()
            except Exception:
                self.failures += 1
                logger.exception("VTuber live poll round failed")
            # 请求、发送与 sleep 抛出的 CancelledError 故意向上传播。
            delay = self.next_delay()
            if self.cooldown_remaining == 0:
                delay = max(1, delay - (time.monotonic() - started))
            await asyncio.sleep(delay)
