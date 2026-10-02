"""每轮轮询每个订阅主播一次；只落库观测到的状态变化。"""
import asyncio
import math
import random
import time

from astrbot.api import logger

from ..bili_client import BiliRateLimited

from ..core.models import VtuberState, FollowLevel, utc_now


class LiveListener:
    def __init__(self, data, bili, dispatcher, interval=120, jitter=60, *, special_only=False):
        if not math.isfinite(interval) or not 10 <= interval <= 86400:
            raise ValueError("live_poll_interval 必须在 10 到 86400 秒之间。")
        if not math.isfinite(jitter) or not 0 <= jitter <= 3600:
            raise ValueError("live_poll_jitter 必须在 0 到 3600 秒之间。")
        self.data = data
        self.bili = bili
        self.dispatcher = dispatcher
        self.interval = interval
        self.jitter = jitter
        self.special_only = special_only
        self.cooldown_until = 0
        self._poll_lock = asyncio.Lock()
        self.last_poll_at = None
        self.last_success_at = None
        self.failures = 0
        self.rounds = 0
        self.schedule_recorder = None

    async def poll_once(self):
        async with self._poll_lock:
            if self.cooldown_remaining > 0:
                return
            self.last_poll_at = utc_now()
            uids = await (self.data.get_special_vtubers() if self.special_only else self.data.get_subscribed_uids())
            for uid in uids:
                try:
                    state = await self.bili.get_live_room_info(uid)
                    if not isinstance(state, VtuberState) or state.uid != uid or state.is_live is None:
                        raise ValueError("Invalid live observation")
                    subscriptions = await self.data.get_subscriptions_by_uid(uid)
                    if self.special_only:
                        subscriptions = [s for s in subscriptions if s.level == FollowLevel.SPECIAL]
                    if not subscriptions:
                        continue
                    changed = await self.data.save_vtuber_state(state)
                    self.last_success_at = utc_now()
                    if self.schedule_recorder is not None:
                        try:
                            await self.schedule_recorder.sync(uid)
                        except Exception:
                            self.failures += 1
                            logger.warning("Live schedule recording deferred uid=%s", uid)
                    if changed:
                        push = (self.dispatcher.push_live_started if state.is_live
                                else self.dispatcher.push_live_ended)
                        await push(state, subscriptions)
                except BiliRateLimited as exc:
                    self.failures += 1
                    self.cooldown_until = time.monotonic() + exc.retry_after
                    logger.warning("Live polling paused for %.0f seconds after rate limit", exc.retry_after)
                    break
                except Exception:
                    self.failures += 1
                    logger.exception("VTuber live poll failed uid=%s", uid)
            self.rounds += 1

    @property
    def cooldown_remaining(self):
        return max(0, self.cooldown_until - time.monotonic())

    def next_delay(self):
        return max(self.interval, self.cooldown_remaining) + random.uniform(0, self.jitter)

    async def run(self):
        while True:
            try:
                await self.poll_once()
            except Exception:
                self.failures += 1
                logger.exception("VTuber live poll round failed")
            # 请求、发送与 sleep 抛出的 CancelledError 故意向上传播。
            await asyncio.sleep(self.next_delay())
