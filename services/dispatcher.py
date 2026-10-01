"""与平台无关的主动通知，按目标会话相互隔离。"""
import asyncio
import logging
import math

from ..core.models import FollowLevel, validate_umo

logger = logging.getLogger(__name__)


def make_message(text):
    from astrbot.api.event import MessageChain

    return MessageChain().message(text)


def make_live_message(text, cover):
    return make_message(text).url_image(cover)


class Dispatcher:
    def __init__(self, context, *, normal_start=True, normal_end=True,
                 special_start=True, special_end=False,
                 send_timeout=15, message_factory=make_message, live_message_factory=make_live_message):
        if not math.isfinite(send_timeout) or send_timeout <= 0:
            raise ValueError("消息发送超时必须是正数。")
        self.context = context
        self.normal_start = normal_start
        self.normal_end = normal_end
        self.special_start = special_start
        self.special_end = special_end
        self.send_timeout = send_timeout
        self.message_factory = message_factory
        self.live_message_factory = live_message_factory
        self.sent = 0
        self.failed = 0
        self._schedule_lock = asyncio.Lock()
        self.schedule_enabled = False
        self.adjustment_enabled = False

    async def push_schedule_updated(self, umo, text):
        return await self._send(umo, text)

    async def push_schedule_adjusted(self, umo, text):
        return await self._send(umo, text)

    async def flush_schedule_notifications(self, data):
        async with self._schedule_lock:
            for job in await data.pending_notifications():
                enabled = self.schedule_enabled if job["kind"] == "schedule_updated" else self.adjustment_enabled
                subscription = await data.get_subscription(job["uid"], job["umo"])
                if not enabled or subscription is None or subscription.level != FollowLevel.SPECIAL:
                    # 直接丢弃，这样以后重新订阅也不会发出过期通知。
                    await data.finish_notification(job["id"], True)
                    continue
                sender = self.push_schedule_updated if job["kind"] == "schedule_updated" else self.push_schedule_adjusted
                await data.finish_notification(job["id"], await sender(job["umo"], job["message"]))

    async def _send(self, umo, text, cover=""):
        validate_umo(umo)
        try:
            result = await asyncio.wait_for(
                self.context.send_message(umo, self.live_message_factory(text, cover)
                                          if cover else self.message_factory(text)), self.send_timeout)
            if result is False:
                raise RuntimeError("Platform rejected the notification")
        except Exception:
            self.failed += 1
            logger.exception("VTuber notification delivery failed")
            return False
        self.sent += 1
        return True

    async def _push(self, state, subscriptions, started):
        action = "开播了" if started else "下播了"
        text = f"{state.name}（UID {state.uid}）{action}。"
        if started and state.live_title:
            text += f"\n直播标题：{state.live_title}"
        if state.room_id:
            text += f"\nhttps://live.bilibili.com/{state.room_id}"
        destinations = {
            s.umo for s in subscriptions if s.uid == state.uid and (
                (self.special_start if started else self.special_end)
                if s.level == FollowLevel.SPECIAL
                else (self.normal_start if started else self.normal_end))
        }
        for umo in sorted(destinations):
            cover = state.live_cover if started else ""
            if not await self._send(umo, text, cover) and cover:
                # 平台拒收图片时，仍可退回发送直播链接与标题。
                await self._send(umo, text)

    async def push_live_started(self, state, subscriptions):
        await self._push(state, subscriptions, True)

    async def push_live_ended(self, state, subscriptions):
        await self._push(state, subscriptions, False)

    async def push_test(self, umo):
        return await self._send(umo, "VTuber Monitor 主动推送测试。")

    async def push_login_status(self, umo, text):
        return await self._send(umo, text)
