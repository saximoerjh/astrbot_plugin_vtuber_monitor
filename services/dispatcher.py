"""与平台无关的主动通知，按目标会话相互隔离。"""
import asyncio
import json
import math

from astrbot.api import logger

from ..core.models import FollowLevel, validate_umo


def make_message(text):
    from astrbot.api.event import MessageChain

    return MessageChain().message(text)


def make_live_message(text, cover):
    return make_message(text).url_image(cover)


def make_image_message(path):
    """本地图片消息：调播通知附带的动态截图。"""
    from astrbot.api.event import MessageChain

    return MessageChain().file_image(path)


def notice_paths(value):
    """调播通知附带的图片：一张是裸路径，多张是 JSON 数组（旧数据仍是裸路径）。"""
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return [str(path) for path in value if path]
    if str(value).startswith("["):
        try:
            return [str(path) for path in json.loads(value) if path]
        except (TypeError, ValueError):
            return []
    return [str(value)]


class Dispatcher:
    def __init__(self, context, *, normal_start=True, normal_end=True,
                 special_start=True, special_end=False, send_timeout=15,
                 message_factory=make_message, live_message_factory=make_live_message,
                 image_message_factory=make_image_message, flags_provider=None):
        if not math.isfinite(send_timeout) or send_timeout <= 0:
            raise ValueError("消息发送超时必须是正数。")
        self.context = context
        self.normal_start = normal_start
        self.normal_end = normal_end
        self.special_start = special_start
        self.special_end = special_end
        # 传入后每次发送都重读一遍开关，改完配置不用重载插件也能立刻生效。
        self.flags_provider = flags_provider
        self.send_timeout = send_timeout
        self.message_factory = message_factory
        self.live_message_factory = live_message_factory
        self.image_message_factory = image_message_factory
        self.sent = 0
        self.failed = 0
        self._schedule_lock = asyncio.Lock()
        self.schedule_enabled = False
        self.adjustment_enabled = False

    async def notification_flags(self):
        """当前生效的四个通知开关；重读失败就退回启动时的值。"""
        flags = {"normal_start": self.normal_start, "normal_end": self.normal_end,
                 "special_start": self.special_start, "special_end": self.special_end}
        if self.flags_provider is None:
            return flags
        try:
            latest = await self.flags_provider()
        except Exception:
            logger.warning("Unable to reload notification switches; using startup values")
            return flags
        for name, value in (latest or {}).items():
            if name in flags:
                flags[name] = bool(value)
        return flags

    async def push_schedule_updated(self, umo, text):
        return await self._send(umo, text)

    async def push_schedule_adjusted(self, umo, text, image_path=""):
        """调播通知分两条发送：先动态原图（可能多张），再文字结果。"""
        for path in notice_paths(image_path):
            await self._send_local_image(umo, path)
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
                if job["kind"] == "schedule_updated":
                    delivered = await self.push_schedule_updated(job["umo"], job["message"])
                else:
                    delivered = await self.push_schedule_adjusted(
                        job["umo"], job["message"], job.get("image_path", ""))
                await data.finish_notification(job["id"], delivered)

    async def _send_local_image(self, umo, path):
        """发送本地截图；失败只记录，不算整条通知失败。"""
        validate_umo(umo)
        try:
            result = await asyncio.wait_for(
                self.context.send_message(umo, self.image_message_factory(path)), self.send_timeout)
            if result is False:
                raise RuntimeError("Platform rejected the notification image")
        except Exception:
            self.failed += 1
            logger.exception("VTuber notification image delivery failed")
            return False
        self.sent += 1
        return True

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
        flags = await self.notification_flags()
        destinations = {
            s.umo for s in subscriptions if s.uid == state.uid and (
                (flags["special_start"] if started else flags["special_end"])
                if s.level == FollowLevel.SPECIAL
                else (flags["normal_start"] if started else flags["normal_end"]))
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

    async def push_login_status(self, umo, text):
        return await self._send(umo, text)
