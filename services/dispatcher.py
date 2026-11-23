"""与平台无关的主动通知，按目标会话相互隔离。"""
import asyncio
import json
import math

from astrbot.api import logger

from ..core.models import FollowLevel, utc_now, validate_umo


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
                 image_message_factory=make_image_message, flags_provider=None, notice_image=None):
        if not math.isfinite(send_timeout) or send_timeout <= 0:
            raise ValueError("消息发送超时必须是正数。")
        self.context = context
        self.normal_start = normal_start
        self.normal_end = normal_end
        self.special_start = special_start
        self.special_end = special_end
        # 传入后每次发送都重读一遍开关，改完配置不用重载插件也能立刻生效。
        self.flags_provider = flags_provider
        self.notice_image = notice_image
        self.send_timeout = send_timeout
        self.message_factory = message_factory
        self.live_message_factory = live_message_factory
        self.image_message_factory = image_message_factory
        self.sent = 0
        self.failed = 0
        self._schedule_lock = asyncio.Lock()
        self._image_lock = asyncio.Lock()
        self.last_success_at = None
        self.last_error = ""
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

    async def flush_schedule_notifications(self, data, *, concurrency=1):
        async with self._schedule_lock:
            groups = {}
            for job in await data.pending_notifications():
                groups.setdefault(job["umo"], []).append(job)
            if not groups:
                return
            flags = await self.notification_flags()
            limit = asyncio.Semaphore(concurrency)

            async def deliver_group(jobs):
                async with limit:
                    for job in jobs:
                        try:
                            if not await self._deliver_job(data, job, flags):
                                break
                        except Exception as exc:
                            self.last_error = f"通知任务处理失败（{type(exc).__name__}）"
                            logger.warning("Notification processing failed job=%s type=%s", job["id"], type(exc).__name__)
                            break

            await asyncio.gather(*(deliver_group(group) for group in groups.values()))

    async def run(self, data, *, interval=5):
        while True:
            try:
                await self.flush_schedule_notifications(data, concurrency=4)
            except Exception as exc:
                self.last_error = f"通知队列读取失败（{type(exc).__name__}）"
                logger.warning("Notification queue read failed type=%s", type(exc).__name__)
            await asyncio.sleep(interval)

    async def _deliver_job(self, data, job, flags):
        kind = job["kind"]
        subscription = await data.get_subscription(job["uid"], job["umo"])
        enabled = False
        if subscription is not None and (not job["created_at"] or subscription.created_at <= job["created_at"]):
            special = subscription.level == FollowLevel.SPECIAL
            if kind in ("live_start", "live_end"):
                level = "special" if special else "normal"
                enabled = flags[f"{level}_{kind.removeprefix('live_')}"]
            elif special:
                enabled = {"schedule_updated": self.schedule_enabled,
                           "schedule_adjusted": self.adjustment_enabled}.get(kind, False)
        if not enabled:
            await data.skip_notification(job["id"])
            return True

        if kind == "schedule_adjusted":
            job = await self._prepare_images(data, job)
            paths = notice_paths(job["image_path"])
            attempts = job["attempts"]
            for index in range(job["next_part"], len(paths)):
                if not await self._send_local_image(job["umo"], paths[index]):
                    if attempts < 4:
                        await data.finish_notification(job["id"], False, error="附件发送失败，等待重试")
                        return False
                    await data.advance_notification(job["id"], index + 1, "附件发送失败，已发送可用部分")
                else:
                    await data.advance_notification(job["id"], index + 1)
                attempts = 0

        cover = job["image_path"] if kind == "live_start" else ""
        delivered = await self._send(job["umo"], job["message"], cover)
        if not delivered and cover:
            delivered = await self._send(job["umo"], job["message"])
        await data.finish_notification(job["id"], delivered, error="" if delivered else "平台发送失败或超时")
        if delivered:
            self.last_success_at = utc_now()
        return delivered

    async def _prepare_images(self, data, job):
        if not job["source_id"]:
            return job
        async with self._image_lock:
            job = await data.get_notification(job["id"])
            if not job["source_id"]:
                return job
            source_id = job["source_id"]
            error = ""
            try:
                paths = notice_paths(await self.notice_image(job["uid"], source_id)) if self.notice_image else []
            except Exception as exc:
                paths, error = [], "附件生成失败，已发送可用部分"
                logger.warning("Notification attachment unavailable uid=%s type=%s", job["uid"], type(exc).__name__)
            await data.set_notification_images(job["uid"], source_id, paths, error)
            return await data.get_notification(job["id"])

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

    @staticmethod
    def live_message(state, started):
        action = "开播了" if started else "下播了"
        text = f"{state.name}（UID {state.uid}）{action}。"
        if started and state.live_title:
            text += f"\n直播标题：{state.live_title}"
        if state.room_id:
            text += f"\nhttps://live.bilibili.com/{state.room_id}"
        return text, state.live_cover if started else ""

    async def _push(self, state, subscriptions, started):
        text, cover = self.live_message(state, started)
        flags = await self.notification_flags()
        destinations = {
            s.umo for s in subscriptions if s.uid == state.uid and (
                (flags["special_start"] if started else flags["special_end"])
                if s.level == FollowLevel.SPECIAL
                else (flags["normal_start"] if started else flags["normal_end"]))
        }
        for umo in sorted(destinations):
            if not await self._send(umo, text, cover) and cover:
                # 平台拒收图片时，仍可退回发送直播链接与标题。
                await self._send(umo, text)

    async def push_live_started(self, state, subscriptions):
        await self._push(state, subscriptions, True)

    async def push_live_ended(self, state, subscriptions):
        await self._push(state, subscriptions, False)

    async def push_login_status(self, umo, text):
        return await self._send(umo, text)
