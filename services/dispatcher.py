"""与平台无关的主动通知，按目标会话相互隔离。"""

import asyncio
import json
import math
from datetime import datetime, timedelta, timezone

from astrbot.api import logger

from ..core.models import FollowLevel, utc_now, validate_umo

LIVE_NOTIFICATION_TTL = timedelta(minutes=30)
LIVE_NOTIFICATION_FLAGS = {"live_start": "start", "live_end": "end"}
UNKNOWN_DELIVERY = "平台送达状态未知，未自动重发"


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


def resolve_notification_route(context, umo):
    """解析实际发送目标。QQ 官方群缺路由时，框架仍可能返回发送成功。"""
    platform_id, separator, session = umo.partition(":")
    if not separator:
        return umo, ""
    message_type, _, session_id = session.partition(":")
    if message_type != "GroupMessage":
        return umo, ""
    platform = context.get_platform_inst(platform_id)
    if platform is None:
        return umo, "目标消息平台尚未加载，等待重试"
    if platform.meta().name not in {"qq_official", "qq_official_webhook"}:
        return umo, ""
    # QQ 官方适配器按群 ID 缓存路由；旧版 AstrBot 由调用方去掉独立会话的前缀。
    session_id = session_id.rsplit("_", 1)[-1]
    message_ids = getattr(platform, "_session_last_message_id", {})
    scenes = getattr(platform, "_session_scene", {})
    if message_ids.get(session_id) or (
        scenes.get(session_id) == "group"
        and getattr(platform, "_allow_group_proactive_send", False)
    ):
        return f"{platform_id}:{message_type}:{session_id}", ""
    return umo, "QQ 官方群会话尚未建立路由，请在群内发一条机器人能收到的消息"


class Dispatcher:
    def __init__(
        self,
        context,
        *,
        normal_start=True,
        normal_end=True,
        special_start=True,
        special_end=False,
        send_timeout=90,
        message_factory=make_message,
        live_message_factory=make_live_message,
        image_message_factory=make_image_message,
        flags_provider=None,
        notice_image=None,
    ):
        if not math.isfinite(send_timeout) or send_timeout <= 0:
            raise ValueError("消息发送超时必须是正数。")
        self.context = context
        self.normal_start = normal_start
        self.normal_end = normal_end
        self.special_start = special_start
        self.special_end = special_end
        self.flags_provider = flags_provider
        self._last_flags = {}
        self._flags_lock = asyncio.Lock()
        self.flags_source = "启动时的配置"
        self.notice_image = notice_image
        self.send_timeout = send_timeout
        # 一轮最多包含图文发送和纯文字回退；多张图每发完一张即续期。
        self.claim_duration = timedelta(seconds=2 * send_timeout + 30)
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
        """读取通知开关；读不到时沿用最近一次成功读取的值。"""
        flags = {
            "normal_start": self.normal_start,
            "normal_end": self.normal_end,
            "special_start": self.special_start,
            "special_end": self.special_end,
        }
        async with self._flags_lock:
            latest = None
            if self.flags_provider is not None:
                try:
                    latest = await self.flags_provider()
                except Exception:
                    logger.warning("Unable to reload notification switches")
            if latest is not None:
                self._last_flags.update(
                    (name, bool(value))
                    for name, value in latest.items()
                    if name in flags
                )
                self.flags_source = "配置实时读取"
            else:
                self.flags_source = (
                    "上次有效配置" if self._last_flags else "启动时的配置"
                )
            return {**flags, **self._last_flags}

    async def flush_schedule_notifications(self, data, *, concurrency=1):
        async with self._schedule_lock:
            groups = {}
            for job in await data.pending_notifications():
                groups.setdefault(job["umo"], []).append(job)
            if not groups:
                return
            limit = asyncio.Semaphore(concurrency)

            async def deliver_group(jobs):
                async with limit:
                    for job in jobs:
                        try:
                            if not await self._deliver_job(data, job):
                                break
                        except Exception as exc:
                            self.last_error = (
                                f"通知任务处理失败（{type(exc).__name__}）"
                            )
                            logger.warning(
                                "Notification processing failed job=%s type=%s",
                                job["id"],
                                type(exc).__name__,
                            )
                            break

            await asyncio.gather(*(deliver_group(group) for group in groups.values()))

    async def run(self, data, *, interval=5):
        while True:
            try:
                await self.flush_schedule_notifications(data, concurrency=4)
            except Exception as exc:
                self.last_error = f"通知队列读取失败（{type(exc).__name__}）"
                logger.warning(
                    "Notification queue read failed type=%s", type(exc).__name__
                )
            await asyncio.sleep(interval)

    async def _deliver_job(self, data, job):
        kind = job["kind"]
        live_flag = LIVE_NOTIFICATION_FLAGS.get(kind)
        if live_flag:
            try:
                created = datetime.fromisoformat(job["created_at"])
                fresh = datetime.now(timezone.utc) - created < LIVE_NOTIFICATION_TTL
            except (TypeError, ValueError):
                fresh = False
            if not fresh:
                await data.expire_notification(job["id"])
                return True
        subscription = await data.get_subscription(job["uid"], job["umo"])
        enabled = False
        if subscription is not None and (
            not job["created_at"] or subscription.created_at <= job["created_at"]
        ):
            special = subscription.level == FollowLevel.SPECIAL
            if live_flag:
                level = "special" if special else "normal"
                enabled = (await self.notification_flags())[f"{level}_{live_flag}"]
            elif special:
                enabled = {
                    "schedule_updated": self.schedule_enabled,
                    "schedule_adjusted": self.adjustment_enabled,
                }.get(kind, False)
        if not enabled:
            await data.skip_notification(job["id"])
            return True
        destination, wait_reason = resolve_notification_route(self.context, job["umo"])
        if wait_reason:
            await data.defer_notification(job["id"], wait_reason)
            return False

        if kind == "schedule_adjusted":
            await self._prepare_images(data, job)
        job = await data.claim_notification(job["id"], self.claim_duration)
        if job is None:
            return False
        logger.info(
            "VTuber notification claimed job=%s kind=%s part=%s attempts=%s",
            job["id"],
            kind,
            job["next_part"],
            job["attempts"],
        )

        if kind == "schedule_adjusted":
            paths = notice_paths(job["image_path"])
            attempts = job["attempts"]
            for index in range(job["next_part"], len(paths)):
                delivered = await self._send(
                    destination, self.image_message_factory, paths[index], job_id=job["id"]
                )
                reason = ""
                if delivered is None:
                    reason = f"附件{UNKNOWN_DELIVERY}"
                elif delivered is False:
                    if attempts < 4:
                        await data.finish_notification(
                            job["id"], False, error="附件发送失败，等待重试"
                        )
                        return False
                    reason = "附件发送失败，已发送可用部分"
                if not await data.advance_notification(
                    job["id"], index + 1, self.claim_duration, reason
                ):
                    return False
                attempts = 0

        cover = job["image_path"] if kind == "live_start" else ""
        delivered = False
        if cover:
            delivered = await self._send(
                destination,
                self.live_message_factory,
                job["message"],
                cover,
                job_id=job["id"],
            )
        if delivered is False:
            delivered = await self._send(
                destination, self.message_factory, job["message"], job_id=job["id"]
            )
        error = {True: "", False: "平台发送失败", None: UNKNOWN_DELIVERY}[delivered]
        await data.finish_notification(job["id"], delivered, error=error)
        logger.info(
            "VTuber notification finished job=%s result=%s",
            job["id"],
            {True: "platform_returned", False: "rejected", None: "unconfirmed"}[delivered],
        )
        if delivered:
            self.last_success_at = utc_now()
        return delivered is not False

    async def _prepare_images(self, data, job):
        if not job["source_id"]:
            return
        async with self._image_lock:
            job = await data.get_notification(job["id"])
            if not job["source_id"]:
                return
            source_id = job["source_id"]
            error = ""
            try:
                paths = (
                    notice_paths(await self.notice_image(job["uid"], source_id))
                    if self.notice_image
                    else []
                )
            except Exception as exc:
                paths, error = [], "附件生成失败，已发送可用部分"
                logger.warning(
                    "Notification attachment unavailable uid=%s type=%s",
                    job["uid"],
                    type(exc).__name__,
                )
            await data.set_notification_images(job["uid"], source_id, paths, error)

    async def _send(self, umo, factory, *args, job_id=None):
        """True 表示平台调用完成，False 表示明确未发出，None 表示结果未知。"""
        validate_umo(umo)
        try:
            message = factory(*args)
        except Exception as exc:
            self.failed += 1
            logger.warning(
                "Notification message build failed job=%s type=%s",
                job_id,
                type(exc).__name__,
            )
            return False
        try:
            result = await asyncio.wait_for(
                self.context.send_message(umo, message), self.send_timeout
            )
        except Exception as exc:
            self.failed += 1
            logger.warning(
                "Notification delivery unconfirmed job=%s type=%s",
                job_id,
                type(exc).__name__,
            )
            return None
        if result is False:
            self.failed += 1
            logger.warning("Notification rejected by platform job=%s", job_id)
            return False
        self.sent += 1
        return True

    async def push_login_status(self, umo, text):
        return (await self._send(umo, self.message_factory, text)) is True
