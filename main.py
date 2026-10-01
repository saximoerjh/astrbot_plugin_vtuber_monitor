from astrbot.api import AstrBotConfig, logger
import asyncio
import time
from datetime import timedelta

from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.core.star.filter.command import GreedyStr

from .bili_client import BiliClient, BiliError
from .core.data_manager import DataManager
from .core.plugin_config import prepare_config
from .core.models import DynamicPost
from .core.schedule_models import parse_week_override
from .core.schedule_diff import schedule_diff, format_diff
from .services.adjustment_agent import AdjustmentAgent, DEFAULT_ADJUSTMENT_REGEX
from .services.subscription_service import SubscriptionService
from .services.dispatcher import Dispatcher
from .services.greeting_service import GreetingService
from .services.live_listener import LiveListener
from .services.live_schedule import LiveScheduleRecorder
from .services.dynamic_listener import DynamicListener
from .services.login_service import LoginService
from .services.schedule_parser import ScheduleParser
from .services.schedule_service import ScheduleService
from .services.schedule_discovery import ScheduleDiscovery, DEFAULT_KEYWORDS
from .services.target_service import TargetService
from .services.pinned_screenshot import PinnedScreenshot
from .services.pinned_service import PinnedService, forward_chain, message_parts
from .services.profile_service import ProfileService
from .services.schedule_image_selector import ScheduleImageSelector
from .services.schedule_report import format_schedule_report
from .services.schedule_display import format_stream, format_live_summary
from .services.schedule_renderer import ScheduleRenderer, build_schedule_view


@register("astrbot_plugin_vtuber_monitor", "hibiscus", "Bilibili VTuber 直播与周表追踪", "0.7.17")
class MyPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = prepare_config(config)
        self.bili = None
        self.subscriptions = None
        self.live_listener = None
        self.live_listener_task = None
        self.dispatcher = None
        self.dynamic_listener = None
        self.dynamic_listener_task = None
        self.login = None
        self.schedules = None
        self.live_recorder = None
        self.discovery = None
        self.targets = None
        self.adjustment = None
        self.greeting = None
        self.pinned = None
        self.profiles = None
        self.profile_task = None
        self.schedule_renderer = None
        self.schedule_watch_task = None

    async def initialize(self):
        if self.live_listener is not None:
            return
        data_dir = StarTools.get_data_dir("astrbot_plugin_vtuber_monitor")
        data = DataManager(data_dir)
        await data.initialize()
        self.targets = TargetService(data)
        self.greeting = GreetingService(
            self.context, data,
            self.config.get("multimodal_provider_id", ""))
        screenshot = PinnedScreenshot(self.config.get("screenshot_browser_channel", "auto"))
        self.schedule_renderer = ScheduleRenderer(
            data_dir, channel=self.config.get("screenshot_browser_channel", "auto"))
        self.dispatcher = Dispatcher(
            self.context,
            normal_start=self.config.get("normal_live_start_push", True),
            normal_end=self.config.get("normal_live_end_push", True),
            special_start=self.config.get("special_live_start_push", True),
            special_end=self.config.get("special_live_end_push", False),
        )
        listener = LiveListener(data, None, self.dispatcher,
                                float(self.config.get("live_poll_interval", 120)),
                                float(self.config.get("live_poll_jitter", 60)),
                                special_only=not self.config.get("enable_live_polling", False))
        dynamic_listener = DynamicListener(data, None,
                                          float(self.config.get("dynamic_poll_interval", 300)))
        self.dispatcher.schedule_enabled = bool(self.config.get("enable_schedule_push", False))
        self.dispatcher.adjustment_enabled = bool(self.config.get("enable_adjustment_push", False))
        self.live_recorder = LiveScheduleRecorder(
            data, unfulfilled_after=timedelta(hours=float(self.config.get("unfulfilled_after_hours", 2))))
        self.schedules = ScheduleService(data, schedule_push=self.dispatcher.schedule_enabled,
                                        adjustment_push=self.dispatcher.adjustment_enabled,
                                        reconciler=self.live_recorder)
        self.adjustment = AdjustmentAgent(
            self.context, self.schedules,
            self.config.get("multimodal_provider_id", ""),
            self.config.get("adjustment_regex", DEFAULT_ADJUSTMENT_REGEX))
        if (self.config.get("enable_adjustment_processing", False) or
                self.config.get("auto_adjustment_with_schedule", True)):
            dynamic_listener.adjustment = self.adjustment
        dynamic_listener.require_schedule = True
        discover = bool(self.config.get("auto_discover_schedule", False))
        # 合并开关：打开时轮询全部特别关注并识别周表图；关闭时只轮询已有本周
        # 周表的主播，够自动调播用，也避免给消费不掉的队列塞任务。
        dynamic_listener.eligible_only = not discover
        dynamic_listener.dispatcher = self.dispatcher
        self.discovery = ScheduleDiscovery(
            data, None, ScheduleParser(self.context, self.config.get("multimodal_provider_id", "")),
            self.schedules, self.config.get("schedule_keywords", list(DEFAULT_KEYWORDS)))
        self.bili = BiliClient(
            timeout=float(self.config.get("request_timeout", 10)),
            max_retry=self.config.get("max_retry", 2),
            sessdata=self.config.get("bilibili_sessdata", ""),
        )
        try:
            saved_credentials = await data.get_credentials()
            if saved_credentials:
                self.bili.set_credentials(saved_credentials)
        except Exception:
            await self.bili.close()
            raise
        self.login = LoginService(data, self.bili, self.dispatcher.push_login_status)
        self.profiles = ProfileService(
            data, self.bili, channel=self.config.get("screenshot_browser_channel", "auto"))
        self.pinned = PinnedService(self.bili, screenshot, ScheduleImageSelector(data, self.bili, self.discovery.parser))
        self.discovery.bili = self.bili
        if discover:
            dynamic_listener.discovery = self.discovery
        self.subscriptions = SubscriptionService(data, self.bili, dynamic_listener.discovery)
        listener.bili = self.bili
        listener.schedule_recorder = self.live_recorder
        self.live_listener = listener
        dynamic_listener.bili = self.bili
        self.dynamic_listener = dynamic_listener
        self._start_tasks()

    def _start_tasks(self):
        if self.config.get("enable_midnight_schedule_check", True) and (
            self.schedule_watch_task is None or self.schedule_watch_task.done()
        ):
            self.schedule_watch_task = asyncio.create_task(
                self.discovery.watch.run(), name="vtuber-monitor-schedule-midnight")
        if (self.config.get("enable_live_polling", False) or
                self.config.get("auto_special_live", True)) and (
            self.live_listener_task is None or self.live_listener_task.done()
        ):
            self.live_listener_task = asyncio.create_task(
                self.live_listener.run(), name="vtuber-monitor-live")
        if (self.config.get("auto_discover_schedule", False) or
                self.config.get("auto_adjustment_with_schedule", True)) and (
            self.dynamic_listener_task is None or self.dynamic_listener_task.done()
        ):
            self.dynamic_listener_task = asyncio.create_task(
                self.dynamic_listener.run(), name="vtuber-monitor-dynamic")
        if self.profile_task is None or self.profile_task.done():
            # 头像与空间头图每天零点刷新，供周表图片使用。
            self.profile_task = asyncio.create_task(
                self.profiles.run(), name="vtuber-monitor-profiles")

    @filter.command("vt_ping")
    async def vt_ping(self, event: AstrMessageEvent):
        """检查插件是否已加载。"""
        yield event.plain_result("VTuber Monitor OK")

    async def _resolve_target(self, event, target):
        return await self.targets.resolve(target, event.unified_msg_origin, event.get_sender_id())

    @filter.command("vt_alias")
    async def vt_alias(self, event: AstrMessageEvent, target: str = "", alias: str = ""):
        """为已订阅主播添加个人别名，保留已有别名。"""
        if not target or not alias:
            yield event.plain_result("用法：/vt_alias <UID或旧别名> <新别名>")
            return
        try:
            uid, display = await self.targets.set_alias(target, alias, event.unified_msg_origin, event.get_sender_id())
            yield event.plain_result(f"已设置别名：{display} → {uid}（已有别名保留）。可使用 /vt_schedule {display}。")
        except ValueError as exc:
            yield event.plain_result(str(exc))

    @filter.command("vt_alias_del")
    async def vt_alias_del(self, event: AstrMessageEvent, target: str = "", alias: str = ""):
        """删除指定别名：/vt_alias_del <别名> 或 <UID> <别名>，不取消订阅。"""
        try:
            removed = await self.targets.remove_alias(target, event.unified_msg_origin, event.get_sender_id(), alias)
            yield event.plain_result("指定别名已删除，其他别名和订阅保持不变。" if removed else "未找到对应别名。")
        except ValueError as exc:
            yield event.plain_result(str(exc))

    @filter.command("bili_login", alias={"vt_login"})
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def bili_login(self, event: AstrMessageEvent):
        """管理员私聊扫码登录 Bilibili；已有凭据在成功后替换。"""
        if not event.is_admin() or event.get_group_id():
            yield event.plain_result("请由 AstrBot 管理员在私聊中使用 /bili_login。")
            return
        try:
            async def send_image(path):
                await event.send(event.image_result(path))
            await self.login.start(event.unified_msg_origin, send_image)
            text = "请用 Bilibili App 扫描二维码并确认登录，二维码约 3 分钟内有效。"
        except BiliError as exc:
            text = str(exc)
        except Exception:
            logger.warning("Unable to start Bilibili QR login")
            text = "无法生成或发送登录二维码，请确认插件依赖已安装并稍后重试。"
        yield event.plain_result(text)

    @filter.command("vt_sub")
    async def vt_sub(self, event: AstrMessageEvent, uid: str = "", level: str = "normal"):
        """订阅主播：/vt_sub <UID> [normal|special]。"""
        if not uid:
            yield event.plain_result("用法：/vt_sub <UID> [normal|special]")
            return
        try:
            uid = await self._resolve_target(event, uid)
            state = await self.subscriptions.subscribe(uid, event.unified_msg_origin, level, user_id=event.get_sender_id())
            running = self.live_listener_task is not None and not self.live_listener_task.done()
            status = "直播监听运行中。" if running else "后台直播监听未启用。"
            text = f"已订阅 {state.name}（UID {state.uid}，{level}）。{status}"
            if level == "special":
                text += "特别关注已保存，动态与周表处理状态请查看 /vt_status。"
        except (ValueError, BiliError) as exc:
            text = str(exc)
        except Exception:
            logger.exception("VTuber subscription failed")
            text = "保存订阅失败，请检查插件日志。"
        yield event.plain_result(text)

    @filter.command("vt_unsub")
    async def vt_unsub(self, event: AstrMessageEvent, uid: str = ""):
        """取消当前会话的主播订阅。"""
        try:
            uid = await self._resolve_target(event, uid)
            removed = await self.subscriptions.unsubscribe(uid, event.unified_msg_origin)
            text = "已取消订阅。" if removed else "当前会话未订阅该主播。"
        except ValueError as exc:
            text = str(exc)
        except Exception:
            logger.exception("VTuber unsubscribe failed")
            text = "取消订阅失败，请检查插件日志。"
        yield event.plain_result(text)

    @filter.command("vt_follow_level")
    async def vt_follow_level(self, event: AstrMessageEvent, uid: str = "", level: str = ""):
        """修改当前会话已有订阅的关注级别。"""
        try:
            uid = await self._resolve_target(event, uid)
            await self.subscriptions.change_level(uid, event.unified_msg_origin, level, user_id=event.get_sender_id())
            text = f"关注级别已更新为 {level}。动态轮询状态请查看 /vt_status。"
        except (ValueError, BiliError) as exc:
            text = str(exc)
        except Exception:
            logger.exception("VTuber follow level update failed")
            text = "修改关注级别失败，请检查插件日志。"
        yield event.plain_result(text)

    @filter.command("vt_latest")
    async def vt_latest(self, event: AstrMessageEvent, uid: str = ""):
        """查询最新动态，不修改检查点。"""
        try:
            uid = await self._resolve_target(event, uid)
            posts = await self.bili.get_latest_dynamics(uid)
            if not posts:
                text = "未查询到动态。"
            else:
                post = posts[-1]
                text = f"动态 {post.id}\n{post.text[:1000] or '（无正文）'}\nhttps://t.bilibili.com/{post.id}"
        except (ValueError, BiliError) as exc:
            text = str(exc)
        yield event.plain_result(text)

    @filter.command("vt_pinned")
    async def vt_pinned(self, event: AstrMessageEvent, uid: str = ""):
        """合并转发动态完整截图及识别为周表的图片，不附带动态文字。"""
        try:
            uid = await self._resolve_target(event, uid)
            parts = await self.pinned.build(uid)
            if parts is None:
                yield event.plain_result("当前动态页未发现置顶动态。")
                return
            if event.get_platform_name() == "aiocqhttp":
                try:
                    await event.send(event.chain_result(forward_chain(parts, event.get_self_id())))
                    return
                except Exception:
                    logger.warning("Pinned forward delivery failed; falling back to ordered messages")
                    yield event.plain_result("合并转发发送失败，改为按顺序发送。")
            for content in message_parts(parts):
                yield event.chain_result(content)
        except (ValueError, BiliError) as exc:
            yield event.plain_result(str(exc))
        except Exception:
            logger.warning("Pinned dynamic presentation failed")
            yield event.plain_result("置顶动态处理失败，请稍后重试或查看 /vt_latest。")

    @filter.command("vt_parse_schedule")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def vt_parse_schedule(self, event: AstrMessageEvent, uid: str = "", week_start: str = ""):
        """管理员解析周表，可指定本周/上周/下周或 YYYY-MM-DD。"""
        target, resolved_uid = uid, None
        try:
            override = parse_week_override(week_start)
            uid = await self._resolve_target(event, uid)
            resolved_uid = uid
            options = {"force": True}
            if override:
                options["week_start"] = override.isoformat()
            records = await self.discovery.scan(uid, **options)
            yield event.plain_result(format_schedule_report(
                records, uid, target=target, week_start=override.isoformat() if override else ""))
        except (ValueError, BiliError) as exc:
            link = f"\n查看动态页：https://space.bilibili.com/{resolved_uid}/dynamic" if resolved_uid else ""
            yield event.plain_result(str(exc) + link)
        except Exception:
            logger.warning("Manual schedule scan failed")
            link = f"\n查看动态页：https://space.bilibili.com/{resolved_uid}/dynamic" if resolved_uid else ""
            yield event.plain_result("周表处理失败，请稍后重试。" + link)

    @filter.command("vt_schedule")
    async def vt_schedule(self, event: AstrMessageEvent, uid: str = "", week_start: str = ""):
        """查看缓存周表：/vt_schedule <UID或别名> [本周|上周|下周|YYYY-MM-DD]。"""
        try:
            # 在异步解析目标之前先固定相对日期，避免跨零点漂移。
            week = parse_week_override(week_start, limit_range=False)
            week_start = week.isoformat() if week else ""
            uid = await self._resolve_target(event, uid)
            schedule = await self.schedules.get_weekly_schedule(uid, week_start)
            if schedule is None:
                text = (f"未找到 {week_start} 起始的周表。" if week_start else "未找到本周周表。")
                text += "可用 /vt_schedule_history <UID> 查看缓存周次，或由管理员运行 /vt_parse_schedule <UID>。"
                yield event.plain_result(text)
                return
            summary = await self._live_summary(uid, schedule["week_start"])
            if self.config.get("schedule_image_enabled", True):
                try:
                    view = build_schedule_view(schedule, uid=uid, summary=summary,
                                               display_name=await self._display_name(uid, event),
                                               banner=await self.profiles.banner(uid))
                    path = await self.schedule_renderer.render(view)
                except Exception:
                    # 图片只是展示形式，任何渲染问题都不应让周表查询失败。
                    logger.exception("VTuber schedule image rendering failed")
                else:
                    yield event.image_result(path)
                    return
            text = f"周表起始日：{schedule['week_start']}；修订 {schedule.get('revision', 0)}\n"
            text += "\n\n".join(format_stream(p) for p in schedule["streams"])
            text += "\n\n" + format_live_summary(summary)
            yield event.plain_result(text)
        except ValueError as exc:
            yield event.plain_result(str(exc))

    async def _live_summary(self, uid, week_start):
        """已记录与缺失的观测统计；失败时返回 None，由展示层标注统计失败。"""
        try:
            return await self.live_recorder.week_summary(uid, week_start)
        except Exception:
            logger.exception("VTuber live summary failed")
            return None

    async def _display_name(self, uid, event):
        """图片标题用主播原名（订阅时记录的 B 站昵称），取不到时退化为 UID。

        别名是给命令用的，图上展示原名更好认人，所以这里不优先取别名。
        """
        try:
            rows = await self.targets.data.get_target_mappings(event.unified_msg_origin, event.get_sender_id())
            row = next((item for item in rows if item["uid"] == uid), None)
        except Exception:
            return f"UID {uid}"
        if row is None:
            return f"UID {uid}"
        return (row.get("name") or "").strip() or f"UID {uid}"

    @filter.command("vt_schedule_history")
    async def vt_schedule_history(self, event: AstrMessageEvent, uid: str = ""):
        """列出缓存的周表；指定日期查询该周最新存档版本。"""
        try:
            uid = await self._resolve_target(event, uid)
            weeks = await self.schedules.get_schedule_history(uid)
            text = "\n".join(f"{w['week_start']}（{w['versions']} 个存档版本）" for w in weeks)
            yield event.plain_result(text or "暂无历史周表缓存。")
        except ValueError as exc:
            yield event.plain_result(str(exc))

    @filter.command("vt_adjust_test")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def vt_adjust_test(self, event: AstrMessageEvent, uid: str = "", *, text: GreedyStr):
        """管理员调播预演：/vt_adjust_test <UID或别名> <动态正文>，不保存、不推送。"""
        if not event.is_admin():
            yield event.plain_result("仅管理员可以调用调播模型测试。")
            return
        if not uid or not text:
            yield event.plain_result("用法：/vt_adjust_test <UID或别名> <动态正文>（仅预演，不修改周表）")
            return
        try:
            uid = await self._resolve_target(event, uid)
            result = await self.adjustment.process(DynamicPost(uid, str(time.time_ns()), text, int(time.time())), dry_run=True)
            yield event.plain_result("调播预演（未保存、未推送）\n" + (format_diff(result["changes"]) or result["reason"]))
        except ValueError as exc:
            yield event.plain_result(str(exc))
        except Exception:
            logger.warning("Adjustment preview failed")
            yield event.plain_result("调播预演失败，未修改周表，请检查模型配置。")

    @filter.command("vt_revisions")
    async def vt_revisions(self, event: AstrMessageEvent, uid: str = ""):
        """查看最近十次周表修订及变更前后。"""
        try:
            uid = await self._resolve_target(event, uid)
            rows = await self.schedules.data.get_schedule_revisions(uid)
            text = "\n\n".join(f"#{r['id']} {r['created_at']} {r['reason'][:200]}\n"
                                 f"来源动态：{r['source_dynamic_id'] or '手动'}\n"
                                 + format_diff(schedule_diff(r['old_value'], r['new_value'])) for r in rows)
            yield event.plain_result(text[:10000] or "暂无周表修订记录。")
        except ValueError as exc:
            yield event.plain_result(str(exc))

    @filter.command("vt_retry_adjust")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def vt_retry_adjust(self, event: AstrMessageEvent, uid: str = "", dynamic_id: str = ""):
        """管理员修复模型/周表后，重试一条已失败的调播动态。"""
        if not event.is_admin():
            yield event.plain_result("仅管理员可以重试调播任务。")
            return
        try:
            uid = await self._resolve_target(event, uid)
            changed = await self.schedules.retry_adjustment(uid, dynamic_id)
            yield event.plain_result("已排入重试队列，需启用动态轮询和自动调播。" if changed else "未找到对应的失败任务。")
        except ValueError as exc:
            yield event.plain_result(str(exc))

    @filter.command("vt_list")
    async def vt_list(self, event: AstrMessageEvent):
        """列出当前会话的订阅。"""
        try:
            text = await self.targets.format_list(event.unified_msg_origin, event.get_sender_id())
        except Exception:
            logger.exception("VTuber list failed")
            text = "读取订阅失败，请检查插件日志。"
        yield event.plain_result(text)

    @filter.command("vt_live")
    async def vt_live(self, event: AstrMessageEvent, uid: str = ""):
        """查询直播状态，不修改监听状态、不触发通知。"""
        try:
            uid = await self._resolve_target(event, uid)
            state = await self.bili.get_live_room_info(uid)
            status = "直播中" if state.is_live else "未开播"
            text = f"{state.name}（UID {state.uid}）：{status}，房间 {state.room_id}。"
        except (ValueError, BiliError) as exc:
            text = str(exc)
        yield event.plain_result(text)

    @filter.command("vt_push_test")
    async def vt_push_test(self, event: AstrMessageEvent):
        """向当前会话发送一条主动消息，用于人工联调。"""
        success = await self.dispatcher.push_test(event.unified_msg_origin)
        yield event.plain_result("推送请求已提交，请确认收到测试消息。" if success else "推送失败，请检查插件日志。")

    @filter.command("vt_4016")
    async def vt_4016(self, event: AstrMessageEvent):
        """看看现在哪个国家是凌晨四点。"""
        try:
            text = await self.greeting.build()
        except Exception:
            logger.exception("VTuber 4016 greeting failed")
            text = "凌晨四点查询失败，请稍后重试。"
        yield event.plain_result(text)

    @filter.command("vt_status")
    async def vt_status(self, event: AstrMessageEvent):
        """查看监听状态和本次运行的计数。"""
        listener = self.live_listener
        if listener is None:
            yield event.plain_result("直播监听尚未初始化。")
            return
        running = self.live_listener_task is not None and not self.live_listener_task.done()
        work = await self.targets.data.get_work_status()
        watch_errors = []
        for uid in await self.targets.data.get_subscribed_uids():
            tracked = await self.targets.data.get_schedule_tracking(uid)
            if tracked and tracked.get("error"):
                watch_errors.append(f"UID {uid}：{tracked['error']}\nhttps://t.bilibili.com/{tracked['dynamic_id']}")
        yield event.plain_result(
            f"VTuber Monitor 0.7.17\n直播监听：{'运行中' if running else '已停止/未启用'}\n"
            f"轮询间隔：{listener.interval:g}–{listener.interval + listener.jitter:g} 秒；已完成 {listener.rounds} 轮\n"
            f"风控冷却剩余：{listener.cooldown_remaining:.0f} 秒\n"
            f"直播监听范围：{'特别关注' if listener.special_only else '全部订阅'}\n"
            f"最近轮询：{listener.last_poll_at or '无'}\n"
            f"最近成功：{listener.last_success_at or '无'}\n"
            f"监听错误：{listener.failures}；发送成功：{self.dispatcher.sent}；发送失败：{self.dispatcher.failed}"
            f"\nSESSDATA：{'已配置（有效性未确认）' if self.bili.has_credentials else '未配置'}"
            f"\n动态监听：{'运行中' if self.dynamic_listener_task and not self.dynamic_listener_task.done() else '未运行'}"
            f"；完成 {self.dynamic_listener.rounds} 轮，新增 {self.dynamic_listener.received} 条，失败 {self.dynamic_listener.failures} 次"
            f"\n自动周表处理：{'已开启（依赖动态轮询）' if self.dynamic_listener.discovery else '关闭'}"
            f"；视觉模型：{'已指定' if self.discovery.parser.provider_id else '未指定'}"
            f"\n零点周表检查：{'运行中（北京时间，需先手动解析建立基准）' if self.schedule_watch_task and not self.schedule_watch_task.done() else '关闭'}"
            f"\n空间资料：{'每天零点刷新' if self.profile_task and not self.profile_task.done() else '未运行'}"
            f"\n自动调播：{'有本周周表时自动处理' if self.dynamic_listener.adjustment else '关闭'}；模型：{'已指定' if self.adjustment.provider_id else '未指定'}"
            f"\n周表推送：{'开启' if self.dispatcher.schedule_enabled else '关闭'}；调播推送：{'开启' if self.dispatcher.adjustment_enabled else '关闭'}（依赖动态轮询）"
            f"\n调播任务：{work['adjustment_jobs']}；周表通知：{work['schedule_outbox']}"
            f"\n实际直播：待落位 {work['live_sessions']['pending']} 场"
            f"；未记录 {work['live_sessions']['skipped']} 场（起点未知）"
            f"；已记录 {work['live_sessions']['recorded']} 场"
            + ("\n周表检查待处理：\n" + "\n".join(watch_errors[:5]) if watch_errors else "")
        )

    async def terminate(self):
        try:
            if self.pinned is not None:
                await self.pinned.close()
            if self.login is not None:
                await self.login.close()
            tasks = [task for task in (self.live_listener_task, self.dynamic_listener_task,
                                       self.schedule_watch_task, self.profile_task)
                     if task is not None]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self.live_listener_task = None
            self.dynamic_listener_task = None
            self.schedule_watch_task = None
            self.profile_task = None
            if self.bili is not None:
                await self.bili.close()
            self.live_listener = None
            self.dynamic_listener = None
