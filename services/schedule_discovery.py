"""置顶动态、图片与关键词的候选发现；所有落盘都归 DataManager。"""
import asyncio
import hashlib
from datetime import timedelta
from pathlib import Path
from dataclasses import replace

from astrbot.api import logger

from ..core.models import utc_now, validate_uid
from ..core.schedule_models import china_today, explicit_week_hint, parse_week_override
from .schedule_parser import ScheduleParseError
from .schedule_watch import DEFAULT_SCAN_TIMES, ScheduleWatch

DEFAULT_KEYWORDS = ("周表", "本周", "schedule", "直播安排", "本周安排")


class ScheduleDiscovery:
    def __init__(self, data, bili, parser, schedules, keywords=DEFAULT_KEYWORDS, *,
                 scan_times=None, auto_parse_normal=False):
        if not isinstance(keywords, (list, tuple)) or any(not isinstance(k, str) or not k.strip() for k in keywords):
            raise ValueError("schedule_keywords 必须为非空字符串列表。")
        self.keywords = tuple(k.casefold() for k in keywords)
        self.data, self.bili, self.parser, self.schedules = data, bili, parser, schedules
        self._lock = asyncio.Lock()
        # 空列表是"关闭定时检查"的合法配置，不能当成未设置。
        self.watch = ScheduleWatch(self,
                                   scan_times=DEFAULT_SCAN_TIMES if scan_times is None else scan_times,
                                   auto_parse_normal=auto_parse_normal)

    def is_candidate(self, post):
        return bool(post.images) and (post.is_pinned or any(k in post.text.casefold() for k in self.keywords))

    async def scan(self, uid, posts=None, force=False, week_start=""):
        uid = validate_uid(uid)
        override = parse_week_override(week_start)
        week_start = override.isoformat() if override else ""
        async with self._lock:
            if posts is None:
                posts = await self.bili.get_latest_dynamics(uid)
            self.data._validate_posts(uid, posts)
            previous = {(r["dynamic_id"], r["image_url"]): r for r in await self.data.get_schedule_candidates(uid)}
            results = []
            remembered = False
            # 扫描整页，包括动态水位线之外的旧置顶动态。
            for post in sorted(posts, key=lambda p: int(p.id), reverse=True):
                if not self.is_candidate(post):
                    continue
                for image_index, url in enumerate(post.images, 1):
                    old = previous.get((post.id, url))
                    fingerprint = hashlib.sha256(post.text.encode()).hexdigest()
                    if (not force and old and old.get("error_code") == "not_schedule"
                            and old.get("text_fingerprint") == fingerprint):
                        # 已经判定过"不是周表"，正文也没变：不再下载、不再花模型。
                        # 这样清理掉这类候选图之后也不会重新拉一遍。
                        continue
                    if (old and not force and not week_start and old.get("text_fingerprint") == fingerprint and
                        (old.get("evaluated_on") == china_today().isoformat() or
                         old["status"] in ("parsed", "unchanged", "archived")) and
                        Path(old["local_image_path"]).is_file() and
                        (old["status"] not in ("pending", "stale") or not self.parser.provider_id)):
                        results.append(old)
                        if old["status"] in ("parsed", "unchanged", "archived"):
                            break
                        continue
                    record = {"uid": uid, "dynamic_id": post.id, "image_url": url,
                              "image_index": image_index,
                              "is_pinned": post.is_pinned, "published_at": post.published_at,
                              "text_fingerprint": fingerprint,
                              "evaluated_on": china_today().isoformat(),
                              "local_image_path": old["local_image_path"] if old else "",
                              "status": "pending", "checked_at": utc_now()}
                    try:
                        record["error_stage"] = "download"
                        if force or not record["local_image_path"] or not Path(record["local_image_path"]).is_file():
                            raw = await self.bili.download_image(url)
                            record["local_image_path"] = await self.data.save_schedule_image(uid, post.id, url, raw)
                        week_hint = explicit_week_hint(post.text)
                        monday = china_today() - timedelta(days=china_today().weekday())
                        if week_hint and week_hint < monday and not self.parser.provider_id:
                            record["status"] = "stale"
                            record["week_start"] = week_hint.isoformat()
                        elif self.parser.provider_id:
                            record["error_stage"] = "parse"
                            options = {"source_text": post.text}
                            if week_start:
                                options["week_start"] = week_start
                            schedule = await self.parser.parse(uid, record["local_image_path"], **options)
                            schedule = replace(schedule, source_dynamic_id=post.id,
                                               source_image_url=url, local_image_path=record["local_image_path"])
                            raw = await asyncio.to_thread(Path(record["local_image_path"]).read_bytes)
                            import_key = f"{post.id}:{url}:{fingerprint}:{hashlib.sha256(raw).hexdigest()}"
                            record["status"] = await self.schedules.store_parsed_schedule(schedule, import_key=import_key)
                            record["week_start"] = schedule.week_start
                            # 任何一次成功解析都更新定时检查的基准，
                            # 这样订阅时扫描、手动解析都会自动进入定时检查。
                            if not remembered:
                                await self.watch.remember(post, schedule, raw)
                                remembered = True
                        record.pop("error_stage", None)
                    except ScheduleParseError as exc:
                        record["status"] = {"not_schedule": "skipped", "needs_date": "needs_date"}.get(exc.code, "failed")
                        record["error_code"] = exc.code
                        record["error_message"] = str(exc)
                    except Exception:
                # 不对外暴露模型提供商的错误信息，其中可能含有凭据。
                        record["status"] = "failed"
                    await self.data.save_schedule_candidate(record)
                    results.append(record)
                    # 一条动态只认一张通过的周表图；
                    # 其余配图不得在同一轮扫描中覆盖它。
                    if record["status"] in ("parsed", "unchanged", "archived"):
                        break
            await self.prune_images()
            return results

    async def prune_images(self):
        """收尾清理：判定临时图与过期候选图；失败不影响扫描结果。"""
        try:
            return await self.data.prune_schedule_images()
        except Exception:
            logger.warning("Unable to prune schedule images")
            return None
