"""挑选已确认的周表图片，不改动任何周表。"""
import asyncio
import hashlib
from pathlib import Path

from ..bili_client import BiliError
from .schedule_parser import ScheduleParseError


class ScheduleImageSelector:
    def __init__(self, data, bili, parser):
        self.data, self.bili, self.parser = data, bili, parser

    async def select(self, post):
        fingerprint = hashlib.sha256(post.text.encode()).hexdigest()
        cached = await self.data.get_image_classifications(post.uid, post.id, fingerprint)
        candidates = {r["image_url"]: r for r in await self.data.get_schedule_candidates(post.uid)
                      if r["dynamic_id"] == post.id and r.get("text_fingerprint") == fingerprint}
        selected = []
        for url in post.images:
            old = candidates.get(url, {})
            known = cached.get(url)
            if old.get("status") in ("parsed", "archived", "unchanged", "needs_date"):
                known = True
            elif old.get("error_code") == "not_schedule":
                known = False
            if known is None and self.parser.provider_id:
                try:
                    async with asyncio.timeout(120):
                        path = old.get("local_image_path", "")
                        if not path or not Path(path).is_file():
        # 分类用有限尺寸的预览即可，避免下载超大立绘原图。
                            preview = self.bili.preview_image_url(url)
                            raw = await self.bili.download_image(preview)
                            path = await self.data.save_schedule_image(post.uid, post.id, preview, raw)
                        known = await self.parser.is_schedule_image(path)
                        await self.data.save_image_classification(post.uid, post.id, url, fingerprint, known)
                except (BiliError, ScheduleParseError, TimeoutError):
        # 未知图片绝不当作已确认周表对外发送；失败结果不缓存。
                    continue
            if known is True:
                selected.append(url)
        return selected
