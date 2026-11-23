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
                            raw = await self.bili.download_image(self.bili.preview_image_url(url))
                        else:
                            raw = await asyncio.to_thread(Path(path).read_bytes)
                        async with self.data.classification_image(raw) as path:
                            known = await self.parser.is_schedule_image(path)
                        await self.data.save_image_classification(post.uid, post.id, url, fingerprint, known)
                except (BiliError, ScheduleParseError, OSError, TimeoutError):
                    continue
            if known is True:
                selected.append(url)
        return selected
