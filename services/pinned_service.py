"""有序的置顶动态内容，不依赖任何平台组件。"""
import asyncio
from dataclasses import dataclass

from ..bili_client import BiliError
from .pinned_screenshot import ScreenshotError


@dataclass(frozen=True)
class PinnedPart:
    text: str = ""
    image: bytes | None = None


class PinnedService:
    def __init__(self, bili, screenshot, selector):
        self.bili, self.screenshot = bili, screenshot
        self.selector = selector
        self._lock = asyncio.Lock()
        self._tasks = set()
        self._closed = False

    async def build(self, uid):
        if self._closed:
            raise BiliError("置顶动态服务已停止，请重载插件。")
        if len(self._tasks) >= 3:
            raise BiliError("置顶动态截图请求较多，请稍后重试。")
        task = asyncio.create_task(self._build(uid), name="vtuber-pinned")
        self._tasks.add(task)
        try:
            return await task
        finally:
            self._tasks.discard(task)

    async def close(self):
        self._closed = True
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _build(self, uid):
        async with self._lock:
            post = await self.bili.get_pinned_dynamic(uid)
            if post is None:
                return None
            try:
                raw = await self.screenshot.capture(post, self.bili.browser_cookies())
                first = PinnedPart(image=raw)
            except ScreenshotError as exc:
                first = PinnedPart(text=f"动态整体截图未生成：{exc}")
            semaphore = asyncio.Semaphore(3)
            async def download(index, url):
                async with semaphore:
                    try:
                        async with asyncio.timeout(45):
                            raw = await self.bili.download_image(url)
                        return PinnedPart(image=raw)
                    except (BiliError, TimeoutError):
                        try:
                            async with asyncio.timeout(30):
                                raw = await self.bili.download_image(self.bili.preview_image_url(url))
                            return PinnedPart(text=f"图片 {index}：原图获取失败，展示压缩预览。", image=raw)
                        except (BiliError, TimeoutError):
                            return PinnedPart(text=f"图片 {index} 下载失败：{url}")
    # gather 保持原始顺序，与完成先后无关。
            selected = set(await self.selector.select(post))
            images = await asyncio.gather(*(download(i, url) for i, url in enumerate(post.images, 1) if url in selected))
            return [first, *images]


def message_parts(parts):
    from astrbot.api.message_components import Image, Plain
    return [[*([Plain(part.text)] if part.text else []),
             *([Image.fromBytes(part.image)] if part.image is not None else [])] for part in parts]


def image_then_text(raw, text):
    """单条消息：图片在前、文字在后（命令回复用）。"""
    from astrbot.api.message_components import Image, Plain
    return [*([Image.fromBytes(raw)] if raw else []), *([Plain(text)] if text else [])]


def forward_chain(parts, sender_id):
    from astrbot.api.message_components import Node, Nodes
    return [Nodes([Node(uin=str(sender_id), name="VTuber Monitor", content=content)
                   for content in message_parts(parts)])]
