"""用隔离的无头浏览器截取真实动态卡片。"""
import asyncio
import sys
from pathlib import Path
from urllib.parse import urlsplit

from ..bili_client import BiliClient


# 手机可读性只看「字号 / 图片宽度」，像素密度只影响清晰度：
# B 站动态页的卡片列宽固定 632px、正文 15px，1x 出图只有 632px 宽，
# 在手机上会按原始尺寸当缩略图排，所以像素密度固定提到 2 倍（出图 1264px，铺满聊天宽度）。
# FONT_SCALE 默认 1.0 = 保留站点自己的字号；调大它会把卡片内所有字号按同一倍率放大，
# 供手机上"不想点开也要看清"时使用。
VIEWPORT_WIDTH = 1280
DEVICE_SCALE = 2
FONT_SCALE = 1.0
# Chromium 单张截图的高度上限约 16000 物理像素。
MAX_PIXEL_HEIGHT = 16000


def card_style():
    """展开被折叠的正文，但不改动动态内容本身。"""
    return """
        .dyn-card-opus__summary, .dyn-card-opus__title, .bili-rich-text__content {
            max-height: none !important; height: auto !important;
            -webkit-line-clamp: unset !important; overflow: visible !important;
            white-space: normal !important;
        }
    """


def card_script():
    """按同一倍率放大卡片内所有字号与 px 行高。

    正文的 class 名会变，而且真正带字形的子元素自带 px 字号、覆盖不掉，
    所以逐个元素按它当前的**计算值**放大；先把尺寸全读完再写回，避免逐层相乘。
    """
    return """(card, scale) => {
        if (!card) return;
        const nodes = [card, ...card.querySelectorAll('*')];
        const metrics = nodes.map(el => {
            const style = getComputedStyle(el);
            return [parseFloat(style.fontSize) || 0, /^([\\d.]+)px$/.exec(style.lineHeight)];
        });
        nodes.forEach((el, index) => {
            const [font, line] = metrics[index];
            if (font) el.style.setProperty('font-size', (font * scale).toFixed(2) + 'px', 'important');
            if (line) el.style.setProperty('line-height', (parseFloat(line[1]) * scale).toFixed(2) + 'px', 'important');
        });
    }"""


class ScreenshotError(Exception):
    """对外显示的安全信息，不含浏览器日志或凭据。"""


class PinnedScreenshot:
    def __init__(self, channel="auto", *, timeout=75, scale=DEVICE_SCALE, font_scale=FONT_SCALE):
        if channel not in ("auto", "chromium", "msedge", "chrome"):
            raise ValueError("截图浏览器请选择 auto、chromium、msedge 或 chrome。")
        self.channel, self.timeout = channel, timeout
        self.scale, self.font_scale = scale, font_scale
        self._lock = asyncio.Lock()

    async def capture(self, post, cookies):
        try:
            async with asyncio.timeout(self.timeout):
                async with self._lock:
                    return await self._capture(post, cookies)
        except ImportError:
            raise ScreenshotError("未安装截图依赖，请安装插件依赖 playwright。") from None
        except ScreenshotError:
            raise
        except Exception:
            raise ScreenshotError("动态截图失败（浏览器不可用、页面风控或加载超时），请检查截图浏览器配置。") from None

    async def _capture(self, post, cookies):
        from playwright.async_api import async_playwright
        async with async_playwright() as playwright:
            channel = self.channel
            if channel == "auto":
                channel = "chromium" if Path(playwright.chromium.executable_path).exists() or sys.platform != "win32" else "msedge"
            browser = await playwright.chromium.launch(channel=channel, headless=True)
            try:
                context = await browser.new_context(viewport={"width": VIEWPORT_WIDTH, "height": 900},
                                                    device_scale_factor=self.scale,
                                                    locale="zh-CN", service_workers="block")
                await context.add_cookies(cookies)
                async def route_request(route):
                    parts = urlsplit(route.request.url)
                    host = parts.hostname or ""
                    allowed = any(host == domain or host.endswith("." + domain)
                                  for domain in ("bilibili.com", "hdslb.com", "biliapi.net", "bilivideo.com"))
                    if parts.scheme == "https" and allowed:
                        await route.continue_()
                    else:
                        await route.abort()
                await context.route("**/*", route_request)
                page = await context.new_page()
                page.set_default_timeout(20000)
                response = await page.goto(BiliClient.dynamic_url(post), wait_until="domcontentloaded", timeout=30000)
                if response is None or response.status >= 400:
                    raise ScreenshotError("动态页面返回错误或风控，未获得完整截图。")
                card = page.locator(".bili-dyn-item, .opus-detail").first
                await card.wait_for(state="visible")
            # 展开已知的文本容器；字号默认保持站点原样，只有调大 FONT_SCALE 时才改写。
                await page.add_style_tag(content=card_style())
                if self.font_scale != 1:
                    await card.evaluate(card_script(), self.font_scale)
                for image in await card.locator("img:visible").all():
                    await image.scroll_into_view_if_needed()
                await card.evaluate("""async el => {
                    await document.fonts.ready;
                    await Promise.all(Array.from(el.querySelectorAll('img')).filter(img =>
                        img.getClientRects().length && getComputedStyle(img).visibility !== 'hidden').map(img => {
                        if (img.complete) return;
                        return new Promise(resolve => {img.addEventListener('load', resolve, {once:true});
                            img.addEventListener('error', resolve, {once:true});});
                    }));
                }""")
        # 悬浮导航不能遮挡长图卡片的中部。
                await card.evaluate("""card => {
                    for (const el of document.querySelectorAll('body *')) {
                        if (el.contains(card) || card.contains(el)) continue;
                        if (['fixed', 'sticky'].includes(getComputedStyle(el).position))
                            el.style.setProperty('visibility', 'hidden', 'important');
                    }
                }""")
                text = "".join((await card.inner_text()).split())
                expected = "".join(post.text.split())[:30]
                if expected and expected not in text:
                    raise ScreenshotError("页面正文与接口动态不一致，未发送可能不完整的截图。")
                box = await card.bounding_box()
                if not box or box["height"] * self.scale > MAX_PIXEL_HEIGHT:
                    raise ScreenshotError("动态过长，无法生成完整截图；请查看动态链接。")
                if await card.locator("img:visible").evaluate_all("els => els.some(img => !img.complete || !img.naturalWidth)"):
                    raise ScreenshotError("动态页面图片未完整加载，未发送残缺截图。")
                raw = await card.screenshot(type="png", animations="disabled", timeout=20000)
                if len(raw) > 20 * 1024 * 1024:
                    raise ScreenshotError("动态完整截图超过 20 MiB，请查看动态链接。")
                return raw
            finally:
                await browser.close()
