"""需用 AstrBot 自带的 Python 手动执行；本机截图动态卡片，不发消息。

用法：python tests/dynamic_preview.py [--uid UID] [--latest] [--out DIR] [--variants]

默认取插件数据目录里第一个已订阅主播的置顶动态（没有置顶就用最新一条），
用生产参数截图，打印卡片尺寸与「字号 / 图片宽度」比例——这个比例决定手机上
不点开时的可读性，像素密度只影响清晰度。加 --variants 额外渲染几组候选参数。
"""
import asyncio
import struct
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

PLUGIN_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_DIR.parent))

from astrbot_plugin_vtuber_monitor.services.pinned_screenshot import (  # noqa: E402
    DEVICE_SCALE, FONT_SCALE, VIEWPORT_WIDTH, card_script, card_style)


async def load_post(uid, latest):
    from astrbot_plugin_vtuber_monitor.bili_client import BiliClient
    from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager

    data_dir = PLUGIN_DIR.parents[1] / "plugin_data" / PLUGIN_DIR.name
    data = DataManager(data_dir)
    await data.initialize()
    bili = BiliClient()
    credentials = await data.get_credentials()
    if credentials:
        bili.set_credentials(credentials)
    if not uid:
        uids = await data.get_subscribed_uids()
        if not uids:
            raise SystemExit(f"数据库里没有订阅主播：{data_dir}")
        uid = str(uids[0])
    posts = await bili.get_latest_dynamics(int(uid))
    post = posts[0] if latest else (next((p for p in reversed(posts) if p.is_pinned), None) or posts[0])
    print(f"UID {uid} 动态 {post.id}  置顶={post.is_pinned}")
    return bili, post


async def capture(bili, post, path, *, width=VIEWPORT_WIDTH, scale=DEVICE_SCALE,
                  font_scale=FONT_SCALE, height=900, crop=None):
    """按给定参数截图，并返回卡片几何信息。"""
    from playwright.async_api import async_playwright

    async with async_playwright() as playwright:
        channel = "chromium" if Path(playwright.chromium.executable_path).exists() or sys.platform != "win32" else "msedge"
        browser = await playwright.chromium.launch(channel=channel, headless=True)
        try:
            context = await browser.new_context(viewport={"width": width, "height": height},
                                                device_scale_factor=scale, locale="zh-CN", service_workers="block")
            await context.add_cookies(bili.browser_cookies())

            async def route_request(route):
                parts = urlsplit(route.request.url)
                host = parts.hostname or ""
                allowed = any(host == domain or host.endswith("." + domain)
                              for domain in ("bilibili.com", "hdslb.com", "biliapi.net", "bilivideo.com"))
                await route.continue_() if parts.scheme == "https" and allowed else await route.abort()

            await context.route("**/*", route_request)
            page = await context.new_page()
            page.set_default_timeout(20000)
            await page.goto(f"https://t.bilibili.com/{post.id}", wait_until="domcontentloaded", timeout=30000)
            card = page.locator(".bili-dyn-item, .opus-detail").first
            await card.wait_for(state="visible")
            await page.add_style_tag(content=card_style())
            await card.evaluate(card_script(), font_scale)
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
            await card.evaluate("""card => {
                for (const el of document.querySelectorAll('body *')) {
                    if (el.contains(card) || card.contains(el)) continue;
                    if (['fixed', 'sticky'].includes(getComputedStyle(el).position))
                        el.style.setProperty('visibility', 'hidden', 'important');
                }
            }""")
            geometry = await page.evaluate("""() => {
                const card = document.querySelector('.bili-dyn-item, .opus-detail');
                const text = document.querySelector('.bili-rich-text, .dyn-card-opus__summary, .dyn-card-opus__title');
                const size = el => el ? {w: Math.round(el.getBoundingClientRect().width),
                                         h: Math.round(el.getBoundingClientRect().height)} : null;
                return {card: size(card), text: size(text),
                        font: text ? parseFloat(getComputedStyle(text).fontSize) : null};
            }""")
            target = card
            if crop:
                locator = page.locator(crop).first
                if await locator.count():
                    target = locator
            box = await target.bounding_box()
            raw = await target.screenshot(type="png", animations="disabled", timeout=20000)
            Path(path).write_bytes(raw)
            return geometry, box, raw
        finally:
            await browser.close()


async def main():
    args = [item for item in sys.argv[1:] if not item.startswith("--")]
    uid = sys.argv[sys.argv.index("--uid") + 1] if "--uid" in sys.argv else ""
    latest = "--latest" in sys.argv
    output = Path(args[0]).resolve() if args else Path(tempfile.gettempdir()) / "vt-dynamic-preview"
    output.mkdir(parents=True, exist_ok=True)
    bili, post = await load_post(uid, latest)
    variants = [("default", {})]
    if "--variants" in sys.argv:
        variants += [
            ("旧参数-1x-不放大", {"scale": 1, "font_scale": 1.0}),
            ("仅提高像素密度", {"scale": 2, "font_scale": 1.0}),
            ("仅放大字号", {"scale": 1, "font_scale": 1.2}),
        ]
    try:
        for name, options in variants:
            path = output / f"{name}.png"
            geometry, box, raw = await capture(bili, post, path, **options)
            png_width, png_height = struct.unpack(">II", raw[16:24])
            font = geometry["font"] or 0
            print(f"{name}: 卡片 {geometry['card']} 正文 {geometry['text']} 字号 {font:g} "
                  f"-> {png_width}x{png_height}px {len(raw) // 1024}KiB "
                  f"字号/宽度 = {font / box['width'] * 100:.2f}%")
    finally:
        await bili.close()
    print(f"输出目录：{output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
