"""需用 AstrBot 自带的 Python 手动执行；本机截图动态卡片，不发消息。

用法：python tests/dynamic_preview.py [--uid UID] [--latest] [--index N]
                                     [--list] [--out DIR] [--variants]

默认取插件数据目录里第一个已订阅主播的置顶动态（没有置顶就用最新一条），
用生产参数截图，打印卡片尺寸与「字号 / 图片宽度」比例——这个比例决定手机上
不点开时的可读性，像素密度只影响清晰度。加 --variants 额外渲染几组候选参数；
--list 列出该主播取到的动态（配合 --index 复现 /vt_latest 取的那一条），
截图前会跑与生产一致的完整性检查，失败时打印同样的原因。
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
    DEVICE_SCALE, FONT_SCALE, MAX_PIXEL_HEIGHT, VIEWPORT_WIDTH, ScreenshotError,
    card_script, card_style, text_matches)


async def load_posts(uid, latest):
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
    if not posts:
        raise SystemExit(f"UID {uid} 没有取到动态。")
    pinned = next((index for index, item in enumerate(posts) if item.is_pinned), None)
    default = 0 if latest or pinned is None else pinned
    return bili, posts, default


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
            # 与生产一致的完整性检查：正文不一致、图片没加载完、过长都直接失败。
            text = "".join((await card.inner_text()).split())
            if not text_matches(post.text, text):
                raise ScreenshotError("页面正文与接口动态不一致"
                                      f"（接口 {post.text[:40]!r} / 页面 {text[:40]!r}）")
            if not box or box["height"] * scale > MAX_PIXEL_HEIGHT:
                raise ScreenshotError("动态过长，无法生成完整截图。")
            if await card.locator("img:visible").evaluate_all(
                    "els => els.some(img => !img.complete || !img.naturalWidth)"):
                raise ScreenshotError("动态页面图片未完整加载，未发送残缺截图。")
            raw = await target.screenshot(type="png", animations="disabled", timeout=20000)
            if len(raw) > 20 * 1024 * 1024:
                raise ScreenshotError("动态完整截图超过 20 MiB。")
            Path(path).write_bytes(raw)
            return geometry, box, raw
        finally:
            await browser.close()


def parse_argv(argv):
    flags, values, positional, index = set(), {}, [], 0
    while index < len(argv):
        item = argv[index]
        if item in ("--uid", "--index"):
            values[item] = argv[index + 1] if index + 1 < len(argv) else ""
            index += 2
            continue
        (flags.add(item) if item.startswith("--") else positional.append(item))
        index += 1
    return flags, values, positional


async def main():
    # Windows 控制台默认 GBK，动态里的 emoji 会让 print 直接抛错。
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    flags, values, positional = parse_argv(sys.argv[1:])
    output = Path(positional[0]).resolve() if positional else Path(tempfile.gettempdir()) / "vt-dynamic-preview"
    output.mkdir(parents=True, exist_ok=True)
    bili, posts, default_index = await load_posts(values.get("--uid", ""), "--latest" in flags)
    if "--list" in flags:
        for index, item in enumerate(posts):
            print(f"[{index}] {item.id} 置顶={item.is_pinned} 图片={len(item.images)} "
                  f"正文={len(item.text)}字 {item.text[:24]!r}")
        await bili.close()
        return 0
    index = int(values["--index"]) if values.get("--index") else default_index
    post = posts[index]
    print(f"动态 [{index}] {post.id}  置顶={post.is_pinned} 图片={len(post.images)} 正文={len(post.text)}字")
    variants = [("default", {})]
    if "--variants" in flags:
        variants += [
            ("旧参数-1x-不放大", {"scale": 1, "font_scale": 1.0}),
            ("仅提高像素密度", {"scale": 2, "font_scale": 1.0}),
            ("仅放大字号", {"scale": 1, "font_scale": 1.2}),
        ]
    try:
        for name, options in variants:
            path = output / f"{name}.png"
            try:
                geometry, box, raw = await capture(bili, post, path, **options)
            except ScreenshotError as error:
                print(f"{name}: 截图失败 -> {error}（生产环境会退回文字版）")
                continue
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
