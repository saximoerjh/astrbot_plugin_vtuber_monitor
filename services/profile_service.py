"""主播空间资料：头像与空间头图，每天零点刷新一次。

空间头图只存在于空间页的渲染结果里：公开接口 ``x/space/wbi/acc/info``
带 WBI 签名也会返回风控（-352），``x/web-interface/card`` 只给头像。
因此这里复用插件已有的无头浏览器读取空间页上的图片地址，再走
BiliClient 的 CDN 下载（带域名、类型与大小校验）。
"""
import asyncio
import base64
import json
import logging
import random
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

from ..bili_client import BiliError
from ..core.models import validate_uid
from ..core.schedule_models import china_today

logger = logging.getLogger(__name__)
CHINA = timezone(timedelta(hours=8))
ASSET_LIMIT = 2 * 1024 * 1024
# 空间页给的地址带宽高后缀；按用途改成够用的小图，避免把整页图塞进 HTML。
RENDITION = {"avatar": "@256w_256h_1c_1s.webp", "header": "@1680w_350h_1c_80q.webp"}
ALLOWED_HOSTS = ("bilibili.com", "hdslb.com", "biliapi.net", "bilivideo.com")
# 头像与头图是装饰性素材（换装、周年才变），按 TTL 刷新而不是每天全量重抓。
PROFILE_TTL_DAYS = 7
# 提取逻辑变更时递增：已缓存的记录会被判定为过期而重抓一次，
# 否则旧的错误结论（例如漏判头图）要等一个 TTL 才会自愈。
PROFILE_VERSION = 2
# 扫描时间避开零点：那时周表任务正在调模型、下图片，两边同时开浏览器没有意义。
SWEEP_AT = time(4, 30)
SWEEP_JITTER_SECONDS = 600


def is_fresh(record, today=None):
    """记录是否还在 TTL 内；没有记录、时间缺失或损坏都算过期。"""
    if not isinstance(record, dict):
        return False
    try:
        fetched_on = date.fromisoformat(record.get("fetched_on") or "")
    except (TypeError, ValueError):
        return False
    return ((today or china_today()) - fetched_on).days < PROFILE_TTL_DAYS


def next_sweep(now):
    """下一次扫描的时刻：每天 04:30 一次，过了就排到明天。"""
    target = datetime.combine(now.date(), SWEEP_AT, CHINA)
    return target if target > now else target + timedelta(days=1)


def rendition_url(url, kind):
    """把 CDN 地址换成固定尺寸的版本；域名不合规时抛错由调用方降级。"""
    parts = urlsplit(url)
    if (parts.scheme != "https" or not parts.hostname or
            not parts.hostname.endswith(".hdslb.com") or parts.username or
            parts.password or parts.port not in (None, 443)):
        raise BiliError("空间图片必须来自 Bilibili HTTPS 图片 CDN。")
    return parts._replace(path=parts.path.split("@")[0] + RENDITION[kind]).geturl()


def image_mime(raw):
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if raw.startswith(b"RIFF") and raw[8:12] == b"WEBP":
        return "image/webp"
    if raw.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    return ""


class SpaceProfileFetcher:
    """用无头浏览器读取空间页上的头像与头图地址；不发送任何 Cookie。"""

    # 头图定位取三种信号，依次退化：类名含 toutu 的元素 → 头图 CDN 路径 →
    # 宽幅背景兜底。只认 /bfs/space/ 会漏掉活动头图（/bfs/activity-plat/）。
    _HELPER = r"""
        const bgUrl = el => {
            const bg = getComputedStyle(el).backgroundImage;
            const match = bg && bg.match(/url\((["']?)([^"')]+)\1\)/);
            return match ? match[2] : '';
        };
        const headerUrl = () => {
            for (const el of document.querySelectorAll('[class*="toutu"]')) {
                const url = bgUrl(el);
                if (url.startsWith('http')) return url;
            }
            for (const el of document.querySelectorAll('*')) {
                const url = bgUrl(el);
                if (url.includes('/bfs/space/') || url.includes('/bfs/activity-plat/')) return url;
            }
            const wide = innerWidth * 0.6;
            for (const el of document.querySelectorAll('*')) {
                const url = bgUrl(el);
                if (!url.startsWith('http') || url.includes('/bfs/face/') || url.includes('/bfs/archive/')) continue;
                const rect = el.getBoundingClientRect();
                if (rect.width >= wide && rect.height >= 120 && rect.height <= 700) return url;
            }
            return '';
        };
    """

    # 头像 img 出现即认为主体已渲染；头图背景可能随后才由脚本挂上。
    READY = """() => !!document.querySelector('img[src*="/bfs/face/"]')"""

    def _extract_js(self):
        return ("() => {" + self._HELPER + """
            let avatar = '', area = 0;
            for (const img of document.querySelectorAll('img')) {
                if (!img.src || !img.src.includes('/bfs/face/')) continue;
                const size = img.naturalWidth * img.naturalHeight;
                if (size > area) { area = size; avatar = img.src; }
            }
            return {header: headerUrl(), avatar: avatar};
        }""")

    def _header_ready_js(self):
        return "() => {" + self._HELPER + " return !!headerUrl(); }"

    def __init__(self, channel="auto", *, timeout=75):
        if channel not in ("auto", "chromium", "msedge", "chrome"):
            raise ValueError("截图浏览器请选择 auto、chromium、msedge 或 chrome。")
        self.channel, self.timeout = channel, timeout

    async def fetch(self, uids):
        uids = [validate_uid(uid) for uid in uids]
        if not uids:
            return {}
        try:
            # 每多一位主播就多开一个页面，超时按批量大小放大。
            async with asyncio.timeout(max(self.timeout, 20 * len(uids))):
                return await self._fetch(uids)
        except ImportError:
            raise BiliError("未安装截图依赖，请安装插件依赖 playwright。") from None
        except BiliError:
            raise
        except Exception:
            raise BiliError("读取主播空间资料失败（浏览器不可用或页面加载超时）。") from None

    async def _fetch(self, uids):
        from playwright.async_api import async_playwright
        async with async_playwright() as playwright:
            channel = self.channel
            if channel == "auto":
                channel = ("chromium" if Path(playwright.chromium.executable_path).exists()
                           or sys.platform != "win32" else "msedge")
            browser = await playwright.chromium.launch(channel=channel, headless=True)
            try:
                context = await browser.new_context(
                    viewport={"width": 1440, "height": 900}, locale="zh-CN",
                    service_workers="block")

                async def route_request(route):
                    parts = urlsplit(route.request.url)
                    host = parts.hostname or ""
                    if parts.scheme == "https" and any(
                            host == domain or host.endswith("." + domain) for domain in ALLOWED_HOSTS):
                        await route.continue_()
                    else:
                        await route.abort()

                await context.route("**/*", route_request)
                results = {}
                for uid in uids:
                    page = await context.new_page()
                    try:
                        await page.goto(f"https://space.bilibili.com/{uid}",
                                        wait_until="domcontentloaded", timeout=45000)
                        # 条件等待代替固定 4 秒：正常页面约 1 秒就绪，
                        # 没有自定义头图的主播也只多花很短的时间。
                        await page.wait_for_function(self.READY, timeout=10000, polling=300)
                        try:
                            await page.wait_for_function(self._header_ready_js(), timeout=2000, polling=200)
                        except Exception:
                            pass
                        await page.wait_for_timeout(200)
                        found = await page.evaluate(self._extract_js())
                        results[uid] = {"avatar": found.get("avatar") or "",
                                        "header": found.get("header") or "", "ok": True}
                    except Exception:
                        logger.warning("Space page read failed uid=%s", uid)
                        results[uid] = {"ok": False}
                    finally:
                        await page.close()
                return results
            finally:
                await browser.close()


class ProfileService:
    """把头像与头图落到 plugin_data/profiles/<uid>/，并缓存成 data URI 供出图使用。"""

    def __init__(self, data, bili, *, channel="auto", timeout=75, fetcher=None):
        self.data = data
        self.directory = Path(data.path).parent / "profiles"
        self.bili = bili
        self.fetcher = fetcher or SpaceProfileFetcher(channel, timeout=timeout)
        self._lock = asyncio.Lock()

    def _paths(self, uid):
        directory = self.directory / str(validate_uid(uid))
        return directory, directory / "profile.json"

    async def load(self, uid):
        """只读本地记录，不访问网络；缺失或损坏时返回 None。"""
        _, path = self._paths(uid)
        try:
            record = json.loads(await asyncio.to_thread(path.read_text, encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return record if isinstance(record, dict) else None

    async def _download(self, uid, kind, url, record):
        """下载单个素材；失败时保留已有文件，只记录错误。"""
        if not url:
            record.setdefault("errors", []).append(f"{kind}_missing")
            return
        if (record.get(f"{kind}_url") == url and record.get(kind)
                and await asyncio.to_thread(Path(record[kind]).is_file)):
            # 地址没变且文件还在，就不重复下载，省一次 CDN 流量。
            return
        try:
            raw = await self.bili.download_image(rendition_url(url, kind))
        except BiliError:
            record.setdefault("errors", []).append(f"{kind}_download_failed")
            return
        mime = image_mime(raw)
        if len(raw) > ASSET_LIMIT or not mime:
            record.setdefault("errors", []).append(f"{kind}_invalid")
            return
        suffix = {"image/png": ".png", "image/webp": ".webp", "image/jpeg": ".jpg"}[mime]

        def write():
            directory, _ = self._paths(uid)
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"{kind}{suffix}"
            temporary = path.with_suffix(path.suffix + ".tmp")
            temporary.write_bytes(raw)
            temporary.replace(path)
            return str(path.resolve())

        record[kind] = await asyncio.to_thread(write)
        record[f"{kind}_url"] = url

    async def _store(self, uid, asset):
        record = await self.load(uid) or {}
        record["uid"] = uid
        record["version"] = PROFILE_VERSION
        record["attempted_on"] = china_today().isoformat()
        record["errors"] = []
        for kind in RENDITION:
            await self._download(uid, kind, (asset or {}).get(kind, ""), record)
        _, path = self._paths(uid)
        # TTL 起点只在“页面读成功且素材都下全了”时前进：页面整个抓失败时
        # 保持原时间，下一轮扫描会继续重试；页面正常但主播没设头图不算失败。
        hard_errors = [item for item in record["errors"] if not item.endswith("_missing")]
        if (asset or {}).get("ok") and not hard_errors:
            record["fetched_on"] = china_today().isoformat()
        record["fetched_at"] = datetime.now(CHINA).isoformat(timespec="seconds")

        def write():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")

        await asyncio.to_thread(write)
        return record

    async def refresh(self, uid):
        """抓取并保存一位主播的资料；失败时保留上一次的图。"""
        uid = validate_uid(uid)
        async with self._lock:
            try:
                assets = await self.fetcher.fetch([uid])
            except BiliError:
                logger.warning("Profile fetch failed uid=%s", uid)
                assets = {}
            return await self._store(uid, assets.get(uid) or {})

    async def refresh_all(self, uids):
        """批量刷新：只开一次浏览器，失败逐个隔离。"""
        uids = [validate_uid(uid) for uid in uids]
        if not uids:
            return 0
        async with self._lock:
            try:
                assets = await self.fetcher.fetch(uids)
            except BiliError:
                logger.warning("Profile batch fetch failed")
                assets = {}
            for uid in uids:
                try:
                    await self._store(uid, assets.get(uid) or {})
                except Exception:
                    logger.warning("Profile store failed uid=%s", uid)
        return len(uids)

    async def ensure(self, uid):
        """出图前调用：已有图直接返回；从未成功抓过且今天没试过才现抓。"""
        record = await self.load(uid)
        if record and (record.get("avatar") or record.get("header")):
            return record
        if not self._needs_fetch(record):
            return record
        return await self.refresh(uid)

    @staticmethod
    def _needs_fetch(record):
        """缺失或超过 TTL 才需要抓取；当天失败过的不重复试。"""
        if record and record.get("version") != PROFILE_VERSION:
            # 提取逻辑升级过：旧记录一律重读一次，否则漏判会一直留到 TTL 结束。
            return True
        if record and (record.get("avatar") or record.get("header")) and is_fresh(record):
            return False
        if record and record.get("attempted_on") == china_today().isoformat():
            # 今天已经试过但没拿到可用结果，等下一次扫描再重试。
            return False
        return True

    async def should_refresh(self, uid):
        """扫描入口：只挑缺失或超过 TTL 的条目。"""
        return self._needs_fetch(await self.load(uid))

    async def refresh_stale(self, uids=None):
        """刷新所有过期条目；全都新鲜时不会打开浏览器。"""
        uids = list(uids) if uids is not None else await self.data.get_subscribed_uids()
        targets = [validate_uid(uid) for uid in uids if await self.should_refresh(uid)]
        if not targets:
            return 0
        logger.info("Refreshing %s streamer profile(s)", len(targets))
        return await self.refresh_all(targets)

    async def banner(self, uid):
        """返回可直接嵌入 HTML 的 data URI；缺哪张就少哪张，不抛异常。"""
        try:
            record = await self.ensure(uid)
        except Exception:
            logger.warning("Profile banner unavailable uid=%s", uid)
            return {}
        result = {}
        for kind in RENDITION:
            path = (record or {}).get(kind)
            if not path:
                continue
            try:
                raw = await asyncio.to_thread(Path(path).read_bytes)
            except OSError:
                continue
            mime = image_mime(raw)
            if mime and len(raw) <= ASSET_LIMIT:
                result[kind] = f"data:{mime};base64,{base64.b64encode(raw).decode('ascii')}"
        return result

    async def run(self):
        """启动时先补一次（TTL 内会直接跳过），之后每天 04:30 左右扫描。"""
        while True:
            try:
                await self.refresh_stale()
            except Exception:
                logger.warning("Unable to enumerate subscribed streamers for profiles")
            delay = (next_sweep(datetime.now(CHINA)) - datetime.now(CHINA)).total_seconds()
            await asyncio.sleep(max(1, delay) + random.uniform(0, SWEEP_JITTER_SECONDS))
