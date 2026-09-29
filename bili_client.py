"""Bilibili 异步请求；绝不记录凭据或响应体。"""
import asyncio
import logging
import math
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit, parse_qs
from http.cookies import SimpleCookie

import httpx

from .core.models import DynamicPost, VtuberState, validate_uid

logger = logging.getLogger(__name__)


class _LoginUrlRedactor(logging.Filter):
    """httpx 的 INFO 日志通常包含扫码密钥与跨域票据。"""
    def filter(self, record):
        if isinstance(record.args, tuple):
            record.args = tuple(
                value.copy_with(query=None) if isinstance(value, httpx.URL) and value.host in
                ("passport.bilibili.com", "passport.biligame.com", "account.bilibili.com") else value
                for value in record.args
            )
        return True


if not any(isinstance(item, _LoginUrlRedactor) for item in logging.getLogger("httpx").filters):
    logging.getLogger("httpx").addFilter(_LoginUrlRedactor())


class BiliError(Exception):
    """可安全对外展示的上游失败。"""


class BiliRateLimited(BiliError):
    def __init__(self, retry_after):
        self.retry_after = max(0, retry_after)
        super().__init__("Bilibili 请求受限，已暂停请求并等待冷却。")


class BiliClient:
    def __init__(self, timeout: float = 10, max_retry: int = 2, *, sessdata="", transport=None):
        if not math.isfinite(timeout) or not 0 < timeout <= 120:
            raise ValueError("request_timeout 必须在 0 到 120 秒之间。")
        if type(max_retry) is not int or not 0 <= max_retry <= 5:
            raise ValueError("max_retry 必须是 0 到 5 的整数。")
        self.max_retry = max_retry
        self._cooldowns = {}
        self._rate_strikes = {}
        if not isinstance(sessdata, str) or any(not 33 <= ord(c) <= 126 or c in ';"\\' for c in sessdata):
            raise ValueError("SESSDATA 请只填写 Cookie 中的值，不要填写完整 Cookie。")
        self.has_credentials = bool(sessdata)
        cookies = httpx.Cookies()
        if sessdata:
            cookies.set("SESSDATA", sessdata, domain=".bilibili.com", path="/")
        self.http = httpx.AsyncClient(
            timeout=timeout, transport=transport,
            cookies=cookies,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://live.bilibili.com/"},
        )

    async def close(self):
        await self.http.aclose()

    def set_credentials(self, credentials):
        allowed = {"SESSDATA", "bili_jct", "DedeUserID", "buvid3", "buvid4"}
        if not isinstance(credentials, dict) or not credentials.get("SESSDATA"):
            raise BiliError("登录凭据缺少 SESSDATA。")
        values = {key: value for key, value in credentials.items() if key in allowed and value}
        if any(not isinstance(value, str) or any(not 33 <= ord(c) <= 126 or c in ';"\\' for c in value)
               for value in values.values()):
            raise BiliError("登录凭据格式异常。")
        self.http.cookies.clear()
        for key, value in values.items():
            self.http.cookies.set(key, value, domain=".bilibili.com", path="/")
        self.has_credentials = True

    async def _login_request(self, url, params=None):
        # 错误信息里绝不带上 URL（含扫码密钥与票据）、响应体或 Cookie。
        for attempt in range(self.max_retry + 1):
            try:
                response = await self.http.get(url, params=params, headers={"Referer": "https://www.bilibili.com/"})
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, dict) or type(body.get("code")) is not int or body["code"] != 0 or not isinstance(body.get("data"), dict):
                    raise BiliError("Bilibili 登录接口响应异常，请稍后重试。")
                return body["data"], response
            except (httpx.RequestError, httpx.HTTPStatusError, ValueError):
                if attempt == self.max_retry:
                    raise BiliError("Bilibili 登录接口网络或风控错误，请稍后重试。") from None
                logger.warning("Bilibili login request failed attempt=%s", attempt + 1)
                await asyncio.sleep(min(2 ** attempt, 8))

    async def generate_login_qr(self):
        data, _ = await self._login_request("https://passport.bilibili.com/x/passport-login/web/qrcode/generate")
        url, key = data.get("url"), data.get("qrcode_key")
        if not isinstance(url, str) or not isinstance(key, str) or not key or len(key) > 256:
            raise BiliError("登录二维码响应缺少必要字段。")
        parts = urlsplit(url)
        if (parts.scheme != "https" or parts.hostname not in ("passport.bilibili.com", "account.bilibili.com")
            or parts.username or parts.password or parts.port not in (None, 443)):
            raise BiliError("登录二维码地址无效。")
        return url, key

    async def poll_login_qr(self, key):
        data, response = await self._login_request(
            "https://passport.bilibili.com/x/passport-login/web/qrcode/poll", {"qrcode_key": key})
        code = data.get("code")
        if type(code) is not int or code not in (0, 86101, 86090, 86038):
            raise BiliError("未知二维码登录状态，请重新发起登录。")
        if code != 0:
            return code, None
        credentials = {}
        def collect(response):
            for header in response.headers.get_list("set-cookie"):
                cookie = SimpleCookie()
                cookie.load(header)
                for name in ("SESSDATA", "bili_jct", "DedeUserID", "buvid3", "buvid4"):
                    if name in cookie:
                        credentials[name] = cookie[name].value
        collect(response)
        url = data.get("url", "")
        if not isinstance(url, str):
            raise BiliError("登录确认响应无效。")
        # 有些响应把凭据放在查询参数里，另一些会下发一次性的跨域票据；
        # 因此只访问明确列出的 passport 域名。
        for _ in range(4):
            if not url:
                break
            parts = urlsplit(url)
            if (parts.scheme not in ("https", "http") or parts.hostname not in
                ("passport.bilibili.com", "passport.biligame.com") or parts.username or parts.password or
                parts.port not in (None, 443, 80)):
                raise BiliError("登录确认地址无效。")
            query = parse_qs(parts.query)
            for name in ("SESSDATA", "bili_jct", "DedeUserID", "buvid3", "buvid4"):
                if query.get(name):
                    credentials[name] = query[name][0]
            if all(credentials.get(name) for name in ("SESSDATA", "bili_jct", "DedeUserID")):
                break
            if not parts.path.endswith("/crossDomain") or not query.get("ticket"):
                break
            if parts.scheme == "http":
                url = "https://" + url[7:]
            try:
                cross = await self.http.get(url)
                collect(cross)
                if cross.is_redirect:
                    from urllib.parse import urljoin
                    url = urljoin(url, cross.headers.get("location", ""))
                else:
                    cross.raise_for_status()
                    break
            except httpx.HTTPError:
                raise BiliError("扫码已确认，但交换登录凭据失败，请重新登录。") from None
        if not all(credentials.get(name) for name in ("SESSDATA", "bili_jct", "DedeUserID")):
            raise BiliError("扫码已确认，但未获取完整登录凭据，请重新登录。")
        # 只做验证，不改动监控客户端的凭据。
        self.set_credentials(credentials)
        return 0, credentials

    async def _request(self, uid: int, *, url=None, params=None) -> dict:
        url = url or "https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids"
        host = urlsplit(url).hostname
        for attempt in range(self.max_retry + 1):
            remaining = self._cooldowns.get(host, 0) - time.monotonic()
            if remaining > 0:
                raise BiliRateLimited(remaining)
            retry = True
            try:
                response = await self.http.get(url, params=params if params is not None else {"uids[]": uid})
                if response.status_code in (412, 429):
                    raise self._rate_limit(host, response.headers.get("Retry-After", ""))
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, dict) or type(body.get("code")) is not int:
                    raise BiliError("Bilibili 返回格式异常。")
                code = body["code"]
                if code != 0:
                    if code in (-352, -412, -509):
                        raise self._rate_limit(host, response.headers.get("Retry-After", ""))
                    retry = False
                    if code in (-101, -111):
                        raise BiliError("Bilibili 登录凭据缺失或失效，请在插件配置中更新 SESSDATA 后重载。")
                    raise BiliError(f"Bilibili API 错误（{code}），请稍后重试。")
                data = body.get("data")
                if not isinstance(data, dict):
                    raise BiliError("Bilibili 返回格式异常。")
                if self._cooldowns.get(host, 0) <= time.monotonic():
                    self._rate_strikes.pop(host, None)
                    self._cooldowns.pop(host, None)
                return data
            except BiliRateLimited:
                raise
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                retry = status in (412, 429) or status >= 500
                error = BiliError(f"Bilibili HTTP 错误（{status}），请稍后重试。")
            except httpx.RequestError:
                error = BiliError("Bilibili 请求超时或网络不可用，请稍后重试。")
            except (ValueError, BiliError) as exc:
                error = exc if isinstance(exc, BiliError) else BiliError("Bilibili 返回格式异常。")
            logger.warning("Bilibili request failed uid=%s attempt=%s: %s", uid, attempt + 1, error)
            if not retry or attempt == self.max_retry:
                raise error from None
            await asyncio.sleep(min(2 ** attempt, 8))
        raise AssertionError("unreachable")

    def _rate_limit(self, host, retry_after=""):
        strikes = min(self._rate_strikes.get(host, 0) + 1, 5)
        self._rate_strikes[host] = strikes
        delay = min(300 * 2 ** (strikes - 1), 3600)
        try:
            server_delay = float(retry_after)
        except (TypeError, ValueError):
            try:
                server_delay = (parsedate_to_datetime(retry_after) - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                server_delay = 0
        if math.isfinite(server_delay):
            delay = max(delay, server_delay)
        self._cooldowns[host] = max(self._cooldowns.get(host, 0), time.monotonic() + delay)
        return BiliRateLimited(self._cooldowns[host] - time.monotonic())

    async def get_user_info(self, uid: int) -> VtuberState:
        uid = validate_uid(uid)
        data = await self._request(uid)
        item = data.get(str(uid))
        if item is None:
            raise BiliError("未查到该 UID 的直播间，请确认填写的是主播 UID，而非房间号。")
        try:
            if not isinstance(item, dict) or type(item.get("live_status")) is not int:
                raise ValueError("invalid live status")
            if item["live_status"] not in (0, 1, 2):
                raise ValueError("unknown live status")
            if validate_uid(item["uid"]) != uid:
                raise ValueError("uid mismatch")
            title = item.get("title")
            title = title.strip()[:300] if isinstance(title, str) else ""
            cover = ""
            for field in ("cover_from_user", "keyframe"):
                value = item.get(field)
                if not isinstance(value, str) or not value.strip():
                    continue
                value = self.normalize_image_url(value.strip())
                parts = urlsplit(value)
                if (parts.scheme == "https" and parts.hostname and parts.hostname.endswith(".hdslb.com")
                        and not parts.username and not parts.password):
                    cover = value
                    break
            started = None
            stamp = item.get("live_time")
            if item["live_status"] == 1 and type(stamp) in (int, float) and 0 < stamp <= time.time():
                try:
                    started = datetime.fromtimestamp(stamp, timezone.utc).isoformat()
                except (ValueError, OverflowError, OSError):
                    pass
            return VtuberState(uid, item["uname"], item["room_id"], item["live_status"] == 1,
                               live_title=title, live_cover=cover, live_started_at=started)
        except (KeyError, TypeError, ValueError):
            logger.warning("Invalid Bilibili streamer metadata uid=%s", uid)
            raise BiliError("Bilibili 主播信息字段缺失或无效。") from None

    async def get_live_room_info(self, uid: int) -> VtuberState:
        return await self.get_user_info(uid)

    async def get_latest_dynamics(self, uid: int) -> list[DynamicPost]:
        uid = validate_uid(uid)
        data = await self._request(uid,
            url="https://api.bilibili.com/x/polymer/web-dynamic/v1/feed/space",
            params={"host_mid": uid, "features": "itemOpusStyle"})
        return self._dynamic_posts(uid, data)

    async def get_dynamic(self, uid, dynamic_id):
        uid = validate_uid(uid)
        if not isinstance(dynamic_id, str) or not dynamic_id.isascii() or not dynamic_id.isdigit():
            raise ValueError("动态 ID 无效")
        data = await self._request(uid,
            url="https://api.bilibili.com/x/polymer/web-dynamic/v1/detail",
            params={"id": dynamic_id, "features": "itemOpusStyle"})
        posts = self._dynamic_posts(uid, {"items": [data.get("item")]})
        if len(posts) != 1 or posts[0].id != dynamic_id:
            raise BiliError("动态详情不匹配")
        return posts[0]

    def _dynamic_posts(self, uid, data):
        try:
            items = data["items"]
            if not isinstance(items, list):
                raise ValueError("invalid items")
            posts = []
            for item in items:
                modules = item["modules"]
                author = modules["module_author"]
                if validate_uid(author["mid"]) != uid:
                    raise ValueError("author mismatch")
                dynamic = modules["module_dynamic"]
                desc = dynamic.get("desc")
                text = "" if desc is None else desc["text"]
                major = dynamic.get("major") or {}
                draw = major.get("draw") or {}
                images = tuple(self.normalize_image_url(image["src"]) for image in draw.get("items", []))
                opus = major.get("opus")
                if opus is not None:
                    title = opus.get("title") or ""
                    summary = opus.get("summary") or {}
                    body_text = summary.get("text", "")
                    if not isinstance(title, str) or not isinstance(body_text, str):
                        raise ValueError("invalid opus text")
                    text = "\n".join(part for part in (title, body_text) if part)
                    images = tuple(self.normalize_image_url(image["url"]) for image in opus.get("pics", []))
                tag = modules.get("module_tag") or {}
                pinned = tag.get("text") == "置顶"
                timestamp = author["pub_ts"]
                if isinstance(timestamp, str) and timestamp.isascii() and timestamp.isdigit():
                    timestamp = int(timestamp)
        # 只取作者本人的正文与图片，绝不读取转发来源的内容。
                posts.append(DynamicPost(uid, item["id_str"], text, timestamp, images, pinned))
            unique = {post.id: post for post in posts}
            return sorted(unique.values(), key=lambda post: int(post.id))
        except (KeyError, TypeError, ValueError, AttributeError):
            logger.warning("Invalid Bilibili dynamic metadata uid=%s", uid)
            raise BiliError("Bilibili 动态字段缺失或无效，本轮不会推进检查点。") from None

    async def get_pinned_dynamic(self, uid):
        return next((p for p in reversed(await self.get_latest_dynamics(uid)) if p.is_pinned), None)

    def browser_cookies(self):
        """只把 Bilibili 的 Cookie 传进隔离的截图上下文，且绝不记录它们。"""
        return [{"name": cookie.name, "value": cookie.value, "domain": ".bilibili.com",
                 "path": "/", "secure": True} for cookie in self.http.cookies.jar
                if cookie.domain in (".bilibili.com", "bilibili.com")]

    @staticmethod
    def dynamic_url(post):
        if not isinstance(post, DynamicPost):
            raise ValueError("动态对象无效。")
        return f"https://t.bilibili.com/{post.id}"

    @staticmethod
    def preview_image_url(url):
        parts = urlsplit(BiliClient.normalize_image_url(url))
        if parts.scheme != "https" or not parts.hostname or not parts.hostname.endswith(".hdslb.com") or parts.username or parts.password or parts.port not in (None, 443):
            raise BiliError("图片必须来自 Bilibili HTTPS 图片 CDN。")
        return parts._replace(path=parts.path.split("@")[0] + "@1600w_90q.webp").geturl()

    @staticmethod
    def normalize_image_url(url):
        if not isinstance(url, str):
            raise ValueError("图片 URL 无效。")
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("http://"):
            url = "https://" + url[7:]
        return url

    async def download_image(self, url: str) -> bytes:
        """有大小上限的 CDN 下载：不跟随跳转，也不向 CDN 域名发送 Bilibili Cookie。"""
        url = self.normalize_image_url(url)
        parts = urlsplit(url)
        if (parts.scheme != "https" or not parts.hostname or
            not parts.hostname.endswith(".hdslb.com") or parts.username or parts.password or
            parts.port not in (None, 443)):
            raise BiliError("周表图片必须来自 Bilibili HTTPS 图片 CDN。")
        limit = 10 * 1024 * 1024
        for attempt in range(self.max_retry + 1):
            retry = True
            try:
                async with self.http.stream("GET", url) as response:
                    response.raise_for_status()
                    if response.headers.get("content-type", "").split(";")[0] not in (
                        "image/jpeg", "image/png", "image/webp"):
                        raise BiliError("周表图片类型不支持。")
                    content = bytearray()
                    async for chunk in response.aiter_bytes():
                        content.extend(chunk)
                        if len(content) > limit:
                            raise BiliError("周表图片超过 10 MiB 限制。")
                    raw = bytes(content)
                    if not (raw.startswith(b"\xff\xd8\xff") or raw.startswith(b"\x89PNG\r\n\x1a\n") or
                            (raw.startswith(b"RIFF") and raw[8:12] == b"WEBP")):
                        raise BiliError("周表图片内容格式异常。")
                    return raw
            except httpx.HTTPStatusError as exc:
                code = exc.response.status_code
                retry = code in (412, 429) or code >= 500
                error = BiliError(f"周表图片 HTTP 错误（{code}）。")
            except httpx.RequestError:
                error = BiliError("周表图片下载超时或网络失败。")
            except BiliError as exc:
                error, retry = exc, False
            logger.warning("Schedule image download failed attempt=%s: %s", attempt + 1, error)
            if not retry or attempt == self.max_retry:
                raise error from None
            await asyncio.sleep(min(2 ** attempt, 8))
