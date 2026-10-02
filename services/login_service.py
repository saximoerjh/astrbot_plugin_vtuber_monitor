"""同一时间只允许一个管理员私聊扫码登录；消息与日志里不得出现凭据。"""
import asyncio
import time

from astrbot.api import logger

from ..bili_client import BiliClient, BiliError


class LoginService:
    def __init__(self, data, bili, notify, *, client_factory=BiliClient, interval=2, lifetime=180):
        self.data, self.bili, self.notify = data, bili, notify
        self.client_factory = client_factory
        self.interval, self.lifetime = interval, lifetime
        self.task = None
        self._lock = asyncio.Lock()
        self._closed = False

    async def start(self, umo, send_image):
        async with self._lock:
            if self._closed:
                raise BiliError("插件正在停止，请重载后重试。")
            if self.task is not None and not self.task.done():
                raise BiliError("已有登录二维码等待确认，请完成登录或等待过期后重试。")
            client = self.client_factory()
            path = None
            try:
                url, key = await client.generate_login_qr()
                path = await self.data.create_login_qr_image(url)
                await asyncio.wait_for(send_image(path), 20)
                self.task = asyncio.create_task(self._poll(client, key, path, umo), name="vtuber-login")
            except BaseException:
                await client.close()
                if path:
                    await self.data.remove_login_qr_image(path)
                raise

    async def _poll(self, client, key, path, umo):
        deadline = time.monotonic() + self.lifetime
        scanned = False
        try:
            while time.monotonic() < deadline:
                code, credentials = await client.poll_login_qr(key)
                if code == 0:
                    await self.data.save_credentials(credentials)
                    self.bili.set_credentials(credentials)
                    await self.notify(umo, "Bilibili 登录成功，凭据已保存并立即生效。可使用 /vt_latest 1512246445 验证动态。")
                    return
                if code == 86038:
                    break
                if code == 86090 and not scanned:
                    scanned = True
                    await self.notify(umo, "二维码已扫描，请在 Bilibili App 中确认登录。")
                await asyncio.sleep(self.interval)
            await self.notify(umo, "登录二维码已过期，请重新发送 /bili_login。")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("Bilibili QR login failed; existing credentials retained unless already saved")
            await self.notify(umo, "扫码登录未完成，请稍后重新发送 /bili_login；未返回任何凭据到聊天。")
        finally:
            await client.close()
            await self.data.remove_login_qr_image(path)

    async def close(self):
        async with self._lock:
            self._closed = True
            if self.task:
                self.task.cancel()
                await asyncio.gather(self.task, return_exceptions=True)
                self.task = None
