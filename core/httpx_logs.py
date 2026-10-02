"""给 httpx 的标准库 logger 挂一个脱敏过滤器。

插件自身的日志一律走 ``from astrbot.api import logger``；这里是唯一需要标准库
``logging`` 的地方，而且**不是为了输出日志**：httpx 库自己用标准库 logging 打
请求日志，INFO 级别会带上完整 URL，而扫码登录的 URL 里含 key 与跨域票据，
只能通过给那个 logger 挂 Filter 才能摘掉。astrbot 的 logger 代理够不到它。
"""
import logging

import httpx

# 只有这些主机的 URL 会被摘掉 query。
SENSITIVE_HOSTS = ("passport.bilibili.com", "passport.biligame.com",
                   "account.bilibili.com")


class LoginUrlRedactor(logging.Filter):
    """httpx 的 INFO 日志通常包含扫码密钥与跨域票据。"""

    def filter(self, record):
        if isinstance(record.args, tuple):
            record.args = tuple(
                value.copy_with(query=None)
                if isinstance(value, httpx.URL) and value.host in SENSITIVE_HOSTS else value
                for value in record.args)
        return True


def install():
    """幂等安装：重复导入插件不会叠加同一个过滤器。"""
    target = logging.getLogger("httpx")
    if not any(isinstance(item, LoginUrlRedactor) for item in target.filters):
        target.addFilter(LoginUrlRedactor())
