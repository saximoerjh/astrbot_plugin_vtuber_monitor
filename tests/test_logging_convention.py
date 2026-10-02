"""插件自己的日志必须走 astrbot.api.logger，而不是标准库 logging。"""
import re
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
# 唯一允许碰标准库 logging 的模块：它给 httpx 库自己的 logger 挂脱敏过滤器，
# 不是在输出插件日志（astrbot 的 logger 代理够不到第三方库的 logger）。
ALLOWED = {"httpx_logs.py"}
LOGGER_CALL = re.compile(r"\blog(?:ger)?\.(?:info|warning|error|exception|debug|critical)\(")


def plugin_sources():
    for path in PLUGIN_DIR.rglob("*.py"):
        if {"tests", "__pycache__"} & set(path.parts):
            continue
        yield path


def test_no_module_uses_stdlib_logging():
    offenders = []
    for path in plugin_sources():
        if path.name in ALLOWED:
            continue
        text = path.read_text(encoding="utf-8")
        if re.search(r"^\s*import logging\b", text, re.M) or "logging.getLogger(" in text:
            offenders.append(str(path.relative_to(PLUGIN_DIR)))
    assert offenders == []


def test_every_module_that_logs_imports_the_astrbot_logger():
    missing = []
    for path in plugin_sources():
        text = path.read_text(encoding="utf-8")
        if LOGGER_CALL.search(text) and not re.search(r"from astrbot\.api import [^\n]*\blogger\b", text):
            missing.append(str(path.relative_to(PLUGIN_DIR)))
    assert missing == []


def test_the_one_stdlib_logging_exception_still_redacts_login_urls():
    """这个例外必须真的摘掉扫码 URL 的 query，否则它会退化成纯泄漏。"""
    import logging

    import httpx

    from astrbot_plugin_vtuber_monitor.core.httpx_logs import LoginUrlRedactor, install

    install()  # 幂等：重复调用不会叠加过滤器
    install()
    record = logging.LogRecord("httpx", logging.INFO, __file__, 1, "HTTP Request: %s %s", None, None)
    record.args = (httpx.URL("https://passport.bilibili.com/qrcode/poll?qrcode_key=SECRET"), "HTTP/1.1")
    LoginUrlRedactor().filter(record)
    assert "SECRET" not in str(record.args)
    assert "passport.bilibili.com/qrcode/poll" in str(record.args)
    record.args = (httpx.URL("https://example.com/a?b=1"),)
    LoginUrlRedactor().filter(record)
    assert "b=1" in str(record.args)
