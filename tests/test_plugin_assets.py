"""WebUI 插件卡片固定读插件根目录的 logo.png，文件名在 star_manager 里写死。"""
import struct
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def test_webui_card_logo_is_a_square_png():
    raw = (PLUGIN_DIR / "logo.png").read_bytes()
    assert raw.startswith(PNG_SIGNATURE)
    width, height = struct.unpack(">II", raw[16:24])
    assert (width, height) == (256, 256)
    # 卡片按圆角方块展示，文件别太大，否则每次打开插件页都要多传一份。
    assert len(raw) < 256 * 1024
