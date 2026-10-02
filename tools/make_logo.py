"""把 docs/4016.png 裁成插件图标 logo.png（WebUI 插件卡片用）。

用法：python tools/make_logo.py [输出路径]

AstrBot 的 WebUI 固定读插件根目录的 ``logo.png``（star_manager 里写死的文件名，
metadata.yaml 里没有对应字段），卡片按圆角方块展示。原图 1448×1086 是半身像，
直接缩到 256 会糊成一团，所以这里裁成以脸部为中心的正方形、在 4 倍尺寸上做圆角，
最后缩到 256。换构图改 CROP，换圆角改 RADIUS。
"""
import sys
from pathlib import Path

from PIL import Image, ImageDraw

PLUGIN_DIR = Path(__file__).resolve().parents[1]
SOURCE = PLUGIN_DIR / "docs" / "4016.png"
# 原图坐标：留出头顶与红色围巾，脸部落在正中偏上。
CROP = (250, 30, 1170, 950)
OUTPUT = 256
RADIUS = 56          # 圆角半径（最终尺寸下），卡片本身就是圆角方块
SUPERSAMPLE = 4


def build():
    with Image.open(SOURCE) as image:
        square = image.convert("RGB").crop(CROP)
    side = OUTPUT * SUPERSAMPLE
    square = square.resize((side, side), Image.LANCZOS)
    canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    mask = Image.new("L", (side, side), 0)
    ImageDraw.Draw(mask).rounded_rectangle([0, 0, side - 1, side - 1],
                                           radius=RADIUS * SUPERSAMPLE, fill=255)
    canvas.paste(square, (0, 0), mask)
    return canvas.resize((OUTPUT, OUTPUT), Image.LANCZOS)


if __name__ == "__main__":
    target = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else PLUGIN_DIR / "logo.png"
    build().save(target, format="PNG", optimize=True)
    print(f"{target} {target.stat().st_size // 1024}KiB")
