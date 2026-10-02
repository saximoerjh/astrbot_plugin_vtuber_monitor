"""把 docs/4016_monitor.png 做成插件图标 logo.png（WebUI 插件卡片用）。

用法：python tools/make_logo.py [输出路径]

AstrBot 的 WebUI 固定读插件根目录的 ``logo.png``（star_manager 里写死的文件名，
metadata.yaml 里没有对应字段），卡片按圆角方块展示。原图是 1254×1254 的方块画：
深蓝底 + 一台显示器里坐着主播，所以先把四角同色的留白按内容裁掉、再补一点点
边距，然后在 4 倍尺寸上做圆角，最后缩到 256。换素材改 SOURCE，换圆角改 RADIUS。
"""
import sys
from pathlib import Path

from PIL import Image, ImageChops, ImageDraw

PLUGIN_DIR = Path(__file__).resolve().parents[1]
SOURCE = PLUGIN_DIR / "docs" / "4016_monitor.png"
OUTPUT = 256
RADIUS = 56              # 圆角半径（最终尺寸下），卡片本身就是圆角方块
PADDING = 0.03           # 内容四周留一点底色，别贴到圆角边上
TOLERANCE = 18           # 与四角底色差多少算"内容"
SUPERSAMPLE = 4


def content_box(image, background):
    """找出与四角底色不同的范围，用来裁掉多余留白。"""
    flat = Image.new("RGB", image.size, background)
    grey = ImageChops.difference(image.convert("RGB"), flat).convert("L")
    return grey.point(lambda value: 255 if value > TOLERANCE else 0).getbbox()


def square_around(image, box, padding=PADDING):
    """围绕内容取一个正方形，边长含 padding，且不越界。"""
    left, top, right, bottom = box
    side = int(max(right - left, bottom - top) * (1 + 2 * padding))
    side = min(side, *image.size)
    center_x, center_y = (left + right) / 2, (top + bottom) / 2
    left = round(min(max(center_x - side / 2, 0), image.size[0] - side))
    top = round(min(max(center_y - side / 2, 0), image.size[1] - side))
    return (left, top, left + side, top + side)


def build():
    with Image.open(SOURCE) as raw:
        image = raw.convert("RGB")
        box = content_box(image, image.getpixel((1, 1))) or (0, 0, *image.size)
        square = image.crop(square_around(image, box))
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
