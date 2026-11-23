"""从 docs/monitor.png 裁出头像，生成 README 和 AstrBot WebUI 共用的 logo.png。

用法：python tools/make_logo.py [输出路径]
"""

import sys
from pathlib import Path

from PIL import Image, ImageDraw

PLUGIN_DIR = Path(__file__).resolve().parents[1]
SOURCE = PLUGIN_DIR / "docs" / "monitor.png"
# 围绕面部取正方形，保留发顶和兔子，裁去原图上方大片留白。
CROP_BOX = (480, 1350, 1648, 2518)
OUTPUT = 256
RADIUS = 56
SUPERSAMPLE = 4


def build():
    with Image.open(SOURCE) as raw:
        square = raw.crop(CROP_BOX).convert("RGB")
    side = OUTPUT * SUPERSAMPLE
    square = square.resize((side, side), Image.LANCZOS)
    canvas = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    mask = Image.new("L", (side, side), 0)
    ImageDraw.Draw(mask).rounded_rectangle(
        [0, 0, side - 1, side - 1], radius=RADIUS * SUPERSAMPLE, fill=255
    )
    canvas.paste(square, (0, 0), mask)
    return canvas.resize((OUTPUT, OUTPUT), Image.LANCZOS)


if __name__ == "__main__":
    target = (
        Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else PLUGIN_DIR / "logo.png"
    )
    build().save(target, format="PNG", optimize=True)
    print(f"{target} {target.stat().st_size // 1024}KiB")
