"""生成插件图标 logo.png（WebUI 插件卡片用）。

用法：python tools/make_logo.py [输出路径]

AstrBot 的 WebUI 固定读插件根目录的 ``logo.png``（star_manager 里写死的文件名，
metadata.yaml 里没有对应字段），卡片按圆角方块展示，所以这里按 256×256 出图：
内部在 4 倍尺寸上绘制再缩小，边缘不糊。改成你的配色只要调下面几个常量。
"""
import sys
from pathlib import Path

from PIL import Image, ImageDraw

SIZE = 1024          # 绘制画布，最后缩到 256
OUTPUT = 256
BACKDROP = ((76, 74, 158), (155, 92, 214))   # 背景渐变：靛蓝 → 紫
CARD = (255, 255, 255)
DAY = (216, 215, 245)
ACCENT = (251, 114, 153)                     # B 站粉


def gradient(size, start, end):
    image = Image.new("RGB", (size, size))
    draw = ImageDraw.Draw(image)
    for y in range(size):
        ratio = y / (size - 1)
        draw.line([(0, y), (size, y)],
                  fill=tuple(round(a + (b - a) * ratio) for a, b in zip(start, end)))
    return image


def build():
    canvas = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    backdrop = Image.new("L", (SIZE, SIZE), 0)
    ImageDraw.Draw(backdrop).rounded_rectangle([0, 0, SIZE - 1, SIZE - 1], radius=224, fill=255)
    canvas.paste(gradient(SIZE, *BACKDROP), (0, 0), backdrop)

    draw = ImageDraw.Draw(canvas)
    # 日历卡片 + 顶部色带
    draw.rounded_rectangle([168, 236, 856, 812], radius=72, fill=CARD)
    draw.rounded_rectangle([168, 236, 856, 372], radius=72, fill=ACCENT)
    draw.rectangle([168, 308, 856, 372], fill=ACCENT)
    # 六个“日子”，其中一个高亮
    for row in range(2):
        for column in range(3):
            left, top = 256 + column * 196, 420 + row * 208
            cell = [left, top, left + 120, top + 120]
            draw.rounded_rectangle(cell, radius=36,
                                   fill=ACCENT if (row, column) == (1, 1) else DAY)
    # 右下角的直播徽标（白色描边把它和卡片分开）
    draw.ellipse([668, 640, 972, 944], fill=CARD)
    draw.ellipse([680, 652, 960, 932], fill=ACCENT)
    draw.polygon([(772, 726), (772, 858), (884, 792)], fill=CARD)
    return canvas.resize((OUTPUT, OUTPUT), Image.LANCZOS)


if __name__ == "__main__":
    target = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else \
        Path(__file__).resolve().parents[1] / "logo.png"
    build().save(target, format="PNG", optimize=True)
    print(f"{target} {target.stat().st_size // 1024}KiB")
