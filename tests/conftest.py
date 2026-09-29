import sys
from pathlib import Path

# 以插件包形式导入，不引入 AstrBot 运行时。
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
