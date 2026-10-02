import logging
import sys
from pathlib import Path
from types import ModuleType

# 以插件包形式导入，不引入 AstrBot 运行时。
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# 插件代码按 AstrBot 规范从 astrbot.api 取 logger；单测环境不装 AstrBot，
# 这里补一个等价的替身，让模块能在包内被直接导入。
if "astrbot.api" not in sys.modules:
    astrbot = ModuleType("astrbot")
    api = ModuleType("astrbot.api")
    api.logger = logging.getLogger("astrbot_plugin_vtuber_monitor")
    astrbot.api = api
    sys.modules["astrbot"] = astrbot
    sys.modules["astrbot.api"] = api
