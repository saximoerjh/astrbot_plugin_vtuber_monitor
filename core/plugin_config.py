"""把配置界面分组，同时保留运行期稳定的扁平选项名。"""
import copy
import json
from pathlib import Path


def prepare_config(config):
    schema = json.loads((Path(__file__).resolve().parents[1] / "_conf_schema.json").read_text(encoding="utf-8"))
    groups = {key: item for key, item in schema.items() if item["type"] == "object"}
    if not config.get("_grouped_config_migrated", False):
        for group, definition in groups.items():
            values = config.setdefault(group, {})
            for key, field in definition["items"].items():
                # AstrBot 会在插件初始化前补齐分组默认值，
                # 隐藏的旧字段保留真实旧值，直到这次一次性迁移搬走。
                if key in config:
                    values[key] = copy.deepcopy(config[key])
                else:
                    values.setdefault(key, copy.deepcopy(field["default"]))
                # 兼容用的隐藏槽位只保留结构里的默认值。
                config[key] = copy.deepcopy(field["default"])
        config["_grouped_config_migrated"] = True
        if callable(getattr(config, "save_config", None)):
            config.save_config()
    flat = {}
    for group, definition in groups.items():
        values = config.get(group, {})
        for key, field in definition["items"].items():
            flat[key] = copy.deepcopy(values.get(key, field["default"]))
    return flat
