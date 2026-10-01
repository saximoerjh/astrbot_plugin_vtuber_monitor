"""把配置界面分组，同时保留运行期稳定的扁平选项名。"""
import copy
import json
from pathlib import Path

MERGED_PROVIDER_KEY = "multimodal_provider_id"
LEGACY_PROVIDER_KEYS = ("schedule_provider_id", "adjustment_provider_id", "greeting_provider_id")
DISCOVERY_KEY = "schedule_scan_times"
LEGACY_DISCOVERY_KEYS = ("enable_dynamic_polling", "enable_schedule_processing",
                         "auto_discover_schedule")
MIDNIGHT_KEY = "enable_midnight_schedule_check"
ADJUSTMENT_KEY = "auto_adjustment_with_schedule"
LEGACY_ADJUSTMENT_KEY = "enable_adjustment_processing"


def merge_adjustment_switches(config):
    """把「手动启用调播处理」并入「有周表时自动调播」，只做一次。

    旧开关只在刻意关掉自动调播时才起作用，语义就是"要不要自动调播"，
    因此旧值为真时把新开关一起打开，随后清掉旧键。
    """
    if config.get("_adjustment_processing_merged", False):
        return False
    common = config.setdefault("common", {})
    schedule = config.setdefault("schedule", {})
    # 旧键历史上在 schedule 分组，也见过落在扁平槽位或 common 里的配置，
    # 三处都查一遍再统一清理。
    legacy = config.get(LEGACY_ADJUSTMENT_KEY)
    for holder in (schedule, common):
        if legacy is None:
            legacy = holder.get(LEGACY_ADJUSTMENT_KEY)
    if legacy:
        common[ADJUSTMENT_KEY] = True
    for holder in (common, schedule):
        holder.pop(LEGACY_ADJUSTMENT_KEY, None)
    config.pop(LEGACY_ADJUSTMENT_KEY, None)
    config["_adjustment_processing_merged"] = True
    return True


def migrate_scan_times(config):
    """把旧的周表发现开关迁移成扫描时间列表，只做一次。

    - 「每日零点检查周表」显式关过 → 迁移成空列表，保持"不做定时检查"。
    - 动态轮询/动态中发现周表已被定时扫描取代，旧键直接清掉；
      特别关注现在总是自动解析，旧开关的语义已被默认行为覆盖。
    """
    if config.get("_scan_times_migrated", False):
        return False
    common = config.setdefault("common", {})
    schedule = config.setdefault("schedule", {})
    legacy = config.get(MIDNIGHT_KEY)
    for holder in (common, schedule):
        if legacy is None:
            legacy = holder.get(MIDNIGHT_KEY)
    if legacy is False:
        schedule[DISCOVERY_KEY] = []
    common.pop(MIDNIGHT_KEY, None)
    schedule.pop(MIDNIGHT_KEY, None)
    config.pop(MIDNIGHT_KEY, None)
    for key in LEGACY_DISCOVERY_KEYS:
        schedule.pop(key, None)
        config.pop(key, None)
    config["_scan_times_migrated"] = True
    return True


def merge_provider_selection(config):
    """把历史上分开的三个模型选择合并成一个，只做一次。

    优先取周表识别 → 调播判断 → 凌晨问候里第一个非空的值（多数用户三者相同，
    或后两者留空表示复用），合并后删掉旧键，避免留下互相矛盾的模型入口。
    """
    if config.get("_provider_merged", False):
        return False
    common = config.setdefault("common", {})
    if not common.get(MERGED_PROVIDER_KEY):
        for key in LEGACY_PROVIDER_KEYS:
            value = common.get(key) or config.get(key)
            if isinstance(value, str) and value.strip():
                common[MERGED_PROVIDER_KEY] = value.strip()
                break
    for key in LEGACY_PROVIDER_KEYS:
        common.pop(key, None)
        config.pop(key, None)
    config["_provider_merged"] = True
    return True


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
    if merge_provider_selection(config) and callable(getattr(config, "save_config", None)):
        config.save_config()
    if migrate_scan_times(config) and callable(getattr(config, "save_config", None)):
        config.save_config()
    if merge_adjustment_switches(config) and callable(getattr(config, "save_config", None)):
        config.save_config()
    flat = {}
    for group, definition in groups.items():
        values = config.get(group, {})
        for key, field in definition["items"].items():
            flat[key] = copy.deepcopy(values.get(key, field["default"]))
    return flat
