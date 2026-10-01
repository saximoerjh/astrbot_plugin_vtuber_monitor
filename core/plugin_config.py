"""把配置界面分组，同时保留运行期稳定的扁平选项名。"""
import copy
import json
from pathlib import Path

MERGED_PROVIDER_KEY = "multimodal_provider_id"
LEGACY_PROVIDER_KEYS = ("schedule_provider_id", "adjustment_provider_id", "greeting_provider_id")
DISCOVERY_KEY = "auto_discover_schedule"
LEGACY_DISCOVERY_KEYS = ("enable_dynamic_polling", "enable_schedule_processing")
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


def merge_discovery_switches(config):
    """把「完整动态轮询」与「动态中发现周表」合成一个开关，只做一次。

    两个旧开关必须同时打开才有意义：一个决定要不要在动态里找周表图，另一个
    决定找的范围要不要包含还没有周表的主播。任一为真即视为开启新的合并开关，
    等价于旧配置里"两个都开"的行为。
    """
    if config.get("_schedule_discovery_merged", False):
        return False
    schedule = config.setdefault("schedule", {})
    if not schedule.get(DISCOVERY_KEY):
        if any(schedule.get(key) or config.get(key) for key in LEGACY_DISCOVERY_KEYS):
            schedule[DISCOVERY_KEY] = True
    for key in LEGACY_DISCOVERY_KEYS:
        schedule.pop(key, None)
        config.pop(key, None)
    config["_schedule_discovery_merged"] = True
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
    if merge_discovery_switches(config) and callable(getattr(config, "save_config", None)):
        config.save_config()
    if merge_adjustment_switches(config) and callable(getattr(config, "save_config", None)):
        config.save_config()
    flat = {}
    for group, definition in groups.items():
        values = config.get(group, {})
        for key, field in definition["items"].items():
            flat[key] = copy.deepcopy(values.get(key, field["default"]))
    return flat
