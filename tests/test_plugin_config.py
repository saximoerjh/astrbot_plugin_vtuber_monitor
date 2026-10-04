import json
from pathlib import Path

from astrbot_plugin_vtuber_monitor.core.plugin_config import prepare_config, read_notification_flags


def test_notification_flags_are_read_live_without_touching_the_file(tmp_path):
    """发送通知前重读开关：只读不写，读不到就返回 None 由调用方兜底。"""
    path = tmp_path / "plugin_config.json"
    path.write_text(json.dumps({"notifications": {
        "normal_live_start_push": True, "normal_live_end_push": False,
        "special_live_start_push": True, "special_live_end_push": True}}), encoding="utf-8")
    before = path.read_text(encoding="utf-8")
    assert read_notification_flags(path) == {
        "normal_start": True, "normal_end": False, "special_start": True, "special_end": True}
    assert path.read_text(encoding="utf-8") == before       # 读取不能改写配置文件
    # 文件不存在或内容坏了：返回 None，让调用方用启动时的值。
    assert read_notification_flags(tmp_path / "missing.json") is None
    path.write_text("{ not json", encoding="utf-8")
    assert read_notification_flags(path) is None


def test_legacy_config_migrates_once_without_resetting_values():
    config = {"auto_special_live": False, "live_poll_interval": 240,
              "multimodal_provider_id": "my/provider", "request_timeout": 15,
              "schedule_keywords": ["custom"]}
    flat = prepare_config(config)
    assert flat["auto_special_live"] is False
    assert flat["live_poll_interval"] == 240
    assert flat["multimodal_provider_id"] == "my/provider"
    assert flat["schedule_keywords"] == ["custom"]
    assert config["advanced"]["request_timeout"] == 15
    config["live"]["live_poll_interval"] = 150
    assert prepare_config(config)["live_poll_interval"] == 150
    assert prepare_config(config)["request_timeout"] == 15


def test_three_provider_settings_merge_into_one_without_losing_the_value():
    """历史上分开的三个模型选择合并成一个；旧值按优先级保留，只处理一次。"""
    config = {"_grouped_config_migrated": True,
              "common": {"schedule_provider_id": "a/vision", "adjustment_provider_id": "b/tools",
                         "greeting_provider_id": "c/greet"}}
    flat = prepare_config(config)
    assert flat["multimodal_provider_id"] == "a/vision"
    assert config["_provider_merged"] is True
    assert "schedule_provider_id" not in config["common"]
    assert "adjustment_provider_id" not in config["common"]
    assert "greeting_provider_id" not in config["common"]
    assert not any(key in flat for key in ("schedule_provider_id", "adjustment_provider_id",
                                           "greeting_provider_id"))


def test_merge_falls_back_to_later_sources_and_keeps_existing_choice():
    only_adjustment = {"_grouped_config_migrated": True,
                       "common": {"schedule_provider_id": "", "adjustment_provider_id": "b/tools"}}
    assert prepare_config(only_adjustment)["multimodal_provider_id"] == "b/tools"
    # 已经有新键时不被旧键覆盖，且合并只做一次。
    already = {"_grouped_config_migrated": True, "_provider_merged": True,
               "common": {"multimodal_provider_id": "new/model",
                          "schedule_provider_id": "old/model"}}
    assert prepare_config(already)["multimodal_provider_id"] == "new/model"


def test_merge_reads_a_flat_legacy_value_before_the_group_migration():
    """从未做过分组迁移的老配置：值还在扁平槽位里，也必须被合并进去。"""
    config = {"schedule_provider_id": "flat/vision"}
    assert prepare_config(config)["multimodal_provider_id"] == "flat/vision"


def test_scan_times_replace_the_midnight_switch_and_clean_old_keys():
    """旧的"每日零点检查"与两个发现开关都换成扫描时间列表。"""
    config = {"_grouped_config_migrated": True,
              "common": {"enable_midnight_schedule_check": True},
              "schedule": {"enable_dynamic_polling": True, "enable_schedule_processing": True}}
    flat = prepare_config(config)
    assert flat["schedule_scan_times"] == ["00:30", "12:30", "20:30"]
    assert config["_scan_times_migrated"] is True
    for holder in (config["common"], config["schedule"]):
        assert not any(key in holder for key in (
            "enable_midnight_schedule_check", "enable_dynamic_polling",
            "enable_schedule_processing", "auto_discover_schedule"))
    assert not any(key in config for key in (
        "enable_midnight_schedule_check", "enable_dynamic_polling",
        "enable_schedule_processing", "auto_discover_schedule"))
    # 新键不受影响，二次读取不会因为旧值残留又变化。
    config["schedule"]["schedule_scan_times"] = ["08:00"]
    assert prepare_config(config)["schedule_scan_times"] == ["08:00"]


def test_midnight_switch_off_migrates_to_an_empty_time_list():
    config = {"_grouped_config_migrated": True,
              "common": {"enable_midnight_schedule_check": False},
              "schedule": {}}
    assert prepare_config(config)["schedule_scan_times"] == []


def test_manual_sessdata_config_is_removed_once():
    """手填 SESSDATA 的登录方式已拿掉，旧配置只清理一次。"""
    config = {"_grouped_config_migrated": True,
              "account": {"bilibili_sessdata": "legacy-value"},
              "bilibili_sessdata": "legacy-value"}
    flat = prepare_config(config)
    assert "bilibili_sessdata" not in flat and "account" not in config
    assert "bilibili_sessdata" not in config and config["_sessdata_removed"] is True
    # 二次读取不再改动，也不会把键加回来。
    assert "bilibili_sessdata" not in prepare_config(config)
    assert "account" not in config


def test_manual_adjustment_switch_merges_into_auto_adjustment():
    """手动调播入口并入自动调播；旧值为真时不能把自动调播留在关闭状态。"""
    # 旧键在 schedule 分组里（历史位置），新开关在 common 分组。
    config = {"_grouped_config_migrated": True,
              "common": {"auto_adjustment_with_schedule": False},
              "schedule": {"enable_adjustment_processing": True}}
    flat = prepare_config(config)
    assert flat["auto_adjustment_with_schedule"] is True
    assert config["_adjustment_processing_merged"] is True
    assert "enable_adjustment_processing" not in config["schedule"]
    assert "enable_adjustment_processing" not in config
    # 旧值本来就是关的，不会把用户显式关闭的自动调播打开。
    off = {"_grouped_config_migrated": True,
           "common": {"auto_adjustment_with_schedule": False},
           "schedule": {"enable_adjustment_processing": False}}
    assert prepare_config(off)["auto_adjustment_with_schedule"] is False
    # 扁平槽位里的旧值同样能被读到并清理。
    flat_slot = {"_grouped_config_migrated": True,
                 "common": {"auto_adjustment_with_schedule": False},
                 "enable_adjustment_processing": True}
    assert prepare_config(flat_slot)["auto_adjustment_with_schedule"] is True
    assert "enable_adjustment_processing" not in flat_slot


def test_grouped_schema_has_short_labels_and_all_legacy_fields_hidden():
    schema = json.loads((Path(__file__).parents[1] / "_conf_schema.json").read_text(encoding="utf-8"))
    groups = [value for value in schema.values() if value["type"] == "object"]
    assert len(groups) == 5
    for group in groups:
        for key, field in group["items"].items():
            assert len(field["description"]) <= 16 and field["hint"]
            assert schema[key]["invisible"]
            assert field["default"] == schema[key]["default"]
    assert schema["common"]["items"]["multimodal_provider_id"]["_special"] == "select_provider"
