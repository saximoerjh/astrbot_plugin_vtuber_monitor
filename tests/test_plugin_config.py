import json
from pathlib import Path

from astrbot_plugin_vtuber_monitor.core.plugin_config import prepare_config


def test_legacy_config_migrates_once_without_resetting_values():
    config = {"auto_special_live": False, "live_poll_interval": 240,
              "multimodal_provider_id": "my/provider", "bilibili_sessdata": "test-only",
              "schedule_keywords": ["custom"]}
    flat = prepare_config(config)
    assert flat["auto_special_live"] is False
    assert flat["live_poll_interval"] == 240
    assert flat["multimodal_provider_id"] == "my/provider"
    assert flat["schedule_keywords"] == ["custom"]
    assert config["account"]["bilibili_sessdata"] == "test-only"
    assert config["bilibili_sessdata"] == ""
    config["live"]["live_poll_interval"] = 150
    assert prepare_config(config)["live_poll_interval"] == 150
    assert prepare_config(config)["bilibili_sessdata"] == "test-only"


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


def test_grouped_schema_has_short_labels_and_all_legacy_fields_hidden():
    schema = json.loads((Path(__file__).parents[1] / "_conf_schema.json").read_text(encoding="utf-8"))
    groups = [value for value in schema.values() if value["type"] == "object"]
    assert len(groups) == 6
    for group in groups:
        for key, field in group["items"].items():
            assert len(field["description"]) <= 16 and field["hint"]
            assert schema[key]["invisible"]
            assert field["default"] == schema[key]["default"]
    assert schema["common"]["items"]["multimodal_provider_id"]["_special"] == "select_provider"
    assert schema["account"]["items"]["bilibili_sessdata"]["secret"]
