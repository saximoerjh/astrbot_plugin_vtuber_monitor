import json
from pathlib import Path

from astrbot_plugin_vtuber_monitor.core.plugin_config import prepare_config


def test_legacy_config_migrates_once_without_resetting_values():
    config = {"auto_special_live": False, "live_poll_interval": 240,
              "schedule_provider_id": "my/provider", "bilibili_sessdata": "test-only",
              "schedule_keywords": ["custom"]}
    flat = prepare_config(config)
    assert flat["auto_special_live"] is False
    assert flat["live_poll_interval"] == 240
    assert flat["schedule_provider_id"] == "my/provider"
    assert flat["schedule_keywords"] == ["custom"]
    assert config["account"]["bilibili_sessdata"] == "test-only"
    assert config["bilibili_sessdata"] == ""
    config["live"]["live_poll_interval"] = 150
    assert prepare_config(config)["live_poll_interval"] == 150
    assert prepare_config(config)["bilibili_sessdata"] == "test-only"


def test_grouped_schema_has_short_labels_and_all_legacy_fields_hidden():
    schema = json.loads((Path(__file__).parents[1] / "_conf_schema.json").read_text(encoding="utf-8"))
    groups = [value for value in schema.values() if value["type"] == "object"]
    assert len(groups) == 6
    for group in groups:
        for key, field in group["items"].items():
            assert len(field["description"]) <= 16 and field["hint"]
            assert schema[key]["invisible"]
            assert field["default"] == schema[key]["default"]
    assert schema["common"]["items"]["schedule_provider_id"]["_special"] == "select_provider"
    assert schema["account"]["items"]["bilibili_sessdata"]["secret"]
