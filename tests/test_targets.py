import asyncio
import sqlite3
from contextlib import closing
from unittest.mock import AsyncMock

import pytest

from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
from astrbot_plugin_vtuber_monitor.core.models import VtuberState
from astrbot_plugin_vtuber_monitor.services.subscription_service import SubscriptionService
from astrbot_plugin_vtuber_monitor.services.target_service import TargetService, normalize_alias


@pytest.mark.parametrize("alias", ["", "123", "１２３", "two words", "a/b", "x" * 25, "a\nname"])
def test_alias_validation(alias):
    with pytest.raises(ValueError):
        normalize_alias(alias)


@pytest.mark.asyncio
async def test_mapping_persistence_alias_and_restart(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    bili = AsyncMock()
    bili.get_user_info.return_value = VtuberState(1512246445, "四时小路Komichi")
    service = SubscriptionService(data, bili)
    await service.subscribe(1512246445, "group", user_id="alice")
    targets = TargetService(data)
    await targets.set_alias("1512246445", "小路", "group", "alice")
    assert await targets.resolve("小路", "group", "alice") == 1512246445
    assert await targets.resolve("四时小路Komichi", "group", "alice") == 1512246445
    reopened = DataManager(tmp_path)
    await reopened.initialize()
    assert await TargetService(reopened).resolve("", "group", "alice") == 1512246445
    assert (await reopened.get_target_mappings("group", "alice"))[0]["personal"] == 1
    assert "小路（UID 1512246445）" in await targets.format_list("group", "alice")
    await targets.set_alias("小路", "路", "group", "alice")
    assert await targets.resolve("小路", "group", "alice") == 1512246445
    assert await targets.resolve("路", "group", "alice") == 1512246445
    assert await targets.resolve("", "group", "alice") == 1512246445
    assert await targets.resolve("四时小路Komichi", "group", "alice") == 1512246445
    assert "小路 / 路（UID 1512246445）" in await targets.format_list("group", "alice")
    assert await targets.remove_alias("路", "group", "alice")
    assert await targets.resolve("小路", "group", "alice") == 1512246445
    with pytest.raises(ValueError):
        await targets.resolve("路", "group", "alice")
    assert await data.get_subscription(1512246445, "group") is not None


@pytest.mark.asyncio
async def test_user_and_session_isolation_ambiguity_and_collisions(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    for uid in (1, 2):
        await data.add_subscription(VtuberState(uid, f"主播{uid}"), "group", user_id="alice")
        await data.add_subscription(VtuberState(uid, f"主播{uid}"), "private", user_id="alice")
    targets = TargetService(data)
    await targets.set_alias(1, "小路", "group", "alice")
    await targets.set_alias(2, "小路", "group", "bob")
    await targets.set_alias(2, "小路", "private", "alice")
    assert await targets.resolve("小路", "group", "alice") == 1
    assert await targets.resolve("小路", "group", "bob") == 2
    assert await targets.resolve("小路", "private", "alice") == 2
    assert await targets.resolve("", "group", "bob") == 2
    with pytest.raises(ValueError, match="多个"):
        await targets.resolve("", "group", "alice")
    with pytest.raises(ValueError, match="已用于"):
        await targets.set_alias(2, "小路", "group", "alice")
    with pytest.raises(ValueError, match="冲突"):
        await targets.set_alias(1, "主播2", "group", "alice")
    with pytest.raises(ValueError, match="先.*订阅"):
        await targets.set_alias(999, "路", "group", "alice")


@pytest.mark.asyncio
async def test_unsubscribe_retains_alias_but_removes_active_user_mapping(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    await data.add_subscription(VtuberState(1, "主播"), "g", user_id="alice")
    targets = TargetService(data)
    await targets.set_alias(1, "路", "g", "alice")
    await data.remove_subscription(1, "g")
    assert await data.get_target_mappings("g", "alice") == []
    assert await targets.resolve("路", "g", "alice") == 1
    with pytest.raises(ValueError, match="暂无订阅"):
        await targets.resolve("", "g", "alice")


@pytest.mark.asyncio
async def test_concurrent_case_insensitive_collision_and_legacy_names(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    for uid in (1, 2):
        await data.add_subscription(VtuberState(uid, "同名"), "g")
    targets = TargetService(data)
    results = await asyncio.gather(targets.set_alias(1, "Komichi", "g", "alice"),
                                   targets.set_alias(2, "KOMICHI", "g", "alice"), return_exceptions=True)
    assert sum(isinstance(result, ValueError) for result in results) == 1
    assert await targets.resolve("ｋｏｍｉｃｈｉ", "g", "alice") in (1, 2)
    with pytest.raises(ValueError, match="重名"):
        await targets.resolve("同名", "g", "alice")


@pytest.mark.asyncio
async def test_multi_alias_migration_preserves_old_data_and_is_repeatable(tmp_path):
    with closing(sqlite3.connect(tmp_path / "monitor.sqlite3")) as db:
        db.execute("""CREATE TABLE streamer_aliases (
            umo TEXT NOT NULL, user_id TEXT NOT NULL, uid INTEGER NOT NULL,
            alias TEXT NOT NULL, alias_key TEXT NOT NULL,
            PRIMARY KEY(umo, user_id, uid), UNIQUE(umo, user_id, alias_key))""")
        db.executemany("INSERT INTO streamer_aliases VALUES (?, ?, ?, ?, ?)",
                       [("g", "alice", 1, "小路", "小路"), ("g", "bob", 1, "路", "路"),
                        ("private", "alice", 2, "路", "路")])
        db.commit()
    data = DataManager(tmp_path)
    await data.initialize()
    await data.initialize()
    await data.add_subscription(VtuberState(1, "主播"), "g", user_id="alice")
    targets = TargetService(data)
    assert await targets.resolve("小路", "g", "alice") == 1
    await targets.set_alias(1, "Komichi", "g", "alice")
    reopened = DataManager(tmp_path)
    await reopened.initialize()
    restored = TargetService(reopened)
    assert await restored.resolve("小路", "g", "alice") == 1
    assert await restored.resolve("komichi", "g", "alice") == 1
    assert await restored.resolve("路", "g", "bob") == 1
    assert await restored.resolve("路", "private", "alice") == 2
    assert len(await reopened.get_target_mappings("g", "alice")) == 1


@pytest.mark.asyncio
async def test_multi_alias_duplicate_concurrency_and_precise_deletion(tmp_path):
    data = DataManager(tmp_path)
    await data.initialize()
    for uid in (1, 2):
        await data.add_subscription(VtuberState(uid, f"主播{uid}"), "g", user_id="alice")
    targets = TargetService(data)
    await asyncio.gather(targets.set_alias(1, "路", "g", "alice"), targets.set_alias(1, "Komichi", "g", "alice"))
    await targets.set_alias(1, "ＫＯＭＩＣＨＩ", "g", "alice")
    assert len(await data.get_aliases("g", "alice")) == 2
    with pytest.raises(ValueError, match="多个别名"):
        await targets.remove_alias(1, "g", "alice")
    await targets.set_alias(2, "二号", "g", "alice")
    assert not await targets.remove_alias(1, "g", "alice", "二号")
    assert await targets.resolve("二号", "g", "alice") == 2
    assert await targets.remove_alias(1, "g", "alice", "komichi")
    assert await targets.resolve("路", "g", "alice") == 1
    assert await targets.remove_alias(1, "g", "alice")
    assert not await targets.remove_alias(1, "g", "alice")
    assert await data.get_subscription(1, "g") is not None
