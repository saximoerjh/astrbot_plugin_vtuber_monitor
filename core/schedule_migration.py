"""从既有修订记录中还原调播前的原定时间，不做任何猜测。"""
import hashlib
import json
from dataclasses import asdict

from .schedule_models import StreamPlan


def migrate_schedule_times(db):
    db.execute("CREATE TABLE IF NOT EXISTS plugin_migrations (name TEXT PRIMARY KEY)")
    if db.execute("SELECT 1 FROM plugin_migrations WHERE name='three_times_v1'").fetchone():
        return
    originals = {}
    for row in db.execute("SELECT uid,old_value,new_value FROM schedule_revisions ORDER BY id"):
        for raw in (row["old_value"], row["new_value"]):
            if not raw:
                continue
            value = json.loads(raw)
            for plan in value["streams"]:
                key = (row["uid"], value["week_start"], plan["id"])
            # 只有来自周表图片的快照才能作为旧计划的依据。
                if plan.get("source", "weekly_image") == "weekly_image":
                    originals.setdefault(key, (plan["date"], plan["start_time"], plan.get("original_end_time")))
    for table, identity in (("weekly_schedule_archive", "id"), ("weekly_schedules", "uid")):
        for row in list(db.execute(f"SELECT {identity},payload FROM {table}")):
            value = json.loads(row["payload"])
            changed = False
            for index, plan in enumerate(value["streams"]):
                if "original_date" in plan:
                    continue
                key = (value["uid"], value["week_start"], plan["id"])
                original = originals.get(key)
                if original is None:
                    original = (plan["date"], plan["start_time"] if plan.get("source", "weekly_image") == "weekly_image" else None, None)
                item = {**plan, "original_date": original[0], "original_start_time": original[1], "original_end_time": original[2]}
                if plan["start_time"] and (plan["date"], plan["start_time"]) != original[:2]:
                    item.update(rescheduled_date=plan["date"], rescheduled_start_time=plan["start_time"])
                value["streams"][index] = asdict(StreamPlan(**item))
                changed = True
            if changed:
                payload = json.dumps(value, ensure_ascii=False)
                if table == "weekly_schedule_archive":
                    db.execute("UPDATE weekly_schedule_archive SET payload=?,content_hash=? WHERE id=?",
                               (payload, hashlib.sha256(payload.encode()).hexdigest(), row[identity]))
                else:
                    db.execute("UPDATE weekly_schedules SET payload=? WHERE uid=?", (payload, row[identity]))
    db.execute("INSERT INTO plugin_migrations VALUES ('three_times_v1')")
