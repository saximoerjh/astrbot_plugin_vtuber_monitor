"""唯一的持久化边界，每次写入都在 SQLite 事务内完成。"""
import asyncio
import json
import hashlib
import os
import sqlite3
from pathlib import Path

from .models import DynamicPost, FollowLevel, Subscription, VtuberState, utc_now, validate_uid, validate_umo

# live_sessions.synced 是一个小状态机：待落位、已写入周表，
# 或因为永远无法确定开播时间而退役。
LIVE_SESSION_PENDING = 0
LIVE_SESSION_RECORDED = 1
LIVE_SESSION_SKIPPED = 2


class DataManager:
    def __init__(self, data_dir: Path):
        self.path = Path(data_dir) / "monitor.sqlite3"

    def _run(self, operation):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                return operation(db)
        finally:
            db.close()

    async def initialize(self):
        def setup():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._run(lambda db: db.executescript("""
                CREATE TABLE IF NOT EXISTS vtubers (
                    uid INTEGER PRIMARY KEY, name TEXT NOT NULL,
                    room_id INTEGER NOT NULL, is_live INTEGER
                );
                CREATE TABLE IF NOT EXISTS schedule_tracking (
                    uid INTEGER PRIMARY KEY, payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS live_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER NOT NULL,
                    start_time TEXT, end_time TEXT, observed_at TEXT NOT NULL,
                    start_basis TEXT NOT NULL, week_start TEXT, stream_id TEXT,
                    synced INTEGER NOT NULL DEFAULT 0, title TEXT NOT NULL DEFAULT ''
                );
                CREATE UNIQUE INDEX IF NOT EXISTS live_sessions_open
                    ON live_sessions(uid) WHERE end_time IS NULL;
                CREATE TABLE IF NOT EXISTS subscriptions (
                    uid INTEGER NOT NULL, umo TEXT NOT NULL,
                    level TEXT NOT NULL CHECK(level IN ('normal', 'special')),
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    PRIMARY KEY(uid, umo)
                );
                CREATE TABLE IF NOT EXISTS dynamic_checkpoints (
                    uid INTEGER PRIMARY KEY, latest_id TEXT NOT NULL,
                    checked_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS dynamic_posts (
                    uid INTEGER NOT NULL, dynamic_id TEXT NOT NULL,
                    payload TEXT NOT NULL, is_baseline INTEGER NOT NULL,
                    PRIMARY KEY(uid, dynamic_id)
                );
                CREATE TABLE IF NOT EXISTS schedule_candidates (
                    uid INTEGER NOT NULL, dynamic_id TEXT NOT NULL, image_url TEXT NOT NULL,
                    payload TEXT NOT NULL, PRIMARY KEY(uid, dynamic_id, image_url)
                );
                CREATE TABLE IF NOT EXISTS weekly_schedules (
                    uid INTEGER PRIMARY KEY, payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS schedule_revisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER NOT NULL,
                    old_value TEXT, new_value TEXT NOT NULL, source_dynamic_id TEXT NOT NULL,
                    reason TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS login_credentials (
                    id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS weekly_schedule_archive (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER NOT NULL,
                    week_start TEXT NOT NULL, content_hash TEXT NOT NULL,
                    payload TEXT NOT NULL, archived_at TEXT NOT NULL,
                    UNIQUE(uid, week_start, content_hash)
                );
                CREATE TABLE IF NOT EXISTS subscription_users (
                    umo TEXT NOT NULL, user_id TEXT NOT NULL, uid INTEGER NOT NULL,
                    PRIMARY KEY(umo, user_id, uid)
                );
                CREATE TABLE IF NOT EXISTS streamer_aliases (
                    umo TEXT NOT NULL, user_id TEXT NOT NULL, uid INTEGER NOT NULL,
                    alias TEXT NOT NULL, alias_key TEXT NOT NULL,
                    PRIMARY KEY(umo, user_id, alias_key)
                );
                CREATE TABLE IF NOT EXISTS schedule_imports (
                    uid INTEGER NOT NULL, fingerprint TEXT NOT NULL, PRIMARY KEY(uid, fingerprint)
                );
                CREATE TABLE IF NOT EXISTS schedule_image_classifications (
                    uid INTEGER NOT NULL, dynamic_id TEXT NOT NULL, image_url TEXT NOT NULL,
                    text_fingerprint TEXT NOT NULL, is_schedule INTEGER NOT NULL,
                    PRIMARY KEY(uid, dynamic_id, image_url, text_fingerprint)
                );
                CREATE TABLE IF NOT EXISTS adjustment_jobs (
                    uid INTEGER NOT NULL, dynamic_id TEXT NOT NULL, payload TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'pending',
                    PRIMARY KEY(uid, dynamic_id)
                );
                CREATE TABLE IF NOT EXISTS schedule_operations (
                    uid INTEGER NOT NULL, operation_id TEXT NOT NULL, PRIMARY KEY(uid, operation_id)
                );
                CREATE TABLE IF NOT EXISTS schedule_outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER NOT NULL,
                    umo TEXT NOT NULL, kind TEXT NOT NULL, message TEXT NOT NULL,
                    image_path TEXT NOT NULL DEFAULT '',
                    attempts INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'pending'
                );
                CREATE TABLE IF NOT EXISTS greeting_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT NOT NULL,
                    picked_at TEXT NOT NULL
                );
            """))
            def migrate(db):
                db.execute("BEGIN IMMEDIATE")
                columns = {row["name"] for row in db.execute("PRAGMA table_info(vtubers)")}
                if "last_live_change_at" not in columns:
                    db.execute("ALTER TABLE vtubers ADD COLUMN last_live_change_at TEXT")
                session_columns = {row["name"] for row in db.execute("PRAGMA table_info(live_sessions)")}
                if "title" not in session_columns:
                    db.execute("ALTER TABLE live_sessions ADD COLUMN title TEXT NOT NULL DEFAULT ''")
                outbox_columns = {row["name"] for row in db.execute("PRAGMA table_info(schedule_outbox)")}
                if "image_path" not in outbox_columns:
                    db.execute("ALTER TABLE schedule_outbox ADD COLUMN image_path TEXT NOT NULL DEFAULT ''")
                alias_columns = list(db.execute("PRAGMA table_info(streamer_aliases)"))
                if any(row["name"] == "uid" and row["pk"] for row in alias_columns):
                    db.execute("""CREATE TABLE streamer_aliases_multi (
                        umo TEXT NOT NULL, user_id TEXT NOT NULL, uid INTEGER NOT NULL,
                        alias TEXT NOT NULL, alias_key TEXT NOT NULL,
                        PRIMARY KEY(umo, user_id, alias_key))""")
                    db.execute("INSERT INTO streamer_aliases_multi SELECT umo, user_id, uid, alias, alias_key FROM streamer_aliases")
                    db.execute("DROP TABLE streamer_aliases")
                    db.execute("ALTER TABLE streamer_aliases_multi RENAME TO streamer_aliases")
                db.execute("CREATE INDEX IF NOT EXISTS aliases_by_target ON streamer_aliases(umo, user_id, uid)")
                from .schedule_migration import migrate_schedule_times
                migrate_schedule_times(db)
            self._run(migrate)
        await asyncio.to_thread(setup)

    async def add_subscription(self, state: VtuberState, umo: str,
                               level: FollowLevel = FollowLevel.NORMAL,
                               initial_dynamics=None, user_id="") -> Subscription:
        validate_umo(umo)
        level = FollowLevel(level)
        now = utc_now()
        if initial_dynamics is not None:
            self._validate_posts(state.uid, initial_dynamics)

        def write(db):
            db.execute("BEGIN IMMEDIATE")
            active = db.execute("SELECT 1 FROM subscriptions WHERE uid=? LIMIT 1",
                                (state.uid,)).fetchone()
            special = db.execute("SELECT 1 FROM subscriptions WHERE uid=? AND level='special' LIMIT 1",
                                 (state.uid,)).fetchone()
            if level == FollowLevel.SPECIAL and special is None:
                db.execute("UPDATE adjustment_jobs SET state='done' WHERE uid=? AND state='pending'", (state.uid,))
                db.execute("DELETE FROM dynamic_checkpoints WHERE uid=?", (state.uid,))
                if initial_dynamics is not None:
                    self._save_posts(db, state.uid, initial_dynamics, baseline=True)
            # 其他会话不得重置监听器的检查点。
            db.execute("""INSERT INTO vtubers (uid, name, room_id, is_live) VALUES (?, ?, ?, ?)
                ON CONFLICT(uid) DO UPDATE SET name=excluded.name,
                room_id=excluded.room_id""",
                       (state.uid, state.name, state.room_id, state.is_live))
            if active is None:
                # 所有人退订后重新订阅，从新的基线开始。
                db.execute("UPDATE vtubers SET is_live=?, last_live_change_at=NULL WHERE uid=?",
                           (state.is_live, state.uid))
            db.execute("""INSERT INTO subscriptions VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(uid, umo) DO UPDATE SET level=excluded.level,
                updated_at=excluded.updated_at""", (state.uid, umo, level.value, now, now))
            if user_id:
                db.execute("INSERT OR IGNORE INTO subscription_users VALUES (?, ?, ?)", (umo, str(user_id), state.uid))
            return Subscription(**dict(db.execute(
                "SELECT * FROM subscriptions WHERE uid=? AND umo=?", (state.uid, umo)
            ).fetchone()))
        return await asyncio.to_thread(self._run, write)

    @staticmethod
    def _validate_posts(uid, posts):
        validate_uid(uid)
        if not isinstance(posts, list) or any(not isinstance(p, DynamicPost) or p.uid != uid for p in posts):
            raise ValueError("动态列表或主播 UID 无效。")

    @staticmethod
    def _save_posts(db, uid, posts, baseline=False):
        previous = db.execute("SELECT latest_id FROM dynamic_checkpoints WHERE uid=?", (uid,)).fetchone()
        watermark = int(previous[0]) if previous else 0
        accepted = []
        for post in sorted(posts, key=lambda p: int(p.id)):
            if not baseline and int(post.id) <= watermark:
                continue
            payload = json.dumps({"uid": post.uid, "id": post.id, "text": post.text,
                                  "published_at": post.published_at, "images": post.images,
                                  "is_pinned": post.is_pinned}, ensure_ascii=False)
            inserted = db.execute("INSERT OR IGNORE INTO dynamic_posts VALUES (?, ?, ?, ?)",
                                  (uid, post.id, payload, baseline)).rowcount
            if inserted and not baseline:
                accepted.append(post)
                db.execute("INSERT OR IGNORE INTO adjustment_jobs (uid, dynamic_id, payload) VALUES (?, ?, ?)",
                           (uid, post.id, payload))
        latest = str(max([watermark] + [int(p.id) for p in posts]))
        db.execute("""INSERT INTO dynamic_checkpoints VALUES (?, ?, ?)
            ON CONFLICT(uid) DO UPDATE SET latest_id=excluded.latest_id, checked_at=excluded.checked_at""",
                   (uid, latest, utc_now()))
        return accepted

    async def ingest_dynamics(self, uid, posts):
        """在同一事务内归档有效的一页动态并更新检查点；首轮不产生新动态。"""
        uid = validate_uid(uid)
        self._validate_posts(uid, posts)
        def write(db):
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM subscriptions WHERE uid=? AND level='special'", (uid,)).fetchone():
                return []
            baseline = db.execute("SELECT 1 FROM dynamic_checkpoints WHERE uid=?", (uid,)).fetchone() is None
            return self._save_posts(db, uid, posts, baseline=baseline)
        return await asyncio.to_thread(self._run, write)

    async def get_dynamic_checkpoint(self, uid):
        uid = validate_uid(uid)
        row = await asyncio.to_thread(self._run, lambda db: db.execute(
            "SELECT latest_id, checked_at FROM dynamic_checkpoints WHERE uid=?", (uid,)).fetchone())
        return dict(row) if row else None

    async def save_schedule_image(self, uid, dynamic_id, url, content):
        validate_uid(uid)
        key = hashlib.sha256(f"{uid}:{dynamic_id}:{url}:".encode() + content).hexdigest()
        suffix = ".png" if content.startswith(b"\x89PNG") else ".webp" if content.startswith(b"RIFF") else ".jpg"
        path = self.path.parent / "schedule_images" / f"{key}{suffix}"
        def write():
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(path.suffix + ".tmp")
            temporary.write_bytes(content)
            os.replace(temporary, path)
            return str(path.resolve())
        return await asyncio.to_thread(write)

    async def save_notice_image(self, uid, dynamic_id, content):
        """通知附带的动态截图；同一张图按内容哈希去重复用。"""
        validate_uid(uid)
        key = hashlib.sha256(f"notice:{uid}:{dynamic_id}:".encode() + content).hexdigest()
        suffix = ".png" if content.startswith(b"\x89PNG") else ".webp" if content.startswith(b"RIFF") else ".jpg"
        path = self.path.parent / "notice_images" / f"{key}{suffix}"

        def write():
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(path.suffix + ".tmp")
            temporary.write_bytes(content)
            os.replace(temporary, path)
            return str(path.resolve())

        return await asyncio.to_thread(write)

    async def get_schedule_tracking(self, uid):
        row = await asyncio.to_thread(self._run, lambda db: db.execute(
            "SELECT payload FROM schedule_tracking WHERE uid=?", (validate_uid(uid),)).fetchone())
        return json.loads(row[0]) if row else None

    async def save_schedule_tracking(self, uid, state):
        await asyncio.to_thread(self._run, lambda db: db.execute(
            "INSERT OR REPLACE INTO schedule_tracking VALUES (?, ?)",
            (validate_uid(uid), json.dumps(state, ensure_ascii=False))))

    async def save_credentials(self, credentials):
        payload = json.dumps(credentials)
        await asyncio.to_thread(self._run, lambda db: db.execute(
            "INSERT OR REPLACE INTO login_credentials VALUES (1, ?)", (payload,)))

    async def get_credentials(self):
        row = await asyncio.to_thread(self._run, lambda db: db.execute(
            "SELECT payload FROM login_credentials WHERE id=1").fetchone())
        return json.loads(row[0]) if row else None

    async def create_login_qr_image(self, url):
        def write():
            import qrcode
            import uuid
            directory = self.path.parent / "login_qr"
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"{uuid.uuid4().hex}.png"
            qrcode.make(url).save(path)
            return str(path.resolve())
        return await asyncio.to_thread(write)

    async def remove_login_qr_image(self, image_path):
        path = Path(image_path).resolve()
        directory = (self.path.parent / "login_qr").resolve()
        if path.parent != directory or path.suffix != ".png":
            raise ValueError("登录二维码清理路径无效。")
        await asyncio.to_thread(path.unlink, missing_ok=True)

    async def save_schedule_candidate(self, record):
        payload = json.dumps(record, ensure_ascii=False)
        await asyncio.to_thread(self._run, lambda db: db.execute(
            "INSERT OR REPLACE INTO schedule_candidates VALUES (?, ?, ?, ?)",
            (validate_uid(record["uid"]), record["dynamic_id"], record["image_url"], payload)))

    async def get_schedule_candidates(self, uid):
        uid = validate_uid(uid)
        return await asyncio.to_thread(self._run, lambda db: [json.loads(row[0]) for row in db.execute(
            "SELECT payload FROM schedule_candidates WHERE uid=?", (uid,))])

    async def get_image_classifications(self, uid, dynamic_id, fingerprint):
        return await asyncio.to_thread(self._run, lambda db: {
            row["image_url"]: bool(row["is_schedule"]) for row in db.execute(
                "SELECT image_url, is_schedule FROM schedule_image_classifications WHERE uid=? AND dynamic_id=? AND text_fingerprint=?",
                (validate_uid(uid), dynamic_id, fingerprint))})

    async def save_image_classification(self, uid, dynamic_id, url, fingerprint, is_schedule):
        if type(is_schedule) is not bool:
            raise ValueError("图片分类结果必须是布尔值。")
        await asyncio.to_thread(self._run, lambda db: db.execute(
            "INSERT OR REPLACE INTO schedule_image_classifications VALUES (?, ?, ?, ?, ?)",
            (validate_uid(uid), dynamic_id, url, fingerprint, is_schedule)))

    async def get_weekly_schedule(self, uid):
        row = await asyncio.to_thread(self._run, lambda db: db.execute(
            "SELECT payload FROM weekly_schedules WHERE uid=?", (validate_uid(uid),)).fetchone())
        return json.loads(row[0]) if row else None

    async def get_schedule_history(self, uid):
        return await asyncio.to_thread(self._run, lambda db: [dict(row) for row in db.execute(
            "SELECT week_start, COUNT(*) AS versions FROM weekly_schedule_archive WHERE uid=? GROUP BY week_start ORDER BY week_start DESC",
            (validate_uid(uid),))])

    async def get_historical_schedule(self, uid, week_start):
        row = await asyncio.to_thread(self._run, lambda db: db.execute(
            "SELECT payload FROM weekly_schedule_archive WHERE uid=? AND week_start=? ORDER BY id DESC LIMIT 1",
            (validate_uid(uid), week_start)).fetchone())
        return json.loads(row[0]) if row else None

    async def schedule_imported_at(self, uid, week_start):
        """这一周周表最早一次写进来的时间（= 首次导入时间）；没有记录返回 None。

        归档表每次写入都会追加一行，所以最早那行的 archived_at 就是首次导入时间，
        后面的修订不会把它往后推。
        """
        row = await asyncio.to_thread(self._run, lambda db: db.execute(
            "SELECT archived_at FROM weekly_schedule_archive WHERE uid=? AND week_start=? ORDER BY id LIMIT 1",
            (validate_uid(uid), week_start)).fetchone())
        return row[0] if row else None

    async def remove_subscription(self, uid: int, umo: str) -> bool:
        args = (validate_uid(uid), validate_umo(umo))
        def remove(db):
            changed = db.execute("DELETE FROM subscriptions WHERE uid=? AND umo=?", args).rowcount > 0
            db.execute("DELETE FROM subscription_users WHERE uid=? AND umo=?", args)
            return changed
        return await asyncio.to_thread(self._run, remove)

    async def save_weekly_schedule(self, content, *, expected, source_id, reason,
                              fingerprint="", operation_id="", notification=None):
        """在同一事务内完成 CAS、审计、归档、幂等标记与待发通知。"""
        uid, week = validate_uid(content["uid"]), content["week_start"]
        content = json.loads(json.dumps(content, ensure_ascii=False))
        def write(db):
            db.execute("BEGIN IMMEDIATE")
            if fingerprint and db.execute("SELECT 1 FROM schedule_imports WHERE uid=? AND fingerprint=?",
                                          (uid, fingerprint)).fetchone():
                return False
            if operation_id and db.execute("SELECT 1 FROM schedule_operations WHERE uid=? AND operation_id=?",
                                           (uid, operation_id)).fetchone():
                return False
            row = db.execute("SELECT payload FROM weekly_schedule_archive WHERE uid=? AND week_start=? ORDER BY id DESC LIMIT 1",
                             (uid, week)).fetchone()
            if row is None:
                row = db.execute("SELECT payload FROM weekly_schedules WHERE uid=?", (uid,)).fetchone()
                if row and json.loads(row[0])["week_start"] != week:
                    row = None
            old = json.loads(row[0]) if row else None
            if old != expected:
                raise ValueError("周表已被其他任务更新，请重新读取后重试。")
            if fingerprint:
                db.execute("INSERT INTO schedule_imports VALUES (?, ?)", (uid, fingerprint))
            if operation_id:
                db.execute("INSERT INTO schedule_operations VALUES (?, ?)", (uid, operation_id))
            if (old and old["streams"] == content["streams"]
                    and old.get("source_image_url") == content.get("source_image_url")
                    and old.get("local_image_path") == content.get("local_image_path")
                    and old.get("source_dynamic_id") == content.get("source_dynamic_id")):
                return False
            content["revision"] = (old.get("revision", 0) if old else 0) + 1
            content["updated_at"] = utc_now()
            payload = json.dumps(content, ensure_ascii=False)
            # 修订号参与摘要计算：回退到旧状态也算一次新修订。
            digest = hashlib.sha256(payload.encode()).hexdigest()
            db.execute("INSERT INTO weekly_schedule_archive (uid, week_start, content_hash, payload, archived_at) VALUES (?, ?, ?, ?, ?)",
                       (uid, week, digest, payload, utc_now()))
            current = db.execute("SELECT payload FROM weekly_schedules WHERE uid=?", (uid,)).fetchone()
            from .schedule_models import china_today
            from datetime import timedelta
            today = china_today()
            monday = (today - timedelta(days=today.weekday())).isoformat()
            if week >= monday and (not current or json.loads(current[0])["week_start"] <= week):
                db.execute("INSERT OR REPLACE INTO weekly_schedules VALUES (?, ?)", (uid, payload))
            db.execute("INSERT INTO schedule_revisions (uid, old_value, new_value, source_dynamic_id, reason, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                       (uid, row[0] if row else None, payload, source_id, reason, utc_now()))
            if notification:
                # 通知可选带一张本地图片（调播通知附触发它的动态截图）。
                kind, message, *rest = notification
                image_path = rest[0] if rest else ""
                db.execute("INSERT INTO schedule_outbox (uid, umo, kind, message, image_path)"
                           " SELECT uid, umo, ?, ?, ? FROM subscriptions WHERE uid=? AND level='special'",
                           (kind, message, image_path, uid))
            return True
        return await asyncio.to_thread(self._run, write)

    async def get_schedule_revisions(self, uid, limit=10):
        def read(db):
            return [{**dict(row), "old_value": json.loads(row["old_value"]) if row["old_value"] else None,
                     "new_value": json.loads(row["new_value"])} for row in db.execute(
                "SELECT * FROM schedule_revisions WHERE uid=? ORDER BY id DESC LIMIT ?",
                (validate_uid(uid), min(50, max(1, int(limit)))))]
        return await asyncio.to_thread(self._run, read)

    async def has_schedule_operation(self, uid, operation_id):
        return await asyncio.to_thread(self._run, lambda db: db.execute(
            "SELECT 1 FROM schedule_operations WHERE uid=? AND operation_id=?",
            (validate_uid(uid), operation_id)).fetchone() is not None)

    async def pending_adjustments(self, uid):
        return await asyncio.to_thread(self._run, lambda db: [dict(row) for row in db.execute(
            "SELECT * FROM adjustment_jobs WHERE uid=? AND state='pending' ORDER BY length(dynamic_id), dynamic_id LIMIT 20",
            (validate_uid(uid),))])

    async def finish_adjustment(self, uid, dynamic_id, success):
        await asyncio.to_thread(self._run, lambda db: db.execute(
            "UPDATE adjustment_jobs SET attempts=attempts+1, state=CASE WHEN ? THEN 'done' WHEN attempts>=2 THEN 'failed' ELSE 'pending' END WHERE uid=? AND dynamic_id=?",
            (success, validate_uid(uid), dynamic_id)))

    async def retry_adjustment(self, uid, dynamic_id):
        return await asyncio.to_thread(self._run, lambda db: db.execute(
            "UPDATE adjustment_jobs SET attempts=0, state='pending' WHERE uid=? AND dynamic_id=? AND state='failed'",
            (validate_uid(uid), dynamic_id)).rowcount > 0)

    async def pending_notifications(self):
        return await asyncio.to_thread(self._run, lambda db: [dict(row) for row in db.execute(
            "SELECT * FROM schedule_outbox WHERE state='pending' ORDER BY id LIMIT 50")])

    async def finish_notification(self, notification_id, success):
        await asyncio.to_thread(self._run, lambda db: db.execute(
            "UPDATE schedule_outbox SET attempts=attempts+1, state=CASE WHEN ? THEN 'done' WHEN attempts>=4 THEN 'failed' ELSE 'pending' END WHERE id=?",
            (success, notification_id)))

    async def get_work_status(self):
        def read(db):
            status = {table: {r["state"]: r["n"] for r in db.execute(
                f"SELECT state, COUNT(*) AS n FROM {table} GROUP BY state")}
                for table in ("adjustment_jobs", "schedule_outbox")}
            sessions = {r["synced"]: r["n"] for r in db.execute(
                "SELECT synced, COUNT(*) AS n FROM live_sessions GROUP BY synced")}
            status["live_sessions"] = {"pending": sessions.get(LIVE_SESSION_PENDING, 0),
                                       "recorded": sessions.get(LIVE_SESSION_RECORDED, 0),
                                       "skipped": sessions.get(LIVE_SESSION_SKIPPED, 0)}
            return status
        return await asyncio.to_thread(self._run, read)

    async def remember_greeting(self, label):
        """记录最近抽选，支撑 /vt_4016 的不重复规则；表内条数有上限。"""
        if not isinstance(label, str) or not label.strip():
            raise ValueError("问候对象不能为空。")
        def write(db):
            db.execute("INSERT INTO greeting_history (label, picked_at) VALUES (?, ?)",
                       (label.strip()[:100], utc_now()))
            db.execute("DELETE FROM greeting_history WHERE id NOT IN "
                       "(SELECT id FROM greeting_history ORDER BY id DESC LIMIT 50)")
        await asyncio.to_thread(self._run, write)

    async def recent_greetings(self, limit=12):
        def read(db):
            return [row["label"] for row in db.execute(
                "SELECT label FROM greeting_history ORDER BY id DESC LIMIT ?", (int(limit),))]
        return await asyncio.to_thread(self._run, read)

    async def get_target_mappings(self, umo, user_id):
        validate_umo(umo)
        def read(db):
            rows = [dict(row) for row in db.execute("""SELECT s.uid, s.level, v.name,
                EXISTS(SELECT 1 FROM subscription_users u WHERE u.umo=s.umo AND u.uid=s.uid AND u.user_id=?) AS personal
                FROM subscriptions s LEFT JOIN vtubers v ON v.uid=s.uid
                WHERE s.umo=? ORDER BY s.uid""", (str(user_id), umo))]
            aliases = {}
            for row in db.execute("SELECT uid, alias, alias_key FROM streamer_aliases WHERE umo=? AND user_id=? ORDER BY rowid",
                                  (umo, str(user_id))):
                aliases.setdefault(row["uid"], []).append(dict(row))
            for row in rows:
                items = aliases.get(row["uid"], [])
                row["aliases"] = [a["alias"] for a in items]
                row["alias"] = items[0]["alias"] if items else None
                row["alias_key"] = items[0]["alias_key"] if items else None
            return rows
        return await asyncio.to_thread(self._run, read)

    async def get_aliases(self, umo, user_id):
        return await asyncio.to_thread(self._run, lambda db: [dict(row) for row in db.execute(
            "SELECT uid, alias, alias_key FROM streamer_aliases WHERE umo=? AND user_id=? ORDER BY uid, rowid",
            (validate_umo(umo), str(user_id)))])

    async def set_alias(self, uid, umo, user_id, alias, alias_key):
        uid, umo = validate_uid(uid), validate_umo(umo)
        def write(db):
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM subscriptions WHERE uid=? AND umo=?", (uid, umo)).fetchone():
                raise ValueError("请先在当前会话订阅该主播，再设置别名。")
            existing = db.execute("SELECT uid FROM streamer_aliases WHERE umo=? AND user_id=? AND alias_key=?",
                                  (umo, str(user_id), alias_key)).fetchone()
            if existing and existing["uid"] != uid:
                raise ValueError("该别名已用于另一位主播，请换一个别名。")
            db.execute("""INSERT INTO streamer_aliases VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(umo, user_id, alias_key) DO UPDATE SET alias=excluded.alias""",
                (umo, str(user_id), uid, alias, alias_key))
            db.execute("INSERT OR IGNORE INTO subscription_users VALUES (?, ?, ?)", (umo, str(user_id), uid))
        await asyncio.to_thread(self._run, write)

    async def remove_alias(self, uid, umo, user_id, alias_key):
        return await asyncio.to_thread(self._run, lambda db: db.execute(
            "DELETE FROM streamer_aliases WHERE uid=? AND umo=? AND user_id=? AND alias_key=?",
            (validate_uid(uid), validate_umo(umo), str(user_id), alias_key)).rowcount > 0)

    async def _subscriptions(self, column: str, value) -> list[Subscription]:
        # 列名只由下面这几个固定的内部方法提供。
        return await asyncio.to_thread(self._run, lambda db: [
            Subscription(**dict(row)) for row in db.execute(
                f"SELECT * FROM subscriptions WHERE {column}=? ORDER BY uid, umo", (value,))])

    async def get_subscriptions_by_umo(self, umo: str) -> list[Subscription]:
        return await self._subscriptions("umo", validate_umo(umo))

    async def get_subscriptions_by_uid(self, uid: int) -> list[Subscription]:
        return await self._subscriptions("uid", validate_uid(uid))

    async def get_subscription(self, uid: int, umo: str) -> Subscription | None:
        validate_umo(umo)
        return next((s for s in await self.get_subscriptions_by_uid(uid) if s.umo == umo), None)

    async def get_special_vtubers(self) -> list[int]:
        return sorted({s.uid for s in await self._subscriptions("level", "special")})

    async def get_vtuber_state(self, uid: int) -> VtuberState | None:
        row = await asyncio.to_thread(self._run, lambda db: db.execute(
            "SELECT * FROM vtubers WHERE uid=?", (validate_uid(uid),)).fetchone())
        if row is None:
            return None
        return VtuberState(row["uid"], row["name"], row["room_id"],
                           None if row["is_live"] is None else bool(row["is_live"]),
                           row["last_live_change_at"])

    async def get_subscribed_uids(self) -> list[int]:
        return await asyncio.to_thread(self._run, lambda db: [row[0] for row in db.execute(
            "SELECT DISTINCT uid FROM subscriptions ORDER BY uid")])

    async def save_vtuber_state(self, state: VtuberState) -> bool:
        """原子保存一次观测，并返回已知的直播状态是否发生了变化。

        先落库再通知，重启或重复轮询都不会重发同一次状态跳变；
        上下播通知是尽力发送，不是持久队列。
        """
        if not isinstance(state, VtuberState) or state.is_live is None:
            raise ValueError("直播观察必须包含有效状态。")

        def write(db):
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM vtubers WHERE uid=?", (state.uid,)).fetchone()
            changed = old is not None and old["is_live"] is not None and bool(old["is_live"]) != state.is_live
            changed_at = utc_now() if changed else (old["last_live_change_at"] if old else None)
            observed = utc_now()
            active = db.execute("SELECT * FROM live_sessions WHERE uid=? AND end_time IS NULL", (state.uid,)).fetchone()
            if state.is_live and active is None:
                started = state.live_started_at or (observed if changed else None)
                basis = "api" if state.live_started_at else "observed" if changed else "unknown"
                db.execute("INSERT INTO live_sessions(uid,start_time,observed_at,start_basis,title) VALUES (?,?,?,?,?)",
                           (state.uid, started, observed, basis, state.live_title[:300]))
            elif not state.is_live and active:
                db.execute("UPDATE live_sessions SET end_time=?,synced=0 WHERE id=?", (observed, active["id"]))
            db.execute("""INSERT INTO vtubers
                (uid, name, room_id, is_live, last_live_change_at) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(uid) DO UPDATE SET name=excluded.name, room_id=excluded.room_id,
                is_live=excluded.is_live, last_live_change_at=excluded.last_live_change_at""",
                (state.uid, state.name, state.room_id, state.is_live, changed_at))
            return changed
        return await asyncio.to_thread(self._run, write)

    async def pending_live_sessions(self, uid):
        return await asyncio.to_thread(self._run, lambda db: [dict(r) for r in db.execute(
            "SELECT * FROM live_sessions WHERE uid=? AND synced=0 ORDER BY id", (validate_uid(uid),))])

    async def bind_live_session(self, session_id, week_start, stream_id):
        """记录落位结果便于排查；周表内容本身才是唯一依据。"""
        await asyncio.to_thread(self._run, lambda db: db.execute(
            "UPDATE live_sessions SET week_start=?,stream_id=? WHERE id=?",
            (week_start, stream_id, session_id)))

    async def finish_live_session(self, session):
        # 并发的下播观测必须保持待处理，即使开播记录刚刚写入。
        await asyncio.to_thread(self._run, lambda db: db.execute(
            "UPDATE live_sessions SET synced=1 WHERE id=? AND end_time IS ?",
            (session["id"], session["end_time"])))

    async def retire_live_session(self, session_id):
        """停止重复扫描永远无法确定开播时间的记录。"""
        await asyncio.to_thread(self._run, lambda db: db.execute(
            "UPDATE live_sessions SET synced=? WHERE id=? AND synced=?",
            (LIVE_SESSION_SKIPPED, session_id, LIVE_SESSION_PENDING)))

    async def live_sessions_for_uid(self, uid):
        return await asyncio.to_thread(self._run, lambda db: [dict(r) for r in db.execute(
            "SELECT * FROM live_sessions WHERE uid=? ORDER BY id", (validate_uid(uid),))])
