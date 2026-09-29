from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
import re


def validate_uid(value: str | int) -> int:
    if isinstance(value, bool) or not re.fullmatch(r"[0-9]{1,19}", str(value)):
        raise ValueError("UID 必须是正整数。")
    uid = int(value)
    if not 0 < uid <= 2**63 - 1:
        raise ValueError("UID 超出有效范围。")
    return uid


def validate_umo(umo: str) -> str:
    if not isinstance(umo, str) or not umo.strip():
        raise ValueError("会话标识不能为空。")
    return umo


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class FollowLevel(str, Enum):
    NORMAL = "normal"
    SPECIAL = "special"


@dataclass(frozen=True)
class Subscription:
    uid: int
    umo: str
    level: FollowLevel
    created_at: str
    updated_at: str

    def __post_init__(self):
        object.__setattr__(self, "uid", validate_uid(self.uid))
        validate_umo(self.umo)
        object.__setattr__(self, "level", FollowLevel(self.level))
        for value in (self.created_at, self.updated_at):
            if datetime.fromisoformat(value).tzinfo is None:
                raise ValueError("时间戳必须包含时区。")

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class VtuberState:
    uid: int
    name: str
    room_id: int = 0
    is_live: bool | None = None
    last_live_change_at: str | None = None
    live_title: str = ""
    live_cover: str = ""
    live_started_at: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "uid", validate_uid(self.uid))
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("主播名称缺失。")
        if type(self.room_id) is not int or self.room_id < 0:
            raise ValueError("直播间 ID 无效。")
        if self.is_live is not None and type(self.is_live) is not bool:
            raise ValueError("直播状态无效。")
        if not isinstance(self.live_title, str) or not isinstance(self.live_cover, str):
            raise ValueError("直播标题或封面无效。")
        if self.live_started_at and datetime.fromisoformat(self.live_started_at).tzinfo is None:
            raise ValueError("开播时间必须包含时区。")
        if self.last_live_change_at is not None:
            if datetime.fromisoformat(self.last_live_change_at).tzinfo is None:
                raise ValueError("直播状态时间戳必须包含时区。")


@dataclass(frozen=True)
class DynamicPost:
    uid: int
    id: str
    text: str
    published_at: int
    images: tuple[str, ...] = ()
    is_pinned: bool = False

    def __post_init__(self):
        object.__setattr__(self, "uid", validate_uid(self.uid))
        if not isinstance(self.id, str) or not re.fullmatch(r"[0-9]{1,30}", self.id) or int(self.id) <= 0:
            raise ValueError("动态 ID 无效。")
        if not isinstance(self.text, str):
            raise ValueError("动态正文无效。")
        if type(self.published_at) is not int or self.published_at < 0:
            raise ValueError("动态时间无效。")
        if type(self.is_pinned) is not bool:
            raise ValueError("置顶状态无效。")
        if not isinstance(self.images, tuple) or any(
            not isinstance(url, str) or not url.startswith("https://") for url in self.images
        ):
            raise ValueError("动态图片地址无效。")
