"""在调用者所在会话内解析 UID、别名或昵称；存在歧义时绝不猜测。"""
import unicodedata

from ..core.models import validate_uid, validate_umo


def normalize_alias(value):
    if not isinstance(value, str):
        raise ValueError("别名必须是文本。")
    value = unicodedata.normalize("NFKC", value).strip()
    if not 1 <= len(value) <= 24 or value.isdecimal() or any(not (c.isalnum() or c in "_-") for c in value):
        raise ValueError("别名需为 1–24 个中文、字母、数字、下划线或短横线，不能为纯数字。")
    return value, value.casefold()


class TargetService:
    def __init__(self, data):
        self.data = data

    async def resolve(self, target, umo, user_id):
        validate_umo(umo)
        target = str(target).strip()
        if target.isascii() and target.isdigit():
            return validate_uid(target)
        mappings = await self.data.get_target_mappings(umo, user_id)
        if not target:
            choices = [row for row in mappings if row["personal"]] or mappings
            if len(choices) == 1:
                return choices[0]["uid"]
            if not choices:
                raise ValueError("当前会话暂无订阅，请先使用 /vt_sub <UID>。")
            names = "、".join(row["alias"] or row["name"] or str(row["uid"]) for row in choices)
            raise ValueError(f"有多个订阅，请指定 UID 或别名：{names}")
        key = unicodedata.normalize("NFKC", target).casefold()
        aliases = await self.data.get_aliases(umo, user_id)
        exact = next((row for row in aliases if row["alias_key"] == key), None)
        if exact:
            return exact["uid"]
        names = [row for row in mappings if unicodedata.normalize("NFKC", row["name"] or "").casefold() == key]
        if len(names) == 1:
            return names[0]["uid"]
        if len(names) > 1:
            raise ValueError("主播名称重名，请使用 UID 或设置不同别名。")
        raise ValueError("未找到该别名或已订阅主播名称。用 /vt_list 查看，或 /vt_alias <UID> <别名> 设置。")

    async def set_alias(self, target, alias, umo, user_id):
        display, key = normalize_alias(alias)
        uid = await self.resolve(target, umo, user_id)
        # 防止自定义别名覆盖另一个已订阅主播的完整昵称。
        mappings = await self.data.get_target_mappings(umo, user_id)
        if any(row["uid"] != uid and unicodedata.normalize("NFKC", row["name"] or "").casefold() == key for row in mappings):
            raise ValueError("该别名与另一位已订阅主播的名称冲突。")
        await self.data.set_alias(uid, umo, user_id, display, key)
        return uid, display

    async def remove_alias(self, target, umo, user_id, alias=""):
        uid = await self.resolve(target, umo, user_id)
        aliases = [row for row in await self.data.get_aliases(umo, user_id) if row["uid"] == uid]
        if alias:
            _, key = normalize_alias(alias)
        else:
            target_key = unicodedata.normalize("NFKC", str(target).strip()).casefold()
            match = next((row for row in aliases if row["alias_key"] == target_key), None)
            if match:
                key = match["alias_key"]
            elif len(aliases) == 1:
                key = aliases[0]["alias_key"]
            elif not aliases:
                return False
            else:
                raise ValueError("该主播有多个别名，请使用 /vt_alias_del <要删除的别名>：" + "、".join(row["alias"] for row in aliases))
        return await self.data.remove_alias(uid, umo, user_id, key)

    async def format_list(self, umo, user_id):
        rows = await self.data.get_target_mappings(umo, user_id)
        return "\n".join(f"{' / '.join(r['aliases']) or r['name'] or r['uid']}（UID {r['uid']}）— {r['level']}"
                         for r in rows) or "当前会话暂无订阅。"
