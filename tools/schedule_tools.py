"""只提供结构定义的 FunctionTool；调用经校验后整批提交。"""

TOOL_FIELDS = {
    "reschedule_stream": ("改期：仅使用明确的日期、时间和已有场次 ID。",
                          ("stream_id", "date", "start_time", "reason"), ()),
    "cancel_stream": ("取消明确指向的已有直播。", ("stream_id", "reason"), ()),
    "add_stream": ("新增明确宣布的直播；未知时间用 null。",
                   ("date", "start_time", "title", "reason"), ()),
    # date/start_time 可选：动态给了时间就补到"时间待定"的场次上，改标题照旧。
    "update_stream_info": ("给已有直播补上日期/开始时间，或改标题；不需要改的字段省略。",
                           ("stream_id", "reason"), ("title", "date", "start_time")),
}


def build_toolset():
    from astrbot.core.agent.tool import FunctionTool, ToolSet
    tools = []
    for name, (description, required, optional) in TOOL_FIELDS.items():
        properties = {field: {"type": "string"} for field in (*required, *optional)}
        if "start_time" in properties:
            properties["start_time"] = {"type": ["string", "null"] if name == "add_stream" else "string",
                                        "description": "北京时间 HH:MM"}
        if "date" in properties:
            properties["date"]["description"] = "明确的 YYYY-MM-DD，本周范围内"
        if name == "update_stream_info":
            properties["start_time"]["description"] = "北京时间 HH:MM；不改就省略（不要传 null）"
        tools.append(FunctionTool(name=name, description=description, parameters={
            "type": "object", "properties": properties, "required": list(required),
            "additionalProperties": False}))
    return ToolSet(tools=tools)


async def execute_calls(schedules, uid, names, arguments, **options):
    if not isinstance(names, list) or not isinstance(arguments, list) or len(names) != len(arguments) or not 1 <= len(names) <= 10:
        raise ValueError("模型返回了无效的调播工具调用。")
    operations = [{"name": name, "arguments": args} for name, args in zip(names, arguments)]
    # 服务层先校验全部操作再提交；UID、来源动态与修订号都由本地绑定。
    return await schedules.apply_operations(uid, operations, **options)
