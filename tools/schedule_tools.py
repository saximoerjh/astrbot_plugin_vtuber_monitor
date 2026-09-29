"""只提供结构定义的 FunctionTool；调用经校验后整批提交。"""

TOOL_FIELDS = {
    "reschedule_stream": ("改期：仅使用明确的日期、时间和已有场次 ID。", ("stream_id", "date", "start_time", "reason")),
    "cancel_stream": ("取消明确指向的已有直播。", ("stream_id", "reason")),
    "add_stream": ("新增明确宣布的直播；未知时间用 null。", ("date", "start_time", "title", "reason")),
    "update_stream_info": ("修改明确指向的已有直播标题。", ("stream_id", "title", "reason")),
}


def build_toolset():
    from astrbot.core.agent.tool import FunctionTool, ToolSet
    tools = []
    for name, (description, fields) in TOOL_FIELDS.items():
        properties = {field: {"type": "string"} for field in fields}
        if "start_time" in properties:
            properties["start_time"] = {"type": ["string", "null"] if name == "add_stream" else "string",
                                        "description": "北京时间 HH:MM"}
        if "date" in properties:
            properties["date"]["description"] = "明确的 YYYY-MM-DD，本周范围内"
        tools.append(FunctionTool(name=name, description=description, parameters={
            "type": "object", "properties": properties, "required": list(fields), "additionalProperties": False}))
    return ToolSet(tools=tools)


async def execute_calls(schedules, uid, names, arguments, **options):
    if not isinstance(names, list) or not isinstance(arguments, list) or len(names) != len(arguments) or not 1 <= len(names) <= 10:
        raise ValueError("模型返回了无效的调播工具调用。")
    operations = [{"name": name, "arguments": args} for name, args in zip(names, arguments)]
    # 服务层先校验全部操作再提交；UID、来源动态与修订号都由本地绑定。
    return await schedules.apply_operations(uid, operations, **options)
