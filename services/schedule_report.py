"""精简的命令输出；逐图的详细诊断信息留在存储里。"""


def format_schedule_report(records, uid, *, target="", week_start=""):
    groups = {}
    for record in records:
        groups.setdefault(record["dynamic_id"], []).append(record)
    lines = [f"指定周次：{week_start} 起"] if week_start else []
    needs_week = False
    for dynamic_id, items in groups.items():
        accepted = next((r for r in items if r["status"] in ("parsed", "archived", "unchanged")), None)
        if accepted:
            summary = {"parsed": "周表已保存", "archived": "历史周表已缓存", "unchanged": "已有周表未变更"}[accepted["status"]]
            if accepted.get("week_start"):
                summary += f"（{accepted['week_start']} 起）"
        elif any(r["status"] == "needs_date" for r in items):
            summary = "周次不明确，未保存。" if not week_start else "日期信息仍不足，未保存。"
            needs_week = not week_start
        elif any(r["status"] in ("pending", "stale") for r in items):
            summary = "未配置视觉模型，原图已保留。"
        elif any(r["status"] == "failed" for r in items):
            reasons = []
            for record in items:
                if record["status"] != "failed":
                    continue
                code = record.get("error_code")
                reason = {"model_error": "模型调用失败", "date_conflict": "日期冲突",
                          "invalid_output": "模型结果无效", "unresolved": "未识别出有效周表"}.get(code)
                reason = reason or ("图片下载失败" if record.get("error_stage") == "download" else "周表识别失败")
                if reason not in reasons:
                    reasons.append(reason)
            summary = "、".join(reasons[:2]) + "，未保存。"
        else:
            summary = "未识别到周表。"
        lines.append(f"{summary}\n查看动态：https://t.bilibili.com/{dynamic_id}")
    if not groups:
        lines.append(f"未发现周表候选。\n查看动态页：https://space.bilibili.com/{uid}/dynamic")
    if needs_week:
        lines.append(f"确认周次后重试：/vt_parse_schedule {target or uid} 本周（也支持上周、下周或日期）")
    return "\n\n".join(lines)
