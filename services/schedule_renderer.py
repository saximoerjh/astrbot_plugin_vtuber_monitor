"""把周表数据结构渲染成 7 列时间轴图片，全程在本机完成。

渲染分两层：``build_schedule_view`` 是纯函数，只把持久化数据整理成模板数据；
``ScheduleRenderer`` 负责 HTML 与 Playwright 截图。这样版式逻辑可以不启动
浏览器就被测试覆盖。
"""
import asyncio
import hashlib
import html
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from ..core.schedule_models import UNFULFILLED_STATUS, china_today
from .schedule_display import format_live_summary

CHINA = timezone(timedelta(hours=8))
WEEKDAYS = ("一", "二", "三", "四", "五", "六", "日")
EXTRA_SOURCE = "live_observation"
EXTRA_TITLE = "突击直播"

# 手机可读性＝字号 ÷ 图片总宽度。整周并排会强制图片变宽、字变小，
# 因此一周拆成两行（4＋3 列），并把字号占比顶到 2% 以上。
BOARD_WIDTH = 840
BOARD_COLUMNS = 4
FONT_SCALE = 1.13
MAX_BOARD_HEIGHT = 20000
CACHE_LIMIT = 40

STATUS_LABELS = {"scheduled": "已排期", "postponed": "已调播", "completed": "已结束",
                 "cancelled": "已取消", "unknown": "待定", UNFULFILLED_STATUS: "未兑现"}
STATUS_CLASSES = {"scheduled": "s-scheduled", "postponed": "s-postponed", "completed": "s-completed",
                  "cancelled": "s-cancelled", "unknown": "s-unknown",
                  UNFULFILLED_STATUS: "s-unfulfilled"}
# 算不算迟到：5 分钟以内属于正常开播准备，不算迟到，否则每场都会命中。
LATE_THRESHOLD = timedelta(minutes=5)


class ScheduleRenderError(Exception):
    """对外显示的安全信息，不含浏览器日志。"""


def _duration_text(span):
    minutes = max(0, int(span.total_seconds() // 60))
    if minutes < 1:
        return "不足1分"
    hours, minutes = divmod(minutes, 60)
    if not hours:
        return f"{minutes}分"
    if not minutes:
        return f"{hours}小时"
    return f"{hours}小时{minutes}分"


def _planned_start(plan):
    """周表承诺的开播时刻：调播后以调播时间为准；待定与突击直播没有承诺。"""
    if plan.get("source") == EXTRA_SOURCE or not plan.get("start_time"):
        return None
    try:
        return datetime.fromisoformat(f"{plan['date']}T{plan['start_time']}:00").replace(tzinfo=CHINA)
    except (KeyError, TypeError, ValueError):
        return None


def _timing_stats(streams):
    """本周时长与准时性：总时长／日均时长／迟到次数／平均迟到。

    时长按场次累计（中途重开的两段相加），仍在直播的场次算不出时长，只参与准时性；
    迟到以「排期开始时间」为基准，没有排期或没有实际开播的场次不参与统计。
    日均时长按「真正开播过的天数」摊：同一天播两场只算一天，休息日不参与平均。
    """
    durations, delays, per_day, comparable = [], [], {}, 0
    for plan in streams or ():
        intervals = [item for item in plan.get("actual_intervals") or () if item.get("start")]
        if not intervals:
            continue
        span = timedelta()
        for item in intervals:
            if item.get("end"):
                span += datetime.fromisoformat(item["end"]) - datetime.fromisoformat(item["start"])
        if span:
            durations.append(span)
            day = str(plan.get("date") or "")
            per_day[day] = per_day.get(day, timedelta()) + span
        planned = _planned_start(plan)
        if planned is None:
            continue
        comparable += 1
        delay = min(datetime.fromisoformat(item["start"]) for item in intervals) - planned
        if delay > LATE_THRESHOLD:
            delays.append(delay)
    total = sum(durations, timedelta())
    return {
        "total": total if durations else None,
        "daily_average": (total / len(per_day)) if durations and per_day else None,
        "late_count": len(delays) if comparable else None,
        "late_average": (sum(delays, timedelta()) / len(delays)) if delays else None,
    }


def _stats_cells(stats):
    return [
        {"label": "直播总时长",
         "value": _duration_text(stats["total"]) if stats["total"] is not None else "—"},
        {"label": "日均直播时长",
         "value": _duration_text(stats["daily_average"]) if stats["daily_average"] is not None else "—"},
        {"label": "迟到次数",
         "value": f"{stats['late_count']} 次" if stats["late_count"] is not None else "—"},
        {"label": "平均迟到",
         "value": _duration_text(stats["late_average"]) if stats["late_average"] is not None else "—"},
    ]


def _actual_line(plan_date, entry):
    """实际直播：只留起止时刻与时长，日期与来源标注都不在图里重复。"""
    start = datetime.fromisoformat(entry["start"]).astimezone(CHINA)
    end = datetime.fromisoformat(entry["end"]).astimezone(CHINA) if entry.get("end") else None
    # 列头已经写明日期，只有观测日期与所在列不一致时才需要写出来。
    start_text = start.strftime("%H:%M") if start.date().isoformat() == plan_date \
        else start.strftime("%m-%d %H:%M")
    if end is None:
        return f"{start_text} 起 · 直播中"
    gap = (end.date() - start.date()).days
    end_text = (end.strftime("%H:%M") if gap == 0 else
                f"次日 {end.strftime('%H:%M')}" if gap == 1 else end.strftime("%m-%d %H:%M"))
    return f"{start_text}–{end_text} · {_duration_text(end - start)}"


def _range(start, end):
    """时间区间，含跨日写法。"""
    if not start:
        return "时间待定"
    if not end:
        return start
    return f"{start}–{'次日 ' if end < start else ''}{end}"


def _slot_time(plan):
    """卡片上的主时间：没被调播时直接显示完整区间，避免与列头重复。"""
    start = plan.get("start_time")
    if not start:
        return "待定"
    if plan.get("rescheduled_start_time") or start != plan.get("original_start_time"):
        # 已调到新时间，原区间另行标注。
        return start
    return _range(start, plan.get("original_end_time"))


def _stream_item(plan):
    status = plan.get("status") or "unknown"
    extra = plan.get("source") == EXTRA_SOURCE
    original_day = plan.get("original_date") or plan["date"]
    item = {
        "title": (plan.get("title") or "").strip() or EXTRA_TITLE,
        "status": status,
        "status_label": ("突击直播" if extra else STATUS_LABELS.get(status, status)),
        "status_class": STATUS_CLASSES.get(status, "s-unknown"),
        "extra": extra,
        "time_label": _slot_time(plan),
        "original_label": "",
        "actual_lines": [],
    }
    if extra:
        item["original_label"] = "周表外记录"
    elif plan.get("rescheduled_start_time") or plan.get("start_time") != plan.get("original_start_time"):
        # 只有实际排期与原定不一致时，才需要额外说明原来的时间。
        item["original_label"] = (
            f"原定 {date.fromisoformat(original_day).strftime('%m-%d')}"
            f"（周{WEEKDAYS[date.fromisoformat(original_day).weekday()]}）"
            f"{_range(plan.get('original_start_time'), plan.get('original_end_time'))}")
    for entry in plan.get("actual_intervals") or ():
        if not entry.get("start"):
            continue
        item["actual_lines"].append(_actual_line(plan["date"], entry))
    item["has_actual"] = bool(item["actual_lines"])
    return item


def build_schedule_view(schedule, *, uid, display_name="", summary=None, today=None, banner=None):
    """把周表整理成模板数据：7 天时间轴 + 场次卡片 + 实际直播统计。"""
    if not isinstance(schedule, dict) or not schedule.get("week_start"):
        raise ValueError("周表数据无效。")
    streams = schedule.get("streams") or ()
    start = date.fromisoformat(schedule["week_start"])
    today = today or china_today()
    days = [{"weekday": WEEKDAYS[(start + timedelta(days=offset)).weekday()],
             "date": (start + timedelta(days=offset)).isoformat(),
             "short": (start + timedelta(days=offset)).strftime("%m-%d"),
             "is_today": start + timedelta(days=offset) == today,
             "streams": []} for offset in range(7)]
    by_date = {day["date"]: day for day in days}
    for plan in streams:
        # 按“有效日期”落列，调播后的场次出现在新的一天，原定时间留在卡片里。
        target = by_date.get(plan.get("date") or "") or by_date.get(plan.get("original_date") or "") or days[0]
        target["streams"].append(_stream_item(plan))
    for day in days:
        day["streams"].sort(key=lambda item: (item["time_label"] == "待定", item["time_label"], item["title"]))
        day["count"] = len(day["streams"])
    end = start + timedelta(days=6)
    counts = dict(summary) if summary is not None else None
    if counts is not None:
        # 未兑现是周表自身的状态，按正在渲染的这份数据统计，
        # 免得调用方传进来的口径与图上的红标对不上。
        counts["unfulfilled"] = sum(1 for plan in streams
                                    if plan.get("status") == UNFULFILLED_STATUS)
    return {
        "uid": int(uid),
        "name": (display_name or "").strip() or f"UID {uid}",
        "week_start": schedule["week_start"],
        "week_range": f"{start.strftime('%m-%d')} ~ {end.strftime('%m-%d')}",
        "week_end": end.strftime("%m-%d"),
        "revision": schedule.get("revision", 0),
        "days": days,
        "total": sum(day["count"] for day in days),
        "summary_text": (("本周还没有排期（周表未发布），下面是直播记录。"
                          if not any(plan.get("source") != EXTRA_SOURCE for plan in streams) else "")
                         + format_live_summary(counts)),
        "summary_ok": summary is not None,
        "stats": _stats_cells(_timing_stats(streams)),
        "banner": {"header": (banner or {}).get("header", ""),
                   "avatar": (banner or {}).get("avatar", "")},
    }


_STYLE = """
  * { box-sizing: border-box; }
  body { margin: 0; background: #eef0f2; }
  #schedule {
    width: __WIDTH__px; margin: 0; padding: 28px 30px 24px;
    background: #fbfbfa; color: #202224;
    font-family: "Microsoft YaHei", "Noto Sans SC", "PingFang SC", "Hiragino Sans GB", sans-serif;
    font-size: 15px; line-height: 1.5;
  }
  .head { padding-bottom: 16px; border-bottom: 2px solid #202224; }
  .name { font-size: 30px; font-weight: 700; letter-spacing: .5px; }
  .sub { margin-top: 6px; color: #666b70; font-size: 15px; }
  .hero { position: relative; display: flex; align-items: flex-end; overflow: hidden;
    border-radius: 14px; min-height: 152px; background-color: #2f333a;
    background-size: cover; background-position: center 32%; }
  .hero-image::after { content: ""; position: absolute; inset: 0;
    background: linear-gradient(180deg, rgba(0,0,0,.10) 0%, rgba(0,0,0,.34) 45%, rgba(0,0,0,.76) 100%); }
  .hero-inner { position: relative; z-index: 1; display: flex; align-items: center; gap: 16px;
    width: 100%; padding: 0 24px 20px; }
  .avatar { flex: none; width: 104px; height: 104px; border-radius: 50%; border: 4px solid #fff;
    object-fit: cover; background: #fff; box-shadow: 0 3px 12px rgba(0,0,0,.35); }
  .hero-name { font-size: 32px; font-weight: 700; color: #fff; word-break: break-word;
    text-shadow: 0 1px 8px rgba(0,0,0,.6); }
  .hero-meta { margin-top: 6px; font-size: 15px; color: rgba(255,255,255,.92);
    text-shadow: 0 1px 6px rgba(0,0,0,.65); }
  .hero-plain { min-height: 0; border-radius: 0; background-color: transparent;
    border-bottom: 2px solid #202224; padding-bottom: 16px; }
  .hero-plain .hero-inner { padding: 0; gap: 14px; }
  .hero-plain .hero-name { color: #202224; text-shadow: none; letter-spacing: .5px; }
  .hero-plain .hero-meta { color: #666b70; text-shadow: none; }
  .hero-plain .avatar { width: 72px; height: 72px; border-color: #e2e6e8; box-shadow: none; }
  .grid { display: grid; grid-template-columns: repeat(__COLUMNS__, 1fr); gap: 10px; margin-top: 18px; align-items: start; }
  .col { background: #f4f5f6; border: 1px solid #dfe3e6; border-radius: 10px; padding: 8px; min-height: 150px; }
  .col-today { background: #eef6fc; border-color: #9dc7e4; }
  .dayhead { display: flex; align-items: baseline; justify-content: space-between; padding: 2px 4px 8px; border-bottom: 1px solid #dfe3e6; }
  .wd { font-size: 16px; font-weight: 700; }
  .col-today .wd { color: #1d6fa5; }
  .dt { color: #7c8288; font-size: 13px; }
  .slots { display: flex; flex-direction: column; gap: 8px; padding-top: 9px; }
  .empty { color: #b6bbbf; font-size: 13px; text-align: center; padding: 22px 0; }
  .card { background: #fff; border: 1px solid #e0e4e7; border-left: 5px solid #9aa0a6; border-radius: 7px; padding: 8px 9px 9px; }
  .card.s-scheduled { border-left-color: #2f86bd; }
  .card.s-postponed { border-left-color: #d98a1f; }
  .card.s-completed { border-left-color: #3f9a63; }
  .card.s-cancelled { border-left-color: #aab0b5; }
  .card.s-unknown { border-left-color: #9aa0a6; border-left-style: dashed; }
  .card.s-unfulfilled { background: #fdf5f4; border-color: #f0cdc9; border-left-color: #d0453f; }
  .ctime { font-size: 20px; font-weight: 700; color: #1f2326; }
  .ctitle { margin-top: 2px; font-size: 15px; font-weight: 600; word-break: break-word; }
  .s-cancelled .ctitle { text-decoration: line-through; color: #8d9297; }
  .tags { margin-top: 6px; display: flex; flex-wrap: wrap; gap: 5px; }
  .tag { border-radius: 4px; padding: 1px 6px; font-size: 12px; background: #eef0f2; color: #5c6266; }
  .s-scheduled .tag-status { background: #e2eff8; color: #1d6fa5; }
  .s-postponed .tag-status { background: #fbeed7; color: #a4680f; }
  .s-completed .tag-status { background: #e2f2e8; color: #2f7a4d; }
  .s-cancelled .tag-status { background: #eceef0; color: #74797d; }
  .s-unknown .tag-status { background: #eceef0; color: #74797d; }
  .s-unfulfilled .tag-status { background: #fbe1df; color: #b02a26; }
  .s-unfulfilled .ctime { color: #b02a26; }
  .meta { margin-top: 6px; color: #6f757a; font-size: 13px; word-break: break-word; }
  .actual { margin-top: 6px; padding-top: 6px; border-top: 1px dashed #e0e4e7; color: #3f7f9e; font-size: 12.5px; overflow-wrap: anywhere; }
  .atom { white-space: nowrap; }
  .foot { margin-top: 18px; padding-top: 12px; border-top: 1px solid #dfe3e6; display: flex; justify-content: space-between; align-items: baseline; gap: 12px; }
  .sum { flex: 1 1 auto; font-size: 15px; font-weight: 600; color: #3c4247; }
  .sum-failed { color: #a4680f; }
  .brand { flex: none; white-space: nowrap; color: #9aa0a6; font-size: 13px; }
  .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-top: 14px; }
  .stat { background: #f4f5f6; border: 1px solid #e3e7ea; border-radius: 10px; padding: 9px 14px; }
  .stat-label { color: #6f757a; font-size: 13px; }
  .stat-value { margin-top: 2px; font-size: 23px; font-weight: 700; color: #202224; }
"""


def _escape(value):
    return html.escape(str(value), quote=True)


def _stream_html(item):
    tags = [f'<span class="tag tag-status">{_escape(item["status_label"])}</span>']
    blocks = [
        f'<div class="ctime">{_escape(item["time_label"])}</div>',
        f'<div class="ctitle">{_escape(item["title"])}</div>',
        f'<div class="tags">{"".join(tags)}</div>',
    ]
    if item["original_label"]:
        blocks.append(f'<div class="meta">{_escape(item["original_label"])}</div>')
    for line in item["actual_lines"]:
        # 时长与“直播中”不可拆行，否则会出现“· 2 / 小时13分”这种断法。
        head, separator, tail = line.rpartition(" · ")
        body = (f'{_escape(head)} · <span class="atom">{_escape(tail)}</span>' if separator
                else f'<span class="atom">{_escape(line)}</span>')
        blocks.append(f'<div class="actual">实际 {body}</div>')
    return f'<article class="card {item["status_class"]}">{"".join(blocks)}</article>'


def _day_html(day):
    if day["streams"]:
        slots = "".join(_stream_html(item) for item in day["streams"])
    else:
        slots = '<div class="empty">无场次</div>'
    return (f'<section class="col{" col-today" if day["is_today"] else ""}">'
            f'<div class="dayhead"><span class="wd">周{day["weekday"]}'
            f'{"（今天）" if day["is_today"] else ""}</span>'
            f'<span class="dt">{day["short"]}</span></div>'
            f'<div class="slots">{slots}</div></section>')


def _scaled_style(width, columns, font_scale):
    """按比例放大所有像素值；外层宽度单独给定，保证输出宽度可控。"""
    css = _STYLE
    if font_scale != 1:
        css = re.sub(r"(?<![\w.])(\d+(?:\.\d+)?)px",
                     lambda match: f"{float(match.group(1)) * font_scale:.1f}px", css)
    # 用占位符替换而不是 % 格式化：CSS 里的百分号（渐变、background-position）
    # 会被 % 当成本地格式化占位，之前就在这里炸过一次。
    return css.replace("__WIDTH__", str(width)).replace("__COLUMNS__", str(columns))


def _hero_html(view):
    """顶部：有空间头图时用横幅＋头像，和 B 站空间页一致；没有就保持原来的标题行。"""
    banner = view.get("banner") or {}
    header, avatar = banner.get("header", ""), banner.get("avatar", "")
    meta = (f'UID {view["uid"]} · 周表 {view["week_start"]} ~ {view["week_end"]}'
            f' · 共 {view["total"]} 场 · 修订 {view["revision"]}')
    if not header and not avatar:
        return ('<header class="head">'
                f'<div class="name">{_escape(view["name"])}</div>'
                f'<div class="sub">{_escape(meta)}</div></header>')
    avatar_html = f'<img class="avatar" src="{_escape(avatar)}" alt="">' if avatar else ""
    style = f' style="background-image:url(\'{_escape(header)}\')"' if header else ""
    return (f'<header class="hero {"hero-image" if header else "hero-plain"}"{style}>'
            f'<div class="hero-inner">{avatar_html}<div>'
            f'<div class="hero-name">{_escape(view["name"])}</div>'
            f'<div class="hero-meta">{_escape(meta)}</div></div></div></header>')


def build_schedule_html(view, *, width=BOARD_WIDTH, columns=BOARD_COLUMNS, font_scale=FONT_SCALE):
    """生成自包含 HTML：样式内联，不引用任何外部资源。"""
    generated = datetime.now(CHINA).strftime("%Y-%m-%d %H:%M")
    day_columns = "".join(_day_html(day) for day in view["days"])
    stats = "".join(f'<div class="stat"><div class="stat-label">{_escape(item["label"])}</div>'
                    f'<div class="stat-value">{_escape(item["value"])}</div></div>'
                    for item in view["stats"])
    summary_class = "sum" if view["summary_ok"] else "sum sum-failed"
    return (
        '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>周表</title>'
        f"<style>{_scaled_style(width, columns, font_scale)}</style></head><body>"
        '<div id="schedule">'
        f'{_hero_html(view)}'
        f'<div class="grid">{day_columns}</div>'
        f'<div class="stats">{stats}</div>'
        f'<footer class="foot"><div class="{summary_class}">{_escape(view["summary_text"])}</div>'
        f'<div class="brand">生成于 {generated} · VTuber Monitor</div></footer>'
        "</div></body></html>"
    )


class ScheduleRenderer:
    """用本机浏览器把周表 HTML 截成图片，并按内容哈希缓存。"""

    def __init__(self, data_dir, *, channel="auto", timeout=45, scale=2,
                 width=BOARD_WIDTH, columns=BOARD_COLUMNS, font_scale=FONT_SCALE):
        if channel not in ("auto", "chromium", "msedge", "chrome"):
            raise ValueError("截图浏览器请选择 auto、chromium、msedge 或 chrome。")
        self.directory = Path(data_dir) / "schedule_render"
        self.channel, self.timeout, self.scale = channel, timeout, scale
        self.width, self.columns, self.font_scale = width, columns, font_scale
        self._lock = asyncio.Lock()

    def cache_path(self, view):
        return self._cache_path(self._html(view))

    def _html(self, view):
        return build_schedule_html(view, width=self.width, columns=self.columns,
                                   font_scale=self.font_scale)

    def _cache_path(self, html_text):
        # 直接按最终 HTML 做键：内容、版式参数、样式与结构任何一处变化都会重新出图。
        return self.directory / f"{hashlib.sha256(html_text.encode('utf-8')).hexdigest()}.png"

    async def render(self, view):
        """返回图片绝对路径；内容未变时复用已有文件。"""
        html_text = self._html(view)
        path = self._cache_path(html_text)
        if path.is_file():
            return str(path.resolve())
        async with self._lock:
            if path.is_file():
                return str(path.resolve())
            raw = await self._capture(html_text)
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(".png.tmp")
            await asyncio.to_thread(temporary.write_bytes, raw)
            await asyncio.to_thread(temporary.replace, path)
            await self._prune()
            return str(path.resolve())

    async def _prune(self):
        def clean():
            files = sorted(self.directory.glob("*.png"), key=lambda item: item.stat().st_mtime, reverse=True)
            for stale in files[CACHE_LIMIT:]:
                stale.unlink(missing_ok=True)
        try:
            await asyncio.to_thread(clean)
        except OSError:
            # 清理失败不影响本次出图。
            pass

    async def _capture(self, html_text):
        try:
            async with asyncio.timeout(self.timeout):
                return await self._screenshot(html_text)
        except ImportError:
            raise ScheduleRenderError("未安装截图依赖，请安装插件依赖 playwright。") from None
        except ScheduleRenderError:
            raise
        except Exception:
            raise ScheduleRenderError("周表图片生成失败（浏览器不可用或渲染超时），已改为发送文字。") from None

    async def _screenshot(self, html_text):
        from playwright.async_api import async_playwright
        async with async_playwright() as playwright:
            channel = self.channel
            if channel == "auto":
                channel = ("chromium" if Path(playwright.chromium.executable_path).exists()
                           or sys.platform != "win32" else "msedge")
            browser = await playwright.chromium.launch(channel=channel, headless=True)
            try:
                context = await browser.new_context(viewport={"width": self.width, "height": 900},
                                                    device_scale_factor=self.scale, locale="zh-CN")
                page = await context.new_page()
                # 页面自包含，不加载任何外部资源，也不需要网络。
                await page.set_content(html_text, wait_until="load")
                board = page.locator("#schedule")
                await board.wait_for(state="visible")
                await page.evaluate("async () => { await document.fonts.ready; }")
                box = await board.bounding_box()
                if not box:
                    raise ScheduleRenderError("周表版面为空，未生成图片。")
                if box["height"] > MAX_BOARD_HEIGHT:
                    raise ScheduleRenderError("周表过长，无法生成完整图片，已改为发送文字。")
                return await board.screenshot(type="png", animations="disabled")
            finally:
                await browser.close()
