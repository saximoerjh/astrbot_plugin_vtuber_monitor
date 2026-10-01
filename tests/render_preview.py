"""需用 AstrBot 自带的 Python 手动执行；本机渲染，不联网、不发消息。

用法：python tests/render_preview.py [输出目录] [--compare] [--live UID]
默认写入系统临时目录，不污染插件目录；把样例周表渲染成 PNG 与 HTML，
便于人工检查时间轴版式。加 --compare 时额外渲染几组“手机可读性”候选版式，
用于比较字号与图片宽度的比例（比例越大，在手机上显示得越大）。
加 --live UID 时用真实主播抓取头像与空间头图（需要装好浏览器、会访问网络）。
"""
import asyncio
import sys
import tempfile
from datetime import date
from pathlib import Path

WEEK = "2026-09-21"

# 手机可读性由「基础字号 / 图片总宽度」决定，与像素密度无关。
# 这里是被否决过的备选版式，用于和当前默认版式对比。
VARIANTS = (
    ("备选-旧版单行7列-1420px", {"width": 1420, "columns": 7, "font_scale": 1.0}),
    ("备选-保守单行7列-1000px", {"width": 1000, "columns": 7, "font_scale": 1.0}),
    ("备选-两行4列-880px", {"width": 880, "columns": 4, "font_scale": 1.0}),
    ("备选-两行4列-800px-字号x1.33", {"width": 800, "columns": 4, "font_scale": 1.33}),
)


def sample_view():
    def plan(day, start, title, **overrides):
        item = {"id": f"{day}-{start}", "date": day, "start_time": start, "title": title,
                "status": "scheduled", "source": "weekly_image", "revision": 1,
                "original_date": day, "original_start_time": start, "original_end_time": None,
                "rescheduled_date": None, "rescheduled_start_time": None, "actual_intervals": []}
        item.update(overrides)
        return item

    streams = [
        plan(f"{WEEK}", "08:00", "早间音乐分享电台", original_end_time="10:30"),
        plan("2026-09-22", "21:00", "小歌回～随便唱唱", original_end_time="23:00"),
        plan("2026-09-23", "21:00", "【游戏联动】满力全开", original_end_time="01:00",
             status="completed", actual_intervals=[
                 {"session_id": "a", "start": "2026-09-23T21:04:00+08:00", "end": "2026-09-24T00:40:00+08:00",
                  "start_basis": "api", "end_basis": "observed"}]),
        plan("2026-09-24", None, "联动（待定）", status="unknown",
             original_start_time=None, original_end_time=None),
        plan("2026-09-25", "20:00", "杂谈：聊聊最近的安排", original_end_time="22:00",
             status="cancelled"),
        plan("2026-09-23", "22:30", "深夜补播：恐怖游戏", status="postponed",
             original_date="2026-09-24", original_start_time="21:00", original_end_time="23:00",
             rescheduled_date="2026-09-23", rescheduled_start_time="22:30"),
        plan("2026-09-26", "21:00", "烟火", original_end_time="23:30",
             actual_intervals=[
                 {"session_id": "b", "start": "2026-09-26T21:02:00+08:00", "end": None,
                  "start_basis": "api", "end_basis": None}]),
        plan("2026-09-22", "14:00", "突击：午后杂谈", status="completed", source="live_observation",
             original_date=None, original_start_time=None, original_end_time=None,
             actual_intervals=[
                 {"session_id": "c", "start": "2026-09-22T14:07:00+08:00", "end": "2026-09-22T16:20:00+08:00",
                  "start_basis": "observed", "end_basis": "observed"}]),
        plan("2026-09-27", "19:00", "例行杂谈（到点未开播）", status="unfulfilled",
             original_end_time="21:00"),
    ]
    schedule = {"uid": 1512246445, "week_start": WEEK, "revision": 4, "streams": streams}
    return schedule


def main():
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from astrbot_plugin_vtuber_monitor.services import schedule_renderer
    from astrbot_plugin_vtuber_monitor.services.schedule_renderer import (
        ScheduleRenderer, build_schedule_html, build_schedule_view)

    args = [item for item in sys.argv[1:] if not item.startswith("--")]
    compare = "--compare" in sys.argv
    live = sys.argv[sys.argv.index("--live") + 1] if "--live" in sys.argv else ""
    output = (Path(args[0]).resolve() if args
              else Path(tempfile.gettempdir()) / "vt-schedule-preview")
    output.mkdir(parents=True, exist_ok=True)
    view = build_schedule_view(sample_view(), uid=1298779265, display_name="灰泽满Hazel",
                               summary={"recorded": 3, "extra": 1, "pending": 0, "unknown": 1},
                               today=date(2026, 9, 23))
    (output / "schedule_preview.html").write_text(build_schedule_html(view), encoding="utf-8")

    async def banner_for(uid):
        if not live:
            return {}
        from astrbot_plugin_vtuber_monitor.bili_client import BiliClient
        from astrbot_plugin_vtuber_monitor.core.data_manager import DataManager
        from astrbot_plugin_vtuber_monitor.services.profile_service import ProfileService
        data = DataManager(output / "data")
        await data.initialize()
        bili = BiliClient()
        try:
            return await ProfileService(data, bili, channel="msedge").banner(uid)
        finally:
            await bili.close()

    async def render():
        banner = await banner_for(int(live)) if live else {}
        if live:
            print(f"banner keys: {sorted(banner)} sizes: "
                  + str({key: len(value) for key, value in banner.items()}))
            view.update(banner=banner)
        renderer = ScheduleRenderer(output / "cache")
        results = [("默认版式", await renderer.render(view), renderer.width,
                    15.0 * renderer.font_scale, renderer.columns)]
        if compare:
            for name, layout in VARIANTS:
                board = ScheduleRenderer(output / "cache", **layout)
                results.append((name, await board.render(view), layout["width"],
                                15.0 * layout["font_scale"], layout["columns"]))
        return results

    try:
        results = asyncio.run(render())
    except Exception as error:
        print(f"FAIL: {type(error).__name__}: {error}")
        return 1
    for name, path, width, font, columns in results:
        print(f"PASS: {name} -> {path}  ({columns} 列, 字号/宽度 = {font / width * 100:.2f}%)")
    print(f"默认版式常量: {schedule_renderer.BOARD_WIDTH} / {schedule_renderer.BOARD_COLUMNS}"
          f" / x{schedule_renderer.FONT_SCALE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
