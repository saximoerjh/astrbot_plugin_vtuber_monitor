"""挑一个此刻正处于 04:00–05:00 的国家或地区，交给模型造句。"""
import asyncio
import json
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from astrbot.api import logger

from ..core.countries import COUNTRIES, WINDOW_END, WINDOW_START

CHINA = timezone(timedelta(hours=8))
NAME = "小路泥"
RECENT_LIMIT = 12
FALLBACK = "现在是{label}凌晨四点多"
SYSTEM_PROMPT = (
    "你在替一个聊天机器人写一句话。只输出一句话，不要引号、不要解释、不要表情、不要编号，"
    "不超过二十个字。必须包含给定的国家或地区名字，国家或地区名字太长的话支持使用简称，以及“凌晨四点”这个说法，"
    "可以按下面的例子自由选择措辞和语气。"
)
EXAMPLES = ("现在是马里凌晨四点半左右。",
            "现在是英国的凌晨四点多哦。",
            "现在是安提瓜和巴布达的凌晨四点。",
            "现在是阿拉斯加的凌晨四点多哦。")
# 前缀问候的整点分界；最后一档跨过零点。
GREETING_BANDS = ((5, "早上好"), (11, "中午好"), (13, "下午好"), (18, "晚上好"), (21, "晚安"))


def greeting_for(hour):
    greeting = "晚安"
    for start, candidate in GREETING_BANDS:
        if hour >= start:
            greeting = candidate
    return greeting


def local_time(zone, moment):
    return moment.astimezone(ZoneInfo(zone))


def at_four(moment):
    """返回在 ``moment`` 时自己时钟正处在 04:00–04:59 的全部条目。"""
    hits = []
    for label, zone in COUNTRIES:
        local_now = local_time(zone, moment)
        if WINDOW_START <= local_now.hour < WINDOW_END:
            hits.append((label, local_now))
    return hits


def nearest_to_four(moment):
    """仅作安全网：返回时钟最接近 04:00 的那些条目。"""
    scored = []
    for label, zone in COUNTRIES:
        local_now = local_time(zone, moment)
        minutes = local_now.hour * 60 + local_now.minute
        distance = min(abs(minutes - WINDOW_START * 60), 1440 - abs(minutes - WINDOW_START * 60))
        scored.append((distance, label, local_now))
    best = min(distance for distance, _, _ in scored)
    return [(label, local_now) for distance, label, local_now in scored if distance == best]


def clean_sentence(text):
    """模型输出不可信：只保留一行短句，并去掉首尾引号。"""
    if not isinstance(text, str):
        return ""
    line = text.strip().splitlines()[0].strip() if text.strip() else ""
    while len(line) >= 2 and line[0] in "“”\"'‘’「」" and line[-1] in "“”\"'‘’「」":
        line = line[1:-1].strip()
    return line if len(line) <= 60 else ""


class GreetingService:
    def __init__(self, context, data, provider_id="", *, timeout=20):
        self.context = context
        self.data = data
        self.provider_id = (provider_id or "").strip()
        self.timeout = timeout

    async def build(self, moment=None):
        now = (moment or datetime.now(CHINA)).astimezone(CHINA)
        pool = at_four(now) or nearest_to_four(now)
        label, local_now = await self._pick(pool)
        sentence = await self._sentence(label, local_now)
        await self.data.remember_greeting(label)
        return f"{NAME}{greeting_for(now.hour)}+\n{sentence}"

    async def _pick(self, pool):
        """随机抽选，避开最近用过的对象，同时保证候选池不会抽空。"""
        labels = {label for label, _ in pool}
        recent = [row for row in await self.data.recent_greetings(RECENT_LIMIT) if row in labels]
        blocked = set(recent[:max(0, len(pool) - 1)])
        return random.choice([entry for entry in pool if entry[0] not in blocked])

    async def _sentence(self, label, local_now):
        if not self.provider_id:
            return FALLBACK.format(label=label)
        prompt = json.dumps({"国家或地区": label, "当地时间": local_now.strftime("%H:%M"),
                             "参考例句": list(EXAMPLES)}, ensure_ascii=False)
        try:
            response = await asyncio.wait_for(
                self.context.llm_generate(chat_provider_id=self.provider_id, prompt=prompt,
                                          system_prompt=SYSTEM_PROMPT), self.timeout)
        except Exception:
            # 模型超时或不可用时，问候仍然要发得出去。
            logger.warning("Greeting model call failed")
            return FALLBACK.format(label=label)
        return clean_sentence(getattr(response, "completion_text", "")) or FALLBACK.format(label=label)
