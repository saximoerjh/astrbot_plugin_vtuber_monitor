import asyncio

from astrbot.api import logger

from ..core.models import FollowLevel, validate_uid, validate_umo


class SubscriptionService:
    def __init__(self, data, bili, discovery=None):
        self.data = data
        self.bili = bili
        self.discovery = discovery
        self._scans = {}

    async def subscribe(
        self, uid: str | int, umo: str, level: str = "normal", user_id=""
    ):
        uid = validate_uid(uid)
        validate_umo(umo)
        try:
            level = FollowLevel(level)
        except ValueError:
            raise ValueError("关注级别必须是 normal 或 special。") from None
        state = await self.bili.get_user_info(uid)
        initial = None
        if level == FollowLevel.SPECIAL and not any(
            sub.level == FollowLevel.SPECIAL
            for sub in await self.data.get_subscriptions_by_uid(uid)
        ):
            initial = await self.bili.get_latest_dynamics(uid)
        await self.data.add_subscription(
            state, umo, level, initial_dynamics=initial, user_id=user_id
        )
        if (
            level == FollowLevel.SPECIAL
            and self.discovery is not None
            and uid not in self._scans
        ):
            self._scans[uid] = asyncio.create_task(
                self._initial_scan(uid, initial), name=f"vtuber-initial-scan-{uid}"
            )
        return state

    async def _initial_scan(self, uid, posts):
        try:
            await self.discovery.scan(uid, posts)
        except Exception as exc:
            logger.warning(
                "Subscription saved; initial schedule scan failed uid=%s type=%s",
                uid,
                type(exc).__name__,
            )
        finally:
            self._scans.pop(uid, None)

    async def close(self):
        tasks = list(self._scans.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def change_level(self, uid, umo, level, user_id=""):
        uid = validate_uid(uid)
        if await self.data.get_subscription(uid, validate_umo(umo)) is None:
            raise ValueError("当前会话未订阅该主播，请先使用 /vt_sub。")
        return await self.subscribe(uid, umo, level, user_id=user_id)

    async def unsubscribe(self, uid: str | int, umo: str) -> bool:
        return await self.data.remove_subscription(validate_uid(uid), validate_umo(umo))

    async def list_subscriptions(self, umo: str):
        return await self.data.get_subscriptions_by_umo(validate_umo(umo))
