"""ScrapeChannelStats Use Case — сбор статистики канала."""


class ScrapeChannelStatsUseCase:
    def __init__(
        self,
        platform_repo: "PlatformRepository",
        userbot_pool: "UserbotPool",
    ):
        self.platform_repo = platform_repo
        self.userbot_pool = userbot_pool

    async def execute(self, platform_id: str) -> dict:
        platform = await self.platform_repo.get(platform_id)
        userbot = await self.userbot_pool.get_client()

        channel = await userbot.get_entity(platform["username"])
        messages = await userbot.get_messages(channel, limit=100)

        stats = {
            "members_count": channel.participants_count,
            "avg_views": sum(m.views for m in messages) / len(messages),
        }

        await self.platform_repo.update_stats(platform_id, stats)
        return stats
