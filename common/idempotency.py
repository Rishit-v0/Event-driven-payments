import redis.asyncio as redis


class Idempotency:
    def __init__(self, url, ttl_seconds=86400):
        self.r = redis.from_url(url)
        self.ttl = ttl_seconds

    def _key(self, group, event_id):
        return f"processed:{group}:{event_id}"

    async def already_processed(self, group, event_id) -> bool:
        return (await self.r.exists(self._key(group, event_id))) == 1

    async def mark_processed(self, group, event_id):
        await self.r.set(self._key(group, event_id), "1", ex=self.ttl)