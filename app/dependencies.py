import redis.asyncio as redis
from .config import settings

async def get_redis():
    return redis.from_url(settings.REDIS_URL)