from .database import test_connection
from .services.cache_service import cache_service
from .services.search import SearchService
import logging

logger = logging.getLogger(__name__)

async def check_redis_health():
    try:
        if cache_service._redis_client:
            await cache_service._redis_client.ping()
            return "ok"
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
    return "error"

def check_db_health():
    if test_connection():
        return "ok"
    return "error"

async def check_opensearch_health():
    try:
        client = await SearchService._get_opensearch_client()
        if client:
            await client.ping()
            return "ok"
    except Exception as e:
        logger.error(f"OpenSearch health check failed: {e}")
    return "error"
