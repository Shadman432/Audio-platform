import uuid
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from redis.asyncio import Redis
from ..models.comment_likes import CommentLike
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class CommentService:

    @staticmethod
    async def add_comment(redis: Redis, comment_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds a comment to Redis and enqueues it for DB sync.
        """
        from ..services.cache_service import cache_service
        comment_id = uuid.uuid4()
        comment_data['comment_id'] = str(comment_id)
        
        # Prepare data for Redis
        redis_comment_data = {k: (str(v) if isinstance(v, uuid.UUID) else v) for k, v in comment_data.items()}
        redis_comment_data['created_at'] = datetime.utcnow().isoformat()
        redis_comment_data['updated_at'] = datetime.utcnow().isoformat()
        redis_comment_data['comment_like_count'] = 0

        # Store comment object in Redis
        redis_key = f"comment:{comment_id}"
        await redis.set(redis_key, json.dumps(redis_comment_data))

        # Add to sync queue
        await redis.sadd("comments:to_sync", str(comment_id))

        # Increment story/episode comment count
        if comment_data.get("story_id"):
            await cache_service.increment_counter(f"story:{comment_data['story_id']}:comments_count")
        elif comment_data.get("episode_id"):
            await cache_service.increment_counter(f"episode:{comment_data['episode_id']}:comments_count")

        # Handle replies
        if comment_data.get("parent_comment_id"):
            parent_key = f"comment:{comment_data['parent_comment_id']}:replies"
            await redis.rpush(parent_key, str(comment_id))

        return redis_comment_data

    @staticmethod
    async def like_comment(redis: Redis, db: Session, comment_id: uuid.UUID, user_id: uuid.UUID):
        """
        Likes a comment, updates the DB and increments the Redis counter.
        """
        from ..services.cache_service import cache_service
        try:
            # Create a like record in the database
            db_like = CommentLike(comment_id=comment_id, user_id=user_id)
            db.add(db_like)
            db.commit()

            # Increment like count in Redis
            await cache_service._redis_client.incr(f"comment:{comment_id}:comment_like_count")
        except Exception as e:
            db.rollback()
            logger.error(f"Error liking comment: {e}", exc_info=True)
            raise

    @staticmethod
    async def get_comments_with_ranking(redis: Redis, db: Session, story_id: uuid.UUID, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Gets comments for a story, ranked by a hotness score.
        Tries to fetch from cache first.
        """
        cache_key = f"story:{story_id}:ranked_comments"
        try:
            cached_comments = await redis.get(cache_key)
            if cached_comments:
                return json.loads(cached_comments)
        except Exception as e:
            logger.error(f"Error getting ranked comments from cache: {e}", exc_info=True)

        # Ranking query
        query = text("""
            SELECT c.comment_id, c.comment_text, c.user_id, c.created_at, c.comment_like_count,
                   COUNT(r.comment_id) AS replies_count,
                   ((c.comment_like_count * 2) + (COUNT(r.comment_id) * 3)) AS score
            FROM comments c
            LEFT JOIN comments r ON r.parent_comment_id = c.comment_id
            WHERE c.story_id = :story_id AND c.parent_comment_id IS NULL
            GROUP BY c.comment_id
            ORDER BY score DESC
            LIMIT :limit;
        """)

        try:
            result = db.execute(query, {"story_id": story_id, "limit": limit}).fetchall()
            
            comments = []
            for row in result:
                row_dict = dict(row._mapping)
                row_dict['comment_id'] = str(row_dict['comment_id'])
                row_dict['user_id'] = str(row_dict['user_id'])
                row_dict['created_at'] = row_dict['created_at'].isoformat()
                comments.append(row_dict)

            # Cache the result
            await redis.set(cache_key, json.dumps(comments), ex=300) # Cache for 5 minutes

            return comments
        except Exception as e:
            logger.error(f"Error getting ranked comments from DB: {e}", exc_info=True)
            return []