import uuid
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text, and_
from redis.asyncio import Redis
from ..models.comment_likes import CommentLike
from ..models.comments import Comment
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class CommentService:

    @staticmethod
    async def add_comment(redis: Redis, comment_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds a comment to Redis and enqueues it for DB sync with improved error handling.
        """
        try:
            from ..services.cache_service import cache_service
            comment_id = uuid.uuid4()
            comment_data['comment_id'] = str(comment_id)
            
            # Validate required fields
            if not comment_data.get('comment_text', '').strip():
                raise ValueError("Comment text is required")
            
            if not comment_data.get('user_id'):
                raise ValueError("User ID is required")
            
            # Ensure either story_id or episode_id is present
            if not comment_data.get('story_id') and not comment_data.get('episode_id'):
                raise ValueError("Either story_id or episode_id is required")
            
            if comment_data.get('story_id') and comment_data.get('episode_id'):
                raise ValueError("Cannot specify both story_id and episode_id")
            
            # Prepare data for Redis with proper type handling
            redis_comment_data = {}
            for key, value in comment_data.items():
                if isinstance(value, uuid.UUID):
                    redis_comment_data[key] = str(value)
                elif value is not None:
                    redis_comment_data[key] = value
            
            redis_comment_data['created_at'] = datetime.utcnow().isoformat()
            redis_comment_data['updated_at'] = datetime.utcnow().isoformat()
            redis_comment_data['comment_like_count'] = 0

            # Store comment object in Redis
            redis_key = f"comment:{comment_id}"
            await redis.set(redis_key, json.dumps(redis_comment_data), ex=86400)  # 24 hour TTL

            # Add to sync queue
            await redis.sadd("comments:to_sync", str(comment_id))

            # Increment parent entity comment count
            if comment_data.get("story_id"):
                await cache_service.increment_story_comments(str(comment_data['story_id']))
                logger.info(f"Incremented comment count for story {comment_data['story_id']}")
            elif comment_data.get("episode_id"):
                await cache_service.increment_episode_comments(str(comment_data['episode_id']))
                logger.info(f"Incremented comment count for episode {comment_data['episode_id']}")

            # Handle replies - add to parent's replies list
            if comment_data.get("parent_comment_id"):
                try:
                    parent_key = f"comment:{comment_data['parent_comment_id']}:replies"
                    await redis.rpush(parent_key, str(comment_id))
                    await redis.expire(parent_key, 86400)  # 24 hour TTL
                except Exception as e:
                    logger.warning(f"Failed to add reply to parent comment: {e}")

            logger.info(f"Successfully created comment {comment_id}")
            return redis_comment_data

        except Exception as e:
            logger.error(f"Error adding comment: {e}", exc_info=True)
            raise

    @staticmethod
    async def like_comment(redis: Redis, db: Session, comment_id: uuid.UUID, user_id: uuid.UUID):
        """
        Toggle like for a comment with improved error handling and duplicate prevention.
        """
        try:
            from ..services.cache_service import cache_service
            
            # Check if user already liked this comment
            existing_like = db.query(CommentLike).filter(
                and_(CommentLike.comment_id == comment_id, CommentLike.user_id == user_id)
            ).first()
            
            if existing_like:
                # Remove like
                db.delete(existing_like)
                db.commit()
                
                # Decrement counter in Redis
                current_count = await redis.get(f"comment:{comment_id}:comment_like_count")
                if current_count and int(current_count) > 0:
                    await cache_service.decrement_counter(f"comment:{comment_id}:comment_like_count")
                
                logger.info(f"Removed like from comment {comment_id} by user {user_id}")
                return {"liked": False, "action": "removed"}
            else:
                # Add like
                db_like = CommentLike(comment_id=comment_id, user_id=user_id)
                db.add(db_like)
                db.commit()

                # Increment like count in Redis
                await cache_service.increment_comment_likes(str(comment_id))
                
                logger.info(f"Added like to comment {comment_id} by user {user_id}")
                return {"liked": True, "action": "added"}
                
        except Exception as e:
            db.rollback()
            logger.error(f"Error toggling comment like: {e}", exc_info=True)
            raise

    @staticmethod
    async def get_comment_from_redis(redis: Redis, comment_id: str) -> Optional[Dict[str, Any]]:
        """Get a single comment from Redis"""
        try:
            redis_key = f"comment:{comment_id}"
            comment_data_json = await redis.get(redis_key)
            
            if comment_data_json:
                comment_data = json.loads(comment_data_json)
                
                # Get real-time like count from Redis
                like_count_key = f"comment:{comment_id}:comment_like_count"
                like_count = await redis.get(like_count_key)
                if like_count:
                    comment_data['comment_like_count'] = int(like_count.decode('utf-8'))
                
                return comment_data
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting comment {comment_id} from Redis: {e}")
            return None

    @staticmethod
    async def get_comments_with_ranking(redis: Redis, db: Session, story_id: uuid.UUID, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Gets comments for a story, ranked by a hotness score with Redis counters.
        """
        cache_key = f"story:{story_id}:ranked_comments"
        try:
            # Try Redis cache first
            cached_comments = await redis.get(cache_key)
            if cached_comments:
                comments_list = json.loads(cached_comments)
                
                # Update like counts from Redis for real-time data
                for comment in comments_list:
                    comment_id = comment.get('comment_id')
                    if comment_id:
                        like_count_key = f"comment:{comment_id}:comment_like_count"
                        like_count = await redis.get(like_count_key)
                        if like_count:
                            comment['comment_like_count'] = int(like_count.decode('utf-8'))
                
                logger.info(f"Retrieved {len(comments_list)} ranked comments from cache for story {story_id}")
                return comments_list
                
        except Exception as e:
            logger.warning(f"Error getting ranked comments from cache: {e}")

        # Fallback to database with ranking query
        ranking_query = text("""
            SELECT c.comment_id, c.comment_text, c.user_id, c.created_at, c.comment_like_count,
                   COUNT(r.comment_id) AS replies_count,
                   ((c.comment_like_count * 2) + (COUNT(r.comment_id) * 3) + 
                    (EXTRACT(EPOCH FROM (NOW() - c.created_at)) / -86400)) AS score
            FROM comments c
            LEFT JOIN comments r ON r.parent_comment_id = c.comment_id
            WHERE c.story_id = :story_id AND c.parent_comment_id IS NULL
            GROUP BY c.comment_id, c.comment_text, c.user_id, c.created_at, c.comment_like_count
            ORDER BY score DESC
            LIMIT :limit;
        """)

        try:
            result = db.execute(ranking_query, {"story_id": story_id, "limit": limit}).fetchall()
            
            comments = []
            for row in result:
                row_dict = dict(row._mapping)
                
                # Convert UUIDs to strings for JSON serialization
                row_dict['comment_id'] = str(row_dict['comment_id'])
                row_dict['user_id'] = str(row_dict['user_id'])
                row_dict['created_at'] = row_dict['created_at'].isoformat()
                
                # Get real-time like count from Redis
                comment_id = row_dict['comment_id']
                like_count_key = f"comment:{comment_id}:comment_like_count"
                try:
                    like_count = await redis.get(like_count_key)
                    if like_count:
                        row_dict['comment_like_count'] = int(like_count.decode('utf-8'))
                except Exception as e:
                    logger.warning(f"Failed to get like count from Redis for comment {comment_id}: {e}")
                
                comments.append(row_dict)

            # Cache the result for 5 minutes
            try:
                await redis.set(cache_key, json.dumps(comments, default=str), ex=300)
                logger.info(f"Cached {len(comments)} ranked comments for story {story_id}")
            except Exception as e:
                logger.warning(f"Failed to cache ranked comments: {e}")

            return comments
            
        except Exception as e:
            logger.error(f"Error getting ranked comments from DB: {e}", exc_info=True)
            return []

    @staticmethod
    async def get_comment_replies(redis: Redis, db: Session, parent_comment_id: uuid.UUID, limit: int = 20) -> List[Dict[str, Any]]:
        """Get replies for a specific comment"""
        try:
            # First try to get reply IDs from Redis
            parent_key = f"comment:{parent_comment_id}:replies"
            reply_ids = await redis.lrange(parent_key, 0, limit - 1)
            
            replies = []
            
            if reply_ids:
                # Get reply data from Redis
                for reply_id_bytes in reply_ids:
                    reply_id = reply_id_bytes.decode('utf-8')
                    reply_data = await CommentService.get_comment_from_redis(redis, reply_id)
                    if reply_data:
                        replies.append(reply_data)
            
            # If no replies in Redis, fallback to database
            if not replies:
                db_replies = db.query(Comment).filter(
                    Comment.parent_comment_id == parent_comment_id
                ).order_by(Comment.created_at.asc()).limit(limit).all()
                
                for reply in db_replies:
                    reply_dict = {
                        'comment_id': str(reply.comment_id),
                        'user_id': str(reply.user_id),
                        'comment_text': reply.comment_text,
                        'created_at': reply.created_at.isoformat() if reply.created_at else None,
                        'updated_at': reply.updated_at.isoformat() if reply.updated_at else None,
                        'parent_comment_id': str(reply.parent_comment_id),
                        'comment_like_count': reply.comment_like_count or 0
                    }
                    
                    # Get real-time like count from Redis
                    like_count_key = f"comment:{reply.comment_id}:comment_like_count"
                    try:
                        like_count = await redis.get(like_count_key)
                        if like_count:
                            reply_dict['comment_like_count'] = int(like_count.decode('utf-8'))
                    except Exception as e:
                        logger.warning(f"Failed to get Redis like count for reply {reply.comment_id}: {e}")
                    
                    replies.append(reply_dict)
            
            return replies
            
        except Exception as e:
            logger.error(f"Error getting comment replies: {e}", exc_info=True)
            return []

    @staticmethod
    async def delete_comment(redis: Redis, db: Session, comment_id: uuid.UUID, user_id: uuid.UUID) -> bool:
        """Delete a comment (only by the author)"""
        try:
            # Check if comment exists and belongs to user
            comment = db.query(Comment).filter(
                and_(Comment.comment_id == comment_id, Comment.user_id == user_id)
            ).first()
            
            if not comment:
                logger.warning(f"Comment {comment_id} not found or user {user_id} not authorized")
                return False
            
            # Delete from database
            db.delete(comment)
            db.commit()
            
            # Remove from Redis
            redis_key = f"comment:{comment_id}"
            await redis.delete(redis_key)
            
            # Remove from sync queue if present
            await redis.srem("comments:to_sync", str(comment_id))
            
            # Clear related cache
            if comment.story_id:
                cache_key = f"story:{comment.story_id}:ranked_comments"
                await redis.delete(cache_key)
            
            logger.info(f"Successfully deleted comment {comment_id}")
            return True
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error deleting comment {comment_id}: {e}", exc_info=True)
            return False

    @staticmethod
    async def update_comment(redis: Redis, db: Session, comment_id: uuid.UUID, user_id: uuid.UUID, new_text: str) -> Optional[Dict[str, Any]]:
        """Update comment text (only by author)"""
        try:
            if not new_text.strip():
                raise ValueError("Comment text cannot be empty")
            
            # Check if comment exists and belongs to user
            comment = db.query(Comment).filter(
                and_(Comment.comment_id == comment_id, Comment.user_id == user_id)
            ).first()
            
            if not comment:
                logger.warning(f"Comment {comment_id} not found or user {user_id} not authorized")
                return None
            
            # Update in database
            comment.comment_text = new_text.strip()
            comment.updated_at = datetime.utcnow()
            db.commit()
            
            # Update in Redis
            redis_key = f"comment:{comment_id}"
            comment_data = await redis.get(redis_key)
            
            if comment_data:
                try:
                    data_dict = json.loads(comment_data)
                    data_dict['comment_text'] = new_text.strip()
                    data_dict['updated_at'] = datetime.utcnow().isoformat()
                    await redis.set(redis_key, json.dumps(data_dict), ex=86400)
                except Exception as e:
                    logger.warning(f"Failed to update comment in Redis: {e}")
            
            # Clear related cache
            if comment.story_id:
                cache_key = f"story:{comment.story_id}:ranked_comments"
                await redis.delete(cache_key)
            
            logger.info(f"Successfully updated comment {comment_id}")
            
            # Return updated comment data
            return {
                'comment_id': str(comment.comment_id),
                'comment_text': comment.comment_text,
                'updated_at': comment.updated_at.isoformat(),
                'user_id': str(comment.user_id)
            }
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error updating comment {comment_id}: {e}", exc_info=True)
            return None

    @staticmethod
    async def get_user_comments(redis: Redis, db: Session, user_id: uuid.UUID, skip: int = 0, limit: int = 20) -> List[Dict[str, Any]]:
        """Get comments by a specific user with real-time counters"""
        try:
            comments = db.query(Comment).filter(
                Comment.user_id == user_id
            ).order_by(Comment.created_at.desc()).offset(skip).limit(limit).all()
            
            result = []
            for comment in comments:
                comment_dict = {
                    'comment_id': str(comment.comment_id),
                    'story_id': str(comment.story_id) if comment.story_id else None,
                    'episode_id': str(comment.episode_id) if comment.episode_id else None,
                    'parent_comment_id': str(comment.parent_comment_id) if comment.parent_comment_id else None,
                    'comment_text': comment.comment_text,
                    'created_at': comment.created_at.isoformat() if comment.created_at else None,
                    'updated_at': comment.updated_at.isoformat() if comment.updated_at else None,
                    'comment_like_count': comment.comment_like_count or 0
                }
                
                # Get real-time like count from Redis
                like_count_key = f"comment:{comment.comment_id}:comment_like_count"
                try:
                    like_count = await redis.get(like_count_key)
                    if like_count:
                        comment_dict['comment_like_count'] = int(like_count.decode('utf-8'))
                except Exception as e:
                    logger.warning(f"Failed to get Redis like count for comment {comment.comment_id}: {e}")
                
                result.append(comment_dict)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting user comments: {e}", exc_info=True)
            return []

    @staticmethod
    async def get_comment_stats(redis: Redis) -> Dict[str, Any]:
        """Get statistics about comments"""
        try:
            stats = {
                "redis_comments": 0,
                "pending_sync": 0,
                "total_likes": 0
            }
            
            # Count Redis comments
            async for key in redis.scan_iter(match="comment:*", count=100):
                key_str = key.decode('utf-8')
                if ':replies' not in key_str and ':comment_like_count' not in key_str:
                    stats["redis_comments"] += 1
            
            # Count pending sync
            pending_sync = await redis.scard("comments:to_sync")
            stats["pending_sync"] = pending_sync
            
            # Count like counters
            async for key in redis.scan_iter(match="comment:*:comment_like_count", count=100):
                try:
                    like_count = await redis.get(key)
                    if like_count:
                        stats["total_likes"] += int(like_count.decode('utf-8'))
                except Exception:
                    continue
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting comment stats: {e}")
            return {"error": str(e)}