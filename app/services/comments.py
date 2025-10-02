# app/services/comments.py - Refactored for Redis-first comments

import re
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from typing import List, Dict, Any, Optional
import uuid
import random
import json
from datetime import datetime, timezone
from redis.asyncio import Redis
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException

from ..models.comments import Comment
from ..models.comment_likes import CommentLike
from ..models.stories import Story
from ..models.episodes import Episode
from ..database import SessionLocal
from ..tasks import save_comment_to_db

class CommentService:

    @staticmethod
    def _linkify_timestamps(text: str, is_story_comment: bool) -> str:
        """Converts timestamp notations in comments to clickable HTML links."""
        if is_story_comment:
            # Pattern for story comments: 1(22:22), 2(33:44), etc.
            pattern = r'(\d+)\((\d{1,2}:\d{2}(?::\d{2})?)\)'
            def repl(match):
                episode_num = match.group(1)
                timestamp = match.group(2)
                return f'<a href="/episode/{episode_num}?time={timestamp}">{match.group(0)}</a>'
            return re.sub(pattern, repl, text)
        else:
            # Pattern for episode comments: 10:22, 29:09, etc.
            pattern = r'(\d{1,2}:\d{2}(?::\d{2})?)'
            def repl(match):
                timestamp = match.group(1)
                return f'<a href="?time={timestamp}">{timestamp}</a>'
            return re.sub(pattern, repl, text)

    @staticmethod
    async def add_comment(db: Session, redis: Redis, comment_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Immediately writes comment to Redis for speed and queues DB write as a background task.
        Validates existence of parent entities before creation.
        """
        story_id = comment_data.get("story_id")
        episode_id = comment_data.get("episode_id")
        parent_comment_id = comment_data.get("parent_comment_id")

        # --- Validation ---
        if story_id and not db.query(Story).filter(Story.story_id == story_id).first():
            raise HTTPException(status_code=404, detail='WRONG STORY ID OR EP ID')
        elif episode_id and not db.query(Episode).filter(Episode.episode_id == episode_id).first():
            raise HTTPException(status_code=404, detail='WRONG STORY ID OR EP ID')

        if parent_comment_id and not db.query(Comment).filter(Comment.comment_id == parent_comment_id).first():
            raise HTTPException(status_code=404, detail='WRONG PARENT ID')

        try:
            new_comment_id = uuid.uuid4()
            now = datetime.now(timezone.utc)

            comment_text = comment_data["comment_text"]
            is_story_comment = story_id is not None
            comment_text_html = CommentService._linkify_timestamps(comment_text, is_story_comment)

            full_comment_data = {
                **comment_data,
                "comment_id": new_comment_id,
                "created_at": now,
                "updated_at": now,
                "comment_like_count": 0,
                "replies_count": 0,
                "comment_text_html": comment_text_html,
            }

            pipe = redis.pipeline()

            parent_type = "story" if story_id else "episode"
            parent_id = str(story_id or episode_id)
            
            pipe.hincrby(f"{parent_type}:{parent_id}", "comments_count", 1)
            
            comment_key = f"comments:{parent_type}:{parent_id}"
            pipe.zadd(comment_key, {str(new_comment_id): 0})
            pipe.expire(comment_key, 43200)

            metadata_key = f"comment:{new_comment_id}"
            
            redis_safe_data = {}
            for k, v in full_comment_data.items():
                if v is None:
                    redis_safe_data[k] = ""
                elif isinstance(v, (str, int, float, bytes)):
                    redis_safe_data[k] = v
                else:
                    redis_safe_data[k] = str(v)

            pipe.hset(metadata_key, mapping=redis_safe_data)
            pipe.expire(metadata_key, 43200)

            if parent_comment_id:
                pipe.hincrby(f"comment:{parent_comment_id}", "replies_count", 1)

            await pipe.execute()

            db_data = {
                "comment_id": str(new_comment_id),
                "story_id": str(story_id) if story_id else None,
                "episode_id": str(episode_id) if episode_id else None,
                "parent_comment_id": str(parent_comment_id) if parent_comment_id else None,
                "user_id": str(full_comment_data["user_id"]),
                "comment_text": full_comment_data["comment_text"],
                "created_at": now.isoformat(),
                "updated_at": now.isoformat(),
            }
            # Add to Redis queue for batch DB write
            await redis.rpush("comments:db_queue", json.dumps(db_data))
            
            # Check queue size and trigger batch save if it reaches 50
            queue_size = await redis.llen("comments:db_queue")
            if queue_size >= 50:
                from ..tasks import batch_save_comments_to_db
                batch_save_comments_to_db.delay()

            await pipe.execute()

            return full_comment_data

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing comment: {e}")

    @staticmethod
    async def get_ranked_comments(db: Session, redis: Redis, story_id: Optional[uuid.UUID] = None, episode_id: Optional[uuid.UUID] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Gets ranked comments from Redis, with a database fallback (cache-aside).
        If comments are not in Redis, it fetches them from the DB, repopulates the cache,
        and sets a 12-hour TTL.
        """
        is_story_comment = story_id is not None
        parent_type = "story" if story_id else "episode"
        parent_id = str(story_id or episode_id)
        parent_key = f"comments:{parent_type}:{parent_id}"

        # 1. Try to fetch from Redis
        try:
            comment_ids_with_scores = await redis.zrevrange(parent_key, 0, limit - 1, withscores=True)

            if comment_ids_with_scores:
                pipe = redis.pipeline()
                for comment_id, score in comment_ids_with_scores:
                    pipe.hgetall(f"comment:{comment_id.decode()}")
                
                comment_hashes = await pipe.execute()

                results = []
                for (comment_id, score), comment_hash in zip(comment_ids_with_scores, comment_hashes):
                    if not comment_hash:
                        continue  # Skip if a comment hash is missing
                    
                    decoded_hash = {k.decode(): v.decode() for k, v in comment_hash.items()}
                    decoded_hash['score'] = score
                    decoded_hash['comment_like_count'] = int(decoded_hash.get('comment_like_count', 0))
                    decoded_hash['replies_count'] = int(decoded_hash.get('replies_count', 0))
                    decoded_hash['comment_text_html'] = CommentService._linkify_timestamps(decoded_hash['comment_text'], is_story_comment)
                    results.append(decoded_hash)
                
                if results:
                    return results
        except Exception:
            # Could be a Redis connection error, proceed to DB fallback
            pass

        # 2. Fallback to DB if Redis is empty or fails
        db_comments = CommentService.get_ranked_comments_from_db(db, story_id=story_id, episode_id=episode_id, limit=limit)

        if not db_comments:
            return []

        # 3. Repopulate Redis
        try:
            pipe = redis.pipeline()
            for comment in db_comments:
                comment_id = str(comment["comment_id"])
                metadata_key = f"comment:{comment_id}"
                
                redis_safe_data = {
                    k: str(v) if v is not None else "" for k, v in comment.items()
                }
                
                pipe.hset(metadata_key, mapping=redis_safe_data)
                pipe.expire(metadata_key, 43200)  # 12h TTL

                score = comment.get("comment_like_count", 0)
                pipe.zadd(parent_key, {comment_id: score})

            pipe.expire(parent_key, 43200)  # 12h TTL on the main set
            await pipe.execute()
        except Exception:
            # If repopulation fails, we still serve from DB
            pass

        return db_comments

    @staticmethod
    def get_ranked_comments_from_db(db: Session, story_id: Optional[uuid.UUID] = None, episode_id: Optional[uuid.UUID] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        [DB-ONLY] Gets ranked comments from the database. For development/debugging.
        """
        is_story_comment = story_id is not None
        query = db.query(Comment).filter(
            (Comment.story_id == story_id) if story_id else (Comment.episode_id == episode_id),
            Comment.parent_comment_id == None
        ).order_by(desc(Comment.comment_like_count))
        
        comments = query.limit(limit).all()
        return [CommentService._format_db_comment(db, c, is_story_comment) for c in comments]

    @staticmethod
    def _format_db_comment(db: Session, comment: Comment, is_story_comment: bool) -> Dict[str, Any]:
        """Helper to format a comment object from the DB."""
        replies_count = db.query(Comment).filter(Comment.parent_comment_id == comment.comment_id).count()
        comment_text_html = CommentService._linkify_timestamps(comment.comment_text, is_story_comment)
        return {
            "comment_id": comment.comment_id,
            "story_id": comment.story_id,
            "episode_id": comment.episode_id,
            "parent_comment_id": comment.parent_comment_id,
            "comment_text": comment.comment_text,
            "comment_text_html": comment_text_html,
            "user_id": comment.user_id,
            "created_at": comment.created_at,
            "updated_at": comment.updated_at,
            "comment_like_count": comment.comment_like_count,
            "replies_count": replies_count
        }

    @staticmethod
    async def like_comment(redis: Redis, db: Session, comment_id: uuid.UUID, user_id: uuid.UUID) -> bool:
        """Toggle comment like and update Redis engagement score"""
        try:
            comment = db.query(Comment).filter(Comment.comment_id == comment_id).first()
            if not comment:
                raise HTTPException(status_code=404, detail="Comment not found")

            existing_like = db.query(CommentLike).filter(
                CommentLike.comment_id == comment_id,
                CommentLike.user_id == user_id
            ).first()

            parent_type = "story" if comment.story_id else "episode"
            parent_id = str(comment.story_id or comment.episode_id)
            parent_key = f"comments:{parent_type}:{parent_id}"
            comment_meta_key = f"comment:{comment_id}"

            if existing_like:
                db.delete(existing_like)
                comment.comment_like_count = max(0, comment.comment_like_count - 1)
                db.commit()
                await redis.hincrby(comment_meta_key, "comment_like_count", -1)
                await redis.zincrby(parent_key, -1, str(comment_id))
                return False
            else:
                like = CommentLike(comment_id=comment_id, user_id=user_id)
                db.add(like)
                comment.comment_like_count += 1
                db.commit()
                await redis.hincrby(comment_meta_key, "comment_like_count", 1)
                await redis.zincrby(parent_key, 1, str(comment_id))
                return True
        except IntegrityError:
            db.rollback()
            raise HTTPException(status_code=400, detail="Like operation failed due to a database error.")
