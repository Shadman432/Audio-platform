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
from ..models.users import User
from ..models.stories_authors import StoriesAuthors
from ..database import SessionLocal
from ..tasks import save_comment_to_db

class CommentService:

    @staticmethod
    async def edit_comment(db: Session, redis: Redis, comment_id: uuid.UUID, user_id: uuid.UUID, new_text: str) -> Dict[str, Any]:
        """
        Edit a comment's text. The update is written to Redis immediately and
        added to a queue for batch database updates.
        Only the comment owner can edit the comment, and only if it is visible.
        """
        # --- Authorization and Validation ---
        comment = db.query(Comment).filter(Comment.comment_id == comment_id).first()
        if not comment:
            raise HTTPException(status_code=404, detail="Comment not found")

        # A comment can only be edited by the user who originally posted it.
        if comment.user_id != user_id:
            raise HTTPException(status_code=403, detail="You are not authorized to edit this comment")

        # If a comment’s visible flag is set to False, it should not be editable.
        if not comment.is_visible:
            raise HTTPException(status_code=403, detail="Comment visibility is off; it cannot be edited.")

        # --- Redis Update ---
        now = datetime.now(timezone.utc)
        comment_key = f"comment:{comment_id}"

        # Determine if it's a story or episode comment for linkify
        is_story_comment = comment.story_id is not None
        comment_text_html = CommentService._linkify_timestamps(new_text, is_story_comment)

        update_data = {
            "comment_text": new_text,
            "comment_text_html": comment_text_html,
            "updated_at": str(now),
            "is_edited": "True",
        }

        await redis.hset(comment_key, mapping=update_data)

        # --- Add to Batch Update Queue ---
        await redis.rpush("comments:edit_queue", str(comment_id))

        # --- Return Updated Data ---
        updated_comment_data = await redis.hgetall(comment_key)
        decoded_data = {k.decode(): v.decode() for k, v in updated_comment_data.items()}

        # Reconstruct response to match CommentResponse model
        parent_comment_id_str = decoded_data.get("parent_comment_id")
        story_id_str = decoded_data.get("story_id")
        episode_id_str = decoded_data.get("episode_id")

        return {
            "message": f"(user:{comment.user_id}) Comment edited successfully by (user:{user_id}).",
            "comment": {
                "comment_id": uuid.UUID(decoded_data["comment_id"]),
                "story_id": uuid.UUID(story_id_str) if story_id_str else None,
                "episode_id": uuid.UUID(episode_id_str) if episode_id_str else None,
                "user_id": uuid.UUID(decoded_data["user_id"]),
                "parent_comment_id": uuid.UUID(parent_comment_id_str) if parent_comment_id_str else None,
                "comment_text": decoded_data["comment_text"],
                "created_at": datetime.fromisoformat(decoded_data["created_at"]),
                "updated_at": datetime.fromisoformat(decoded_data["updated_at"]),
                "comment_like_count": int(decoded_data.get("comment_like_count", 0)),
                "is_edited": decoded_data.get("is_edited", "False").lower() == "true",
                "is_visible": decoded_data.get("is_visible", "True").lower() == "true",
                "is_reply": parent_comment_id_str is not None and parent_comment_id_str != "",
            }
        }


    @staticmethod
    def _linkify_timestamps(text: str, is_story_comment: bool) -> str:
        """Converts timestamp notations in comments to clickable HTML links."""
        if is_story_comment:
            # Pattern for story comments: 1(22:22) or (22:22)
            pattern = r'(\d+)?\((\d{1,2}:\d{2}(?::\d{2})?)\)'
            def repl(match):
                episode_num = match.group(1) or '1'  # Default to 1 if not present
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

        if parent_comment_id:
            # Validate parent comment exists and belongs to the same story/episode
            parent_comment = db.query(Comment).filter(Comment.comment_id == parent_comment_id).first()
            if not parent_comment:
                raise HTTPException(status_code=404, detail='WRONG PARENT ID')
            
            if (story_id and parent_comment.story_id != story_id) or                (episode_id and parent_comment.episode_id != episode_id):
                raise HTTPException(status_code=400, detail='Parent cmt id is of differnt story/episode comment')

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
                "is_visible": True,
                "is_reply": parent_comment_id is not None,
                "is_edited": False,
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
                else:
                    redis_safe_data[k] = str(v)

            pipe.hset(metadata_key, mapping=redis_safe_data)
            pipe.expire(metadata_key, 43200)

            if parent_comment_id:
                # Increment replies_count in Redis for immediate feedback
                pipe.hincrby(f"comment:{parent_comment_id}", "replies_count", 1)
                # Queue the parent_comment_id for DB update by Celery
                pipe.rpush("comments:reply_count_updates", str(parent_comment_id))

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
                    
                    # Filter out not visible comments
                    if decoded_hash.get('is_visible', 'true').lower() != 'true':
                        continue

                    decoded_hash['score'] = score
                    decoded_hash['comment_like_count'] = int(decoded_hash.get('comment_like_count', 0))
                    decoded_hash['replies_count'] = int(decoded_hash.get('replies_count', 0))
                    decoded_hash['comment_text_html'] = CommentService._linkify_timestamps(decoded_hash.get('comment_text', ''), is_story_comment)
                    decoded_hash.pop('comment_text', None)  # Remove the raw text
                    decoded_hash.pop('hidden_due_to_parent', None) # Remove hidden_due_to_parent
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
            Comment.is_visible == True
        ).order_by(desc(Comment.comment_like_count))
        
        comments = query.limit(limit).all()
        return [CommentService._format_db_comment(db, c, is_story_comment) for c in comments]

    @staticmethod
    def _format_db_comment(db: Session, comment: Comment, is_story_comment: bool) -> Dict[str, Any]:
        """Helper to format a comment object from the DB."""
        comment_text_html = CommentService._linkify_timestamps(comment.comment_text, is_story_comment)
        return {
            "comment_id": comment.comment_id,
            "story_id": comment.story_id,
            "episode_id": comment.episode_id,
            "parent_comment_id": comment.parent_comment_id,
            "comment_text_html": comment_text_html,
            "user_id": comment.user_id,
            "created_at": comment.created_at,
            "updated_at": comment.updated_at,
            "comment_like_count": comment.comment_like_count,
            "replies_count": comment.reply_count,
            "is_visible": comment.is_visible,
            "is_reply": comment.is_reply,
        }

    @staticmethod
    def _get_all_reply_ids(db: Session, comment_id: uuid.UUID) -> List[uuid.UUID]:
        """
        Recursively fetches all comment IDs that are replies to the given comment_id.
        """
        # Find direct replies
        direct_replies = db.query(Comment.comment_id).filter(Comment.parent_comment_id == comment_id).all()
        direct_reply_ids = [reply[0] for reply in direct_replies]

        all_reply_ids = list(direct_reply_ids)

        # For each direct reply, find its replies
        for reply_id in direct_reply_ids:
            all_reply_ids.extend(CommentService._get_all_reply_ids(db, reply_id))

        return all_reply_ids

    @staticmethod
    async def update_comment_visibility(db: Session, redis: Redis, comment_id: uuid.UUID, user_id: uuid.UUID) -> dict:
        """
        Toggle visibility of a comment and its replies in Redis + queue DB update.
        - Parent visible OFF => all replies hidden with flag hidden_due_to_parent=True (only if they were visible)
        - Parent visible ON => only replies with hidden_due_to_parent=True become visible again
        - Manually hidden replies stay hidden.
        """
        # Fetch user and comment
        user = db.query(User).filter(User.user_id == user_id).first()
        comment = db.query(Comment).filter(Comment.comment_id == comment_id).first()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        if not comment:
            raise HTTPException(status_code=404, detail="Comment not found")

        is_admin = user.role == 'admin'
        is_owner = comment.user_id == user_id
        is_creator = False

        if not is_owner and not is_admin:
            if comment.story_id:
                author_entry = db.query(StoriesAuthors).filter(
                    StoriesAuthors.story_id == comment.story_id,
                    StoriesAuthors.user_id == user_id,
                    StoriesAuthors.role == 'primary_author'
                ).first()
                if author_entry:
                    is_creator = True
            elif comment.episode_id:
                episode = db.query(Episode).filter(Episode.episode_id == comment.episode_id).first()
                if episode:
                    author_entry = db.query(StoriesAuthors).filter(
                        StoriesAuthors.story_id == episode.story_id,
                        StoriesAuthors.user_id == user_id,
                        StoriesAuthors.role == 'primary_author'
                    ).first()
                    if author_entry:
                        is_creator = True

        if not (is_admin or is_owner or is_creator):
            raise HTTPException(status_code=403, detail="Normal users can't hide/show others' comments")

        # Toggle visibility
        comment_meta_key = f"comment:{comment_id}"
        current_vis_raw = await redis.hget(comment_meta_key, "is_visible")

        if current_vis_raw is None:
            # Cache miss: Fetch from DB and warm up the cache
            current_visible = comment.is_visible
            await redis.hset(comment_meta_key, "is_visible", str(current_visible))
        else:
            # Cache hit: Use the value from Redis
            current_visible = current_vis_raw.decode().lower() == "true"

        new_visible = not current_visible

        # A reply cannot be made visible if its parent is hidden.
        if new_visible and comment.parent_comment_id:
            parent_meta_key = f"comment:{comment.parent_comment_id}"
            parent_vis_raw = await redis.hget(parent_meta_key, "is_visible")
            parent_visible = not parent_vis_raw or parent_vis_raw.decode().lower() == "true"
            if not parent_visible:
                raise HTTPException(status_code=400, detail="Cannot make a reply visible when its parent is hidden.")

        comment_ids = [comment_id] + CommentService._get_all_reply_ids(db, comment_id)
        parent_type = "story" if comment.story_id else "episode"
        parent_id = str(comment.story_id or comment.episode_id)
        parent_key = f"comments:{parent_type}:{parent_id}"

        pipe = redis.pipeline()

        if new_visible:
            # Re-show parent and replies that were hidden due to the parent
            pipe_fetch = redis.pipeline()
            for cid in comment_ids:
                pipe_fetch.hget(f"comment:{cid}", "comment_like_count")
                pipe_fetch.hget(f"comment:{cid}", "hidden_due_to_parent")
            res = await pipe_fetch.execute()

            for i, c_id in enumerate(comment_ids):
                like_str = res[i*2]
                hidden_by_parent_raw = res[i*2 + 1]
                
                was_hidden_by_parent = hidden_by_parent_raw and hidden_by_parent_raw.decode().lower() == 'true'

                # Show the main comment, or any reply that was hidden by the parent
                if (c_id == comment_id) or was_hidden_by_parent:
                    score = 0
                    if like_str:
                        try:
                            score = int(like_str.decode() if hasattr(like_str, "decode") else like_str)
                        except (ValueError, AttributeError):
                            score = 0
                    
                    pipe.hset(f"comment:{c_id}", "is_visible", "True")
                    pipe.hdel(f"comment:{c_id}", "hidden_due_to_parent")
                    pipe.zadd(parent_key, {str(c_id): score})
                    pipe.rpush("comments:visibility_updates", json.dumps({
                        "comment_id": str(c_id),
                        "is_visible": True
                    }))
        else:
            # Hide parent + all replies
            # First, fetch current visibility of all replies to check which ones are already hidden
            pipe_check = redis.pipeline()
            for c_id in comment_ids:
                if c_id != comment_id:  # Only check replies
                    pipe_check.hget(f"comment:{c_id}", "is_visible")
            visibility_results = await pipe_check.execute()
            
            # Now hide all comments
            reply_idx = 0
            for c_id in comment_ids:
                pipe.hset(f"comment:{c_id}", "is_visible", "False")
                
                if c_id != comment_id:
                    # Only mark as hidden_due_to_parent if the reply was currently visible
                    curr_vis_raw = visibility_results[reply_idx]
                    reply_idx += 1
                    
                    # If reply was visible, mark it as hidden due to parent
                    if curr_vis_raw and curr_vis_raw.decode().lower() == "true":
                        pipe.hset(f"comment:{c_id}", "hidden_due_to_parent", "True")
                    # If reply was already hidden (manually), don't add the flag
                
                pipe.zrem(parent_key, str(c_id))
                pipe.rpush("comments:visibility_updates", json.dumps({
                    "comment_id": str(c_id),
                    "is_visible": False
                }))

        await pipe.execute()

        # Construct response
        actor = "admin" if is_admin else "primary author" if is_creator else "user"
        action = "shown" if new_visible else "hidden"
        msg = f"Comment and its {len(comment_ids) - 1} replies have been {action} by {actor}."

        updated_data = await redis.hgetall(comment_meta_key)
        decoded = {k.decode(): v.decode() for k, v in updated_data.items()}

        return {"message": msg, "comment": decoded}

        
            
    @staticmethod
    async def like_comment(redis: Redis, db: Session, comment_id: uuid.UUID, user_id: uuid.UUID) -> dict:
        """Toggle comment like - Redis first, DB sync via Celery"""
        
        # --- Validation ---
        comment = db.query(Comment).filter(Comment.comment_id == comment_id).first()
        if not comment:
            raise HTTPException(status_code=404, detail="Comment not found")

        try:
            # Redis keys
            like_key = f"comment_like:{comment_id}:{user_id}"
            comment_meta_key = f"comment:{comment_id}"
            
            # Check if like exists in Redis
            existing_like = await redis.get(like_key)
            
            if existing_like:
                # Unlike - remove from Redis
                await redis.delete(like_key)
                await redis.hincrby(comment_meta_key, "comment_like_count", -1)
                
                # Queue for DB deletion
                await redis.rpush("comment_likes:delete_queue", json.dumps({
                    "comment_id": str(comment_id),
                    "user_id": str(user_id)
                }))
                
                return {"liked": False, "message": "Comment like removed", "user_id": str(user_id), "comment_like_id": None}
            else:
                # Like - add to Redis
                await redis.setex(like_key, 43200, "1")  # 12 hour TTL
                await redis.hincrby(comment_meta_key, "comment_like_count", 1)
                
                comment_like_id = uuid.uuid4()
                # Queue for DB insertion
                await redis.rpush("comment_likes:insert_queue", json.dumps({
                    "comment_like_id": str(comment_like_id),
                    "comment_id": str(comment_id),
                    "user_id": str(user_id),
                    "created_at": datetime.now(timezone.utc).isoformat()
                }))
                
                return {"liked": True, "message": "Comment liked", "user_id": str(user_id), "comment_like_id": comment_like_id}
        except IntegrityError:
            db.rollback()
            raise HTTPException(status_code=400, detail="Like operation failed due to a database error.")