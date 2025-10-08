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
        # Reconstruct the response from the DB object and the new data to avoid issues with expired Redis keys.
        
        # The like count is primarily managed in Redis, so we fetch it.
        # If it's not in Redis (cache miss), we fall back to the value from the DB.
        like_count_str = await redis.hget(comment_key, "comment_like_count")
        try:
            # The value from DB is already an int.
            like_count = int(like_count_str) if like_count_str else comment.comment_like_count
        except (ValueError, TypeError):
            like_count = comment.comment_like_count # Fallback to DB value on parsing error

        return {
            "message": f"(user:{comment.user_id}) Comment edited successfully by (user:{user_id}).",
            "comment": {
                "comment_id": comment.comment_id,
                "story_id": comment.story_id,
                "episode_id": comment.episode_id,
                "user_id": comment.user_id,
                "parent_comment_id": comment.parent_comment_id,
                "comment_text": new_text,
                "created_at": comment.created_at,
                "updated_at": now,
                "comment_like_count": like_count,
                "is_edited": True,
                "is_visible": comment.is_visible,
                "is_reply": comment.is_reply,
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
                "is_pinned": False,
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
    async def get_ranked_comments(db: Session, redis: Redis, story_id: Optional[uuid.UUID] = None, episode_id: Optional[uuid.UUID] = None, limit: int = 50) -> Dict[str, Any]:
        """
        Gets ranked comments from Redis, falling back to DB if cache is empty or corrupted.
        """
        is_story_comment = story_id is not None
        parent_type = "story" if story_id else "episode"
        parent_id = str(story_id or episode_id)
        parent_key = f"comments:{parent_type}:{parent_id}"

        results = []
        cache_is_corrupted = False
        try:
            comment_ids_with_scores = await redis.zrevrange(parent_key, 0, limit - 1, withscores=True)

            if comment_ids_with_scores:
                pipe = redis.pipeline()
                for comment_id, score in comment_ids_with_scores:
                    pipe.hgetall(f"comment:{comment_id.decode()}")
                
                comment_hashes = await pipe.execute()

                for (comment_id, score), comment_hash in zip(comment_ids_with_scores, comment_hashes):
                    if not comment_hash:
                        cache_is_corrupted = True
                        break

                    decoded_hash = {k.decode(): v.decode() for k, v in comment_hash.items()}

                    # If essential fields are missing, the cache is corrupt.
                    if not all(k in decoded_hash for k in ['comment_id', 'user_id', 'created_at']):
                        cache_is_corrupted = True
                        break
                    
                    if decoded_hash.get('is_visible', 'true').lower() != 'true':
                        continue

                    decoded_hash['score'] = score
                    decoded_hash['comment_like_count'] = int(decoded_hash.get('comment_like_count', 0))
                    decoded_hash['replies_count'] = int(decoded_hash.get('replies_count', 0))
                    decoded_hash['is_pinned'] = decoded_hash.get('is_pinned', 'false').lower() == 'true'
                    decoded_hash['comment_text_html'] = CommentService._linkify_timestamps(decoded_hash.get('comment_text', ''), is_story_comment)
                    decoded_hash.pop('comment_text', None)
                    decoded_hash.pop('hidden_due_to_parent', None)
                    decoded_hash.pop('story_id', None)
                    decoded_hash.pop('episode_id', None)
                    results.append(decoded_hash)
            
            if results and not cache_is_corrupted:
                return {
                    "story_id": story_id,
                    "episode_id": episode_id,
                    "comments": results
                }
            
            if cache_is_corrupted:
                results = []  # Ensure we fall through to DB

        except Exception:
            # On any exception, fall back to DB
            pass

        # Fallback to DB
        db_comments = CommentService.get_ranked_comments_from_db(db, story_id=story_id, episode_id=episode_id, limit=limit)

        if not db_comments:
            return []

        # Repopulate Redis
        try:
            pipe = redis.pipeline()
            for comment in db_comments:
                comment_id = str(comment["comment_id"])
                metadata_key = f"comment:{comment_id}"
                
                redis_safe_data = {
                    k: str(v) if v is not None else "" for k, v in comment.items()
                }
                
                pipe.hset(metadata_key, mapping=redis_safe_data)
                pipe.expire(metadata_key, 43200)

                score = comment.get("comment_like_count", 0)
                pipe.zadd(parent_key, {comment_id: score})

            pipe.expire(parent_key, 43200)
            await pipe.execute()
        except Exception:
            pass

        return db_comments

    @staticmethod
    def get_ranked_comments_from_db(db: Session, story_id: Optional[uuid.UUID] = None, episode_id: Optional[uuid.UUID] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        [DB-ONLY] Gets ranked comments from the database.
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
            "is_pinned": comment.is_pinned,
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
        - Primary author can hide/unhide any comment
        - Owner can only hide their own comment (not unhide if creator hid it)
        - Parent visible OFF => all replies hidden with flag hidden_due_to_parent=True (only if they were visible)
        - Parent visible ON => only replies with hidden_due_to_parent=True become visible again
        - Manually hidden replies stay hidden and cannot be unhidden until parent is unhidden first
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

        # Check if user is primary author of the story
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

        comment_meta_key = f"comment:{comment_id}"
        current_vis_raw = await redis.hget(comment_meta_key, "is_visible")
        hidden_by_creator_raw = await redis.hget(comment_meta_key, "hidden_by_creator")
        hidden_by_creator = hidden_by_creator_raw and hidden_by_creator_raw.decode().lower() == 'true'

        if current_vis_raw is None:
            current_visible = comment.is_visible
            await redis.hset(comment_meta_key, "is_visible", str(current_visible))
        else:
            current_visible = current_vis_raw.decode().lower() == "true"

        new_visible = not current_visible

        # Authorization checks
        if not (is_admin or is_creator):
            if is_owner:
                if not current_visible:  # Trying to unhide
                    # Owner CANNOT unhide if creator hid it
                    if hidden_by_creator:
                        raise HTTPException(
                            status_code=403, 
                            detail="You cannot unhide your comment because it was hidden by the primary author."
                        )
                else:  # Trying to hide their own comment
                    pass  # Owner can always hide their own comment
            else:
                raise HTTPException(status_code=403, detail="You are not authorized to modify this comment's visibility.")

        # A reply cannot be made visible if its parent is hidden
        if new_visible and comment.parent_comment_id:
            parent_meta_key = f"comment:{comment.parent_comment_id}"
            parent_vis_raw = await redis.hget(parent_meta_key, "is_visible")
            
            if parent_vis_raw is None:
                # Check DB
                parent_comment = db.query(Comment).filter(Comment.comment_id == comment.parent_comment_id).first()
                parent_visible = parent_comment.is_visible if parent_comment else False
            else:
                parent_visible = parent_vis_raw.decode().lower() == "true"
            
            if not parent_visible:
                raise HTTPException(
                    status_code=400, 
                    detail="Cannot make a reply visible when its parent comment is hidden. The parent must be unhidden first."
                )

        comment_ids = [comment_id] + CommentService._get_all_reply_ids(db, comment_id)
        parent_type = "story" if comment.story_id else "episode"
        parent_id = str(comment.story_id or comment.episode_id)
        parent_key = f"comments:{parent_type}:{parent_id}"

        pipe = redis.pipeline()

        if new_visible:
            # Re-show parent and replies that were hidden due to the parent
            pipe.hdel(f"comment:{comment_id}", "hidden_by_creator")  # Clear the flag
            
            pipe_fetch = redis.pipeline()
            for cid in comment_ids:
                pipe_fetch.hget(f"comment:{cid}", "comment_like_count")
                pipe_fetch.hget(f"comment:{cid}", "hidden_due_to_parent")
            res = await pipe_fetch.execute()

            for i, c_id in enumerate(comment_ids):
                like_str = res[i*2]
                hidden_by_parent_raw = res[i*2 + 1]
                
                was_hidden_by_parent = hidden_by_parent_raw and hidden_by_parent_raw.decode().lower() == 'true'

                # Show the main comment, or any reply that was ONLY hidden because of the parent
                # (replies that were manually hidden should stay hidden)
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
            # Hide parent + all visible replies
            if is_creator and not is_owner:
                # Mark that creator hid this comment
                pipe.hset(f"comment:{comment_id}", "hidden_by_creator", "True")

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
    async def pin_unpin_comment(db: Session, redis: Redis, comment_id: uuid.UUID, user_id: uuid.UUID) -> dict:
        """
        Toggle the pinned status of a comment.
        Only the primary author of the story can pin or unpin a comment.
        """
        comment = db.query(Comment).filter(Comment.comment_id == comment_id).first()
        if not comment:
            raise HTTPException(status_code=404, detail="Comment not found")

        is_creator = False
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

        if not is_creator:
            raise HTTPException(status_code=403, detail="Only the primary author can pin or unpin comments.")

        # Toggle the is_pinned status
        new_pinned_status = not comment.is_pinned
        comment.is_pinned = new_pinned_status
        db.commit()

        # Update Redis
        comment_key = f"comment:{comment_id}"
        await redis.hset(comment_key, "is_pinned", str(new_pinned_status))

        action = "pinned" if new_pinned_status else "unpinned"
        return {"message": f"Comment has been successfully {action}.", "is_pinned": new_pinned_status}
             
    @staticmethod
    async def like_comment(redis: Redis, db: Session, comment_id: uuid.UUID, user_id: uuid.UUID):
        """
        Toggles a like on a comment. If the user has already liked the comment, it unlikes it.
        Otherwise, it adds a like. Updates are reflected in Redis immediately and queued for DB persistence.
        """
        # --- Validate Comment Existence ---
        comment_key = f"comment:{comment_id}"
        comment_in_db = db.query(Comment).filter(Comment.comment_id == comment_id).first()
        if not await redis.exists(comment_key):
            # Fallback to check DB if not in Redis
            if not comment_in_db:
                raise HTTPException(status_code=404, detail="Comment not found")
        
        # --- Check for Existing Like ---
        existing_like = db.query(CommentLike).filter(
            CommentLike.comment_id == comment_id,
            CommentLike.user_id == user_id
        ).first()

        pipe = redis.pipeline()
        parent_type = "story" if comment_in_db.story_id else "episode"
        parent_id = str(comment_in_db.story_id or comment_in_db.episode_id)
        parent_key = f"comments:{parent_type}:{parent_id}"

        if existing_like:
            # --- Unlike ---
            db.delete(existing_like)
            db.commit()

            # Decrement Redis counters
            pipe.hincrby(comment_key, "comment_like_count", -1)
            pipe.zincrby(parent_key, -1, str(comment_id))
            
            await pipe.execute()

            return {
                "liked": False,
                "message": "Comment unliked successfully.",
                "user_id": user_id,
                "comment_like_id": None
            }
        else:
            # --- Like ---
            new_like = CommentLike(comment_id=comment_id, user_id=user_id)
            db.add(new_like)
            db.commit()

            # Increment Redis counters
            pipe.hincrby(comment_key, "comment_like_count", 1)
            pipe.zincrby(parent_key, 1, str(comment_id))

            await pipe.execute()

            return {
                "liked": True,
                "message": "Comment liked successfully.",
                "user_id": user_id,
                "comment_like_id": new_like.comment_like_id
            }