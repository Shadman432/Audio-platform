from typing import Dict, Any
from ..models.stories import Story
from ..models.episodes import Episode
from ..models.home_content import HomeContent
from ..models.home_content_series import HomeContentSeries
from ..models.home_slideshow import HomeSlideshow
from ..models.comments import Comment
from ..models.comment_likes import CommentLike
from ..models.users import User
from ..models.notifications import Notification
from ..models.push_notifications import PushNotification
from ..models.user_devices import UserDevice
from ..models.user_events import UserEvent
from ..models.engagements import Engagement
from ..models.reports import Report
from ..models.home_continue_watching import HomeContinueWatching
from ..models.stories_authors import StoriesAuthors
from ..models.episode_authors import EpisodeAuthors
from ..models.likes import Like
from ..models.ratings import Rating
from ..models.views import View
from ..models.shares import Share

def story_to_dict(s: Story) -> Dict[str, Any]:
    """Convert Story ORM to dict - optimized"""
    return {
        "story_id": str(s.story_id),
        "title": s.title,
        "meta_title": s.meta_title,
        "thumbnail_square": s.thumbnail_square,
        "thumbnail_rect": s.thumbnail_rect,
        "thumbnail_responsive": s.thumbnail_responsive,
        "description": s.description,
        "meta_description": s.meta_description,
        "genre": s.genre,
        "subgenre": s.subgenre,
        "rating": s.rating,
        "avg_rating": s.avg_rating,
        "avg_rating_count": s.avg_rating_count,
        "likes_count": s.likes_count,
        "comments_count": s.comments_count,
        "shares_count": s.shares_count,
        "views_count": s.views_count,
        "author_json": s.author_json,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
    }

def episode_to_dict(episode: Episode) -> Dict[str, Any]:
    return {
        "episode_id": str(episode.episode_id),
        "story_id": str(episode.story_id),
        "title": episode.title,
        "meta_title": episode.meta_title,
        "thumbnail_square": episode.thumbnail_square,
        "thumbnail_rect": episode.thumbnail_rect,
        "thumbnail_responsive": episode.thumbnail_responsive,
        "description": episode.description,
        "meta_description": episode.meta_description,
        "hls_url": episode.hls_url,
        "duration": episode.duration,
        "release_date": episode.release_date.isoformat() if episode.release_date else None,
        "genre": episode.genre,
        "subgenre": episode.subgenre,
        "rating": episode.rating,
        "avg_rating": episode.avg_rating,
        "avg_rating_count": episode.avg_rating_count,
        "likes_count": episode.likes_count,
        "comments_count": episode.comments_count,
        "shares_count": episode.shares_count,
        "views_count": episode.views_count,
        "author_json": episode.author_json,
        "created_at": episode.created_at.isoformat() if episode.created_at else None,
        "updated_at": episode.updated_at.isoformat() if episode.updated_at else None,
    }

def home_content_to_dict(hc: HomeContent) -> Dict[str, Any]:
    """Convert HomeContent ORM to dict"""
    return {
        "category_id": str(hc.category_id),
        "category_name": hc.category_name,
        "created_at": hc.created_at.isoformat() if hc.created_at else None,
        "series": [
            {
                "content_series_id": str(s.content_series_id),
                "story_id": str(s.story_id),
                "title": s.title,
                "thumbnail_square": s.thumbnail_square,
                "thumbnail_rect": s.thumbnail_rect,
                "thumbnail_responsive": s.thumbnail_responsive,
                "description": s.description,
                "genre": s.genre,
                "rating": s.rating,
                "created_at": s.created_at.isoformat() if s.created_at else None,
            } for s in hc.series
        ] if hc.series else []
    }

def home_content_series_to_dict(hcs: HomeContentSeries) -> Dict[str, Any]:
    """Convert HomeContentSeries ORM to dict"""
    return {
        "content_series_id": str(hcs.content_series_id),
        "category_id": str(hcs.category_id),
        "story_id": str(hcs.story_id),
        "title": hcs.title,
        "thumbnail_square": hcs.thumbnail_square,
        "thumbnail_rect": hcs.thumbnail_rect,
        "thumbnail_responsive": hcs.thumbnail_responsive,
        "description": hcs.description,
        "genre": hcs.genre,
        "subgenre": hcs.subgenre,
        "rating": hcs.rating,
        "avg_rating": hcs.avg_rating,
        "avg_rating_count": hcs.avg_rating_count,
        "created_at": hcs.created_at.isoformat() if hcs.created_at else None,
    }

def home_slideshow_to_dict(hs: HomeSlideshow) -> Dict[str, Any]:
    """Convert HomeSlideshow ORM to dict"""
    return {
        "slideshow_id": str(hs.slideshow_id),
        "story_id": str(hs.story_id),
        "title": hs.title,
        "thumbnail_square": hs.thumbnail_square,
        "thumbnail_rect": hs.thumbnail_rect,
        "thumbnail_responsive": hs.thumbnail_responsive,
        "backdrop_url": hs.backdrop_url,
        "description": hs.description,
        "genre": hs.genre,
        "subgenre": hs.subgenre,
        "rating": hs.rating,
        "avg_rating": hs.avg_rating,
        "avg_rating_count": hs.avg_rating_count,
        "trailer_url": hs.trailer_url,
        "button_text": hs.button_text,
        "button_link": hs.button_link,
        "display_order": hs.display_order,
        "is_active": hs.is_active,
        "created_at": hs.created_at.isoformat() if hs.created_at else None,
    }

def comment_to_dict(comment: Comment) -> Dict[str, Any]:
    return {
        "comment_id": str(comment.comment_id),
        "story_id": str(comment.story_id) if comment.story_id else None,
        "episode_id": str(comment.episode_id) if comment.episode_id else None,
        "user_id": str(comment.user_id),
        "parent_comment_id": str(comment.parent_comment_id) if comment.parent_comment_id else None,
        "comment_text": comment.comment_text,
        "comment_like_count": comment.comment_like_count,
        "created_at": comment.created_at.isoformat() if comment.created_at else None,
        "updated_at": comment.updated_at.isoformat() if comment.updated_at else None,
    }

def comment_like_to_dict(comment_like: CommentLike) -> Dict[str, Any]:
    return {
        "comment_like_id": str(comment_like.comment_like_id),
        "comment_id": str(comment_like.comment_id),
        "user_id": str(comment_like.user_id),
        "created_at": comment_like.created_at.isoformat() if comment_like.created_at else None,
    }

def user_to_dict(user: User) -> Dict[str, Any]:
    return {
        "user_id": str(user.user_id),
        "email": user.email,
        "full_name": user.full_name,
        "role": user.role,
        "created_at": user.created_at.isoformat() if user.created_at else None,
    }

def notification_to_dict(notification: Notification) -> Dict[str, Any]:
    return {
        "notification_id": str(notification.notification_id),
        "user_id": str(notification.user_id),
        "type": notification.type,
        "ref_id": str(notification.ref_id) if notification.ref_id else None,
        "message": notification.message,
        "is_read": notification.is_read,
        "is_clicked": notification.is_clicked,
        "created_at": notification.created_at.isoformat() if notification.created_at else None,
        "read_at": notification.read_at.isoformat() if notification.read_at else None,
        "clicked_at": notification.clicked_at.isoformat() if notification.clicked_at else None,
    }

def push_notification_to_dict(push_notification: PushNotification) -> Dict[str, Any]:
    return {
        "push_id": str(push_notification.push_id),
        "user_id": str(push_notification.user_id) if push_notification.user_id else None,
        "title": push_notification.title,
        "body": push_notification.body,
        "deep_link": push_notification.deep_link,
        "sent_at": push_notification.sent_at.isoformat() if push_notification.sent_at else None,
        "opened_at": push_notification.opened_at.isoformat() if push_notification.opened_at else None,
        "is_opened": push_notification.is_opened,
    }

def user_device_to_dict(user_device: UserDevice) -> Dict[str, Any]:
    return {
        "device_id": str(user_device.device_id),
        "user_id": str(user_device.user_id),
        "device_token": user_device.device_token,
        "device_type": user_device.device_type,
        "is_active": user_device.is_active,
        "last_active_at": user_device.last_active_at.isoformat() if user_device.last_active_at else None,
        "created_at": user_device.created_at.isoformat() if user_device.created_at else None,
    }

def user_event_to_dict(user_event: UserEvent) -> Dict[str, Any]:
    return {
        "event_id": str(user_event.event_id),
        "user_id": str(user_event.user_id) if user_event.user_id else None,
        "session_id": str(user_event.session_id) if user_event.session_id else None,
        "story_id": str(user_event.story_id) if user_event.story_id else None,
        "episode_id": str(user_event.episode_id) if user_event.episode_id else None,
        "group_id": str(user_event.group_id) if user_event.group_id else None,
        "genre_id": str(user_event.genre_id) if user_event.genre_id else None,
        "subgenre_id": str(user_event.subgenre_id) if user_event.subgenre_id else None,
        "category_id": str(user_event.category_id) if user_event.category_id else None,
        "level": user_event.level,
        "event_type": user_event.event_type,
        "metadata": user_event.metadata,
        "device": user_event.device,
        "platform": user_event.platform,
        "ip_address": str(user_event.ip_address) if user_event.ip_address else None,
        "created_at": user_event.created_at.isoformat() if user_event.created_at else None,
    }

def engagement_to_dict(engagement: Engagement) -> Dict[str, Any]:
    return {
        "engagement_id": str(engagement.engagement_id),
        "user_id": str(engagement.user_id),
        "episode_id": str(engagement.episode_id) if engagement.episode_id else None,
        "story_id": str(engagement.story_id) if engagement.story_id else None,
        "type": engagement.type,
        "ref_id": str(engagement.ref_id) if engagement.ref_id else None,
        "created_at": engagement.created_at.isoformat() if engagement.created_at else None,
    }

def report_to_dict(report: Report) -> Dict[str, Any]:
    return {
        "report_id": str(report.report_id),
        "type": report.type,
        "ref_id": str(report.ref_id),
        "reported_by": str(report.reported_by),
        "reason": report.reason,
        "status": report.status,
        "created_at": report.created_at.isoformat() if report.created_at else None,
    }

def home_continue_watching_to_dict(hcw: HomeContinueWatching) -> Dict[str, Any]:
    return {
        "continue_id": str(hcw.continue_id),
        "user_id": str(hcw.user_id),
        "story_id": str(hcw.story_id),
        "episode_id": str(hcw.episode_id),
        "progress_seconds": hcw.progress_seconds,
        "total_duration": hcw.total_duration,
        "last_watched_at": hcw.last_watched_at.isoformat() if hcw.last_watched_at else None,
        "completed": hcw.completed,
    }

def stories_authors_to_dict(sa: StoriesAuthors) -> Dict[str, Any]:
    return {
        "stories_author_id": str(sa.stories_author_id),
        "story_id": str(sa.story_id),
        "user_id": str(sa.user_id),
        "role": sa.role,
        "contribution_percentage": sa.contribution_percentage,
    }

def episode_authors_to_dict(ea: EpisodeAuthors) -> Dict[str, Any]:
    return {
        "episode_author_id": str(ea.episode_author_id),
        "episode_id": str(ea.episode_id),
        "story_id": str(ea.story_id),
        "user_id": str(ea.user_id),
        "role": ea.role,
        "contribution_percentage": ea.contribution_percentage,
    }

def like_to_dict(like: Like) -> Dict[str, Any]:
    return {
        "like_id": str(like.like_id),
        "story_id": str(like.story_id) if like.story_id else None,
        "episode_id": str(like.episode_id) if like.episode_id else None,
        "user_id": str(like.user_id),
        "created_at": like.created_at.isoformat() if like.created_at else None,
    }

def rating_to_dict(rating: Rating) -> Dict[str, Any]:
    return {
        "rating_id": str(rating.rating_id),
        "story_id": str(rating.story_id) if rating.story_id else None,
        "episode_id": str(rating.episode_id) if rating.episode_id else None,
        "user_id": str(rating.user_id),
        "rating_value": rating.rating_value,
        "created_at": rating.created_at.isoformat() if rating.created_at else None,
    }

def view_to_dict(view: View) -> Dict[str, Any]:
    return {
        "view_id": str(view.view_id),
        "story_id": str(view.story_id) if view.story_id else None,
        "episode_id": str(view.episode_id) if view.episode_id else None,
        "user_id": str(view.user_id) if view.user_id else None,
        "ip_address": str(view.ip_address) if view.ip_address else None,
        "created_at": view.created_at.isoformat() if view.created_at else None,
    }

def share_to_dict(share: Share) -> Dict[str, Any]:
    return {
        "share_id": str(share.share_id),
        "story_id": str(share.story_id) if share.story_id else None,
        "episode_id": str(share.episode_id) if share.episode_id else None,
        "user_id": str(share.user_id) if share.user_id else None,
        "platform": share.platform,
        "created_at": share.created_at.isoformat() if share.created_at else None,
    }