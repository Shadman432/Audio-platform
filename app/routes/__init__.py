# app/routes/__init__.py

from fastapi import APIRouter

# Import all routers
from .auth import router as auth_router
from .home_content import router as home_content_router
from .home_slideshow import router as home_slideshow_router
from .home_continue_watching import router as home_continue_watching_router
from .stories import router as stories_router
from .comments import router as comments_router
from .likes import router as likes_router
from .ratings import router as ratings_router
from .views import router as views_router
from .episodes import router as episodes_router
from .search import router as search_router
from .shares import router as shares_router

# Create main API router
api_router = APIRouter()

# ✅ Authentication routes
api_router.include_router(auth_router, prefix="/auth", tags=["authentication"])

# ✅ Home-related routes
api_router.include_router(home_content_router, prefix="/content", tags=["home-content"])
api_router.include_router(home_slideshow_router, prefix="/slideshow", tags=["home-slideshow"])
api_router.include_router(home_continue_watching_router, prefix="/continue", tags=["continue-watching"])

# ✅ Stories, Comments, Likes, Ratings, Views, Episodes
api_router.include_router(stories_router, prefix="/stories", tags=["stories"])
api_router.include_router(episodes_router, prefix="/episodes", tags=["episodes"])
api_router.include_router(comments_router, prefix="/comments", tags=["comments"])
api_router.include_router(views_router, prefix="/views", tags=["views"])
api_router.include_router(likes_router, prefix="/likes", tags=["likes"])
api_router.include_router(ratings_router, prefix="/ratings", tags=["ratings"])
api_router.include_router(shares_router, prefix="/shares", tags=["shares"])

# ✅ Search
api_router.include_router(search_router, prefix="/search", tags=["search"])
