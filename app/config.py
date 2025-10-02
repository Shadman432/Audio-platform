from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List


class Settings(BaseSettings):
    # =============================
    # Supabase Configuration
    # =============================
    supabase_url: str = Field(default="", description="Supabase project URL")
    supabase_anon_key: str = Field(default="", description="Supabase anonymous key")
    supabase_service_role_key: str = Field(default="", description="Supabase service role key")

    # =============================
    # Database Configuration
    # =============================
    database_url: str = Field(
        default="sqlite:///./home_audio.db",
        description="Database URL. Use SQLite for development or PostgreSQL for production"
    )
    db_host: str = Field(default="localhost", description="Database host")
    db_port: int = Field(default=5432, description="Database port")
    db_name: str = Field(default="home_audio", description="Database name")
    db_user: str = Field(default="", description="Database username")
    db_password: str = Field(default="", description="Database password")

    # =============================
    # JWT Configuration
    # =============================
    jwt_secret_key: str = Field(default="your-jwt-secret-key", description="JWT secret key")
    jwt_algorithm: str = Field(default="HS256", description="JWT algorithm")
    jwt_access_token_expire_minutes: int = Field(default=300, description="JWT token expiration in minutes")

    # =============================
    # Application Configuration
    # =============================
    secret_key: str = Field(
        default="your-secret-key-change-this-in-production",
        description="Secret key for app sessions"
    )
    debug: bool = Field(default=True, description="Debug mode")
    environment: str = Field(default="development", description="App environment")

    # =============================
    # CORS Configuration
    # =============================
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:3001"],
        description="Allowed CORS origins"
    )

    # =============================
    # Redis Configuration
    # =============================
    redis_host: str = Field(default="localhost", description="Redis server host")
    redis_port: int = Field(default=6379, description="Redis server port")
    redis_db: int = Field(default=0, description="Redis DB number")
    redis_url: str = Field(default="", description="Optional Redis connection URL")
    redis_max_connections: int = Field(default=50, description="Max Redis connections for pooling") # New

    # Cache configuration with proper naming
    use_redis: bool = Field(default=True, description="Enable Redis")
    redis_enabled: bool = Field(default=True, description="Enable Redis (legacy)")
    redis_connection_timeout: int = Field(default=5, description="Redis connection timeout (s)")
    redis_max_retries: int = Field(default=3, description="Max Redis retries")

    # =============================
    # RediSearch Configuration
    # =============================
    redisearch_stories_index: str = Field(default="stories_index", description="RediSearch index name for stories")
    redisearch_episodes_index: str = Field(default="episodes_index", description="RediSearch index name for episodes")
    search_cache_ttl: int = Field(default=15000, description="TTL for search query cache in seconds (5 minutes)")

    # =============================
    # OpenSearch Configuration
    # =============================
    opensearch_url: str = Field(default="https://opensearch:9200", description="OpenSearch URL")
    opensearch_username: str = Field(default="admin", description="OpenSearch username")
    opensearch_password: str = Field(default="admin", description="OpenSearch password")
    opensearch_stories_index: str = Field(default="stories", description="OpenSearch index for stories")
    opensearch_episodes_index: str = Field(default="episodes", description="OpenSearch index for episodes")

    # =============================
    # Enhanced Cache Configuration
    # =============================
    # Stale cache configuration
    STALE_CACHE_EXTENSION = Field(default=3600, description="Extension time for stale cache (seconds)")
    stale_cache_extension: int = Field(default=3600, description="Extension time for stale cache serving")

    # Pagination cache keys
    stories_paginated_cache_prefix: str = Field(default="fastapi_cache:stories_page", description="Stories pagination cache prefix")
    episodes_paginated_cache_prefix: str = Field(default="fastapi_cache:episodes_page", description="Episodes pagination cache prefix")

    # Background refresh intervals
    redis_refresh_interval: int = Field(default=43200, description="Refresh Redis cache interval (seconds)")
    counter_sync_interval: int = Field(default=300, description="Interval for syncing Redis counters to DB (seconds)")

    # Cache keys
    cache_key_prefix: str = Field(default="fastapi_cache", description="Cache key prefix")
    stories_cache_key: str = Field(default="fastapi_cache:stories", description="Stories cache key")
    story_cache_key_prefix: str = Field(default="fastapi_cache:story", description="Story cache key prefix")
    episodes_cache_key: str = Field(default="fastapi_cache:episodes", description="Episodes cache key")
    episode_cache_key_prefix: str = Field(default="fastapi_cache:episode", description="Episode cache key prefix")
    story_authors_cache_key: str = Field(default="fastapi_cache:story_authors", description="Story authors cache key")
    episode_authors_cache_key: str = Field(default="fastapi_cache:episode_authors", description="Episode authors cache key")
    home_categories_cache_key: str = Field(default="fastapi_cache:home_categories", description="Home categories cache key")
    home_series_cache_key: str = Field(default="fastapi_cache:home_series", description="Home series cache key")
    home_slideshow_cache_key: str = Field(default="fastapi_cache:home_slideshow", description="Home slideshow cache key")
    all_comments_cache_key: str = Field(default="fastapi_cache:all_comments", description="All comments cache key")

    # Performance settings
    enable_compression: bool = Field(default=True, description="Enable cache compression")
    enable_stale_while_revalidate: bool = Field(default=True, description="Enable stale-while-revalidate")

    # Debug and monitoring
    cache_debug_mode: bool = Field(default=True, description="Cache debug logging")
    cache_debug: bool = Field(default=False, description="Legacy cache debug flag")
    enable_cache_metrics: bool = Field(default=True, description="Enable cache metrics monitoring")

    # Browser cache settings (disabled for instant responses)
    browser_cache_ttl: int = Field(default=0, description="Browser cache max-age (0=disabled)")
    disable_browser_cache: bool = Field(default=True, description="Force disable browser caching")

    # Performance targets
    memory_hit_target_ms: float = Field(default=1.0, description="Target response time for memory hits (ms)")
    redis_hit_target_ms: float = Field(default=10.0, description="Target response time for Redis hits (ms)")
    db_fallback_target_ms: float = Field(default=100.0, description="Target response time for DB fallback (ms)")

    redis_counter_ttl: int = Field(default=86400 * 7, description="TTL for Redis counters (seconds)")
    redis_cache_ttl: int = Field(default=3600, description="TTL for general Redis cache (seconds)")

    # =============================
    # Application Configuration
    # =============================
    secret_key: str = Field(
        default="your-secret-key-change-this-in-production",
        description="Secret key for app sessions"
    )
    debug: bool = Field(default=True, description="Debug mode")
    environment: str = Field(default="development", description="App environment")
    enable_query_logging: bool = Field(default=False, description="Enable SQLAlchemy query logging (disable in prod)") # New

    # =============================
    # Uppercase aliases for backward compatibility
    # =============================
    @property
    def USE_REDIS(self) -> bool:
        return self.use_redis

    @property
    def REDIS_URL(self) -> str:
        return self.get_redis_url()

    @property
    def REDIS_CONNECTION_TIMEOUT(self) -> int:
        return self.redis_connection_timeout

    @property
    def REDIS_MAX_RETRIES(self) -> int:
        return self.redis_max_retries

    @property
    def BROWSER_CACHE_TTL(self) -> int:
        return self.browser_cache_ttl

    @property
    def REDIS_REFRESH_INTERVAL(self) -> int:
        return self.redis_refresh_interval

    @property
    def CACHE_KEY_PREFIX(self) -> str:
        return self.cache_key_prefix

    @property
    def STORIES_CACHE_KEY(self) -> str:
        return self.stories_cache_key

    @property
    def STORY_CACHE_KEY_PREFIX(self) -> str:
        return self.story_cache_key_prefix

    @property
    def ENABLE_COMPRESSION(self) -> bool:
        return self.enable_compression

    @property
    def ENABLE_STALE_WHILE_REVALIDATE(self) -> bool:
        return self.enable_stale_while_revalidate

    @property
    def CACHE_DEBUG_MODE(self) -> bool:
        return self.cache_debug_mode or self.cache_debug

    @property
    def ENABLE_CACHE_METRICS(self) -> bool:
        return self.enable_cache_metrics

    @property
    def DISABLE_BROWSER_CACHE(self) -> bool:
        return self.disable_browser_cache

    @property
    def STALE_CACHE_EXTENSION(self) -> int:
        return self.stale_cache_extension

    @property
    def STORIES_PAGINATED_CACHE_PREFIX(self) -> str:
        return self.stories_paginated_cache_prefix

    @property
    def EPISODES_PAGINATED_CACHE_PREFIX(self) -> str:
        return self.episodes_paginated_cache_prefix

    # =============================
    # Pydantic Config
    # =============================
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"  # accept unknown vars safely

    # =============================
    # Utility Functions
    # =============================
    def get_postgresql_url(self) -> str:
        """Generate PostgreSQL URL if components are set"""
        if self.db_user and self.db_password:
            return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        return f"postgresql://{self.db_host}:{self.db_port}/{self.db_name}"

    def get_redis_url(self) -> str:
        """Generate Redis URL (fallback if redis_url not set)"""
        if self.redis_url:
            return self.redis_url
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    def should_disable_browser_cache(self) -> bool:
        """Check if browser caching should be disabled"""
        return self.disable_browser_cache or self.browser_cache_ttl == 0


# =============================
# Global settings instance
# =============================
settings = Settings()

# Override database_url if PostgreSQL components provided
if settings.db_user and settings.db_password and not settings.database_url.startswith("postgresql://"):
    settings.database_url = settings.get_postgresql_url()