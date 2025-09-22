import asyncio
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from opensearchpy import AsyncOpenSearch
from opensearchpy.exceptions import NotFoundError
from sqlalchemy.orm import Session

from ..config import settings
from ..database import get_db
from ..models.stories import Story
from ..models.episodes import Episode
from .cache_service import cache_service # Added import
from .stories import StoryService # Added import for get_all_stories
from .episodes import EpisodeService # Added import for get_all_episodes

logger = logging.getLogger(__name__)

LAST_SYNCED_AT_KEY = "last_synced_at" # This might become less relevant
import asyncio
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from opensearchpy import AsyncOpenSearch
from opensearchpy.exceptions import NotFoundError
from sqlalchemy.orm import Session

from ..config import settings
from ..database import get_db
from ..models.stories import Story
from ..models.episodes import Episode
from .cache_service import cache_service
from .stories import StoryService
from .episodes import EpisodeService

logger = logging.getLogger(__name__)

class SearchService:
    _opensearch_client: Optional[AsyncOpenSearch] = None

    @classmethod
    async def _get_opensearch_client(cls) -> Optional[AsyncOpenSearch]:
        if cls._opensearch_client is None:
            try:
                cls._opensearch_client = AsyncOpenSearch(
                    hosts=[settings.opensearch_url],
                    http_auth=(settings.opensearch_username, settings.opensearch_password),
                    verify_certs=False,
                    ssl_assert_hostname=False,
                    ssl_show_warn=False,
                    timeout=60,
                    max_retries=3,
                    retry_on_timeout=True
                )
                await cls._opensearch_client.ping()
                logger.info(f"OpenSearch client initialized: {settings.opensearch_url}")
            except Exception as e:
                logger.error(f"Failed to connect to OpenSearch: {e}")
                cls._opensearch_client = None
        return cls._opensearch_client

    @classmethod
    async def create_indexes_if_not_exist(cls):
        """Create OpenSearch indexes only if they don't already exist"""
        client = await cls._get_opensearch_client()
        if not client:
            return

        # Story Index Schema - without counter fields
        story_index_body = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "fuzzy_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding", "stop"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "story_title": {
                        "type": "text",
                        "analyzer": "fuzzy_analyzer",
                        "boost": 10.0,
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "story_description": {
                        "type": "text",
                        "analyzer": "fuzzy_analyzer",
                        "boost": 5.0
                    },
                    "story_meta_title": {
                        "type": "text",
                        "analyzer": "fuzzy_analyzer",
                        "boost": 2.0
                    },
                    "story_meta_description": {
                        "type": "text",
                        "analyzer": "fuzzy_analyzer",
                        "boost": 1.0
                    },
                    "genre": {"type": "keyword"},
                    "subgenre": {"type": "keyword"},
                    "rating": {"type": "keyword"},
                    "avg_rating": {"type": "float"},
                    "author_json": {"type": "object"},
                    "thumbnail_square": {"type": "keyword"},
                    "thumbnail_rect": {"type": "keyword"},
                    "thumbnail_responsive": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    "story_id": {"type": "keyword"}
                }
            }
        }

        # Episode Index Schema - without counter fields
        episode_index_body = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "fuzzy_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding", "stop"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "episode_title": {
                        "type": "text",
                        "analyzer": "fuzzy_analyzer",
                        "boost": 10.0,
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "episode_description": {
                        "type": "text",
                        "analyzer": "fuzzy_analyzer",
                        "boost": 5.0
                    },
                    "episode_meta_title": {
                        "type": "text",
                        "analyzer": "fuzzy_analyzer",
                        "boost": 2.0
                    },
                    "episode_meta_description": {
                        "type": "text",
                        "analyzer": "fuzzy_analyzer",
                        "boost": 1.0
                    },
                    "story_id": {"type": "keyword"},
                    "genre": {"type": "keyword"},
                    "subgenre": {"type": "keyword"},
                    "rating": {"type": "keyword"},
                    "avg_rating": {"type": "float"},
                    "author_json": {"type": "object"},
                    "release_date": {"type": "date"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    "episode_id": {"type": "keyword"},
                    "thumbnail_square": {"type": "keyword"},
                    "thumbnail_rect": {"type": "keyword"},
                    "thumbnail_responsive": {"type": "keyword"},
                    # Denormalized story fields for search only
                    "story_title": {
                        "type": "text",
                        "analyzer": "fuzzy_analyzer",
                        "boost": 8.0
                    },
                    "story_description": {
                        "type": "text",
                        "analyzer": "fuzzy_analyzer",
                        "boost": 3.0
                    },
                    "story_meta_title": {
                        "type": "text",
                        "analyzer": "fuzzy_analyzer",
                        "boost": 1.5
                    },
                    "story_meta_description": {
                        "type": "text",
                        "analyzer": "fuzzy_analyzer",
                        "boost": 0.5
                    }
                }
            }
        }

        # Create indexes if they don't exist
        indexes = [
            (settings.opensearch_stories_index, story_index_body),
            (settings.opensearch_episodes_index, episode_index_body)
        ]

        for index_name, index_body in indexes:
            try:
                exists = await client.indices.exists(index=index_name)
                if exists:
                    logger.info(f"Index '{index_name}' already exists, skipping creation.")
                else:
                    logger.info(f"Index '{index_name}' does not exist. Creating it now.")
                    await client.indices.create(index=index_name, body=index_body)
                    logger.info(f"Created OpenSearch index '{index_name}'")
            except Exception as e:
                logger.error(f"Failed to create OpenSearch index '{index_name}': {e}")

    @staticmethod
    async def _get_redis_counters(entity_type: str, entity_id: str) -> Dict[str, int]:
        """Get real-time counters from Redis for an entity"""
        counters = {}
        try:
            if not cache_service._redis_client:
                return counters
                
            counter_keys = {
                'likes_count': f"{entity_type}:{entity_id}:likes_count",
                'comments_count': f"{entity_type}:{entity_id}:comments_count", 
                'views_count': f"{entity_type}:{entity_id}:views_count",
                'shares_count': f"{entity_type}:{entity_id}:shares_count"
            }
            
            # Get all counters in one pipeline call
            pipe = cache_service._redis_client.pipeline()
            for counter_name, redis_key in counter_keys.items():
                pipe.get(redis_key)
            
            results = await pipe.execute()
            
            for (counter_name, _), result in zip(counter_keys.items(), results):
                if result is not None:
                    try:
                        counters[counter_name] = int(result.decode('utf-8'))
                    except (ValueError, AttributeError):
                        counters[counter_name] = 0
                else:
                    counters[counter_name] = 0
                    
        except Exception as e:
            logger.error(f"Error getting Redis counters for {entity_type}:{entity_id}: {e}")
            # Return default counters if Redis fails
            counters = {
                'likes_count': 0,
                'comments_count': 0,
                'views_count': 0,
                'shares_count': 0
            }
        
        return counters

    @staticmethod
    def _story_to_document(story: Story) -> dict:
        """Convert Story to OpenSearch document (without counter fields)"""
        return {
            "story_id": str(story.story_id),
            "story_title": story.title,
            "story_description": story.description,
            "story_meta_title": story.meta_title,
            "story_meta_description": story.meta_description,
            "genre": story.genre,
            "subgenre": story.subgenre,
            "rating": story.rating,
            "avg_rating": float(story.avg_rating) if story.avg_rating is not None else None,
            "author_json": story.author_json,
            "thumbnail_square": story.thumbnail_square,
            "thumbnail_rect": story.thumbnail_rect,
            "thumbnail_responsive": story.thumbnail_responsive,
            "created_at": story.created_at.isoformat() if story.created_at else None,
            "updated_at": story.updated_at.isoformat() if story.updated_at else None,
        }

    @staticmethod
    def _episode_to_document(episode: Episode) -> dict:
        """Convert Episode to OpenSearch document (without counter fields)"""
        episode_genre = episode.genre if episode.genre else (episode.story.genre if episode.story and episode.story.genre else "uncategorized")
        episode_rating = episode.rating if episode.rating else (episode.story.rating if episode.story and episode.story.rating else "B")
        episode_author_json = episode.author_json if episode.author_json else (episode.story.author_json if episode.story and episode.story.author_json else None)

        doc = {
            "episode_id": str(episode.episode_id),
            "episode_title": episode.title,
            "episode_description": episode.description,
            "episode_meta_title": episode.meta_title,
            "episode_meta_description": episode.meta_description,
            "story_id": str(episode.story_id),
            "genre": episode_genre,
            "subgenre": episode.subgenre,
            "rating": episode_rating,
            "avg_rating": float(episode.avg_rating) if episode.avg_rating is not None else None,
            "author_json": episode_author_json,
            "release_date": episode.release_date.isoformat() if episode.release_date else None,
            "created_at": episode.created_at.isoformat() if episode.created_at else None,
            "updated_at": episode.updated_at.isoformat() if episode.updated_at else None,
            "thumbnail_square": episode.thumbnail_square,
            "thumbnail_rect": episode.thumbnail_rect,
            "thumbnail_responsive": episode.thumbnail_responsive,
        }

        # Add denormalized story fields for SEARCH ONLY
        if episode.story:
            doc["story_title"] = episode.story.title
            doc["story_description"] = episode.story.description
            doc["story_meta_title"] = episode.story.meta_title
            doc["story_meta_description"] = episode.story.meta_description
        else:
            doc["story_title"] = ""
            doc["story_description"] = ""
            doc["story_meta_title"] = ""
            doc["story_meta_description"] = ""

        return doc

    @staticmethod
    async def _clean_story_response(story_doc: dict) -> dict:
        """Clean story document for API response with real-time Redis counters"""
        story_id = story_doc.get("story_id")
        
        # Get real-time counters from Redis
        redis_counters = await SearchService._get_redis_counters("story", story_id)
        
        return {
            "story_id": story_doc.get("story_id"),
            "story_title": story_doc.get("story_title"),
            "story_description": story_doc.get("story_description"),
            "story_meta_title": story_doc.get("story_meta_title"),
            "story_meta_description": story_doc.get("story_meta_description"),
            "genre": story_doc.get("genre"),
            "subgenre": story_doc.get("subgenre"),
            "rating": story_doc.get("rating"),
            "avg_rating": story_doc.get("avg_rating"),
            "author_json": story_doc.get("author_json"),
            "thumbnail_square": story_doc.get("thumbnail_square"),
            "thumbnail_rect": story_doc.get("thumbnail_rect"),
            "thumbnail_responsive": story_doc.get("thumbnail_responsive"),
            "created_at": story_doc.get("created_at"),
            "updated_at": story_doc.get("updated_at"),
            "type": "story",
            "score": story_doc.get("score", 0.0),
            # Real-time counters from Redis
            "likes_count": redis_counters.get("likes_count", 0),
            "comments_count": redis_counters.get("comments_count", 0),
            "views_count": redis_counters.get("views_count", 0),
            "shares_count": redis_counters.get("shares_count", 0),
        }

    @staticmethod
    async def _clean_episode_response(episode_doc: dict) -> dict:
        """Clean episode document for API response with real-time Redis counters"""
        episode_id = episode_doc.get("episode_id")
        
        # Get real-time counters from Redis
        redis_counters = await SearchService._get_redis_counters("episode", episode_id)
        
        return {
            "episode_id": episode_doc.get("episode_id"),
            "episode_title": episode_doc.get("episode_title"),
            "episode_description": episode_doc.get("episode_description"),
            "episode_meta_title": episode_doc.get("episode_meta_title"),
            "episode_meta_description": episode_doc.get("episode_meta_description"),
            "story_id": episode_doc.get("story_id"),
            "genre": episode_doc.get("genre"),
            "subgenre": episode_doc.get("subgenre"),
            "rating": episode_doc.get("rating"),
            "avg_rating": episode_doc.get("avg_rating"),
            "author_json": episode_doc.get("author_json"),
            "release_date": episode_doc.get("release_date"),
            "created_at": episode_doc.get("created_at"),
            "updated_at": episode_doc.get("updated_at"),
            "thumbnail_square": episode_doc.get("thumbnail_square"),
            "thumbnail_rect": episode_doc.get("thumbnail_rect"),
            "thumbnail_responsive": episode_doc.get("thumbnail_responsive"),
            "type": "episode",
            "score": episode_doc.get("score", 0.0),
            # Real-time counters from Redis
            "likes_count": redis_counters.get("likes_count", 0),
            "comments_count": redis_counters.get("comments_count", 0),
            "views_count": redis_counters.get("views_count", 0),
            "shares_count": redis_counters.get("shares_count", 0),
        }

    @classmethod
    async def sync_from_redis_to_opensearch(cls):
        """Sync data from Redis cache to OpenSearch without counters"""
        logger.info("--- Starting sync from Redis to OpenSearch (without counters) ---")
        client = await cls._get_opensearch_client()
        if not client:
            return

        db: Session = next(get_db())
        try:
            # Get all stories from Redis
            stories_data = await cache_service.get(
                settings.stories_cache_key, 
                lambda: cls._fetch_all_stories_from_db_and_cache(db)
            )
            stories_to_index = stories_data.get("python", []) if stories_data else []

            # Get all episodes from Redis  
            episodes_data = await cache_service.get(
                settings.episodes_cache_key,
                lambda: cls._fetch_all_episodes_from_db_and_cache(db)
            )
            episodes_to_index = episodes_data.get("python", []) if episodes_data else []

            # Bulk index stories (without counters)
            if stories_to_index:
                await cls._bulk_index_stories(client, stories_to_index)

            # Bulk index episodes (without counters)
            if episodes_to_index:
                await cls._bulk_index_episodes(client, episodes_to_index)

            logger.info("--- Sync from Redis to OpenSearch completed ---")

        except Exception as e:
            logger.error(f"Error during sync from Redis to OpenSearch: {e}", exc_info=True)
        finally:
            db.close()

    @classmethod
    async def _bulk_index_stories(cls, client, stories_to_index):
        """Bulk index stories to OpenSearch"""
        bulk_body = []
        
        # Get existing story IDs
        current_opensearch_story_ids = set()
        try:
            scroll_response = await client.search(
                index=settings.opensearch_stories_index,
                scroll='2m',
                size=1000,
                body={
                    "query": {"match_all": {}},
                    "_source": False
                }
            )
            
            while True:
                hits = scroll_response['hits']['hits']
                if not hits:
                    break
                for hit in hits:
                    current_opensearch_story_ids.add(hit['_id'])
                
                if 'scroll_id' in scroll_response:
                    scroll_response = await client.scroll(
                        scroll_id=scroll_response['scroll_id'],
                        scroll='2m'
                    )
                else:
                    break
        except Exception as e:
            logger.error(f"Error fetching existing story IDs: {e}")

        for story_doc in stories_to_index:
            story_id = story_doc["story_id"]
            current_opensearch_story_ids.discard(story_id)

            try:
                # Check if document needs updating
                existing_doc = await client.get(
                    index=settings.opensearch_stories_index, 
                    id=story_id,
                    _source_includes=["updated_at"]
                )
                existing_updated_at = datetime.fromisoformat(existing_doc['_source']["updated_at"]) if existing_doc['_source'].get("updated_at") else None
                current_updated_at = datetime.fromisoformat(story_doc["updated_at"]) if story_doc.get("updated_at") else None

                if current_updated_at and existing_updated_at and current_updated_at > existing_updated_at:
                    # Document needs updating
                    bulk_body.extend([
                        {"index": {"_index": settings.opensearch_stories_index, "_id": story_id}},
                        story_doc
                    ])
            except NotFoundError:
                # Document doesn't exist, index it
                bulk_body.extend([
                    {"index": {"_index": settings.opensearch_stories_index, "_id": story_id}},
                    story_doc
                ])

        # Delete stories that are in OpenSearch but not in Redis
        for story_id_to_delete in current_opensearch_story_ids:
            bulk_body.extend([
                {"delete": {"_index": settings.opensearch_stories_index, "_id": story_id_to_delete}}
            ])

        if bulk_body:
            logger.info(f"OpenSearch: Syncing {len(bulk_body)//2} story operations")
            await client.bulk(body=bulk_body, refresh=True)
            logger.info(f"Synced {len(bulk_body)//2} stories to OpenSearch")

    @classmethod
    async def _bulk_index_episodes(cls, client, episodes_to_index):
        """Bulk index episodes to OpenSearch"""
        bulk_body = []
        
        # Get existing episode IDs
        current_opensearch_episode_ids = set()
        try:
            scroll_response = await client.search(
                index=settings.opensearch_episodes_index,
                scroll='2m',
                size=1000,
                body={
                    "query": {"match_all": {}},
                    "_source": False
                }
            )
            
            while True:
                hits = scroll_response['hits']['hits']
                if not hits:
                    break
                for hit in hits:
                    current_opensearch_episode_ids.add(hit['_id'])
                
                if 'scroll_id' in scroll_response:
                    scroll_response = await client.scroll(
                        scroll_id=scroll_response['scroll_id'],
                        scroll='2m'
                    )
                else:
                    break
        except Exception as e:
            logger.error(f"Error fetching existing episode IDs: {e}")

        for episode_doc in episodes_to_index:
            episode_id = episode_doc["episode_id"]
            current_opensearch_episode_ids.discard(episode_id)

            try:
                # Check if document needs updating
                existing_doc = await client.get(
                    index=settings.opensearch_episodes_index,
                    id=episode_id,
                    _source_includes=["updated_at"]
                )
                existing_updated_at = datetime.fromisoformat(existing_doc['_source']["updated_at"]) if existing_doc['_source'].get("updated_at") else None
                current_updated_at = datetime.fromisoformat(episode_doc["updated_at"]) if episode_doc.get("updated_at") else None

                if current_updated_at and existing_updated_at and current_updated_at > existing_updated_at:
                    # Document needs updating
                    bulk_body.extend([
                        {"index": {"_index": settings.opensearch_episodes_index, "_id": episode_id}},
                        episode_doc
                    ])
            except NotFoundError:
                # Document doesn't exist, index it
                bulk_body.extend([
                    {"index": {"_index": settings.opensearch_episodes_index, "_id": episode_id}},
                    episode_doc
                ])

        # Delete episodes that are in OpenSearch but not in Redis
        for episode_id_to_delete in current_opensearch_episode_ids:
            bulk_body.extend([
                {"delete": {"_index": settings.opensearch_episodes_index, "_id": episode_id_to_delete}}
            ])

        if bulk_body:
            logger.info(f"OpenSearch: Syncing {len(bulk_body)//2} episode operations")
            await client.bulk(body=bulk_body, refresh=True)
            logger.info(f"Synced {len(bulk_body)//2} episodes to OpenSearch")

    @classmethod
    async def _fetch_all_stories_from_db_and_cache(cls, db: Session) -> Dict[str, Any]:
        """Fetch all stories from database and cache them"""
        stories = await StoryService.get_all_stories(db)
        serialized_stories = [cls._story_to_document(s) for s in stories]
        await cache_service.set(
            settings.stories_cache_key, 
            {"python": serialized_stories, "json": json.dumps(serialized_stories, default=str)}
        )
        return {"python": serialized_stories, "json": json.dumps(serialized_stories, default=str)}

    @classmethod
    async def _fetch_all_episodes_from_db_and_cache(cls, db: Session) -> Dict[str, Any]:
        """Fetch all episodes from database and cache them"""
        episodes = await EpisodeService.get_all_episodes(db)
        serialized_episodes = [cls._episode_to_document(e) for e in episodes]
        await cache_service.set(
            settings.episodes_cache_key,
            {"python": serialized_episodes, "json": json.dumps(serialized_episodes, default=str)}
        )
        return {"python": serialized_episodes, "json": json.dumps(serialized_episodes, default=str)}

    @staticmethod
    def _get_cache_key(query: str, skip: int, limit: int) -> str:
        return f"opensearch_unified_search_cache_v2:{query}:{skip}:{limit}"

    @classmethod
    async def unified_search(cls, query: str, skip: int, limit: int) -> List[Dict[str, Any]]:
        """Enhanced unified search with real-time Redis counters"""
        if not query:
            return []

        cache_key = cls._get_cache_key(query, skip, limit)
        
        # Check cache first
        cached_results_json = await cache_service.get(cache_key)
        if cached_results_json:
            logger.info(f"Cache hit for search query: '{query}'")
            return json.loads(cached_results_json)

        client = await cls._get_opensearch_client()
        if not client:
            return []

        try:
            # Build search queries
            search_body = {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": [
                                        "story_title^10",
                                        "story_description^5",
                                        "story_meta_title^2",
                                        "story_meta_description^1"
                                    ],
                                    "type": "best_fields",
                                    "fuzziness": "AUTO",
                                    "prefix_length": 1
                                }
                            }
                        ]
                    }
                },
                "from": 0,
                "size": 200,
                "_source": True
            }

            episode_search_body = {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": [
                                        "episode_title^10",
                                        "episode_description^5",
                                        "episode_meta_title^2",
                                        "episode_meta_description^1",
                                        "story_title^8",
                                        "story_description^3",
                                        "story_meta_title^1.5",
                                        "story_meta_description^0.5"
                                    ],
                                    "type": "best_fields",
                                    "fuzziness": "AUTO",
                                    "prefix_length": 1
                                }
                            }
                        ]
                    }
                },
                "from": 0,
                "size": 200,
                "_source": True
            }

            logger.info(f"Executing OpenSearch search for query: '{query}'")
            
            # Execute searches in parallel
            stories_result, episodes_result = await asyncio.gather(
                client.search(index=settings.opensearch_stories_index, body=search_body),
                client.search(index=settings.opensearch_episodes_index, body=episode_search_body)
            )

            logger.info(f"Found {stories_result['hits']['total']['value']} stories and {episodes_result['hits']['total']['value']} episodes")

            combined_results = []
            
            # Process story results with Redis counters
            for hit in stories_result['hits']['hits']:
                doc = hit['_source']
                doc["score"] = hit['_score'] * 1.5  # Boost stories slightly
                cleaned_doc = await cls._clean_story_response(doc)
                combined_results.append(cleaned_doc)

            # Process episode results with Redis counters
            for hit in episodes_result['hits']['hits']:
                doc = hit['_source']
                doc["score"] = hit['_score']
                cleaned_doc = await cls._clean_episode_response(doc)
                combined_results.append(cleaned_doc)

            # Sort by score
            combined_results.sort(key=lambda x: x.get("score", 0.0), reverse=True)
            
            # Apply pagination
            final_results = combined_results[skip : skip + limit]
            logger.info(f"Returning {len(final_results)} paginated results")

            # Cache results
            json_results = json.dumps(final_results, default=str)
            await cache_service.set(cache_key, json_results, ttl=settings.search_cache_ttl)
            logger.info(f"Cached search results for query: '{query}'")

            return final_results

        except Exception as e:
            logger.error(f"Search error for query '{query}': {e}", exc_info=True)
            return []

    @classmethod
    async def reindex_all_from_db(cls):
        """Full reindex from Redis cache"""
        logger.info("--- Starting reindex from Redis cache ---")
        await cls.create_indexes_if_not_exist()
        await cls.sync_from_redis_to_opensearch()

    @classmethod
    async def force_full_reindex(cls):
        """Force complete reindex"""
        logger.info("--- Starting FULL re-index ---")
        client = await cls._get_opensearch_client()
        if not client:
            return

        # Clear existing data
        try:
            await client.delete_by_query(
                index=settings.opensearch_stories_index,
                body={"query": {"match_all": {}}}
            )
            await client.delete_by_query(
                index=settings.opensearch_episodes_index,
                body={"query": {"match_all": {}}}
            )
            logger.info("Existing OpenSearch data cleared")
        except Exception as e:
            logger.warning(f"Error clearing data: {e}")

        await cls.create_indexes_if_not_exist()
        await cls.sync_from_redis_to_opensearch()

    @classmethod
    async def search(cls, query: str, skip: int = 0, limit: int = 20) -> List[Dict[str, Any]]:
        """Simple search wrapper"""
        return await cls.unified_search(query, skip, limit)