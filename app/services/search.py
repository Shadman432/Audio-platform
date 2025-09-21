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
                # Test connection
                await cls._opensearch_client.ping()
                logger.info(f"✅ OpenSearch client initialized: {settings.opensearch_url}")
            except Exception as e:
                logger.error(f"❌ Failed to connect to OpenSearch: {e}")
                cls._opensearch_client = None
        return cls._opensearch_client

    @classmethod
    async def create_indexes_if_not_exist(cls):
        """Create OpenSearch indexes only if they don't already exist"""
        client = await cls._get_opensearch_client()
        if not client:
            return

        # Story Index Schema
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
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
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

        # Episode Index Schema
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
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
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
                    logger.info(f"✅ Index '{index_name}' already exists, skipping creation.")
                else:
                    logger.info(f"Index '{index_name}' does not exist. Creating it now.")
                    await client.indices.create(index=index_name, body=index_body)
                    logger.info(f"✅ Created OpenSearch index '{index_name}'")
            except Exception as e:
                logger.error(f"❌ Failed to create OpenSearch index '{index_name}': {e}")

    @classmethod
    async def setup_opensearch_indexes(cls):
        """Setup OpenSearch indexes"""
        await cls.create_indexes_if_not_exist()

    @classmethod
    async def _get_last_synced_at(cls) -> datetime:
        """Get last sync time from OpenSearch metadata index"""
        client = await cls._get_opensearch_client()
        if not client:
            return datetime.fromtimestamp(0, tz=timezone.utc)

        try:
            # Create metadata index if it doesn't exist
            metadata_index = f"{settings.opensearch_stories_index}_metadata"
            exists = await client.indices.exists(index=metadata_index)
            if not exists:
                await client.indices.create(
                    index=metadata_index,
                    body={
                        "mappings": {
                            "properties": {
                                "key": {"type": "keyword"},
                                "value": {"type": "text"},
                                "timestamp": {"type": "date"}
                            }
                        }
                    }
                )

            # Try to get the last synced timestamp
            response = await client.get(
                index=metadata_index,
                id=LAST_SYNCED_AT_KEY
            )
            return datetime.fromisoformat(response['_source']['value'])
        except (NotFoundError, KeyError):
            return datetime.fromtimestamp(0, tz=timezone.utc)
        except Exception as e:
            logger.error(f"Error getting last sync time: {e}")
            return datetime.fromtimestamp(0, tz=timezone.utc)

    @classmethod
    async def _set_last_synced_at(cls, timestamp: datetime):
        """Set last sync time in OpenSearch metadata index"""
        client = await cls._get_opensearch_client()
        if not client:
            return

        try:
            metadata_index = f"{settings.opensearch_stories_index}_metadata"
            await client.index(
                index=metadata_index,
                id=LAST_SYNCED_AT_KEY,
                body={
                    "key": LAST_SYNCED_AT_KEY,
                    "value": timestamp.isoformat(),
                    "timestamp": timestamp
                }
            )
        except Exception as e:
            logger.error(f"Error setting last sync time: {e}")

    @staticmethod
    def _story_to_document(story: Story) -> dict:
        return {
            "story_id": str(story.story_id),
            "story_title": story.title,
            "story_description": story.description,
            "story_meta_title": story.meta_title,
            "story_meta_description": story.meta_description,
            "genre": story.genre,
            "subgenre": story.subgenre,
            "rating": story.rating,
            "avg_rating": story.avg_rating,
            "author_json": story.author_json,
            "thumbnail_square": story.thumbnail_square,
            "thumbnail_rect": story.thumbnail_rect,
            "thumbnail_responsive": story.thumbnail_responsive,
            "created_at": story.created_at.isoformat() if story.created_at else None,
            "updated_at": story.updated_at.isoformat() if story.updated_at else None,
        }

    @staticmethod
    def _episode_to_document(episode: Episode) -> dict:
        # Get episode's own genre/rating, or fallback to story's genre/rating
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
            "avg_rating": episode.avg_rating,
            "author_json": episode_author_json,
            "release_date": episode.release_date.isoformat() if episode.release_date else None,
            "created_at": episode.created_at.isoformat() if episode.created_at else None,
            "updated_at": episode.updated_at.isoformat() if episode.updated_at else None,
            "thumbnail_square": episode.thumbnail_square,
            "thumbnail_rect": episode.thumbnail_rect,
            "thumbnail_responsive": episode.thumbnail_responsive,
        }

        # Add denormalized story fields for SEARCH ONLY (these won't be in final response)
        if episode.story:
            doc["story_title"] = episode.story.title
            doc["story_description"] = episode.story.description
            doc["story_meta_title"] = episode.story.meta_title
            doc["story_meta_description"] = episode.story.meta_description
        else:
            # Add empty strings for consistency if story is missing
            doc["story_title"] = ""
            doc["story_description"] = ""
            doc["story_meta_title"] = ""
            doc["story_meta_description"] = ""

        return doc

    @staticmethod
    def _clean_story_response(story_doc: dict) -> dict:
        """Clean story document for API response"""
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
            "score": story_doc.get("score", 0.0)
        }

    @staticmethod
    def _clean_episode_response(episode_doc: dict) -> dict:
        """Clean episode document for API response - REMOVE denormalized story fields"""
        return {
            "episode_id": episode_doc.get("episode_id"),
            "episode_title": episode_doc.get("episode_title"),
            "episode_description": episode_doc.get("episode_description"),
            "episode_meta_title": episode_doc.get("episode_meta_title"),
            "episode_meta_description": episode_doc.get("episode_meta_description"),
            "story_id": episode_doc.get("story_id"),  # Keep this - needed for linking
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
            "score": episode_doc.get("score", 0.0)
            # NOTE: story_title, story_description, etc. are NOT included in response
        }

    @classmethod
    async def _fetch_all_stories_from_db_and_cache(cls, db: Session) -> Dict[str, Any]:
        """Fetch all stories from database, update Redis, and return serialized data."""
        stories = StoryService.get_all_stories(db)
        serialized_stories = [cls._story_to_document(s) for s in stories]
        # Store in Redis
        await cache_service.set(settings.stories_cache_key, {"python": serialized_stories, "json": json.dumps(serialized_stories, default=str)})
        return {"python": serialized_stories, "json": json.dumps(serialized_stories, default=str)}

    @classmethod
    async def _fetch_all_episodes_from_db_and_cache(cls, db: Session) -> Dict[str, Any]:
        """Fetch all episodes from database, update Redis, and return serialized data."""
        episodes = EpisodeService.get_all_episodes(db)
        serialized_episodes = [cls._episode_to_document(e) for e in episodes]
        # Store in Redis
        await cache_service.set(settings.episodes_cache_key, {"python": serialized_episodes, "json": json.dumps(serialized_episodes, default=str)})
        return {"python": serialized_episodes, "json": json.dumps(serialized_episodes, default=str)}

    @classmethod
    async def sync_from_redis_to_opensearch(cls):
        """Sync data from Redis cache to OpenSearch."""
        logger.info("--- Starting sync from Redis to OpenSearch ---")
        client = await cls._get_opensearch_client()
        if not client:
            return

        db: Session = next(get_db()) # Get a new DB session for this task
        try:
            # Get all stories from Redis (with DB fallback if needed)
            stories_data = await cache_service.get(settings.stories_cache_key, lambda: cls._fetch_all_stories_from_db_and_cache(db))
            stories_to_index = stories_data.get("python", []) if stories_data else []

            # Get all episodes from Redis (with DB fallback if needed)
            episodes_data = await cache_service.get(settings.episodes_cache_key, lambda: cls._fetch_all_episodes_from_db_and_cache(db))
            episodes_to_index = episodes_data.get("python", []) if episodes_data else []

            # Bulk index stories
            if stories_to_index:
                bulk_body = []
                current_opensearch_story_ids = set()
                # Fetch all existing story IDs from OpenSearch
                try:
                    while True:
                        response = await client.search(
                            index=settings.opensearch_stories_index,
                            scroll='2m',
                            size=1000,
                            body={
                                "query": {"match_all": {}},
                                "_source": False
                            }
                        )
                        hits = response['hits']['hits']
                        if not hits:
                            break
                        for hit in hits:
                            current_opensearch_story_ids.add(hit['_id'])
                except Exception as e:
                    logger.error(f"Error fetching existing story IDs from OpenSearch: {e}")

                for story_doc in stories_to_index:
                    story_id = story_doc["story_id"]
                    current_opensearch_story_ids.discard(story_id) # Remove from set if it exists in Redis data

                    try:
                        # Try to get the existing document
                        existing_doc = await client.get(index=settings.opensearch_stories_index, id=story_id, _source_includes=["updated_at", "avg_rating", "likes_count", "comments_count", "shares_count", "views_count"])
                        existing_updated_at = datetime.fromisoformat(existing_doc['_source']["updated_at"]) if existing_doc['_source'].get("updated_at") else None
                        current_updated_at = datetime.fromisoformat(story_doc["updated_at"]) if story_doc.get("updated_at") else None

                        # Compare updated_at timestamps
                        if current_updated_at and existing_updated_at and current_updated_at > existing_updated_at:
                            # Create a partial update document with only changed fields
                            update_doc = {
                                "avg_rating": story_doc.get("avg_rating"),
                                "likes_count": story_doc.get("likes_count"),
                                "comments_count": story_doc.get("comments_count"),
                                "shares_count": story_doc.get("shares_count"),
                                "views_count": story_doc.get("views_count"),
                                "updated_at": story_doc.get("updated_at")
                            }
                            bulk_body.extend([
                                {"update": {"_index": settings.opensearch_stories_index, "_id": story_id}},
                                {"doc": update_doc}
                            ])
                        # else: If not newer, or no updated_at, no update is needed for counts
                    except NotFoundError:
                        # Document does not exist, so index it
                        bulk_body.extend([
                            {"index": {"_index": settings.opensearch_stories_index, "_id": story_id}},
                            story_doc
                        ])
                    except Exception as e:
                        logger.warning(f"Error processing story {story_id} for OpenSearch sync: {e}")

                # Delete stories that are in OpenSearch but not in Redis data
                if current_opensearch_story_ids:
                    logger.info(f"OpenSearch: Deleting {len(current_opensearch_story_ids)} stories from OpenSearch.")
                    for story_id_to_delete in current_opensearch_story_ids:
                        bulk_body.extend([
                            {"delete": {"_index": settings.opensearch_stories_index, "_id": story_id_to_delete}}
                        ])

                if bulk_body:
                    logger.info(f"OpenSearch: Syncing {len(bulk_body)//2} stories (updates/indexes/deletes) to OpenSearch.")
                    await client.bulk(body=bulk_body, refresh=True)
                    logger.info(f"✅ Synced {len(bulk_body)//2} stories to OpenSearch.")

            # Bulk index episodes
            if episodes_to_index:
                bulk_body = []
                current_opensearch_episode_ids = set()
                # Fetch all existing episode IDs from OpenSearch
                try:
                    while True:
                        response = await client.search(
                            index=settings.opensearch_episodes_index,
                            scroll='2m',
                            size=1000,
                            body={
                                "query": {"match_all": {}},
                                "_source": False
                            }
                        )
                        hits = response['hits']['hits']
                        if not hits:
                            break
                        for hit in hits:
                            current_opensearch_episode_ids.add(hit['_id'])
                except Exception as e:
                    logger.error(f"Error fetching existing episode IDs from OpenSearch: {e}")

                for episode_doc in episodes_to_index:
                    episode_id = episode_doc["episode_id"]
                    current_opensearch_episode_ids.discard(episode_id) # Remove from set if it exists in Redis data

                    try:
                        # Try to get the existing document
                        existing_doc = await client.get(index=settings.opensearch_episodes_index, id=episode_id, _source_includes=["updated_at", "avg_rating"])
                        existing_updated_at = datetime.fromisoformat(existing_doc['_source']["updated_at"]) if existing_doc['_source'].get("updated_at") else None
                        current_updated_at = datetime.fromisoformat(episode_doc["updated_at"]) if episode_doc.get("updated_at") else None

                        # Compare updated_at timestamps
                        if current_updated_at and existing_updated_at and current_updated_at > existing_updated_at:
                            # Create a partial update document with only changed fields
                            update_doc = {
                                "avg_rating": episode_doc.get("avg_rating"),
                                "updated_at": episode_doc.get("updated_at")
                            }
                            bulk_body.extend([
                                {"update": {"_index": settings.opensearch_episodes_index, "_id": episode_id}},
                                {"doc": update_doc}
                            ])
                        # else: If not newer, or no updated_at, no update is needed for counts
                    except NotFoundError:
                        # Document does not exist, so index it
                        bulk_body.extend([
                            {"index": {"_index": settings.opensearch_episodes_index, "_id": episode_id}},
                            episode_doc
                        ])
                    except Exception as e:
                        logger.warning(f"Error processing episode {episode_id} for OpenSearch sync: {e}")

                # Delete episodes that are in OpenSearch but not in Redis data
                if current_opensearch_episode_ids:
                    logger.info(f"OpenSearch: Deleting {len(current_opensearch_episode_ids)} episodes from OpenSearch.")
                    for episode_id_to_delete in current_opensearch_episode_ids:
                        bulk_body.extend([
                            {"delete": {"_index": settings.opensearch_episodes_index, "_id": episode_id_to_delete}}
                        ])

                if bulk_body:
                    logger.info(f"OpenSearch: Syncing {len(bulk_body)//2} episodes (updates/indexes/deletes) to OpenSearch.")
                    await client.bulk(body=bulk_body, refresh=True)
                    logger.info(f"✅ Synced {len(bulk_body)//2} episodes to OpenSearch.")

            logger.info("--- Sync from Redis to OpenSearch completed ---")

        except Exception as e:
            logger.error(f"❌ An error occurred during sync from Redis to OpenSearch: {e}", exc_info=True)
        finally:
            db.close() # Close the session obtained via next(get_db())

    @classmethod
    async def reindex_all_from_db(cls):
        """Full reindex - now pulls from Redis cache with DB fallback."""
        logger.info("--- Starting reindex (pulling from Redis cache) ---")
        await cls.create_indexes_if_not_exist()
        await cls.sync_from_redis_to_opensearch() # Call the new sync method

    @classmethod
    async def force_full_reindex(cls):
        """Force a complete reindex of all data - now pulls from Redis cache with DB fallback."""
        logger.info("--- Starting FULL re-index into OpenSearch (pulling from Redis cache) ---")
        client = await cls._get_opensearch_client()
        if not client:
            return

        # Delete existing data
        try:
            await client.delete_by_query(
                index=settings.opensearch_stories_index,
                body={"query": {"match_all": {}}}
            )
            await client.delete_by_query(
                index=settings.opensearch_episodes_index,
                body={"query": {"match_all": {}}}
            )
            logger.info("Existing OpenSearch data cleared for full reindex.")
        except Exception as e:
            logger.warning(f"Error clearing existing data during force full reindex: {e}")

        # Recreate indexes
        await cls.create_indexes_if_not_exist()
        
        # Now sync everything from Redis
        await cls.sync_from_redis_to_opensearch()

    @staticmethod
    def _get_cache_key(query: str, skip: int, limit: int) -> str:
        # Cache key for OpenSearch results
        return f"opensearch_unified_search_cache_v1:{query}:{skip}:{limit}"

    @classmethod
    async def unified_search(cls, query: str, skip: int, limit: int) -> List[Dict[str, Any]]:
        if not query:
            return []

        cache_key = cls._get_cache_key(query, skip, limit)
        
        # 1. Check Redis Cache
        cached_results_json = await cache_service.get(cache_key)
        if cached_results_json:
            logger.info(f"Cache hit for search query: '{query}'")
            return json.loads(cached_results_json)

        client = await cls._get_opensearch_client()
        if not client:
            return []

        try:
            # Build multi-match query with fuzzy matching
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
                                        "story_title^8",  # Search in denormalized story fields
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

            logger.info(f"Executing OpenSearch unified search for query: '{query}'")
            
            # Execute searches in parallel
            stories_result, episodes_result = await asyncio.gather(
                client.search(index=settings.opensearch_stories_index, body=search_body),
                client.search(index=settings.opensearch_episodes_index, body=episode_search_body)
            )

            logger.info(f"Found {stories_result['hits']['total']['value']} stories and {episodes_result['hits']['total']['value']} episodes.")

            combined_results = []
            
            # Process story results with cleaned response
            for hit in stories_result['hits']['hits']:
                doc = hit['_source']
                doc["score"] = hit['_score'] * 1.5  # Boost stories slightly
                cleaned_doc = cls._clean_story_response(doc)
                combined_results.append(cleaned_doc)

            # Process episode results with cleaned response (removes denormalized story fields)
            for hit in episodes_result['hits']['hits']:
                doc = hit['_source']
                doc["score"] = hit['_score']
                cleaned_doc = cls._clean_episode_response(doc)
                combined_results.append(cleaned_doc)

            # Sort by score
            combined_results.sort(key=lambda x: x.get("score", 0.0), reverse=True)
            logger.info(f"Combined and sorted {len(combined_results)} results.")

            # Fallback logic to fill results if needed
            if len(combined_results) < limit:
                needed = limit - len(combined_results)
                
                if combined_results and combined_results[0].get("genre"):
                    top_genre = combined_results[0]["genre"]
                    logger.info(f"Query returned < {limit} results. Fetching {needed} more from genre: {top_genre}")
                    
                    genre_search_body = {
                        "query": {"term": {"genre": top_genre}},
                        "from": 0,
                        "size": needed,
                        "_source": True
                    }

                    genre_stories, genre_episodes = await asyncio.gather(
                        client.search(index=settings.opensearch_stories_index, body=genre_search_body),
                        client.search(index=settings.opensearch_episodes_index, body=genre_search_body)
                    )
                    
                    existing_ids = {r.get('story_id') or r.get('episode_id') for r in combined_results}

                    for hit in genre_stories['hits']['hits']:
                        if hit['_source'].get('story_id') not in existing_ids:
                            doc = hit['_source']
                            doc["score"] = -1.0
                            cleaned_doc = cls._clean_story_response(doc)
                            combined_results.append(cleaned_doc)

                    for hit in genre_episodes['hits']['hits']:
                        if hit['_source'].get('episode_id') not in existing_ids:
                            doc = hit['_source']
                            doc["score"] = -1.0
                            cleaned_doc = cls._clean_episode_response(doc)
                            combined_results.append(cleaned_doc)
                else:
                    logger.info(f"Query returned 0 results or no genre. Fetching {needed} most recent items.")
                    
                    fallback_search_body = {
                        "query": {"match_all": {}},
                        "from": 0,
                        "size": needed,
                        "sort": [{"created_at": {"order": "desc"}}],
                        "_source": True
                    }

                    try:
                        fallback_stories, fallback_episodes = await asyncio.gather(
                            client.search(index=settings.opensearch_stories_index, body=fallback_search_body),
                            client.search(index=settings.opensearch_episodes_index, body=fallback_search_body)
                        )

                        existing_ids = {r.get('story_id') or r.get('episode_id') for r in combined_results}

                        for hit in fallback_stories['hits']['hits']:
                            if hit['_source'].get('story_id') not in existing_ids:
                                doc = hit['_source']
                                doc["score"] = -2.0
                                cleaned_doc = cls._clean_story_response(doc)
                                combined_results.append(cleaned_doc)

                        for hit in fallback_episodes['hits']['hits']:
                            if hit['_source'].get('episode_id') not in existing_ids:
                                doc = hit['_source']
                                doc["score"] = -2.0
                                cleaned_doc = cls._clean_episode_response(doc)
                                combined_results.append(cleaned_doc)
                    except Exception as fallback_error:
                        logger.warning(f"Fallback query failed: {fallback_error}")

            # Apply pagination
            final_results = combined_results[skip : skip + limit]
            logger.info(f"Paginated results: {len(final_results)}")

            # 5. Store in Redis Cache
            # Assuming settings.search_cache_ttl_seconds is defined for cache expiration
            await cache_service.set(cache_key, json.dumps(final_results), ex=settings.search_cache_ttl)
            logger.info(f"Stored search results in cache for query: '{query}'")

            return final_results

        except Exception as e:
            logger.error(f"Unexpected error during unified search for query '{query}': {e}", exc_info=True)
            return []

    @classmethod
    async def update_counters_in_opensearch(cls, entity_type: str, entity_id: str, counters: Dict[str, Any]):
        """
        Performs a partial update on an OpenSearch document to update only counter fields.
        """
        client = await cls._get_opensearch_client()
        if not client:
            return

        index_name = settings.opensearch_stories_index if entity_type == "story" else settings.opensearch_episodes_index
        
        try:
            # Add updated_at to the counters to mark the document as updated
            counters["updated_at"] = datetime.now(timezone.utc).isoformat()
            
            response = await client.update(
                index=index_name,
                id=entity_id,
                body={"doc": counters},
                retry_on_conflict=3
            )
            logger.info(f"✅ OpenSearch: Updated {entity_type} {entity_id} counters. Result: {response['result']}")
        except NotFoundError:
            logger.warning(f"OpenSearch: Document {entity_id} not found in index {index_name}. Cannot update counters.")
        except Exception as e:
            logger.error(f"❌ OpenSearch: Error updating {entity_type} {entity_id} counters: {e}", exc_info=True)

    @classmethod
    async def search(cls, query: str, skip: int = 0, limit: int = 20) -> List[Dict[str, Any]]:
        """Simplified search method - delegates to unified_search"""
        return await cls.unified_search(query, skip, limit)