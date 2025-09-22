import asyncio
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from opensearchpy import AsyncOpenSearch
from opensearchpy.exceptions import NotFoundError, RequestError
from sqlalchemy.orm import Session

from ..config import settings
from ..database import SessionLocal, get_db
from ..models.stories import Story
from ..models.episodes import Episode
from .cache_service import cache_service

logger = logging.getLogger(__name__)

class OpenSearchDataService:
    _opensearch_client: Optional[AsyncOpenSearch] = None

    @classmethod
    async def get_client(cls) -> Optional[AsyncOpenSearch]:
        """Get OpenSearch client with proper error handling"""
        if cls._opensearch_client is None:
            try:
                cls._opensearch_client = AsyncOpenSearch(
                    hosts=[settings.opensearch_url],
                    http_auth=(settings.opensearch_username, settings.opensearch_password),
                    verify_certs=False,
                    ssl_assert_hostname=False,
                    ssl_show_warn=False,
                    timeout=30,
                    max_retries=3,
                    retry_on_timeout=True,
                    http_compress=True
                )
                
                # Test connection
                await cls._opensearch_client.ping()
                logger.info(f"OpenSearch connected: {settings.opensearch_url}")
                return cls._opensearch_client
                
            except Exception as e:
                logger.error(f"OpenSearch connection failed: {e}")
                cls._opensearch_client = None
                return None
        
        return cls._opensearch_client

    @classmethod
    async def create_indexes(cls):
        """Create OpenSearch indexes with exact field mappings"""
        client = await cls.get_client()
        if not client:
            logger.error("Cannot create indexes - OpenSearch not available")
            return False

        # Stories index mapping - matches your data structure exactly
        stories_mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "5s",
                "analysis": {
                    "analyzer": {
                        "search_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding", "stop"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "story_id": {"type": "keyword"},
                    "title": {
                        "type": "text",
                        "analyzer": "search_analyzer",
                        "boost": 3.0,
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "meta_title": {
                        "type": "text", 
                        "analyzer": "search_analyzer",
                        "boost": 2.0
                    },
                    "thumbnail_square": {"type": "keyword", "index": False},
                    "thumbnail_rect": {"type": "keyword", "index": False},
                    "thumbnail_responsive": {"type": "keyword", "index": False},
                    "description": {
                        "type": "text",
                        "analyzer": "search_analyzer",
                        "boost": 1.5
                    },
                    "meta_description": {
                        "type": "text",
                        "analyzer": "search_analyzer"
                    },
                    "genre": {"type": "keyword"},
                    "subgenre": {"type": "keyword"},
                    "rating": {"type": "keyword"},
                    "avg_rating": {"type": "float"},
                    "avg_rating_count": {"type": "integer"},
                    "author_json": {"type": "object", "enabled": False},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"}
                }
            }
        }

        # Episodes index mapping - matches your data structure
        episodes_mapping = {
            "settings": {
                "number_of_shards": 1, 
                "number_of_replicas": 0,
                "refresh_interval": "5s",
                "analysis": {
                    "analyzer": {
                        "search_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding", "stop"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "episode_id": {"type": "keyword"},
                    "story_id": {"type": "keyword"},
                    "ep_title": {
                        "type": "text",
                        "analyzer": "search_analyzer", 
                        "boost": 3.0,
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "ep_meta_title": {
                        "type": "text",
                        "analyzer": "search_analyzer",
                        "boost": 2.0
                    },
                    "thumbnail_square": {"type": "keyword", "index": False},
                    "thumbnail_rect": {"type": "keyword", "index": False},
                    "thumbnail_responsive": {"type": "keyword", "index": False},
                    "ep_description": {
                        "type": "text",
                        "analyzer": "search_analyzer",
                        "boost": 1.5
                    },
                    "ep_meta_description": {
                        "type": "text", 
                        "analyzer": "search_analyzer"
                    },
                    "genre": {"type": "keyword"},
                    "subgenre": {"type": "keyword"},
                    "rating": {"type": "keyword"},
                    "avg_rating": {"type": "float"},
                    "avg_rating_count": {"type": "integer"},
                    "author_json": {"type": "object", "enabled": False},
                    "release_date": {"type": "date"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    # Story fields for cross-search
                    "story_title": {
                        "type": "text",
                        "analyzer": "search_analyzer",
                        "boost": 2.5
                    },
                    "story_description": {
                        "type": "text",
                        "analyzer": "search_analyzer",
                        "boost": 1.0
                    }
                }
            }
        }

        try:
            # Create stories index
            stories_exists = await client.indices.exists(index=settings.opensearch_stories_index)
            if stories_exists:
                logger.info(f"Deleting existing stories index")
                await client.indices.delete(index=settings.opensearch_stories_index)
                
            await client.indices.create(index=settings.opensearch_stories_index, body=stories_mapping)
            logger.info(f"Created stories index: {settings.opensearch_stories_index}")

            # Create episodes index  
            episodes_exists = await client.indices.exists(index=settings.opensearch_episodes_index)
            if episodes_exists:
                logger.info(f"Deleting existing episodes index")
                await client.indices.delete(index=settings.opensearch_episodes_index)
                
            await client.indices.create(index=settings.opensearch_episodes_index, body=episodes_mapping)
            logger.info(f"Created episodes index: {settings.opensearch_episodes_index}")

            return True

        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
            return False

    @staticmethod
    def story_to_opensearch_doc(story: Story) -> dict:
        """Convert Story model to OpenSearch document with exact field mapping"""
        return {
            "story_id": str(story.story_id),
            "title": story.title or "",
            "meta_title": story.meta_title or "",
            "thumbnail_square": story.thumbnail_square or "",
            "thumbnail_rect": story.thumbnail_rect or "",
            "thumbnail_responsive": story.thumbnail_responsive or "",
            "description": story.description or "",
            "meta_description": story.meta_description or "",
            "genre": story.genre or "uncategorized",
            "subgenre": story.subgenre or "",
            "rating": story.rating or "G",
            "avg_rating": float(story.avg_rating) if story.avg_rating else 0.0,
            "avg_rating_count": story.avg_rating_count or 0,
            "author_json": story.author_json or [],
            "created_at": story.created_at.isoformat() if story.created_at else datetime.utcnow().isoformat(),
            "updated_at": story.updated_at.isoformat() if story.updated_at else datetime.utcnow().isoformat()
        }

    @staticmethod
    def episode_to_opensearch_doc(episode: Episode) -> dict:
        """Convert Episode model to OpenSearch document with exact field mapping"""
        doc = {
            "episode_id": str(episode.episode_id),
            "story_id": str(episode.story_id),
            "ep_title": episode.title or "",
            "ep_meta_title": episode.meta_title or "",
            "thumbnail_square": episode.thumbnail_square or "",
            "thumbnail_rect": episode.thumbnail_rect or "",
            "thumbnail_responsive": episode.thumbnail_responsive or "",
            "ep_description": episode.description or "",
            "ep_meta_description": episode.meta_description or "",
            "genre": episode.genre or (episode.story.genre if episode.story else "uncategorized"),
            "subgenre": episode.subgenre or (episode.story.subgenre if episode.story else ""),
            "rating": episode.rating or (episode.story.rating if episode.story else "G"),
            "avg_rating": float(episode.avg_rating) if episode.avg_rating else 0.0,
            "avg_rating_count": episode.avg_rating_count or 0,
            "author_json": episode.author_json or (episode.story.author_json if episode.story else []),
            "release_date": episode.release_date.isoformat() if episode.release_date else None,
            "created_at": episode.created_at.isoformat() if episode.created_at else datetime.utcnow().isoformat(),
            "updated_at": episode.updated_at.isoformat() if episode.updated_at else datetime.utcnow().isoformat()
        }

        # Add story fields for cross-search
        if episode.story:
            doc["story_title"] = episode.story.title or ""
            doc["story_description"] = episode.story.description or ""
        else:
            doc["story_title"] = ""
            doc["story_description"] = ""

        return doc

    @classmethod
    async def index_all_data_from_db(cls):
        """Index all data directly from database to OpenSearch"""
        client = await cls.get_client()
        if not client:
            logger.error("OpenSearch client not available")
            return False

        logger.info("Starting direct database to OpenSearch indexing...")
        
        try:
            db: Session = next(get_db())
            
            # Index stories
            logger.info("Fetching stories from database...")
            stories = db.query(Story).all()
            logger.info(f"Found {len(stories)} stories to index")
            
            if stories:
                story_bulk_data = []
                for story in stories:
                    doc = cls.story_to_opensearch_doc(story)
                    story_bulk_data.extend([
                        {"index": {"_index": settings.opensearch_stories_index, "_id": str(story.story_id)}},
                        doc
                    ])
                
                # Bulk index stories
                if story_bulk_data:
                    logger.info(f"Indexing {len(stories)} stories...")
                    response = await client.bulk(body=story_bulk_data, refresh=True, timeout="60s")
                    
                    # Check for errors
                    errors = [item for item in response.get('items', []) if 'error' in item.get('index', {})]
                    if errors:
                        logger.warning(f"Stories indexing had {len(errors)} errors")
                        for error in errors[:3]:  # Log first 3 errors
                            logger.warning(f"Story index error: {error}")
                    else:
                        logger.info(f"Successfully indexed {len(stories)} stories")

            # Index episodes
            logger.info("Fetching episodes from database...")  
            episodes = db.query(Episode).options(db.joinedload(Episode.story)).all()
            logger.info(f"Found {len(episodes)} episodes to index")
            
            if episodes:
                episode_bulk_data = []
                for episode in episodes:
                    doc = cls.episode_to_opensearch_doc(episode)
                    episode_bulk_data.extend([
                        {"index": {"_index": settings.opensearch_episodes_index, "_id": str(episode.episode_id)}},
                        doc
                    ])
                
                # Bulk index episodes
                if episode_bulk_data:
                    logger.info(f"Indexing {len(episodes)} episodes...")
                    response = await client.bulk(body=episode_bulk_data, refresh=True, timeout="60s")
                    
                    # Check for errors
                    errors = [item for item in response.get('items', []) if 'error' in item.get('index', {})]
                    if errors:
                        logger.warning(f"Episodes indexing had {len(errors)} errors")
                        for error in errors[:3]:  # Log first 3 errors
                            logger.warning(f"Episode index error: {error}")
                    else:
                        logger.info(f"Successfully indexed {len(episodes)} episodes")

            db.close()
            
            # Verify indexing
            await asyncio.sleep(2)  # Wait for refresh
            await cls.verify_indexing()
            
            logger.info("Direct database indexing completed successfully")
            return True

        except Exception as e:
            logger.error(f"Direct database indexing failed: {e}", exc_info=True)
            return False

    @classmethod
    async def verify_indexing(cls):
        """Verify that data was actually indexed"""
        client = await cls.get_client()
        if not client:
            return
            
        try:
            # Check stories count
            stories_response = await client.count(index=settings.opensearch_stories_index)
            stories_count = stories_response.get('count', 0)
            logger.info(f"Stories in OpenSearch: {stories_count}")
            
            # Check episodes count
            episodes_response = await client.count(index=settings.opensearch_episodes_index)
            episodes_count = episodes_response.get('count', 0)
            logger.info(f"Episodes in OpenSearch: {episodes_count}")
            
            # Sample a few documents to verify structure
            if stories_count > 0:
                sample_story = await client.search(
                    index=settings.opensearch_stories_index,
                    body={"query": {"match_all": {}}, "size": 1}
                )
                if sample_story['hits']['hits']:
                    logger.info(f"Sample story document keys: {list(sample_story['hits']['hits'][0]['_source'].keys())}")
            
            if episodes_count > 0:
                sample_episode = await client.search(
                    index=settings.opensearch_episodes_index,
                    body={"query": {"match_all": {}}, "size": 1}
                )
                if sample_episode['hits']['hits']:
                    logger.info(f"Sample episode document keys: {list(sample_episode['hits']['hits'][0]['_source'].keys())}")
                    
        except Exception as e:
            logger.error(f"Verification failed: {e}")

    @classmethod
    async def search_with_redis_counters(cls, query: str, skip: int = 0, limit: int = 20) -> List[Dict[str, Any]]:
        """Search OpenSearch and add real-time Redis counters"""
        client = await cls.get_client()
        if not client:
            return []

        if not query or len(query.strip()) < 2:
            return []

        query = query.strip()
        logger.info(f"Searching for: '{query}'")

        try:
            # Search stories
            story_search = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["title^3", "description^2", "meta_title^1.5", "meta_description"],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                },
                "size": 50
            }

            # Search episodes
            episode_search = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["ep_title^3", "ep_description^2", "story_title^2.5", "story_description^1.5"],
                        "type": "best_fields", 
                        "fuzziness": "AUTO"
                    }
                },
                "size": 50
            }

            # Execute searches
            story_results, episode_results = await asyncio.gather(
                client.search(index=settings.opensearch_stories_index, body=story_search),
                client.search(index=settings.opensearch_episodes_index, body=episode_search),
                return_exceptions=True
            )

            combined_results = []

            # Process story results with Redis counters
            if not isinstance(story_results, Exception):
                for hit in story_results.get('hits', {}).get('hits', []):
                    doc = hit['_source']
                    story_id = doc.get('story_id')
                    
                    # Get real-time counters from Redis
                    counters = await cls._get_redis_counters('story', story_id)
                    
                    result = {
                        **doc,
                        "type": "story",
                        "score": hit['_score'],
                        # Real-time counters
                        "likes_count": counters.get('likes_count', 0),
                        "views_count": counters.get('views_count', 0),
                        "shares_count": counters.get('shares_count', 0),
                        "comments_count": counters.get('comments_count', 0)
                    }
                    combined_results.append(result)

            # Process episode results with Redis counters
            if not isinstance(episode_results, Exception):
                for hit in episode_results.get('hits', {}).get('hits', []):
                    doc = hit['_source']
                    episode_id = doc.get('episode_id')
                    
                    # Get real-time counters from Redis
                    counters = await cls._get_redis_counters('episode', episode_id)
                    
                    # Clean response - remove story fields used only for search
                    clean_doc = {k: v for k, v in doc.items() 
                               if k not in ['story_title', 'story_description']}
                    
                    result = {
                        **clean_doc,
                        "type": "episode",
                        "score": hit['_score'] * 0.9,
                        # Real-time counters
                        "likes_count": counters.get('likes_count', 0),
                        "views_count": counters.get('views_count', 0),
                        "shares_count": counters.get('shares_count', 0),
                        "comments_count": counters.get('comments_count', 0)
                    }
                    combined_results.append(result)

            # Sort and paginate
            combined_results.sort(key=lambda x: x.get('score', 0), reverse=True)
            return combined_results[skip:skip + limit]

        except Exception as e:
            logger.error(f"Search failed: {e}", exc_info=True)
            return []

    @staticmethod
    async def _get_redis_counters(entity_type: str, entity_id: str) -> Dict[str, int]:
        """Get real-time counters from Redis"""
        counters = {'likes_count': 0, 'views_count': 0, 'shares_count': 0, 'comments_count': 0}
        
        try:
            if not cache_service._redis_client:
                return counters

            pipe = cache_service._redis_client.pipeline()
            counter_keys = [
                f"{entity_type}:{entity_id}:likes_count",
                f"{entity_type}:{entity_id}:views_count", 
                f"{entity_type}:{entity_id}:shares_count",
                f"{entity_type}:{entity_id}:comments_count"
            ]
            
            for key in counter_keys:
                pipe.get(key)
                
            results = await pipe.execute()
            
            counter_names = ['likes_count', 'views_count', 'shares_count', 'comments_count']
            for i, result in enumerate(results):
                if result:
                    try:
                        counters[counter_names[i]] = int(result.decode('utf-8'))
                    except (ValueError, AttributeError):
                        pass
                        
        except Exception as e:
            logger.debug(f"Redis counters error for {entity_type}:{entity_id}: {e}")
            
        return counters

    @classmethod
    async def setup_complete_opensearch(cls):
        """Complete setup: create indexes and populate with data"""
        logger.info("Starting complete OpenSearch setup...")
        
        # Step 1: Create indexes
        success = await cls.create_indexes()
        if not success:
            return False
            
        # Step 2: Index data from database
        success = await cls.index_all_data_from_db()
        if not success:
            return False
            
        logger.info("OpenSearch setup completed successfully!")
        return True


# Alias for backward compatibility
SearchService = OpenSearchDataService