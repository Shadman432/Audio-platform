from fastapi import APIRouter, Query
from typing import List, Dict, Any
from ..services.opensearch_service import opensearch_service

router = APIRouter()

# The response model can be a generic Dict since the unified search now returns
# a list of dictionaries with a 'type' field to distinguish between stories and episodes.
# For more strict validation, you could use a Union of Pydantic models.

@router.get("/all", response_model=List[Dict[str, Any]], summary="Unified Search API")
async def unified_search_api(
    query: str = Query(..., min_length=1, description="Search query string. Supports prefix search and typo tolerance."),
    skip: int = Query(0, ge=0, description="Number of items to skip for pagination."),
    limit: int = Query(20, ge=1, le=100, description="Number of items to return per page."),
):
    """
    Performs a powerful, unified search across both **stories** and **episodes**.

    - **Weighted Search**: `title` fields are given the highest priority.
    - **Typo Tolerance**: Results will be found even with spelling mistakes.
    - **Caching**: Frequent queries are cached for sub-millisecond response times.
    - **Ranking**: Results are ranked by relevance, with stories prioritized over episodes.
    """
    results = await opensearch_service.search_unified(query.strip(), skip, limit)
    return results
