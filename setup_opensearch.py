# setup_opensearch.py - Run this script to fix OpenSearch data issues

import asyncio
import sys
import os

# Add the app directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.opensearch_service import OpenSearchService as OpenSearchDataService
from app.database import SessionLocal
from app.models.stories import Story
from app.models.episodes import Episode

async def setup_opensearch():
    """Setup OpenSearch with proper data indexing"""
    print("OpenSearch Setup Starting...")
    print("=" * 50)
    
    # Step 1: Check database data
    print("1. Checking database data...")
    with SessionLocal() as db:
        story_count = db.query(Story).count()
        episode_count = db.query(Episode).count()
        print(f"   - Stories in database: {story_count}")
        print(f"   - Episodes in database: {episode_count}")
        
        if story_count == 0 and episode_count == 0:
            print("   ERROR: No data in database to index!")
            print("   Please add some stories and episodes first.")
            return False
    
    # Step 2: Test OpenSearch connection
    print("2. Testing OpenSearch connection...")
    client = await OpenSearchDataService.get_client()
    if not client:
        print("   ERROR: Cannot connect to OpenSearch!")
        print("   Check your OpenSearch URL and credentials in config.py")
        return False
    print("   SUCCESS: OpenSearch connected")
    
    # Step 3: Create indexes
    print("3. Creating OpenSearch indexes...")
    success = await OpenSearchDataService.create_indexes()
    if not success:
        print("   ERROR: Failed to create indexes!")
        return False
    print("   SUCCESS: Indexes created")
    
    # Step 4: Index data from database
    print("4. Indexing data from database...")
    success = await OpenSearchDataService.index_all_data_from_db()
    if not success:
        print("   ERROR: Failed to index data!")
        return False
    print("   SUCCESS: Data indexed")
    
    # Step 5: Verify data was saved
    print("5. Verifying indexed data...")
    await OpenSearchDataService.verify_indexing()
    
    # Step 6: Test search
    print("6. Testing search functionality...")
    results = await OpenSearchDataService.search_unified("story", limit=3)
    print(f"   Search test returned {len(results)} results")
    
    if results:
        print("   Sample result:")
        sample = results[0]
        print(f"   - ID: {sample.get('story_id', sample.get('episode_id'))}")
        print(f"   - Title: {sample.get('title', sample.get('ep_title'))}")
        print(f"   - Type: {sample.get('type')}")
        print(f"   - Score: {sample.get('score', 0):.2f}")
    
    print("=" * 50)
    print("OpenSearch setup completed successfully!")
    print("Your data is now searchable with real-time Redis counters.")
    
    return True

async def test_specific_search():
    """Test search with specific terms"""
    print("\nTesting specific search terms...")
    
    test_queries = ["story", "family", "romantic", "256"]
    
    for query in test_queries:
        print(f"\nSearching for: '{query}'")
        results = await OpenSearchDataService.search_unified(query, limit=3)
        
        if results:
            print(f"Found {len(results)} results:")
            for i, result in enumerate(results, 1):
                title = result.get('title') or result.get('ep_title', 'N/A')
                type_name = result.get('type', 'unknown')
                score = result.get('score', 0)
                print(f"   {i}. [{type_name.upper()}] {title} (Score: {score:.2f})")
        else:
            print("No results found")

if __name__ == "__main__":
    print("OpenSearch Data Fix Script")
    print("This will setup OpenSearch to properly save and search your metadata")
    
    try:
        success = asyncio.run(setup_opensearch())
        
        if success:
            print("\nRunning additional search tests...")
            asyncio.run(test_specific_search())
            
            print("\nSetup complete! You can now:")
            print("- Use search API: GET /api/v1/search/all?query=your_search")
            print("- Search will include real-time Redis counters")
            print("- Data includes all the metadata you specified")
        else:
            print("\nSetup failed. Please check the error messages above.")
            
    except KeyboardInterrupt:
        print("\nSetup cancelled by user")
    except Exception as e:
        print(f"\nSetup failed with error: {e}")
        import traceback
        traceback.print_exc()
