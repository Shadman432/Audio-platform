import asyncio
import logging
from supabase import create_client, Client

from .config import settings

logger = logging.getLogger(__name__)

async def listen_for_changes():
    if not settings.supabase_url or not settings.supabase_anon_key:
        logger.warning("Supabase URL or anon key not provided. Realtime updates are disabled.")
        return

    try:
        supabase: Client = create_client(settings.supabase_url, settings.supabase_anon_key)
        
        channel = supabase.channel("public:stories")

        def on_change(payload):
            logger.info(f"Change received: {payload}")
            # This is a simplified handler. You'll need to parse the payload
            # and update RediSearch accordingly.
            # For example, for an INSERT or UPDATE, you would get the new record
            # and call a method to update the search index.
            # For a DELETE, you would get the old record's ID and delete it from the index.
            
            # Example of handling an insert/update
            if payload.get("type") in ["INSERT", "UPDATE"]:
                record = payload.get("record")
                if record:
                    # This is a simplified example. You would need to map this to your document
                    # and decide whether it's a story or episode.
                    # A better approach is to have separate channels for each table.
                    logger.info(f"Record to update/insert: {record}")
                    # await SearchService.update_document_from_realtime(record)
            
            # Example of handling a delete
            if payload.get("type") == "DELETE":
                old_record = payload.get("old_record")
                if old_record:
                    logger.info(f"Record to delete: {old_record}")
                    # record_id = old_record.get("id")
                    # await SearchService.delete_document(f"story:{record_id}")


        channel.on("postgres_changes", {"event": "*", "schema": "public", "table": "stories"}).subscribe(on_change)
        
        # Do the same for episodes
        # episodes_channel = supabase.channel("public:episodes")
        # ...

        logger.info("📡 Subscribed to Supabase Realtime changes.")
        
        # Keep the listener running
        while True:
            await asyncio.sleep(1)

    except Exception as e:
        logger.error(f"Error with Supabase Realtime listener: {e}", exc_info=True)
