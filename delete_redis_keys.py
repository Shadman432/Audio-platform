
import redis
import asyncio

def delete_redis_keys():
    r = redis.Redis(host='localhost', port=6379, db=0)
    
    # Keys related to comments
    keys_to_delete = []
    for key in r.scan_iter("comments:*"):
        keys_to_delete.append(key)
    for key in r.scan_iter("comment:*"):
        keys_to_delete.append(key)
    for key in r.scan_iter("comment_like:*"):
        keys_to_delete.append(key)
    for key in r.scan_iter("story:*:comments_count"):
        keys_to_delete.append(key)
    for key in r.scan_iter("episode:*:comments_count"):
        keys_to_delete.append(key)
    
    # Queues
    keys_to_delete.append("comments:reply_count_updates")
    keys_to_delete.append("comments:db_queue")
    keys_to_delete.append("comments:update_queue")
    keys_to_delete.append("comments:visibility_updates")
    keys_to_delete.append("comment_likes:delete_queue")
    keys_to_delete.append("comment_likes:insert_queue")

    if keys_to_delete:
        r.delete(*keys_to_delete)
        print(f"Deleted {len(keys_to_delete)} keys from Redis.")
    else:
        print("No comment-related keys found in Redis.")

if __name__ == "__main__":
    delete_redis_keys()
