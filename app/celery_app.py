from celery import Celery
from .config import settings

celery_app = Celery(
    "home_audio_fastapi_backend",
    broker=settings.get_redis_url(),
    backend=settings.get_redis_url()
)

celery_app.conf.update(
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=100,
    broker_connection_retry_on_startup=True,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
)

celery_app.conf.beat_schedule = {
    'batch-save-comments-every-5-minutes': {
        'task': 'app.tasks.batch_save_comments_to_db',
        'schedule': 30.0,  # 30 seconds for developing(afterwards 5 minutes)
    },
}

# Optional: Autodiscover tasks in specified modules
celery_app.autodiscover_tasks(['app.tasks'])

# Example task (can be moved to app/tasks.py)
@celery_app.task(name="example_task")
def example_task(a, b):
    print(f"Executing example_task with {a} and {b}")
    return a + b