"""RabbitMQ topology shared by gateway and workers."""
import os

RABBITMQ_URL = os.environ.get('RABBITMQ_URL', 'amqp://guest:guest@rabbitmq:5672/%2F')

INGEST_QUEUE = 'logs.ingest'
CONTROL_EXCHANGE = 'control.purge'  # fanout: broadcasts "pause"/"resume" to all workers
PURGE_LOCK_QUEUE = 'purge.lock'  # exclusive queue used as a distributed mutex

PAUSE_MSG = b'pause'
RESUME_MSG = b'resume'
