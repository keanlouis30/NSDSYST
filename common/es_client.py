"""Elasticsearch helpers shared by gateway and workers.

The ES cluster (3 nodes, sharded + replicated) IS the distributed data layer
required by the spec -- it handles partitioning and scatter-gather natively,
so the gateway/workers don't need to reimplement sharding by hand.
"""
import os

INDEX_NAME = 'logs'

INDEX_BODY = {
    'settings': {
        'number_of_shards': 3,
        'number_of_replicas': 1,
    },
    'mappings': {
        'properties': {
            'timestamp': {'type': 'date'},
            'hostname': {'type': 'keyword'},
            'process': {'type': 'keyword'},
            'pid': {'type': 'keyword'},
            'severity': {'type': 'keyword'},
            'message': {'type': 'text'},
            'raw': {'type': 'text'},
            'source_file': {'type': 'keyword'},
            'job_id': {'type': 'keyword'},
            'ingested_at': {'type': 'date'},
        }
    },
}


def es_hosts():
    return os.environ.get('ES_HOSTS', 'http://elasticsearch-01:9200').split(',')


def doc_id(job_id: str, seq: int) -> str:
    """Deterministic id so redelivery after a worker crash overwrites the same
    document instead of creating a duplicate (idempotent indexing)."""
    return f'{job_id}:{seq}'
