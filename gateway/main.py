"""API Gateway / Search Head.

Fronts the RabbitMQ ingest queue (async publish, never blocks on workers)
and the Elasticsearch cluster (which does the actual scatter-gather across
its shards -- the gateway just builds the query and forwards it).

PURGE coordination: a RabbitMQ exclusive queue is used as a distributed
mutex (only one connection can ever hold it), combined with a fanout
broadcast telling every worker to pause/resume consumption, so no worker
is mid-write while the index is cleared.
"""
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import aio_pika
from dateutil import parser as dateparser
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.mq import (
    CONTROL_EXCHANGE, INGEST_QUEUE, PAUSE_MSG, PURGE_LOCK_QUEUE,
    RABBITMQ_URL, RESUME_MSG,
)
from common.es_client import INDEX_BODY, INDEX_NAME, es_hosts

PURGE_PAUSE_GRACE_SECONDS = float(os.environ.get('PURGE_PAUSE_GRACE_SECONDS', '3'))
MAX_RESULTS = 500

app = FastAPI(title='Mini-Splunk Gateway')

state = {}


class IngestRequest(BaseModel):
    job_id: str
    source_host: str
    filename: str
    start_seq: int
    lines: List[str]


@app.on_event('startup')
async def startup():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.declare_queue(INGEST_QUEUE, durable=True)
    control_exchange = await channel.declare_exchange(CONTROL_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True)

    es = AsyncElasticsearch(hosts=es_hosts())
    if not await es.indices.exists(index=INDEX_NAME):
        try:
            await es.indices.create(index=INDEX_NAME, **INDEX_BODY)
        except Exception:
            pass  # another gateway replica / race: index already created concurrently

    state['connection'] = connection
    state['channel'] = channel
    state['control_exchange'] = control_exchange
    state['es'] = es


@app.on_event('shutdown')
async def shutdown():
    await state['es'].close()
    await state['connection'].close()


@app.get('/health')
async def health():
    return {'status': 'ok'}


@app.post('/ingest')
async def ingest(req: IngestRequest):
    channel = state['channel']
    for i, raw_line in enumerate(req.lines):
        if not raw_line.strip():
            continue
        seq = req.start_seq + i
        payload = {
            'job_id': req.job_id,
            'seq': seq,
            'source_host': req.source_host,
            'filename': req.filename,
            'raw_line': raw_line,
        }
        message = aio_pika.Message(
            body=json.dumps(payload).encode('utf-8'),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type='application/json',
        )
        await channel.default_exchange.publish(message, routing_key=INGEST_QUEUE)
    return {'job_id': req.job_id, 'lines_queued': len(req.lines)}


def _day_range(date_string: str):
    parsed = dateparser.parse(date_string, fuzzy=True, default=datetime.now(timezone.utc))
    start = parsed.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start.isoformat(), end.isoformat()


def build_query(search_type: str, value: str) -> dict:
    if search_type == 'SEARCH_DATE':
        start, end = _day_range(value)
        return {'range': {'timestamp': {'gte': start, 'lt': end}}}
    if search_type == 'SEARCH_HOST':
        return {'term': {'hostname': value}}
    if search_type == 'SEARCH_DAEMON':
        return {'term': {'process': value}}
    if search_type == 'SEARCH_SEVERITY':
        return {'term': {'severity': value.upper()}}
    if search_type in ('SEARCH_KEYWORD', 'COUNT_KEYWORD'):
        return {'match_phrase': {'message': value}}
    raise HTTPException(400, f'unknown search type {search_type}')


@app.get('/query')
async def query(type: str, value: str):
    es = state['es']
    es_query = build_query(type, value)

    if type == 'COUNT_KEYWORD':
        result = await es.count(index=INDEX_NAME, query=es_query)
        return {'keyword': value, 'count': result['count']}

    result = await es.search(
        index=INDEX_NAME,
        query=es_query,
        size=MAX_RESULTS,
        sort=[{'timestamp': 'asc'}],
    )
    hits = [hit['_source'] for hit in result['hits']['hits']]
    return {'count': result['hits']['total']['value'], 'results': hits}


@app.post('/purge')
async def purge():
    lock_connection = await aio_pika.connect(RABBITMQ_URL)
    lock_channel = await lock_connection.channel()
    try:
        await lock_channel.declare_queue(PURGE_LOCK_QUEUE, exclusive=True, auto_delete=True)
    except Exception:
        await lock_connection.close()
        raise HTTPException(409, 'a purge is already in progress')

    try:
        control_exchange = state['control_exchange']
        await control_exchange.publish(aio_pika.Message(body=PAUSE_MSG), routing_key='')
        await asyncio.sleep(PURGE_PAUSE_GRACE_SECONDS)

        es = state['es']
        try:
            await es.delete_by_query(index=INDEX_NAME, query={'match_all': {}}, refresh=True, conflicts='proceed')
        except NotFoundError:
            pass

        await control_exchange.publish(aio_pika.Message(body=RESUME_MSG), routing_key='')
    finally:
        await lock_connection.close()

    return {'status': 'purged'}
