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
import logging
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
PUBLISH_ATTEMPTS = int(os.environ.get('PUBLISH_ATTEMPTS', '4'))
PUBLISH_RETRY_BACKOFF = float(os.environ.get('PUBLISH_RETRY_BACKOFF', '1.5'))
MAX_RESULTS = 500

# Broker failures worth rebuilding the connection for (as opposed to real bugs).
BROKER_ERRORS = (aio_pika.exceptions.AMQPError, ConnectionError, OSError, asyncio.TimeoutError)

app = FastAPI(title='Mini-Splunk Gateway')

log = logging.getLogger('gateway')

state = {}


class IngestRequest(BaseModel):
    job_id: str
    source_host: str
    filename: str
    start_seq: int
    lines: List[str]


async def connect_broker():
    """Open a connection/channel and (re)declare the topology."""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.declare_queue(INGEST_QUEUE, durable=True)
    control_exchange = await channel.declare_exchange(
        CONTROL_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True)

    state['connection'] = connection
    state['channel'] = channel
    state['control_exchange'] = control_exchange
    return channel, control_exchange


async def get_broker():
    """Return a live (channel, control_exchange).

    A broker restart force-closes our connection; the objects cached in
    `state` then refer to a dead channel forever. Rebuilding them on demand
    is what makes the gateway recover autonomously instead of failing every
    request until it is manually restarted.
    """
    async with state['broker_lock']:
        connection = state.get('connection')
        channel = state.get('channel')
        if connection is not None and not connection.is_closed and channel is not None and not channel.is_closed:
            return channel, state['control_exchange']

        log.warning('broker connection is not usable, re-establishing')
        if connection is not None:
            try:
                await connection.close()
            except Exception:
                pass
        return await connect_broker()


async def invalidate_broker():
    """Drop the cached connection so the next get_broker() rebuilds it."""
    async with state['broker_lock']:
        connection = state.pop('connection', None)
        state.pop('channel', None)
        state.pop('control_exchange', None)
        if connection is not None:
            try:
                await connection.close()
            except Exception:
                pass


@app.on_event('startup')
async def startup():
    state['broker_lock'] = asyncio.Lock()
    try:
        await connect_broker()
    except BROKER_ERRORS as exc:
        # Don't die on startup if the broker isn't up yet -- the first request
        # will establish the connection instead.
        log.warning('broker unavailable at startup (%s); will connect on demand', exc)

    es = AsyncElasticsearch(hosts=es_hosts())
    if not await es.indices.exists(index=INDEX_NAME):
        try:
            await es.indices.create(index=INDEX_NAME, **INDEX_BODY)
        except Exception:
            pass  # another gateway replica / race: index already created concurrently

    state['es'] = es


@app.on_event('shutdown')
async def shutdown():
    await state['es'].close()
    connection = state.get('connection')
    if connection is not None:
        await connection.close()


@app.get('/health')
async def health():
    return {'status': 'ok'}


async def publish_line(payload: dict):
    """Publish one line, rebuilding the broker connection and retrying if the
    broker went away. Safe to retry: a line republished after an ambiguous
    failure lands on the same deterministic ES document id (job_id:seq), so a
    duplicate publish collapses into an overwrite rather than a second row."""
    message = aio_pika.Message(
        body=json.dumps(payload).encode('utf-8'),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        content_type='application/json',
    )

    last_exc = None
    for attempt in range(PUBLISH_ATTEMPTS):
        try:
            channel, _ = await get_broker()
            await channel.default_exchange.publish(message, routing_key=INGEST_QUEUE)
            return
        except BROKER_ERRORS as exc:
            last_exc = exc
            log.warning('publish attempt %s/%s failed: %s', attempt + 1, PUBLISH_ATTEMPTS, exc)
            await invalidate_broker()
            if attempt + 1 < PUBLISH_ATTEMPTS:
                await asyncio.sleep(PUBLISH_RETRY_BACKOFF * (attempt + 1))

    raise HTTPException(503, f'broker unavailable, could not queue line: {last_exc}')


@app.post('/ingest')
async def ingest(req: IngestRequest):
    queued = 0
    for i, raw_line in enumerate(req.lines):
        if not raw_line.strip():
            continue
        await publish_line({
            'job_id': req.job_id,
            'seq': req.start_seq + i,
            'source_host': req.source_host,
            'filename': req.filename,
            'raw_line': raw_line,
        })
        queued += 1
    return {'job_id': req.job_id, 'lines_queued': queued}


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
        _, control_exchange = await get_broker()
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
