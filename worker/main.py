"""Distributed worker node.

Consumes raw syslog lines from the RabbitMQ ingest queue, parses them, and
indexes them into the Elasticsearch data layer. Two things make this safe
under chaos testing (worker killed mid-ingestion):

  * messages are only ack'd *after* a successful ES index, so a killed
    worker's unacked messages are redelivered to another worker -- no data
    loss;
  * the ES document id is deterministic (job_id:seq), so redelivery
    overwrites the same document instead of creating a duplicate -- no
    duplicate processing, even though delivery is at-least-once.

A second consumer on the same connection listens on the fanout control
exchange for pause/resume signals broadcast by the gateway during PURGE,
so this worker stops touching the index while it's being cleared.
"""
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime, timezone

import pika
from elasticsearch import Elasticsearch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.mq import CONTROL_EXCHANGE, INGEST_QUEUE, PAUSE_MSG, RABBITMQ_URL, RESUME_MSG
from common.es_client import INDEX_NAME, doc_id, es_hosts
from common.parser import parse_line

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(socket.gethostname())

PREFETCH = int(os.environ.get('WORKER_PREFETCH', '20'))
RECONNECT_DELAY_SECONDS = 5


def make_es_client():
    return Elasticsearch(hosts=es_hosts())


def process_ingest_message(channel, method, properties, body, es):
    try:
        data = json.loads(body)
        parsed = parse_line(data['raw_line'])
        document = {
            **parsed,
            'source_file': data.get('filename'),
            'job_id': data.get('job_id'),
            'ingested_at': datetime.now(timezone.utc).isoformat(),
        }
        es.index(index=INDEX_NAME, id=doc_id(data['job_id'], data['seq']), document=document)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        log.exception('failed to process message, requeuing')
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def run():
    es = make_es_client()

    while True:
        try:
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))

            ingest_channel = connection.channel()
            ingest_channel.queue_declare(queue=INGEST_QUEUE, durable=True)
            ingest_channel.basic_qos(prefetch_count=PREFETCH)

            control_channel = connection.channel()
            control_channel.exchange_declare(exchange=CONTROL_EXCHANGE, exchange_type='fanout', durable=True)
            control_queue = control_channel.queue_declare(queue='', exclusive=True, auto_delete=True)
            control_queue_name = control_queue.method.queue
            control_channel.queue_bind(exchange=CONTROL_EXCHANGE, queue=control_queue_name)

            paused = {'value': False}
            consumer_tag = {'value': None}

            def start_ingest_consumer():
                consumer_tag['value'] = ingest_channel.basic_consume(
                    queue=INGEST_QUEUE,
                    on_message_callback=lambda ch, method, props, body: process_ingest_message(ch, method, props, body, es),
                )

            def on_control_message(ch, method, properties, body):
                ch.basic_ack(delivery_tag=method.delivery_tag)
                if body == PAUSE_MSG and not paused['value']:
                    log.info('received PAUSE: cancelling ingest consumption')
                    ingest_channel.basic_cancel(consumer_tag['value'])
                    paused['value'] = True
                elif body == RESUME_MSG and paused['value']:
                    log.info('received RESUME: resuming ingest consumption')
                    start_ingest_consumer()
                    paused['value'] = False

            control_channel.basic_consume(queue=control_queue_name, on_message_callback=on_control_message)
            start_ingest_consumer()

            log.info('worker started, waiting for messages')
            while True:
                connection.process_data_events(time_limit=1)

        except (pika.exceptions.AMQPConnectionError, pika.exceptions.StreamLostError, pika.exceptions.ChannelClosedByBroker) as exc:
            log.warning('connection lost (%s), reconnecting in %ss', exc, RECONNECT_DELAY_SECONDS)
            time.sleep(RECONNECT_DELAY_SECONDS)
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    run()
