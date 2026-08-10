#!/usr/bin/env python3
"""Forwarder CLI client.

Usage:
  forwarder.py INGEST <file_path> <Gateway_IP>
  forwarder.py QUERY <Gateway_IP> SEARCH_DATE "<date_string>"
  forwarder.py QUERY <Gateway_IP> SEARCH_HOST <hostname>
  forwarder.py QUERY <Gateway_IP> SEARCH_DAEMON <daemon_name>
  forwarder.py QUERY <Gateway_IP> SEARCH_SEVERITY <severity_level>
  forwarder.py QUERY <Gateway_IP> SEARCH_KEYWORD <keyword>
  forwarder.py QUERY <Gateway_IP> COUNT_KEYWORD <keyword>
  forwarder.py PURGE <Gateway_IP>

<Gateway_IP> may be given as "host", "host:port" or a full "http://host:port" URL
(default port 8000 is assumed when omitted).
"""
import socket
import sys
import time
import uuid

import requests

BATCH_SIZE = 1000
DEFAULT_PORT = 8000
BATCH_ATTEMPTS = 5
BATCH_RETRY_BACKOFF = 2.0
QUERY_TYPES = {
    'SEARCH_DATE', 'SEARCH_HOST', 'SEARCH_DAEMON',
    'SEARCH_SEVERITY', 'SEARCH_KEYWORD', 'COUNT_KEYWORD',
}


def normalize_gateway(gateway_ip: str) -> str:
    url = gateway_ip.strip()
    if not url.startswith('http://') and not url.startswith('https://'):
        url = f'http://{url}'
    if url.count(':') < 2:  # no port given after the scheme's "://"
        url = f'{url}:{DEFAULT_PORT}'
    return url.rstrip('/')


def post_batch(gateway_url: str, payload: dict) -> int:
    """POST one batch, retrying if the gateway or broker is briefly unavailable.

    Retrying a whole batch is safe: each line carries a fixed (job_id, seq),
    which becomes its Elasticsearch document id, so a replayed batch overwrites
    its own documents instead of duplicating them.
    """
    last_error = None
    for attempt in range(BATCH_ATTEMPTS):
        try:
            resp = requests.post(f'{gateway_url}/ingest', json=payload, timeout=60)
            if resp.status_code >= 500:
                raise requests.exceptions.HTTPError(f'{resp.status_code} {resp.text[:200]}')
            resp.raise_for_status()
            return resp.json()['lines_queued']
        except (requests.exceptions.RequestException, ValueError, KeyError) as exc:
            last_error = exc
            if attempt + 1 < BATCH_ATTEMPTS:
                delay = BATCH_RETRY_BACKOFF * (attempt + 1)
                print(f'  batch failed ({exc}); retrying in {delay:.0f}s', file=sys.stderr)
                time.sleep(delay)

    raise SystemExit(f'ingest aborted after {BATCH_ATTEMPTS} attempts: {last_error}')


def cmd_ingest(file_path: str, gateway_ip: str):
    gateway_url = normalize_gateway(gateway_ip)
    job_id = str(uuid.uuid4())
    source_host = socket.gethostname()

    with open(file_path, 'r', errors='replace') as f:
        lines = [line.rstrip('\n') for line in f]

    total_queued = 0
    for start in range(0, len(lines), BATCH_SIZE):
        batch = lines[start:start + BATCH_SIZE]
        total_queued += post_batch(gateway_url, {
            'job_id': job_id,
            'source_host': source_host,
            'filename': file_path,
            'start_seq': start,
            'lines': batch,
        })
        print(f'  queued lines {start}-{start + len(batch) - 1}')

    print(f'INGEST complete: job_id={job_id} lines_queued={total_queued}')


def cmd_query(gateway_ip: str, search_type: str, value: str):
    gateway_url = normalize_gateway(gateway_ip)
    if search_type not in QUERY_TYPES:
        print(f'error: unknown query type {search_type}', file=sys.stderr)
        sys.exit(1)

    resp = requests.get(f'{gateway_url}/query', params={'type': search_type, 'value': value})
    resp.raise_for_status()
    data = resp.json()

    if search_type == 'COUNT_KEYWORD':
        print(f"COUNT_KEYWORD '{value}': {data['count']}")
        return

    print(f"{search_type} '{value}': {data['count']} match(es)")
    for entry in data['results']:
        print(f"  [{entry['timestamp']}] {entry['hostname']} {entry['process']} "
              f"({entry['severity']}): {entry['message']}")


def cmd_purge(gateway_ip: str):
    gateway_url = normalize_gateway(gateway_ip)
    resp = requests.post(f'{gateway_url}/purge')
    if resp.status_code == 409:
        print('PURGE rejected: another purge is already in progress')
        sys.exit(1)
    resp.raise_for_status()
    print('PURGE complete: all indexed log entries deleted')


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].upper()
    args = sys.argv[2:]

    if command == 'INGEST':
        if len(args) != 2:
            print('usage: forwarder.py INGEST <file_path> <Gateway_IP>', file=sys.stderr)
            sys.exit(1)
        cmd_ingest(args[0], args[1])
    elif command == 'QUERY':
        if len(args) != 3:
            print('usage: forwarder.py QUERY <Gateway_IP> <SEARCH_TYPE> <value>', file=sys.stderr)
            sys.exit(1)
        cmd_query(args[0], args[1].upper(), args[2])
    elif command == 'PURGE':
        if len(args) != 1:
            print('usage: forwarder.py PURGE <Gateway_IP>', file=sys.stderr)
            sys.exit(1)
        cmd_purge(args[0])
    else:
        print(f'unknown command: {command}', file=sys.stderr)
        print(__doc__)
        sys.exit(1)


if __name__ == '__main__':
    main()
