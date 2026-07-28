"""Syslog line parser: extracts Timestamp, Hostname, Process, Severity, Message."""
import re
from datetime import datetime, timezone

# Optional leading <PRI> (RFC3164) followed by "Mon DD HH:MM:SS host process[pid]: message"
LOG_RE = re.compile(
    r'^(?:<(?P<pri>\d{1,3})>)?'
    r'(?P<timestamp>[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+'
    r'(?P<hostname>\S+)\s+'
    r'(?P<process>[^\[:\s]+)(?:\[(?P<pid>\d+)\])?:\s*'
    r'(?P<message>.*)$'
)

SEVERITY_BY_CODE = ['EMERG', 'ALERT', 'CRIT', 'ERR', 'WARNING', 'NOTICE', 'INFO', 'DEBUG']

# Fallback when no PRI is present (typical for on-disk /var/log files, which strip PRI).
_SEVERITY_KEYWORDS = [
    ('EMERGENCY', 'EMERG'), ('EMERG', 'EMERG'),
    ('ALERT', 'ALERT'),
    ('CRITICAL', 'CRIT'), ('CRIT', 'CRIT'),
    ('ERROR', 'ERR'), ('ERR', 'ERR'), ('FAIL', 'ERR'),
    ('WARNING', 'WARNING'), ('WARN', 'WARNING'),
    ('NOTICE', 'NOTICE'),
    ('DEBUG', 'DEBUG'),
    ('INFO', 'INFO'),
]


def _guess_severity(message: str) -> str:
    upper = message.upper()
    for keyword, severity in _SEVERITY_KEYWORDS:
        if keyword in upper:
            return severity
    return 'INFO'


def _parse_timestamp(raw_ts: str, reference: datetime) -> str:
    """RFC3164 timestamps omit the year; assume the reference year, rolling back
    one year if that would place the entry in the future."""
    parsed = datetime.strptime(f'{reference.year} {raw_ts}', '%Y %b %d %H:%M:%S')
    parsed = parsed.replace(tzinfo=timezone.utc)
    if parsed > reference:
        parsed = parsed.replace(year=reference.year - 1)
    return parsed.isoformat()


def parse_line(raw_line: str, reference: datetime = None) -> dict:
    """Parse a single syslog line. Falls back to a permissive record (severity
    INFO, hostname/process 'unknown') if the line doesn't match RFC3164 shape,
    so no line is ever silently dropped."""
    reference = reference or datetime.now(timezone.utc)
    match = LOG_RE.match(raw_line.strip())
    if not match:
        return {
            'timestamp': reference.isoformat(),
            'hostname': 'unknown',
            'process': 'unknown',
            'pid': None,
            'severity': _guess_severity(raw_line),
            'message': raw_line.strip(),
            'raw': raw_line,
        }

    groups = match.groupdict()
    if groups['pri'] is not None:
        severity = SEVERITY_BY_CODE[int(groups['pri']) % 8]
    else:
        severity = _guess_severity(groups['message'])

    return {
        'timestamp': _parse_timestamp(groups['timestamp'], reference),
        'hostname': groups['hostname'],
        'process': groups['process'],
        'pid': groups['pid'],
        'severity': severity,
        'message': groups['message'],
        'raw': raw_line,
    }
