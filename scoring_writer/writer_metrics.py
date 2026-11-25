"""Метрики только подтверждённых записей PostgreSQL."""
import time
from prometheus_client import Counter, Gauge, Histogram, REGISTRY


class WriterMetrics:
    def __init__(self, registry=REGISTRY):
        self.written = Counter('fraud_written_total', 'New committed scores (duplicates excluded)', registry=registry)
        self.flagged = Counter('fraud_written_flagged_total', 'New committed model flags', registry=registry)
        self.latency = Histogram('fraud_db_write_seconds', 'Insert and commit duration', buckets=(.001,.005,.01,.025,.05,.1,.25,.5,1,2), registry=registry)
        self.scores = Histogram('fraud_written_score', 'New committed probability scores', buckets=tuple(i/20 for i in range(21)), registry=registry)
        self.errors = Counter('fraud_writer_errors_total', 'Writer failures', ['stage'], registry=registry)
        for stage in ('input','database'): self.errors.labels(stage)
        self.duplicates = Counter('fraud_writer_duplicates_total', 'Already committed transaction IDs', registry=registry)
        self.last_success = Gauge('fraud_writer_last_success_timestamp_seconds', 'Last commit epoch', registry=registry)

    def observe(self, data, duration):
        self.written.inc()
        self.flagged.inc(data['fraud_flag'])
        self.latency.observe(duration)
        self.scores.observe(data['score'])
        self.last_success.set(time.time())
