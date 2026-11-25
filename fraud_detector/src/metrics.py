"""Метрики с фиксированными labels; бизнес-разрезы хранятся в SQL."""
import time
from datetime import datetime, timezone
from prometheus_client import Counter, Gauge, Histogram, REGISTRY

SCORE_BUCKETS = tuple(i / 20 for i in range(21))


class ScoringMetrics:
    def __init__(self, registry=REGISTRY):
        self.scored = Counter('fraud_scored_total', 'Successfully delivered scores', registry=registry)
        self.flagged = Counter('fraud_flagged_total', 'Scores flagged by the model', registry=registry)
        self.latency = Histogram('fraud_processing_seconds', 'Preprocessing and inference of one transaction', buckets=(.001,.005,.01,.025,.05,.1,.25,.5,1,2,5), registry=registry)
        self.scores = Histogram('fraud_score', 'Delivered probability scores', buckets=SCORE_BUCKETS, registry=registry)
        self.age = Histogram('fraud_event_age_seconds', 'Wall clock age since producer emission', buckets=(.1,.5,1,2,5,10,30,60,120,300), registry=registry)
        self.errors = Counter('fraud_errors_total', 'Failed operations', ['stage'], registry=registry)
        for stage in ('input','inference','publish','reload'): self.errors.labels(stage)
        self.last_success = Gauge('fraud_last_success_timestamp_seconds', 'Last delivered score epoch', registry=registry)
        self.last_success.set(time.time())
        self.ratio = Gauge('fraud_flag_ratio', 'Cumulative flagged fraction since process start', registry=registry)
        self.reloads = Counter('fraud_model_loads_total', 'Successful loads including bootstrap', ['version'], registry=registry)
        self.loaded_at = Gauge('fraud_model_loaded_timestamp_seconds', 'Last model load epoch', registry=registry)
        self.total = self.fraud = 0

    def observe(self, result, duration):
        self.scored.inc()
        self.flagged.inc(result['fraud_flag'])
        self.latency.observe(duration)
        self.scores.observe(result['score'])
        self.total += 1
        self.fraud += result['fraud_flag']
        self.ratio.set(self.fraud / self.total)
        self.last_success.set(time.time())
        if result.get('emitted_at'):
            try:
                emitted = datetime.fromisoformat(result['emitted_at'].replace('Z','+00:00'))
                if emitted.tzinfo is None: emitted = emitted.replace(tzinfo=timezone.utc)
                self.age.observe(max(0, time.time() - emitted.timestamp()))
            except (ValueError, TypeError):
                self.errors.labels('input').inc()

    def model_loaded(self, version):
        self.reloads.labels(version).inc()
        self.loaded_at.set(time.time())
