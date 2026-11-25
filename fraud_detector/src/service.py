"""Живой Kafka-скоринг: подтверждение доставки и метрики на транзакцию."""
import json
import logging
import math
import os
import signal
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server
from metrics import ScoringMetrics
from scorer import VersionedScorer, ensure_bootstrap

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('fraud_detector')
running = True


def stop(signum, frame):
    global running
    running = False


def parse(messages, metrics=None):
    ids, rows = [], []
    for msg in messages:
        if msg.error():
            raise RuntimeError(str(msg.error()))
        try:
            payload = json.loads(msg.value())
            tid, row = payload['transaction_id'], payload['data']
            if not isinstance(tid,str) or not tid.strip() or '\x00' in tid:
                raise ValueError('transaction_id must be a non-empty string')
            if not isinstance(row, dict):
                raise ValueError('data must be an object')
            row=dict(row)
            if row.get('amount') is not None:
                if isinstance(row['amount'],bool) or not math.isfinite(float(row['amount'])) or float(row['amount'])<0:
                    raise ValueError('amount must be finite and non-negative')
            for name in ('emitted_at','transaction_time'):
                value=row.get(name)
                if value is not None:
                    if not isinstance(value,str):raise ValueError(f'{name} must be an ISO timestamp')
                    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
                    row[name]=stamp.replace(tzinfo=timezone.utc).isoformat() if stamp.tzinfo is None else stamp.isoformat()
            ids.append(tid)
            rows.append(row)
        except (ValueError, KeyError, TypeError, OverflowError) as exc:
            if metrics:
                metrics.errors.labels('input').inc()
            logger.warning('Malformed offset %s: %s', msg.offset(), exc)
    return ids, rows


def score_batch(scorer, ids, rows):
    scores, flags, version = scorer.predict(pd.DataFrame(rows))
    now = datetime.now(timezone.utc).isoformat()
    return [dict(transaction_id=tid, score=float(s), fraud_flag=int(f), model_version=version,
                 us_state=str(row.get('us_state') or 'unknown'), merch=str(row.get('merch') or 'unknown'),
                 cat_id=str(row.get('cat_id') or 'unknown'), amount=float(row.get('amount') or 0),
                 transaction_time=row.get('transaction_time'), emitted_at=row.get('emitted_at'), scored_at=now)
            for tid, row, s, f in zip(ids, rows, scores, flags)]


def publish_score(producer,result,metrics):
    delivery_errors=[]
    def delivered(error,_message):
        if error:delivery_errors.append(str(error))
    try:
        producer.produce(os.getenv('KAFKA_SCORING_TOPIC','scoring'),key=result['transaction_id'],
                         value=json.dumps(result,allow_nan=False),on_delivery=delivered)
        remaining=producer.flush(15)
        if remaining or delivery_errors:
            raise RuntimeError(f'Kafka delivery failed: remaining={remaining}, errors={delivery_errors}')
    except Exception:
        metrics.errors.labels('publish').inc()
        raise


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    metrics = ScoringMetrics()
    start_http_server(int(os.getenv('METRICS_PORT', '8000')))
    ensure_bootstrap()
    scorer = VersionedScorer()
    brokers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    consumer = Consumer({'bootstrap.servers': brokers, 'group.id': 'fraud-monitoring-scorer',
                         'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    producer = Producer({'bootstrap.servers': brokers, 'enable.idempotence': True, 'acks': 'all'})
    consumer.subscribe([os.getenv('KAFKA_TRANSACTIONS_TOPIC', 'transactions')])
    try:
        while running:
            previous=scorer.version
            if scorer.refresh():
                metrics.model_loaded(scorer.version,kind='startup' if previous is None else 'reload')
            elif scorer.last_reload_error:
                metrics.errors.labels('reload').inc()
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            ids, rows = parse([msg], metrics)
            if not rows:
                consumer.commit(message=msg, asynchronous=False)
                continue
            started = time.perf_counter()
            try:
                result = score_batch(scorer, ids, rows)[0]
            except Exception:
                metrics.errors.labels('inference').inc()
                logger.exception('Cannot score %s; skip invalid transaction', ids[0])
                consumer.commit(message=msg, asynchronous=False)
                continue
            duration = time.perf_counter() - started
            publish_score(producer,result,metrics)
            metrics.observe(result, duration)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        producer.flush(5)


if __name__ == '__main__':
    main()
