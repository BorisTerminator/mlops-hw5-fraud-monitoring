"""Kafka scoring -> PostgreSQL: at-least-once delivery, idempotent writes."""
import json
import logging
import os
import signal
import time
import psycopg2
from confluent_kafka import Consumer
from prometheus_client import start_http_server
try:
    from .writer_metrics import WriterMetrics
except ImportError:
    from writer_metrics import WriterMetrics

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)
running = True


def get_db_config():
    return dict(host=os.getenv('POSTGRES_HOST','postgres'), port=os.getenv('POSTGRES_PORT','5432'),
                dbname=os.getenv('POSTGRES_DB','fraud_db'), user=os.getenv('POSTGRES_USER','fraud_user'),
                password=os.getenv('POSTGRES_PASSWORD','fraud_pass'))


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS scores (
            id BIGSERIAL PRIMARY KEY, transaction_id TEXT NOT NULL,
            score DOUBLE PRECISION NOT NULL, fraud_flag INT NOT NULL,
            model_version TEXT, created_at TIMESTAMPTZ DEFAULT clock_timestamp())''')
        for name, dtype in [('us_state','TEXT'),('merch','TEXT'),('cat_id','TEXT'),
                            ('amount','DOUBLE PRECISION'),('scored_at','TIMESTAMPTZ'),
                            ('emitted_at','TIMESTAMPTZ'),('transaction_time','TIMESTAMPTZ')]:
            cur.execute(f'ALTER TABLE scores ADD COLUMN IF NOT EXISTS {name} {dtype}')
        cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS scores_transaction_id_uq ON scores(transaction_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS scores_created_at_idx ON scores(created_at)')
        cur.execute('CREATE INDEX IF NOT EXISTS scores_segments_idx ON scores(us_state,merch,created_at)')
        cur.execute('GRANT SELECT ON scores TO grafana_reader')
    conn.commit()


def insert_score(conn, data, metrics=None):
    started = time.perf_counter()
    try:
        with conn.cursor() as cur:
            cur.execute('''INSERT INTO scores
                (transaction_id,score,fraud_flag,model_version,us_state,merch,cat_id,amount,scored_at,emitted_at,transaction_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (transaction_id) DO NOTHING RETURNING id''',
                tuple(data.get(k) for k in ('transaction_id','score','fraud_flag','model_version',
                                            'us_state','merch','cat_id','amount','scored_at','emitted_at','transaction_time')))
            inserted = cur.fetchone() is not None
        conn.commit()
    except Exception:
        conn.rollback()
        if metrics: metrics.errors.labels('database').inc()
        raise
    if metrics:
        if inserted: metrics.observe(data, time.perf_counter()-started)
        else: metrics.duplicates.inc()
    return inserted


def stop(signum, frame):
    global running
    running = False


def run_consumer():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    metrics = WriterMetrics()
    start_http_server(int(os.getenv('METRICS_PORT','8001')))
    consumer = Consumer({'bootstrap.servers':os.getenv('KAFKA_BOOTSTRAP_SERVERS','kafka:9092'),
                         'group.id':'fraud-monitoring-writer','auto.offset.reset':'earliest','enable.auto.commit':False})
    consumer.subscribe([os.getenv('KAFKA_SCORING_TOPIC','scoring')])
    conn = psycopg2.connect(**get_db_config())
    create_table(conn)
    try:
        while running:
            msg = consumer.poll(1)
            if msg is None: continue
            if msg.error(): raise RuntimeError(str(msg.error()))
            try:
                payload = json.loads(msg.value())
                items = payload if isinstance(payload,list) else [payload]
                if not items or any(not isinstance(d,dict) or not all(k in d for k in ('transaction_id','score','fraud_flag')) for d in items):
                    raise ValueError('Invalid scoring payload')
            except (ValueError, TypeError):
                metrics.errors.labels('input').inc()
                logger.exception('Invalid payload at offset %s; skip', msg.offset())
                consumer.commit(message=msg,asynchronous=False)
                continue
            for item in items:
                # A database failure exits without committing this Kafka offset;
                # restart reconnects and replays, unique transaction_id makes it safe.
                insert_score(conn,item,metrics)
            consumer.commit(message=msg,asynchronous=False)
    finally:
        consumer.close()
        conn.close()


if __name__ == '__main__':
    run_consumer()
