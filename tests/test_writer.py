import os
import uuid
import psycopg2
import pytest
from prometheus_client import CollectorRegistry
from scoring_writer import app


@pytest.fixture
def connection():
    conn = psycopg2.connect(host=os.getenv('TEST_POSTGRES_HOST','127.0.0.1'), port=int(os.getenv('TEST_POSTGRES_PORT','5433')), dbname='fraud_db', user='fraud_user', password='fraud_pass')
    app.create_table(conn)
    yield conn
    conn.rollback()
    conn.close()


def test_duplicate_delivery_is_one_committed_row_and_one_metric(connection):
    from scoring_writer.writer_metrics import WriterMetrics
    registry = CollectorRegistry()
    metrics = WriterMetrics(registry)
    tid = 'test-' + str(uuid.uuid4())
    data = dict(transaction_id=tid, score=.8, fraud_flag=1, model_version='test',
                us_state='CA', merch="O'Brien", cat_id='grocery', amount=12.5,
                scored_at='2026-11-25T12:00:00+00:00')
    try:
        assert app.insert_score(connection, data, metrics) is True
        assert app.insert_score(connection, data, metrics) is False
        with connection.cursor() as cur:
            cur.execute('SELECT us_state,merch,cat_id,amount FROM scores WHERE transaction_id=%s', (tid,))
            assert cur.fetchall() == [('CA', "O'Brien", 'grocery', 12.5)]
        assert registry.get_sample_value('fraud_written_total') == 1
        assert registry.get_sample_value('fraud_written_flagged_total') == 1
    finally:
        with connection.cursor() as cur: cur.execute('DELETE FROM scores WHERE transaction_id=%s',(tid,))
        connection.commit()


def test_failed_insert_rolls_back_and_does_not_count_success(connection):
    from scoring_writer.writer_metrics import WriterMetrics
    registry = CollectorRegistry()
    metrics = WriterMetrics(registry)
    with connection.cursor() as cur:
        cur.execute('CREATE TEMP TABLE scores (LIKE public.scores INCLUDING ALL)')
        cur.execute('ALTER TABLE scores ADD CHECK (score < 0.9)')
    connection.commit()
    with pytest.raises(psycopg2.Error):
        app.insert_score(connection, dict(transaction_id='test-valid-but-db-rejects',score=.95,fraud_flag=0), metrics)
    assert registry.get_sample_value('fraud_written_total') == 0
    with connection.cursor() as cur:
        cur.execute('SELECT 1')
        assert cur.fetchone() == (1,)


@pytest.mark.parametrize('field,value',[('transaction_id',None),('score',float('nan')),('score',1.1),
                                      ('fraud_flag',2),('amount',float('inf')),('emitted_at','bad-date')])
def test_writer_rejects_permanently_invalid_items_before_database(connection,field,value):
    data=dict(transaction_id='test-input-invalid',score=.3,fraud_flag=0)
    data[field]=value
    with pytest.raises(ValueError):
        app.insert_score(connection,data)


def test_writer_validates_entire_batch_before_returning_items():
    import json
    with pytest.raises(ValueError):
        app.parse_payload(json.dumps([dict(transaction_id='one',score=.3,fraud_flag=0),
                                      dict(transaction_id='two',score='NaN',fraud_flag=0)]))


def test_event_times_are_timezone_aware_after_schema_migration(connection):
    with connection.cursor() as cur:
        cur.execute("SELECT data_type FROM information_schema.columns WHERE table_name='scores' AND column_name='created_at'")
        assert cur.fetchone()==('timestamp with time zone',)
