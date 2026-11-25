import os
from pathlib import Path
import psycopg2
from psycopg2 import sql
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def database():
    conn=psycopg2.connect(host=os.getenv('TEST_POSTGRES_HOST','127.0.0.1'),port=int(os.getenv('TEST_POSTGRES_PORT','5433')),dbname='fraud_db',user='fraud_user',password='fraud_pass')
    with conn.cursor() as cur:
        cur.execute('CREATE TEMP TABLE scores (LIKE public.scores INCLUDING DEFAULTS)')
        # The first 2 rows are outside the most recent global 1000.
        cur.execute('''INSERT INTO scores (id,transaction_id,score,fraud_flag,us_state,merch,cat_id,amount,scored_at)
                       SELECT i, 'fixture-'||i, CASE WHEN i%2=0 THEN .8 ELSE .2 END,
                              CASE WHEN i%2=0 THEN 1 ELSE 0 END,
                              CASE WHEN i%2=0 THEN 'CA' ELSE 'NY' END,
                              'O''Brien', CASE WHEN i<=2 THEN 'excluded' ELSE 'recent' END,
                              10, now() FROM generate_series(1,1002) i''')
    yield conn
    conn.rollback()
    conn.close()


def execute_query(conn,name,state='CA',merchant="O'Brien"):
    query=(ROOT/'grafana/queries'/f'{name}.sql').read_text()
    query=query.replace('$__timeFilter(created_at)','TRUE')
    query=query.replace("$__timeGroupAlias(created_at, '30s')", "to_timestamp(floor(extract(epoch from created_at)/30)*30) AS time")
    for key,value in [('us_state',state),('merch',merchant)]:
        query=query.replace('${'+key+':sqlstring}',sql.Literal(value).as_string(conn))
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def test_density_filters_merchant_with_apostrophe_and_integrates_to_one(database):
    rows=execute_query(database,'score_density')
    assert len(rows)==20
    assert sum(float(r[1])*.05 for r in rows)==pytest.approx(1)
    assert [r for r in rows if r[1]>0]==[('0.80',20.0)]


def test_empty_segment_has_zero_density(database):
    rows=execute_query(database,'score_density',state='XX')
    assert len(rows)==20
    assert all(r[1]==0 for r in rows)


def test_category_uses_global_latest_1000_before_segment_filter(database):
    assert execute_query(database,'category_fraud')==[('recent',1.0)]
    assert execute_query(database,'category_fraud',state='NY')==[('recent',0.0)]


def test_grafana_database_user_cannot_modify_data():
    with psycopg2.connect(host=os.getenv('TEST_POSTGRES_HOST','127.0.0.1'),port=int(os.getenv('TEST_POSTGRES_PORT','5433')),dbname='fraud_db',user='grafana_reader',password='grafana_reader') as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM scores')
            assert cur.fetchone()[0]>0
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute('DELETE FROM scores WHERE false')


def test_grafana_reader_cannot_read_raw_customer_details():
    with psycopg2.connect(host=os.getenv('TEST_POSTGRES_HOST','127.0.0.1'),port=int(os.getenv('TEST_POSTGRES_PORT','5433')),dbname='fraud_db',user='grafana_reader',password='grafana_reader') as conn:
        with conn.cursor() as cur:
            with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                cur.execute('SELECT name_1,street FROM labeled_transactions LIMIT 1')


def test_risky_amount_fraction_and_zero_turnover(database):
    assert execute_query(database,'risky_turnover')[0][3]==1.0
    assert execute_query(database,'risky_turnover',state='NY')[0][3]==0.0
    with database.cursor() as cur:cur.execute('UPDATE scores SET amount=0')
    assert execute_query(database,'risky_turnover')[0][3] is None
