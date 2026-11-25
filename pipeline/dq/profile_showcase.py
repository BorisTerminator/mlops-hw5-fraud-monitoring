"""Сбор агрегатов витрины, по которым проверяются правила DQ."""

import logging

from pipeline.db import LABELED_COLUMNS, get_conn

logger = logging.getLogger(__name__)

CRITICAL_COLUMNS = ["transaction_time", "amount", "target", "lat", "lon", "merchant_lat", "merchant_lon"]


def profile_showcase() -> dict:
    """Считает профиль labeled_transactions одним проходом SQL-запросов.

    Читает: витрину labeled_transactions и information_schema.
    Пишет: ничего, профиль уходит следующей задаче через XCom.
    Задача только читает, поэтому её можно повторять сколько угодно.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'labeled_transactions'"
            )
            columns = [row[0] for row in cur.fetchall()]

            nulls_sql = ", ".join(f"count(*) - count({c})" for c in LABELED_COLUMNS if c in columns)
            cur.execute(f"SELECT count(*), {nulls_sql} FROM labeled_transactions")
            row = cur.fetchone()
            rows = row[0]
            nulls = dict(zip([c for c in LABELED_COLUMNS if c in columns], row[1:]))

            cur.execute(
                """
                SELECT
                    array_agg(DISTINCT target),
                    array_agg(DISTINCT gender),
                    min(amount), max(amount),
                    count(*) FILTER (WHERE amount <= 0 OR amount > 100000),
                    count(*) FILTER (WHERE lat NOT BETWEEN -90 AND 90 OR lon NOT BETWEEN -180 AND 180
                        OR merchant_lat NOT BETWEEN -90 AND 90 OR merchant_lon NOT BETWEEN -180 AND 180)
                FROM labeled_transactions
                """
            )
            targets, genders, amount_min, amount_max, amount_bad, geo_bad = cur.fetchone()

            all_cols = ", ".join(LABELED_COLUMNS)
            cur.execute(
                f"""
                SELECT coalesce(sum(n - 1), 0) FROM (
                    SELECT count(*) AS n FROM labeled_transactions GROUP BY {all_cols}
                ) groups WHERE n > 1
                """
            )
            duplicates = cur.fetchone()[0]

            cur.execute(
                """
                SELECT to_char(date_trunc('month', transaction_time), 'YYYY-MM'), count(*), sum(target)
                FROM labeled_transactions
                GROUP BY 1 ORDER BY 1
                """
            )
            months = [{"month": m, "rows": int(n), "frauds": int(f or 0)} for m, n, f in cur.fetchall()]
    finally:
        conn.close()

    profile = {
        "columns": columns,
        "rows": int(rows),
        "nulls": {c: int(v) for c, v in nulls.items()},
        "targets": sorted(t for t in targets if t is not None) if targets else [],
        "genders": sorted(g for g in genders if g is not None) if genders else [],
        "amount_min": float(amount_min) if amount_min is not None else None,
        "amount_max": float(amount_max) if amount_max is not None else None,
        "amount_out_of_range": int(amount_bad),
        "geo_out_of_range": int(geo_bad),
        "duplicates": int(duplicates),
        "months": months,
    }
    logger.info("Профиль витрины: %s строк, %s месяцев", profile["rows"], len(months))
    return profile
