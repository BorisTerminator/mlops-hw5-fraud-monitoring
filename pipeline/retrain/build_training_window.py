"""Сборка скользящего окна обучения по указателю месяца."""

import logging

import pandas as pd
from airflow.exceptions import AirflowSkipException
from dateutil.relativedelta import relativedelta

from pipeline.db import LABELED_COLUMNS, get_conn, month_end_exclusive, read_training_state
from pipeline.settings import WINDOW_MONTHS, WINDOWS_DIR

logger = logging.getLogger(__name__)


def build_training_window() -> dict:
    """Выгружает из витрины окно из WINDOW_MONTHS месяцев, которое заканчивается новым месяцем.

    Читает: next_batch_month из training_state и строки labeled_transactions
    за [next_batch_month - (WINDOW_MONTHS - 1); next_batch_month].
    Пишет: windows/window_<YYYYMM>.parquet.

    По сравнению с прошлым окном добавляется next_batch_month, а самый старый
    месяц выпадает. Окно целиком вычисляется из указателя, а указатель сдвигает
    только publish_model, поэтому повторный запуск после любой ошибки соберёт
    то же самое окно и перезапишет тот же файл: месяц не пропускается и окно
    не сдвигается дважды.
    Если нового месяца в витрине ещё нет, задача и весь прогон помечаются skipped.
    """
    WINDOWS_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    try:
        state = read_training_state(conn)
        batch_month = state["next_batch_month"]
        window_start = batch_month - relativedelta(months=WINDOW_MONTHS - 1)
        columns = ", ".join(LABELED_COLUMNS)
        frame = pd.read_sql(
            f"""
            SELECT {columns}
            FROM labeled_transactions
            WHERE transaction_time >= %s AND transaction_time < %s
            ORDER BY transaction_time
            """,
            conn,
            params=(window_start, month_end_exclusive(batch_month)),
        )
    finally:
        conn.close()

    months = pd.to_datetime(frame["transaction_time"]).dt.to_period("M")
    if (months == pd.Period(batch_month, "M")).sum() == 0:
        raise AirflowSkipException(f"В витрине нет наблюдений за {batch_month}: новый месяц ещё не поставлен.")

    path = WINDOWS_DIR / f"window_{batch_month:%Y%m}.parquet"
    frame.to_parquet(path, index=False)
    logger.info(
        "Окно %s — %s: %s строк, по месяцам %s. Выпал месяц %s.",
        window_start,
        batch_month,
        len(frame),
        months.value_counts().sort_index().to_dict(),
        window_start - relativedelta(months=1),
    )
    return {"window_path": str(path), "batch_month": batch_month.isoformat()}
