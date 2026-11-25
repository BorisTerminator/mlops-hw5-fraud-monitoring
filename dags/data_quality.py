"""DAG проверки качества данных витрины.

Файл только связывает задачи. Логика каждой задачи лежит в pipeline.dq.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.dq.check_critical import check_critical
from pipeline.dq.evaluate_rules import evaluate_rules
from pipeline.dq.profile_showcase import profile_showcase
from pipeline.dq.render_report import render_report


@dag(
    dag_id="data_quality",
    description="Раз в 10 минут проверяет витрину на 10 правил DQ и пишет HTML-отчёт в /reports",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "dq"],
    doc_md="""
### Проверка качества данных

1. `profile_showcase` — SQL-агрегаты витрины `labeled_transactions`.
2. `evaluate_rules` — 10 правил DQ (список в README).
3. `render_report` — `reports/dq_report_<время>.html` и `reports/latest.html`.
4. `check_critical` — прогон красный, если не прошло critical-правило.
    """,
)
def data_quality():
    """Профиль → правила → HTML-отчёт → вердикт."""
    profile = task(profile_showcase, task_id="profile_showcase")()
    results = task(evaluate_rules, task_id="evaluate_rules")(profile)
    report = task(render_report, task_id="render_report")(results)
    task(check_critical, task_id="check_critical")(report)


data_quality()
