"""DAG скользящего переобучения CatBoost.

Файл только связывает задачи. Логика каждой задачи лежит в pipeline.retrain.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.retrain.build_training_window import build_training_window
from pipeline.retrain.compare_with_production import compare_with_production
from pipeline.retrain.fit_catboost import fit_catboost
from pipeline.retrain.preprocess_dataset import preprocess_dataset
from pipeline.retrain.publish_model import publish_model
from pipeline.retrain.time_train_test_split import time_train_test_split


@dag(
    dag_id="retrain_catboost",
    description="Раз в 5 минут сдвигает 4-месячное окно, переобучает CatBoost и выкатывает, если не хуже прода",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "catboost"],
    doc_md="""
### Переобучение my_catboost

1. `build_training_window` — окно из 4 месяцев, заканчивается месяцем `next_batch_month`.
2. `time_train_test_split` — 3 ранних месяца в train, свежий в test.
3. `preprocess_dataset` — статистики препроцессинга считаются только по train.
4. `fit_catboost` — обучение кандидата, метрики на test-месяце.
5. `compare_with_production` — гейт: PR-AUC кандидата против прод-модели на том же месяце.
6. `publish_model` — модель в `/models`, `latest.txt` только при успехе гейта, затем сдвиг указателя.
    """,
)
def retrain_catboost():
    """Окно → split → препроцессинг → обучение → гейт → публикация."""
    window = task(build_training_window, task_id="build_training_window")()
    split = task(time_train_test_split, task_id="time_train_test_split")(window)
    processed = task(preprocess_dataset, task_id="preprocess_dataset")(split)
    trained = task(fit_catboost, task_id="fit_catboost")(processed)
    gated = task(compare_with_production, task_id="compare_with_production")(trained)
    task(publish_model, task_id="publish_model")(gated)


retrain_catboost()
