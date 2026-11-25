"""Общие пути и параметры DAG'ов переобучения, поставки и проверки качества данных."""

import os
from pathlib import Path

TRAINING_DIR = Path(os.getenv("TRAINING_DATA_DIR", "/opt/airflow/training_data"))
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/opt/airflow/models"))
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "/opt/airflow/reports"))
FRAUD_SRC = os.getenv("FRAUD_SRC", "/opt/airflow/fraud_src")

WINDOWS_DIR = TRAINING_DIR / "windows"
SPLITS_DIR = TRAINING_DIR / "splits"
SUPPLY_DIR = TRAINING_DIR / "supply"

LABEL = "target"

# Окно обучения: WINDOW_MONTHS - 1 ранних месяцев в train, самый свежий — в test
WINDOW_MONTHS = 4
# Поставка: столько строк в батче нового месяца, столько лет истории хранит витрина
SAMPLE_SIZE = 50_000
HISTORY_YEARS = 2

# Год и месяц в скользящем окне бесполезны: в test-месяце их значение всегда новое,
# а в потоке transaction_time — текущая дата. Модель учится без них.
DROPPED_FEATURES = ["year", "month", "year_mean_enc", "month_mean_enc"]

# Гейт публикации: новая модель выходит в прод, если её PR-AUC на свежем месяце
# не ниже PR-AUC текущей прод-модели на том же месяце минус допуск
GATE_METRIC = "pr_auc"
GATE_TOLERANCE = 0.0
FRAUD_THRESHOLD = 0.5
