"""Обучение CatBoost на train-месяцах окна."""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pipeline.retrain.metrics import evaluate
from pipeline.settings import DROPPED_FEATURES, FRAUD_SRC, LABEL

logger = logging.getLogger(__name__)


def fit_catboost(preproc_info: dict) -> dict:
    """Обучает кандидата и считает его метрики на свежем месяце.

    Читает: train_proc.parquet, test_raw.parquet и preprocessing.json из preproc_info.
    Пишет: candidate.cbm рядом со сплитом. В общий каталог моделей ничего не кладёт,
    поэтому падение или повтор этой задачи не может задеть прод.

    Test-месяц не используется ни для ранней остановки, ни для подбора параметров:
    его метрика — честная оценка на будущем, и по ней гейт сравнивает с продом.
    Версия = месяц батча + время обучения, по ней видно, на каком окне училась модель.
    """
    from catboost import CatBoostClassifier

    if FRAUD_SRC not in sys.path:
        sys.path.insert(0, FRAUD_SRC)
    from preprocessing import CAT_FEATURES

    train = pd.read_parquet(preproc_info["train_proc_path"])
    x_train = train.drop(columns=[LABEL] + DROPPED_FEATURES)
    cat_features = [c for c in CAT_FEATURES if c in x_train.columns]
    for column in cat_features:
        x_train[column] = x_train[column].astype(str)

    model = CatBoostClassifier(
        iterations=300,
        depth=6,
        learning_rate=0.08,
        loss_function="Logloss",
        auto_class_weights="SqrtBalanced",
        random_seed=42,
        thread_count=2,
        allow_writing_files=False,
        verbose=50,
    )
    model.fit(x_train, train[LABEL], cat_features=cat_features)

    test_raw = pd.read_parquet(preproc_info["test_raw_path"])
    metrics = evaluate(model, preproc_info["artifacts_path"], test_raw)
    logger.info("Кандидат на test %s: %s", preproc_info["test_month"], metrics)

    month = preproc_info["batch_month"][:7].replace("-", "")
    version = f"{month}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    candidate_path = Path(preproc_info["train_proc_path"]).parent / "candidate.cbm"
    model.save_model(str(candidate_path))
    return {
        **preproc_info,
        "version": version,
        "candidate_path": str(candidate_path),
        "train_rows": int(len(x_train)),
        "metrics": metrics,
    }
