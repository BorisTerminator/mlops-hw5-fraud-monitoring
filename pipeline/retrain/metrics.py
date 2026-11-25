"""Одинаковая оценка кандидата и прод-модели на сырых строках test-месяца."""

import sys

import pandas as pd

from pipeline.settings import FRAUD_SRC, FRAUD_THRESHOLD, LABEL


def evaluate(model, artifacts, test_raw: pd.DataFrame) -> dict:
    """Скорит test_raw моделью с её собственными статистиками препроцессинга.

    artifacts — словарь или путь к preprocessing_<версия>.json той же версии.
    Возвращает PR-AUC (главная метрика: фрода ~0.6%, ROC-AUC на таком дисбалансе
    почти всегда близок к 1), ROC-AUC и F1 при пороге сервиса.
    """
    from sklearn.metrics import average_precision_score, f1_score, roc_auc_score

    if FRAUD_SRC not in sys.path:
        sys.path.insert(0, FRAUD_SRC)
    from preprocessing import Preprocessor

    features = Preprocessor(artifacts).transform(test_raw)
    names = model.feature_names_
    frame = features[names].copy()
    for idx in model.get_cat_feature_indices():
        frame[names[idx]] = frame[names[idx]].astype(str)
    proba = model.predict_proba(frame)[:, 1]
    y_true = test_raw[LABEL].to_numpy()

    single_class = len(set(y_true)) < 2
    return {
        "pr_auc": None if single_class else float(average_precision_score(y_true, proba)),
        "roc_auc": None if single_class else float(roc_auc_score(y_true, proba)),
        "f1": float(f1_score(y_true, (proba > FRAUD_THRESHOLD).astype(int), zero_division=0)),
        "rows": int(len(y_true)),
        "frauds": int(y_true.sum()),
    }
