"""Гейт качества: сравнение кандидата с текущей прод-моделью."""

import logging

import pandas as pd

from pipeline.retrain.metrics import evaluate
from pipeline.settings import GATE_METRIC, GATE_TOLERANCE, MODELS_DIR

logger = logging.getLogger(__name__)

# Модель из образа сервиса: обучена в ДЗ-1 на всём train.csv, включая будущие test-месяцы окна
LEAKY_VERSIONS = {"initial"}


def compare_with_production(train_info: dict) -> dict:
    """Решает, выпускать ли кандидата в прод.

    Читает: latest.txt, модель и preprocessing_<версия>.json текущей прод-версии,
    test_raw.parquet свежего месяца. Ничего не пишет, только возвращает решение.

    Прод-модель скорит тот же test-месяц, что и кандидат, своими статистиками.
    Кандидат проходит, если его GATE_METRIC не ниже, чем у прода, минус GATE_TOLERANCE.
    Версия initial видела все месяцы train.csv при обучении, её метрика завышена
    утечкой, поэтому против неё гейт не применяется: первая модель DAG публикуется сразу.
    Задача без побочных эффектов, повтор даёт то же решение.
    """
    from catboost import CatBoostClassifier

    candidate = train_info["metrics"]
    prod_version = (MODELS_DIR / "latest.txt").read_text(encoding="utf-8").strip()

    if prod_version in LEAKY_VERSIONS:
        decision = {"approved": True, "reason": f"прод-версия {prod_version} обучена с утечкой, сравнение не показательно"}
        prod_metrics = None
    else:
        model = CatBoostClassifier()
        model.load_model(str(MODELS_DIR / f"my_catboost_{prod_version}.cbm"))
        test_raw = pd.read_parquet(train_info["test_raw_path"])
        prod_metrics = evaluate(model, str(MODELS_DIR / f"preprocessing_{prod_version}.json"), test_raw)
        new, old = candidate[GATE_METRIC], prod_metrics[GATE_METRIC]
        if new is None or old is None:
            decision = {"approved": False, "reason": f"{GATE_METRIC} не определён: в test один класс"}
        elif new >= old - GATE_TOLERANCE:
            decision = {"approved": True, "reason": f"{GATE_METRIC} {new:.4f} >= прод {old:.4f}"}
        else:
            decision = {"approved": False, "reason": f"{GATE_METRIC} {new:.4f} < прод {old:.4f}"}

    logger.info("Прод %s: %s. Кандидат: %s. Решение: %s", prod_version, prod_metrics, candidate, decision)
    return {**train_info, "prod_version": prod_version, "prod_metrics": prod_metrics, **decision}
