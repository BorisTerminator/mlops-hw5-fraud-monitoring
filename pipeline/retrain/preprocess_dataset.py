"""Препроцессинг окна тем же кодом, что и онлайн-скоринг."""

import json
import logging
import sys
from pathlib import Path

import pandas as pd

from pipeline.settings import FRAUD_SRC, LABEL

logger = logging.getLogger(__name__)


def preprocess_dataset(split_info: dict) -> dict:
    """Считает статистики препроцессинга по train и применяет их к train и test.

    Читает: сырые train_raw/test_raw из split_info.
    Пишет рядом с ними: preprocessing.json (статистики), train_proc.parquet, test_proc.parquet.

    Коды категорий, mean target encoding и средние для пропусков считаются
    только по train-месяцам, иначе target свежего месяца утёк бы в признаки
    и метрика на test была бы завышена. Код берётся из fraud_detector/src,
    поэтому сервис скоринга строит признаки ровно так же.
    Выход зависит только от входных файлов, повтор перезапишет те же файлы.
    """
    if FRAUD_SRC not in sys.path:
        sys.path.insert(0, FRAUD_SRC)
    from preprocessing import Preprocessor, fit_artifacts

    train_raw = pd.read_parquet(split_info["train_raw_path"])
    test_raw = pd.read_parquet(split_info["test_raw_path"])
    artifacts = fit_artifacts(train_raw)
    preprocessor = Preprocessor(artifacts)

    out_dir = Path(split_info["train_raw_path"]).parent
    train = preprocessor.transform(train_raw)
    train[LABEL] = train_raw[LABEL].to_numpy()
    test = preprocessor.transform(test_raw)
    test[LABEL] = test_raw[LABEL].to_numpy()

    artifacts_path = out_dir / "preprocessing.json"
    artifacts_path.write_text(json.dumps(artifacts, ensure_ascii=False), encoding="utf-8")
    train_path = out_dir / "train_proc.parquet"
    test_path = out_dir / "test_proc.parquet"
    train.to_parquet(train_path, index=False)
    test.to_parquet(test_path, index=False)
    logger.info("После препроцессинга: train %s, test %s", train.shape, test.shape)
    return {
        **split_info,
        "artifacts_path": str(artifacts_path),
        "train_proc_path": str(train_path),
        "test_proc_path": str(test_path),
    }
