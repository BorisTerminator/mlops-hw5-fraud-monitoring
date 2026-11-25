"""Временной train/test split окна."""

import logging

import pandas as pd

from pipeline.settings import SPLITS_DIR

logger = logging.getLogger(__name__)


def time_train_test_split(window_info: dict) -> dict:
    """Делит окно по времени: ранние месяцы в train, самый свежий месяц в test.

    Читает: parquet окна из window_info.
    Пишет: splits/<YYYYMM>/train_raw.parquet и test_raw.parquet.

    Случайный split здесь нельзя: модель в проде всегда скорит будущее
    относительно своих обучающих данных, и test должен это повторять.
    Файлы лежат в каталоге месяца и при повторе просто перезаписываются.
    """
    frame = pd.read_parquet(window_info["window_path"])
    frame["transaction_time"] = pd.to_datetime(frame["transaction_time"])
    periods = frame["transaction_time"].dt.to_period("M")
    latest = periods.max()
    train = frame.loc[periods < latest]
    test = frame.loc[periods == latest]
    if train.empty or test.empty:
        raise RuntimeError(f"Не удалось разрезать окно по времени: train={len(train)}, test={len(test)}")

    out_dir = SPLITS_DIR / pd.Period(latest).strftime("%Y%m")
    out_dir.mkdir(parents=True, exist_ok=True)
    train_path = out_dir / "train_raw.parquet"
    test_path = out_dir / "test_raw.parquet"
    train.to_parquet(train_path, index=False)
    test.to_parquet(test_path, index=False)

    train_months = sorted(periods.loc[periods < latest].unique().astype(str))
    logger.info("Train %s (%s строк), test %s (%s строк).", train_months, len(train), latest, len(test))
    return {
        "train_raw_path": str(train_path),
        "test_raw_path": str(test_path),
        "batch_month": window_info["batch_month"],
        "train_months": ",".join(train_months),
        "test_month": str(latest),
    }
