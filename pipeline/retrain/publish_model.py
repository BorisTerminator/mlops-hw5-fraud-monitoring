"""Сохранение модели в общий каталог, выкатка по решению гейта и сдвиг указателя месяца."""

import json
import logging
import os
import shutil
from datetime import date
from pathlib import Path

from pipeline.db import advance_training_state, get_conn
from pipeline.settings import MODELS_DIR

logger = logging.getLogger(__name__)


def atomic_copy(src: Path, dst: Path) -> None:
    """Копирует через временный файл: сервис не увидит недописанный файл."""
    tmp = dst.with_name(f".{dst.name}.tmp")
    shutil.copyfile(src, tmp)
    os.replace(tmp, dst)


def publish_model(gate_info: dict) -> str:
    """Кладёт модель в каталог моделей и, если гейт пропустил, делает её текущей.

    Читает: candidate.cbm и preprocessing.json кандидата, решение гейта.
    Пишет в MODELS_DIR: my_catboost_<версия>.cbm, preprocessing_<версия>.json,
    my_catboost_<версия>.metrics.json; при approved — latest.txt; затем training_state.

    Порядок важен. Сначала файлы модели, потом latest.txt: сервис никогда не увидит
    указатель на версию, которой ещё нет. Указатель месяца сдвигается последним,
    только после успешного сохранения модели: если что-то упало раньше, следующий
    прогон заново обучит тот же месяц. Сдвиг записывает абсолютные значения
    от batch_month, а не «+1 месяц», поэтому повтор задачи не сдвинет окно дважды.
    Отклонённая гейтом модель тоже сохраняется (для истории) и месяц тоже считается
    пройденным, иначе окно застряло бы на нём навсегда.
    """
    version = gate_info["version"]
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    atomic_copy(Path(gate_info["candidate_path"]), MODELS_DIR / f"my_catboost_{version}.cbm")
    atomic_copy(Path(gate_info["artifacts_path"]), MODELS_DIR / f"preprocessing_{version}.json")

    status = "published" if gate_info["approved"] else "rejected"
    report = {
        "version": version,
        "status": status,
        "reason": gate_info["reason"],
        "train_months": gate_info["train_months"],
        "test_month": gate_info["test_month"],
        "train_rows": gate_info["train_rows"],
        "metrics": gate_info["metrics"],
        "prod_version": gate_info["prod_version"],
        "prod_metrics": gate_info["prod_metrics"],
    }
    (MODELS_DIR / f"my_catboost_{version}.metrics.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    if gate_info["approved"]:
        latest_tmp = MODELS_DIR / ".latest.txt.tmp"
        latest_tmp.write_text(version + "\n", encoding="utf-8")
        os.replace(latest_tmp, MODELS_DIR / "latest.txt")
        logger.info("Модель %s опубликована: %s", version, gate_info["reason"])
    else:
        logger.warning("Модель %s НЕ выкачена, прод остаётся %s: %s",
                       version, gate_info["prod_version"], gate_info["reason"])

    conn = get_conn()
    try:
        advance_training_state(conn, date.fromisoformat(gate_info["batch_month"]))
    finally:
        conn.close()
    return f"{version}: {status}"
