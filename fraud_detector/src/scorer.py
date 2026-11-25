"""Скоринг версией модели из общего каталога с подхватом новой версии без перезапуска.

Раскладка каталога моделей (его же пишет DAG retrain_catboost):
    latest.txt                    — id текущей версии
    my_catboost_<версия>.cbm      — бинарник CatBoost
    preprocessing_<версия>.json   — статистики препроцессинга, на которых училась модель
"""
import logging
import os
import shutil
from pathlib import Path

from catboost import CatBoostClassifier

from preprocessing import Preprocessor

logger = logging.getLogger(__name__)

MODELS_DIR = Path(os.getenv('MODELS_DIR', '/models'))
BUNDLED_MODEL = Path(os.getenv('BUNDLED_MODEL_PATH', '/app/models/my_catboost.cbm'))
BUNDLED_ARTIFACTS = Path(os.getenv('BUNDLED_ARTIFACTS_PATH', '/app/artifacts/preprocessing.json'))
THRESHOLD = float(os.getenv('MODEL_THRESHOLD', '0.5'))
INITIAL_VERSION = 'initial'


def model_path(version):
    return MODELS_DIR / f'my_catboost_{version}.cbm'


def artifacts_path(version):
    return MODELS_DIR / f'preprocessing_{version}.json'


def ensure_bootstrap():
    """Пока DAG ничего не опубликовал, кладёт в общий каталог модель из образа как версию initial."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    latest = MODELS_DIR / 'latest.txt'
    if latest.exists() and latest.read_text(encoding='utf-8').strip():
        return
    shutil.copyfile(BUNDLED_MODEL, model_path(INITIAL_VERSION))
    shutil.copyfile(BUNDLED_ARTIFACTS, artifacts_path(INITIAL_VERSION))
    latest.write_text(INITIAL_VERSION + '\n', encoding='utf-8')
    logger.info('Bootstrap: модель из образа опубликована как %s', INITIAL_VERSION)


class VersionedScorer:
    def __init__(self):
        self.version = None
        self.model = None
        self.preprocessor = None

    def refresh(self):
        """Сверяет версию с latest.txt и при смене загружает модель и препроцессинг."""
        version = (MODELS_DIR / 'latest.txt').read_text(encoding='utf-8').strip()
        if version == self.version:
            return
        model = CatBoostClassifier()
        model.load_model(str(model_path(version)))
        self.preprocessor = Preprocessor(str(artifacts_path(version)))
        self.model = model
        self.version = version
        logger.info('Загружена модель версии %s: %d признаков', version, len(model.feature_names_))

    def predict(self, raw_df):
        """Сырые транзакции -> (скоры, флаги, версия модели)."""
        features = self.preprocessor.transform(raw_df)
        names = self.model.feature_names_
        df = features[names].copy()
        for idx in self.model.get_cat_feature_indices():
            df[names[idx]] = df[names[idx]].astype(str)
        scores = self.model.predict_proba(df)[:, 1]
        flags = (scores > THRESHOLD).astype(int)
        return scores, flags, self.version
