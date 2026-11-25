"""Скоринг версией модели из общего каталога с подхватом новой версии без перезапуска.

Раскладка каталога моделей (его же пишет DAG retrain_catboost):
    latest.txt                    — id текущей версии
    my_catboost_<версия>.cbm      — бинарник CatBoost
    preprocessing_<версия>.json   — статистики препроцессинга, на которых училась модель
"""
import logging
import os
import shutil
import math
from pathlib import Path

from catboost import CatBoostClassifier
import pandas as pd

from preprocessing import CATEGORICAL_COLS, Preprocessor

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
        self.last_reload_error = None

    def refresh(self):
        """Сверяет версию с latest.txt и при смене загружает модель и препроцессинг."""
        self.last_reload_error=None
        try:
            version = (MODELS_DIR / 'latest.txt').read_text(encoding='utf-8').strip()
            if not version:raise ValueError('latest.txt is empty')
            if version == self.version:return False
            model = CatBoostClassifier(thread_count=2)
            model.load_model(str(model_path(version)))
            preprocessor = Preprocessor(str(artifacts_path(version)))
            # A readable JSON is not proof the pair can score. Smoke-test all
            # generated feature names and a real CPU prediction before activation.
            probe={c:'__validation_unknown__' for c in CATEGORICAL_COLS}
            probe.update(lat=0.,lon=0.,merchant_lat=1.,merchant_lon=1.,amount=1.,
                         population_city=100,transaction_time='2026-01-01 12:00:00')
            features=preprocessor.transform(pd.DataFrame([probe]))[model.feature_names_]
            for idx in model.get_cat_feature_indices():
                name=model.feature_names_[idx]
                features[name]=features[name].astype(str)
            prediction=float(model.predict_proba(features,thread_count=2)[0,1])
            if not math.isfinite(prediction) or not 0<=prediction<=1:
                raise ValueError('Candidate returned an invalid probability')
        except Exception as exc:
            self.last_reload_error=str(exc)
            if self.model is None:
                raise
            logger.exception('Не удалось обновить модель; работаем на %s', self.version)
            return False
        self.model, self.preprocessor, self.version = model, preprocessor, version
        logger.info('Загружена модель версии %s: %d признаков', version, len(model.feature_names_))
        return True

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
