import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from prometheus_client import CollectorRegistry

import scorer as scorer_module
from scorer import VersionedScorer
from service import parse, score_batch


class FixedScorer:
    def predict(self, frame):
        return np.array([0.8]), np.array([1]), 'v-test'


def test_score_preserves_business_context():
    rows = [{'us_state': 'CA', 'merch': "O'Brien", 'cat_id': 'grocery',
             'amount': 12.5, 'transaction_time': '2026-11-25 12:00:00',
             'emitted_at': '2026-11-25T12:00:01+00:00'}]
    out = score_batch(FixedScorer(), ['txn-1'], rows)[0]
    assert out['us_state'] == 'CA'
    assert out['merch'] == "O'Brien"
    assert out['cat_id'] == 'grocery'
    assert out['amount'] == 12.5
    assert out['emitted_at'] == '2026-11-25T12:00:01+00:00'
    assert out['model_version'] == 'v-test'
    assert out['fraud_flag'] == 1


def test_malformed_message_does_not_desynchronise_ids_and_rows():
    class Message:
        def __init__(self, value): self.raw = value
        def value(self): return json.dumps(self.raw).encode()
        def error(self): return None
        def offset(self): return 1
    ids, rows = parse([Message({'transaction_id': 'bad'}),
                       Message({'transaction_id': 'ok', 'data': {'amount': 3}})])
    assert ids == ['ok']
    assert rows == [{'amount': 3}]


def test_success_metrics_include_age_and_window_counts():
    from metrics import ScoringMetrics
    registry = CollectorRegistry()
    m = ScoringMetrics(registry)
    m.observe({'score': 0.8, 'fraud_flag': 1, 'emitted_at': '2020-01-01T00:00:00+00:00'}, 0.02)
    assert registry.get_sample_value('fraud_scored_total') == 1
    assert registry.get_sample_value('fraud_flagged_total') == 1
    assert registry.get_sample_value('fraud_event_age_seconds_count') == 1
    assert registry.get_sample_value('fraud_processing_seconds_sum') == pytest.approx(0.02)


def test_failed_reload_preserves_previous_model_and_preprocessor(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[1]
    monkeypatch.setattr(scorer_module, 'MODELS_DIR', tmp_path)
    shutil.copy(root / 'fraud_detector/models/my_catboost.cbm', tmp_path / 'my_catboost_good.cbm')
    shutil.copy(root / 'fraud_detector/artifacts/preprocessing.json', tmp_path / 'preprocessing_good.json')
    (tmp_path / 'latest.txt').write_text('good')
    s = VersionedScorer()
    s.refresh()
    old_model, old_pre = s.model, s.preprocessor
    (tmp_path / 'my_catboost_bad.cbm').write_text('broken model')
    (tmp_path / 'latest.txt').write_text('bad')
    assert s.refresh() is False
    assert s.model is old_model
    assert s.preprocessor is old_pre
    assert s.version == 'good'


@pytest.mark.parametrize('bad', [123, 'not-a-timestamp'])
def test_bad_emission_time_is_rejected_before_scoring(bad):
    from metrics import ScoringMetrics
    registry=CollectorRegistry()
    m=ScoringMetrics(registry)
    class Message:
        def value(self): return json.dumps({'transaction_id':'bad','data':{'amount':5,'emitted_at':bad}}).encode()
        def error(self): return None
        def offset(self): return 1
    assert parse([Message()],m)==([],[])
    assert registry.get_sample_value('fraud_errors_total',{'stage':'input'})==1


@pytest.mark.parametrize('field,value', [('transaction_id',None),('amount',float('inf')),('amount',-2)])
def test_invalid_id_or_amount_is_rejected(field,value):
    payload={'transaction_id':'id-1','data':{'amount':5}}
    if field=='transaction_id':payload[field]=value
    else:payload['data'][field]=value
    class Message:
        def value(self):return json.dumps(payload).encode()
        def error(self):return None
        def offset(self):return 1
    assert parse([Message()])==([],[])


def test_metrics_cannot_crash_on_non_string_emission_time():
    from metrics import ScoringMetrics
    registry=CollectorRegistry();m=ScoringMetrics(registry)
    m.observe({'score':.4,'fraud_flag':0,'emitted_at':123},.01)
    assert registry.get_sample_value('fraud_errors_total',{'stage':'input'})==1


def bootstrap_good(tmp_path,monkeypatch):
    root=Path(__file__).resolve().parents[1]
    monkeypatch.setattr(scorer_module,'MODELS_DIR',tmp_path)
    shutil.copy(root/'fraud_detector/models/my_catboost.cbm',tmp_path/'my_catboost_good.cbm')
    shutil.copy(root/'fraud_detector/artifacts/preprocessing.json',tmp_path/'preprocessing_good.json')
    (tmp_path/'latest.txt').write_text('good')
    s=VersionedScorer();s.refresh()
    return s


def test_missing_latest_pointer_preserves_working_pair(tmp_path,monkeypatch):
    s=bootstrap_good(tmp_path,monkeypatch)
    old=s.model
    (tmp_path/'latest.txt').unlink()
    assert s.refresh() is False
    assert s.model is old and s.version=='good'
    assert s.last_reload_error


def test_parseable_but_incomplete_preprocessor_is_not_activated(tmp_path,monkeypatch):
    s=bootstrap_good(tmp_path,monkeypatch)
    old=s.model
    shutil.copy(tmp_path/'my_catboost_good.cbm',tmp_path/'my_catboost_bad.cbm')
    (tmp_path/'preprocessing_bad.json').write_text(json.dumps({'cat_maps':{},'mean_enc':{},'impute_means':{}}))
    (tmp_path/'latest.txt').write_text('bad')
    assert s.refresh() is False
    assert s.model is old and s.version=='good'


def test_synchronous_publication_failure_is_counted_without_success():
    import service
    from metrics import ScoringMetrics
    registry=CollectorRegistry();m=ScoringMetrics(registry)
    class FullQueue:
        def produce(self,*args,**kwargs):raise BufferError('Kafka queue is full')
    with pytest.raises(BufferError):
        service.publish_score(FullQueue(),{'transaction_id':'id-1','score':.3},m)
    assert registry.get_sample_value('fraud_errors_total',{'stage':'publish'})==1
    assert registry.get_sample_value('fraud_scored_total')==0
