import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import demo


def test_manual_rejected_run_is_not_mistaken_for_other_publication():
    with pytest.raises(RuntimeError,match='rejected'):
        demo.published_version('success','201909_candidate: rejected')


def test_skipped_publish_is_not_reported_as_retrain_success():
    with pytest.raises(RuntimeError,match='skipped'):
        demo.published_version('skipped',None)


def test_approved_version_is_derived_from_its_task_result():
    assert demo.published_version('success','201909_candidate: published')=='201909_candidate'
