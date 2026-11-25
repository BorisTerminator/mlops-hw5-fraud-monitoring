import sys
from pathlib import Path
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import verify_stack


def test_scoring_growth_cannot_hide_stalled_commits():
    with pytest.raises(AssertionError,match='written'):
        verify_stack.assert_progress({'scored':10,'written':10,'rows':10},
                                    {'scored':40,'written':10,'rows':10})


def test_metrics_growth_cannot_hide_missing_persisted_rows():
    with pytest.raises(AssertionError,match='rows'):
        verify_stack.assert_progress({'scored':10,'written':10,'rows':10},
                                    {'scored':40,'written':40,'rows':10})


def test_unrelated_or_unhealthy_alerts_do_not_pass_acceptance():
    with pytest.raises(AssertionError):
        verify_stack.assert_alerts([{'uid':'other'},{'uid':'unrelated'}],{})
    rules=[{'uid':'fraud-stream-stalled','title':'Fraud · stream stalled'},
           {'uid':'fraud-high-flag-ratio','title':'Fraud · high flagged fraction'}]
    with pytest.raises(AssertionError,match='health'):
        verify_stack.assert_alerts(rules,{'Fraud · stream stalled':{'health':'error'},
                                        'Fraud · high flagged fraction':{'health':'ok'}})
