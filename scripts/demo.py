"""Repeatable live experiments; restores normal traffic/settings in finally."""
import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from verify_stack import Stack, ROOT


def compose(*args, env=None, capture=False):
    return subprocess.run(['docker','compose',*args],cwd=ROOT,env=env,check=True,
                          stdout=subprocess.PIPE if capture else None,text=True,encoding='utf-8').stdout


def wait_alert(stack,name,state,history,timeout=120):
    until=time.monotonic()+timeout
    while time.monotonic()<until:
        alerts=stack.alerts()
        history.append({'utc_epoch':time.time(),'alerts':alerts})
        print(name,alerts[name],flush=True)
        if alerts[name]['state']==state and alerts[name]['health']=='ok':return
        time.sleep(10)
    raise TimeoutError(f'{name} did not become {state}')


def stall():
    s=Stack();history=[]
    try:
        compose('stop','transaction_simulator')
        wait_alert(s,'Fraud · stream stalled','firing',history)
    finally:
        compose('start','transaction_simulator')
    wait_alert(s,'Fraud · stream stalled','inactive',history)
    return history


def high_fraud():
    s=Stack();history=[]
    config=json.loads(compose('config','--format','json',capture=True))
    original=str(config['services']['fraud_detector']['environment']['MODEL_THRESHOLD'])
    env=os.environ.copy()
    def recreate(threshold):
        run_env=dict(env,MODEL_THRESHOLD=threshold)
        compose('up','-d','--no-deps','--force-recreate','fraud_detector',env=run_env)
    try:
        print('DEMO: threshold=0.0, all positive scores flagged; this changes the decision threshold, not model quality.',flush=True)
        recreate('0.0')
        wait_alert(s,'Fraud · high flagged fraction','firing',history,150)
    finally:
        print('Restore threshold='+original,flush=True)
        recreate(original)
    wait_alert(s,'Fraud · high flagged fraction','inactive',history,150)
    return history


def retrain():
    s=Stack()
    before=compose('exec','-T','fraud_detector','cat','/models/latest.txt',capture=True).strip()
    cid=compose('ps','-q','fraud_detector',capture=True).strip()
    started=subprocess.check_output(['docker','inspect',cid,'--format','{{.State.StartedAt}}'],text=True).strip()
    run_id='monitoring_demo_'+str(int(time.time()))
    compose('exec','-T','airflow','airflow','dags','unpause','retrain_catboost')
    compose('exec','-T','airflow','airflow','dags','trigger','retrain_catboost','-r',run_id)
    deadline=time.monotonic()+600
    while time.monotonic()<deadline:
        text=compose('exec','-T','airflow','airflow','dags','list-runs','-d','retrain_catboost','-o','json',capture=True)
        runs=json.loads(text)
        run=next((r for r in runs if r['run_id']==run_id),None)
        print('Retraining:',run['state'] if run else 'waiting',flush=True)
        if run and run['state']=='failed':raise RuntimeError('DAG failed; inspect Airflow task logs')
        if run and run['state']=='success':break
        time.sleep(10)
    else:raise TimeoutError('Retraining exceeded 10 minutes')
    after=compose('exec','-T','fraud_detector','cat','/models/latest.txt',capture=True).strip()
    same_start=subprocess.check_output(['docker','inspect',cid,'--format','{{.State.StartedAt}}'],text=True).strip()==started
    if after==before:
        raise RuntimeError('DAG completed but candidate was rejected by the quality gate. See models/*.metrics.json. This is expected behavior; no reload event should appear.')
    for _ in range(30):
        versions=s.sql_values('SELECT DISTINCT model_version FROM scores')
        if after in versions:break
        time.sleep(1)
    else:raise RuntimeError('New version not found in PostgreSQL')
    assert same_start,'Scorer unexpectedly restarted'
    return {'old_version':before,'new_version':after,'scorer_started_at':started,
            'same_container_start':same_start,'run_id':run_id,'versions_in_database':versions}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('experiment',choices=['stall','high-fraud','retrain'])
    args=parser.parse_args()
    result={'stall':stall,'high-fraud':high_fraud,'retrain':retrain}[args.experiment]()
    path=ROOT/'docs'/('demo-'+args.experiment+'.json')
    path.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('Verified, evidence:',path,flush=True)
