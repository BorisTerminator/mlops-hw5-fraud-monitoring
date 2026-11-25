"""Acceptance check against the running stack. Requires requests, no Docker SDK.
Every dashboard query is executed through the same Grafana API used by its UI.
"""
import argparse
import json
import math
import os
import sys
import time
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[1]


class Stack:
    def __init__(self):
        self.session = requests.Session()
        self.session.trust_env = False
        self.grafana = os.getenv('GRAFANA_URL', 'http://127.0.0.1:3000')
        self.prometheus = os.getenv('PROMETHEUS_URL', 'http://127.0.0.1:9091')
        self.auth = (os.getenv('GRAFANA_USER','admin'), os.getenv('GRAFANA_PASSWORD','admin'))

    def get(self, path):
        r = self.session.get(self.grafana+path, auth=self.auth, timeout=20)
        r.raise_for_status()
        return r.json()

    def prom(self, expr):
        r = self.session.get(self.prometheus+'/api/v1/query', params={'query':expr}, timeout=20)
        r.raise_for_status()
        result = r.json()
        if result['status'] != 'success': raise RuntimeError(result)
        return result['data']['result']

    def query(self, query):
        now = int(time.time()*1000)
        r = self.session.post(self.grafana+'/api/ds/query',auth=self.auth,
                             json={'from':str(now-15*60*1000),'to':str(now),'queries':[dict(query,intervalMs=1000,maxDataPoints=900)]}, timeout=30)
        r.raise_for_status()
        body = r.json()['results'][query.get('refId','A')]
        if body.get('error'): raise RuntimeError(body['error'])
        return body.get('frames',[])

    def sql_values(self, sql):
        frames=self.query({'refId':'A','datasource':{'uid':'fraud-postgres','type':'grafana-postgresql-datasource'},'rawSql':sql,'format':'table'})
        return frames[0]['data']['values'][0] if frames else []

    def alerts(self):
        groups=self.get('/api/prometheus/grafana/api/v1/rules')['data']['groups']
        return {r['name']:{'state':r['state'],'health':r['health']} for g in groups for r in g['rules']}


def quote(values):
    return ','.join("'"+str(v).replace("'","''")+"'" for v in values) or "''"


def check():
    stack=Stack()
    evidence={'checked_at_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
    targets=stack.session.get(stack.prometheus+'/api/v1/targets',timeout=10).json()['data']['activeTargets']
    evidence['targets']={t['labels']['job']:t['health'] for t in targets}
    assert len(targets)==3 and all(t['health']=='up' for t in targets), evidence['targets']
    evidence['dashboards']=[d['uid'] for d in stack.get('/api/search?type=dash-db')]
    assert set(evidence['dashboards'])=={'fraud-scoring','fraud-storage','fraud-host','fraud-business'}
    for uid in ['fraud-prometheus','fraud-postgres']:
        health=stack.get('/api/datasources/uid/'+uid+'/health')
        assert health['status']=='OK',health
    before=float(stack.prom('sum(fraud_scored_total)')[0]['value'][1])
    time.sleep(3)
    after=float(stack.prom('sum(fraud_scored_total)')[0]['value'][1])
    assert after>before,(before,after)
    evidence['scoring_growth_3s']={'before':before,'after':after}
    replacements={name:quote(stack.sql_values(f'SELECT DISTINCT {name} FROM scores WHERE {name} IS NOT NULL ORDER BY 1')) for name in ['us_state','merch']}
    evidence['panels']=[]
    failures=[]
    for filename in sorted((ROOT/'grafana/dashboards').glob('*.json')):
        doc=json.loads(filename.read_text(encoding='utf-8'))
        for p in doc['panels']:
            for t in p.get('targets',[]):
                q=dict(t)
                if 'rawSql' in q:
                    for name,value in replacements.items():q['rawSql']=q['rawSql'].replace('${'+name+':sqlstring}',value)
                try:
                    frames=stack.query(q)
                    points=sum(len(v) for frame in frames for v in frame.get('data',{}).get('values',[]))
                    assert points>0,'Empty data'
                    # Prometheus may encode undefined quantiles as null/NaN.
                    numbers=[v for f in frames for field,vals in zip(f['schema']['fields'],f.get('data',{}).get('values',[])) if field['type']=='number' for v in vals]
                    if numbers: assert any(isinstance(v,(float,int)) and math.isfinite(v) for v in numbers),'Only null/NaN values'
                    evidence['panels'].append({'dashboard':doc['uid'],'panel':p['id'],'query':q['refId'],'points':points})
                except Exception as exc:
                    failures.append(f"{doc['uid']} / {p['title']} / {q.get('refId')}: {exc}")
    evidence['alerts']=stack.alerts()
    assert len(evidence['alerts'])>=2,evidence['alerts']
    if failures: raise AssertionError('\n'.join(failures))
    return evidence


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--save',type=Path)
    args=parser.parse_args()
    result=check()
    output=json.dumps(result,ensure_ascii=False,indent=2)
    if args.save:
        args.save.parent.mkdir(parents=True,exist_ok=True)
        args.save.write_text(output+'\n',encoding='utf-8')
    print(output)
