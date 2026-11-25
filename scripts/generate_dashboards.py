"""Generate our four dashboards and Grafana alert rules from explicit queries.
Run after editing: python scripts/generate_dashboards.py. No seminar JSON is used.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROM = {'type': 'prometheus', 'uid': 'fraud-prometheus'}
PG = {'type': 'grafana-postgresql-datasource', 'uid': 'fraud-postgres'}


def target(expr, legend='', ref='A'):
    return dict(refId=ref, expr=expr, legendFormat=legend, range=True, datasource=PROM)


def panel(pid, title, x, y, w=12, h=8, kind='timeseries', targets=None, unit='short', description=''):
    p = dict(id=pid, title=title, type=kind, gridPos=dict(x=x,y=y,w=w,h=h),
             datasource=PROM, targets=targets or [], description=description,
             fieldConfig={'defaults': {'unit':unit, 'color':{'mode':'palette-classic'},
                 'custom':{'drawStyle':'line','lineInterpolation':'smooth','lineWidth':2,'fillOpacity':10},
                 'thresholds':{'mode':'absolute','steps':[{'color':'green','value':None}]}},'overrides':[]},
             options={'legend':{'displayMode':'list','placement':'bottom'},'tooltip':{'mode':'multi'}})
    if kind=='stat':
        p['options']={'reduceOptions':{'calcs':['lastNotNull'],'fields':'','values':False},'colorMode':'value','graphMode':'area','textMode':'auto'}
    if kind=='barchart':
        p['options']={'orientation':'auto','barWidth':.8,'groupWidth':.7,'showValue':'auto','legend':{'displayMode':'list','placement':'bottom'},'tooltip':{'mode':'multi'}}
    if kind=='table': p['options']={'showHeader':True,'cellHeight':'sm'}
    return p


def sql_panel(pid,title,query,x,y,kind='timeseries',unit='short',description=''):
    p=panel(pid,title,x,y,kind=kind,unit=unit,description=description)
    p['datasource']=PG
    p['targets']=[{'refId':'A','datasource':PG,'rawSql':(ROOT/'grafana/queries'/f'{query}.sql').read_text(),
                   'format':'time_series' if kind=='timeseries' else 'table','rawQuery':True,'editorMode':'code'}]
    return p


def dashboard(uid,title,panels,variables=None,annotations=None):
    return dict(uid=uid,title=title,schemaVersion=39,version=1,tags=['mlops','fraud','hw-5'],
                timezone='browser',refresh='5s',time={'from':'now-15m','to':'now'},editable=False,
                graphTooltip=1,panels=panels,templating={'list':variables or []},
                annotations={'list':annotations or []},links=[{'type':'dashboards','title':'Fraud Lab','tags':['hw-5'],'asDropdown':True}])


def heatmap(pid,title,expr,x,y):
    p=panel(pid,title,x,y,kind='heatmap',targets=[target(expr,'{{le}}')])
    p['targets'][0]['format']='heatmap'
    p['options']={'calculate':False,'cellGap':1,'color':{'mode':'scheme','scheme':'Oranges','exponent':.5,'steps':64},
                  'yAxis':{'axisPlacement':'left','unit':'short','decimals':2},'legend':{'show':True},
                  'tooltip':{'mode':'single','yHistogram':True},'filterValues':{'le':1e-9}}
    return p


def make_scoring():
    panels=[
      panel(1,'Поток / TPS',0,0,6,4,'stat',[target('sum(rate(fraud_scored_total[30s]))')],'ops'),
      panel(2,'p95 инференса',6,0,6,4,'stat',[target('histogram_quantile(0.95, sum by(le)(rate(fraud_processing_seconds_bucket[1m])))')],'s'),
      panel(3,'Доля флагов модели / 1 мин',12,0,6,4,'stat',[target('sum(rate(fraud_flagged_total[1m])) / clamp_min(sum(rate(fraud_scored_total[1m])), 0.001)')],'percentunit'),
      panel(4,'Секунд без успешного скоринга',18,0,6,4,'stat',[target('time() - fraud_last_success_timestamp_seconds')],'s'),
      panel(5,'Скорость скоринга и записи',0,4,targets=[target('sum(rate(fraud_scored_total[30s]))','скоринг'),target('sum(rate(fraud_written_total[30s]))','PostgreSQL','B')],unit='ops'),
      panel(6,'Задержки одной транзакции',12,4,targets=[target('histogram_quantile(0.5,sum by(le)(rate(fraud_processing_seconds_bucket[1m])))','p50'),target('histogram_quantile(0.95,sum by(le)(rate(fraud_processing_seconds_bucket[1m])))','p95','B')],unit='s',description='Только preprocessing + inference; не время ожидания в Kafka.'),
      heatmap(7,'Распределение скоров во времени', 'sum by(le)(increase(fraud_score_bucket[10s]))',0,12),
      panel(8,'Возраст события / p95',12,12,targets=[target('histogram_quantile(0.95,sum by(le)(rate(fraud_event_age_seconds_bucket[1m])))','эмиссия → результат')],unit='s',description='Собственная метрика: обнаруживает очередь, даже когда inference быстрый. Часы producer и scorer должны быть синхронизированы.'),
      panel(9,'Ошибки по этапам',0,20,targets=[target('sum by(stage)(increase(fraud_errors_total[1m]))','{{stage}}')]),
      panel(10,'История загрузок моделей',12,20,kind='table',targets=[dict(target('fraud_model_version_loaded_timestamp_seconds * 1000','{{version}} · {{kind}}'),instant=True,range=False,format='table')],description='Время в миллисекундах для Grafana. kind=startup — запуск процесса; reload — горячее обновление. История версий — также в Storage / PostgreSQL.')]
    annotations=[{'name':'Model loaded (30 seconds)','datasource':PROM,'enable':True,'hide':False,
                  'iconColor':'#FFB357','expr':'(time() - fraud_model_version_loaded_timestamp_seconds) < 30',
                  'titleFormat':'Модель {{version}} · {{kind}}','textFormat':'Успешная загрузка {{version}} ({{kind}}: startup или reload)',
                  'step':'1s','tagKeys':'version'}]
    return dashboard('fraud-scoring','01 · Scoring — поток и модель',panels,annotations=annotations)


def make_storage():
    return dashboard('fraud-storage','02 · Storage — подтверждённые записи',[
      panel(1,'Записей / секунду',0,0,6,4,'stat',[target('sum(rate(fraud_written_total[30s]))')],'ops'),
      panel(2,'Флаги модели с запуска writer',6,0,6,4,'stat',[target('sum(fraud_written_flagged_total)')]),
      panel(3,'p95 записи + commit',12,0,6,4,'stat',[target('histogram_quantile(.95,sum by(le)(rate(fraud_db_write_seconds_bucket[1m])))')],'s'),
      panel(4,'Повторные доставки / всего',18,0,6,4,'stat',[target('sum(fraud_writer_duplicates_total)')]),
      panel(5,'Скорость записи',0,4,targets=[target('sum(rate(fraud_written_total[30s]))','commit/s')],unit='ops'),
      panel(6,'Задержки записи',12,4,targets=[target('histogram_quantile(.95,sum by(le)(rate(fraud_db_write_seconds_bucket[1m])))','p95')],unit='s'),
      heatmap(7,'Распределение записанных скоров','sum by(le)(increase(fraud_written_score_bucket[10s]))',0,12),
      panel(8,'Ошибки и повторы',12,12,targets=[target('sum by(stage)(increase(fraud_writer_errors_total[1m]))','{{stage}}'),target('sum(increase(fraud_writer_duplicates_total[1m]))','duplicate','B')]),
      sql_panel(9,'PostgreSQL: результаты по версии модели','model_summary',0,20,'table',description='Число скоров и доля флагов каждой версии в выбранном интервале. Это решения модели, не подтверждённый фрод.')])


def make_host():
    return dashboard('fraud-host','03 · Host — Linux Docker / WSL2',[
      panel(1,'CPU занят',0,0,6,4,'stat',[target('1 - avg(rate(node_cpu_seconds_total{mode="idle"}[1m]))')],'percentunit'),
      panel(2,'RAM занята',6,0,6,4,'stat',[target('1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes')],'percentunit'),
      panel(3,'Свободно / крупнейшая файловая система',12,0,6,4,'stat',[target('max(node_filesystem_avail_bytes{fstype!~"tmpfs|overlay|squashfs|rootfs|9p"})')],'bytes'),
      panel(4,'Экспортер доступен',18,0,6,4,'stat',[target('up{job="docker-host"}')]),
      panel(5,'CPU по режимам',0,4,targets=[target('avg by(mode)(rate(node_cpu_seconds_total{mode!="idle"}[1m]))','{{mode}}')],unit='percentunit'),
      panel(6,'Память',12,4,targets=[target('node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes','used'),target('node_memory_MemAvailable_bytes','available','B')],unit='bytes'),
      panel(7,'Сеть без loopback',0,12,targets=[target('sum(rate(fraud_host_network_receive_bytes_total{device!~"lo|veth.*|br-.*|docker.*"}[1m]))','receive'),target('sum(rate(fraud_host_network_transmit_bytes_total{device!~"lo|veth.*|br-.*|docker.*"}[1m]))','transmit','B')],unit='Bps'),
      panel(8,'Дисковые операции',12,12,targets=[target('sum(rate(node_disk_read_bytes_total[1m]))','read'),target('sum(rate(node_disk_written_bytes_total[1m]))','write','B')],unit='Bps'),
      panel(9,'Использование файловых систем',0,20,targets=[target('1 - node_filesystem_avail_bytes{fstype!~"tmpfs|overlay|squashfs",device!~"rootfs"} / node_filesystem_size_bytes','{{device}} · {{mountpoint}}')],unit='percentunit')])


def variable(name,label,query):
    return dict(name=name,label=label,type='query',datasource=PG,query=query,definition=query,
                refresh=1,multi=True,includeAll=True,sort=1,options=[],current={'selected':True,'text':['All'],'value':['$__all']})


def make_business():
    variables=[variable('us_state','Штат / us_state','SELECT DISTINCT us_state FROM scores WHERE us_state IS NOT NULL ORDER BY 1'),
               variable('merch','Мерчант / merch','SELECT DISTINCT merch FROM scores WHERE us_state IN (${us_state:sqlstring}) AND merch IS NOT NULL ORDER BY 1')]
    return dashboard('fraud-business','04 · Business — сегменты и риск',[
      sql_panel(1,'Скоры, флаги и оборот сегмента','segment_summary',0,0,'table',description='Все бизнес-панели учитывают оба фильтра. Флаги — решения модели, не подтверждённое мошенничество.'),
      sql_panel(2,'Плотность распределения скоров','score_density',12,0,'barchart',description='20 бинов шириной 0.05. Сумма density × 0.05 = 1 для непустого сегмента.'),
      sql_panel(3,'TPS выбранного сегмента','segment_tps',0,8,unit='ops',description='Число обработанных транзакций / 10 секунд по scored_at. Крайние неполные интервалы могут занижать TPS.'),
      sql_panel(4,'Доля флагов по cat_id / последние 1000','category_fraud',12,8,'barchart','percentunit',description='Сначала глобально последние 1000 транзакций по id, затем фильтры; выбранный диапазон времени сюда не применяется.'),
      sql_panel(5,'Оборот и сумма подозрительных операций','risky_turnover',0,16,unit='currencyUSD',description='Бизнес-метрика: одинаковая доля флагов может означать разный денежный риск. Это flagged amount, не фактические потери.')],variables)


def alert(uid,title,expr,op,threshold,pending,description,panel_id):
    return {'uid':uid,'title':title,'condition':'C','for':pending,'noDataState':'Alerting','execErrState':'Alerting',
            'annotations':{'summary':description,'__dashboardUid__':'fraud-scoring','__panelId__':str(panel_id)},
            'labels':{'team':'fraud-lab','severity':'warning'},'isPaused':False,
            'data':[{'refId':'A','relativeTimeRange':{'from':120,'to':0},'datasourceUid':PROM['uid'],
                     'model':dict(target(expr),refId='A',instant=True,range=False,intervalMs=1000,maxDataPoints=43200)},
                    {'refId':'C','relativeTimeRange':{'from':0,'to':0},'datasourceUid':'__expr__',
                     'model':{'refId':'C','type':'threshold','expression':'A','datasource':{'type':'__expr__','uid':'__expr__'},
                              'conditions':[{'type':'query','evaluator':{'type':op,'params':[threshold]},'operator':{'type':'and'},'query':{'params':['C']},'reducer':{'type':'last','params':[]}}]}}]}


def main():
    for filename,body in [('scoring',make_scoring()),('storage',make_storage()),('host',make_host()),('business',make_business())]:
        for p in body['panels']:
            if filename=='scoring' and p['id']==10:
                p['fieldConfig']['overrides'].append({'matcher':{'id':'byName','options':'Value'},'properties':[{'id':'unit','value':'dateTimeAsIso'}]})
                p['transformations']=[{'id':'organize','options':{'excludeByName':{'Time':True,'instance':True,'job':True}}}]
            if p['type']=='barchart':
                p['fieldConfig']['defaults']['custom']['fillOpacity']=75
                p['options']['xTickLabelRotation']=45
            if filename=='business' and p['id']==4:
                p['options']['orientation']='horizontal'
                p['options']['xTickLabelRotation']=0
                p['gridPos']['h']=12
            if filename=='business' and p['id']==3:
                p['gridPos']['h']=12
            if filename=='business' and p['id']==5:
                p['gridPos'].update(y=20,w=24)
                p['fieldConfig']['overrides'].append({'matcher':{'id':'byName','options':'flagged_amount_fraction'},'properties':[{'id':'unit','value':'percentunit'},{'id':'custom.axisPlacement','value':'right'}]})
            if filename=='storage' and p['id']==9:
                p['gridPos']['w']=24
            if p['type']=='heatmap':
                p['targets'][0]['expr']=p['targets'][0]['expr'].replace('_bucket[','_bucket{le!="+Inf"}[')
            if p['type']=='table':
                p['fieldConfig']['overrides'].append({'matcher':{'id':'byName','options':'flagged_fraction'},'properties':[{'id':'unit','value':'percentunit'}]})
        (ROOT/'grafana/dashboards'/f'{filename}.json').write_text(json.dumps(body,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    rules=[alert('fraud-stream-stalled','Fraud · stream stalled',
                 'time() - max(fraud_last_success_timestamp_seconds)', 'gt',30,'15s',
                 'Нет успешных результатов >30 секунд; условие держится 15 секунд. Проверить producer, Kafka и scorer.',4),
           alert('fraud-high-flag-ratio','Fraud · high flagged fraction',
                 '(sum(increase(fraud_flagged_total[1m])) / clamp_min(sum(increase(fraud_scored_total[1m])),1)) * (sum(increase(fraud_scored_total[1m])) >= bool 20)',
                 'gt',.2,'20s','Более 20% флагов за 1 минуту, минимум 20 обработанных сообщений; pending 20 секунд.',3)]
    rules[1]['noDataState']='OK'
    rules[1]['execErrState']='Error'
    cfg={'apiVersion':1,'groups':[{'orgId':1,'name':'fraud-service','folder':'Fraud Lab','interval':'10s','rules':rules}]}
    (ROOT/'grafana/provisioning/alerting/rules.yml').write_text(json.dumps(cfg,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('Generated 4 dashboards and 2 alert rules')


if __name__=='__main__': main()
