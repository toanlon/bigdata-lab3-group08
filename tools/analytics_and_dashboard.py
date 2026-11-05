"""Run after incremental. Small Gold snapshot only is collected for plotting.
The SQL samples are supplied analytical checks, not the assessed build_gold code.
"""
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from common import read_json,write_json
from session import make_spark
from run_lab import snapshot,serial
p=argparse.ArgumentParser();p.add_argument('--config',default='config/local.json');a=p.parse_args()
c=read_json(a.config);ev=Path(c['evidence_dir']);state=read_json(ev/'state.json');spark=make_spark(c)
try:
    snapshot(spark,state['gold_path'],state['gold_final_version']).createOrReplaceTempView('gold')
    queries={
       'Q1_site_day':'''SELECT site,event_date,n_observations,mean_temperature_c,n_late,late_rate
                          FROM gold ORDER BY event_date,site''',
       'Q2_weighted_day':'''SELECT event_date, SUM(n_observations) AS n_observations,
                          CAST(SUM(sum_temperature_c)/SUM(n_observations) AS DECIMAL(18,6)) AS weighted_mean_c,
                          CAST(AVG(mean_temperature_c) AS DECIMAL(18,6)) AS naive_mean_of_means_c
                          FROM gold GROUP BY event_date ORDER BY event_date''',
       'Q3_above25_rate':'''SELECT site,event_date,n_observations,n_above_25,
                         CAST(n_above_25/n_observations AS DECIMAL(18,6)) AS above25_rate
                         FROM gold ORDER BY event_date,above25_rate DESC,site'''}
    out={}
    for key,sql in queries.items():
        rows=[serial(r.asDict()) for r in spark.sql(sql).collect()]
        write_json(ev/(key+'.json'),{'gold_version':state['gold_final_version'],'sql':sql,'rows':rows});out[key]=rows
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    rows=out['Q2_weighted_day'];fig,ax=plt.subplots(figsize=(9,4.5))
    ax.plot([r['event_date'] for r in rows],[float(r['weighted_mean_c']) for r in rows],marker='o')
    ax.set(title='Synthetic telemetry: observation-weighted mean',xlabel='UTC event date',ylabel='Temperature (C)')
    ax.tick_params(axis='x',rotation=20);fig.tight_layout();fig.savefig(ev/'daily_mean.png',dpi=160);plt.close(fig)
    rows=out['Q1_site_day'];sites=sorted(set(r['site'] for r in rows));fig,ax=plt.subplots(figsize=(9,4.5))
    for site in sites:
        rs=[r for r in rows if r['site']==site]
        ax.plot([r['event_date'] for r in rs],[float(r['late_rate']) for r in rs],marker='o',label=site)
    ax.set(title='Synthetic telemetry: late-arrival fraction',xlabel='UTC event date',ylabel='Late observations / observations')
    ax.tick_params(axis='x',rotation=20);ax.legend();fig.tight_layout();fig.savefig(ev/'late_fraction.png',dpi=160);plt.close(fig)
    print('Saved 3 SQL results and 2 plots; annotate the exact Silver/Gold versions in your report.')
finally:spark.stop()
