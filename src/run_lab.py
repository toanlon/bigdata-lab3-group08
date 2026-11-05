"""Lab 3 task runner. Infrastructure/plumbing is supplied; five functions are assessed.
Usage from the package root: python src/run_lab.py --config config/local.json \
  --trusted-manifest /instructor/trusted_lab3_inputs.json --step bootstrap
The local manifest argument must come from an independent trusted channel.
"""
from __future__ import annotations
import argparse, contextlib, copy, csv, hashlib, importlib, io, json, os
from pathlib import Path
import statistics, sys, time, uuid, shutil
from datetime import datetime, timezone
from decimal import Decimal
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'templates'))
from common import (COLUMNS, MAX_FIXTURE_ROWS, ContractError, read_json, write_json, load_csv,
    load_registry, validate_rows, logical_sha256, write_rows, verify_inputs, code_digest,
    emit_lineage, as_time, sha256_file)
from session import make_spark

def serial(r):
    return {k:(str(v) if isinstance(v,(Decimal,datetime)) or type(v).__name__=='date' else v) for k,v in r.items()}

def small_records(df,limit=MAX_FIXTURE_ROWS):
    rs=df.limit(limit+1).collect()
    if len(rs)>limit:raise ContractError('BOUNDED_EXPORT_LIMIT')
    return [r.asDict(recursive=True) for r in rs]

def transport_df(spark, rows):
    from pyspark.sql import types as T, functions as F
    schema=T.StructType([
      T.StructField('record_id',T.StringType(),False),T.StructField('sensor_id',T.StringType(),False),
      T.StructField('site',T.StringType(),False),T.StructField('event_time_utc',T.TimestampType(),False),
      T.StructField('ingest_time_utc',T.TimestampType(),False),T.StructField('temperature_c',T.DecimalType(8,2),False),
      T.StructField('is_late',T.BooleanType(),False),T.StructField('source_object',T.StringType(),False),
      T.StructField('source_row',T.LongType(),False),T.StructField('source_sha256',T.StringType(),False)])
    objects=[]
    for r in rows:
        x=dict(r);x['temperature_c']=Decimal(x['temperature_c'])
        x['event_time_utc']=as_time(x['event_time_utc']);x['ingest_time_utc']=as_time(x['ingest_time_utc']);objects.append(x)
    # Bounded transport fixture only; Spark distributes the resulting rows.
    return spark.createDataFrame(objects,schema).repartition(8).withColumn('event_date',F.to_date('event_time_utc'))

def version(spark,path):
    from delta.tables import DeltaTable
    return int(DeltaTable.forPath(spark,path).history(1).select('version').first()[0])

def snapshot(spark,path,v):
    return spark.read.format('delta').option('versionAsOf',int(v)).load(path)

def rows_from_silver(spark,path,v,registry):
    return validate_rows(small_records(snapshot(spark,path,v)),registry,True,allow_event_date=True)

def write_initial(df,path):
    df.write.format('delta').mode('errorifexists').save(path)

def append_receipts(spark,df,path,batch_id):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    new=df.withColumn('receipt_batch_id',F.lit(batch_id))
    if not DeltaTable.isDeltaTable(spark,path):write_initial(new,path)
    else:
        (DeltaTable.forPath(spark,path).alias('t').merge(new.alias('s'),
          't.source_object = s.source_object AND t.source_row = s.source_row AND t.source_sha256 = s.source_sha256')
         .whenNotMatchedInsertAll().execute())

def materialize_gold(spark,impl,silver_path,sv,gold_path,initial=False):
    df=impl.build_gold(snapshot(spark,silver_path,sv))
    if initial:write_initial(df,gold_path)
    else:df.write.format('delta').mode('overwrite').save(gold_path)
    return version(spark,gold_path)

def save_gold(spark,path,v,out):
    rows=[serial(r) for r in small_records(snapshot(spark,path,v))]
    rows.sort(key=lambda r:(r['site'],r['event_date']))
    write_json(out,rows)
    return rows

def runtime_info(spark,config):
    import importlib.metadata as md
    return {'python':sys.version.split()[0], 'spark':spark.version,'delta_spark':md.version('delta-spark'),
       'java':spark._jvm.java.lang.System.getProperty('java.version'),
       'hadoop':spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
       'spark_master':spark.sparkContext.master,'application_id':spark.sparkContext.applicationId,
       'timezone':spark.conf.get('spark.sql.session.timeZone'),
       'aqe':spark.conf.get('spark.sql.adaptive.enabled'),
       'shuffle_partitions':spark.conf.get('spark.sql.shuffle.partitions'),
       'group_id':config['group_id'],'storage_mode':'s3a' if config['lakehouse_root'].startswith('s3a:') else 'local_file',
       'warning':'local[*] is parallel local execution, NOT multi-node execution.'}

def worker_probe(spark):
    def f(iterator):
        import os,socket,time
        from pyspark import TaskContext
        list(iterator);time.sleep(.10)
        yield {'host':socket.gethostname(),'pid':os.getpid(),'partition':TaskContext.get().partitionId()}
    workers=spark.sparkContext.parallelize(range(24),24).mapPartitions(f).collect()
    return {'partition_tasks':workers,'distinct_hosts':len({r['host'] for r in workers}),
      'interpretation':'Distinct hosts demonstrate placements, not distinct physical servers. Keep Spark UI/event-log evidence too.'}

def check_provenance(rows,root):
    mapping={'observations_a.csv':root/'data/lab2_sources/observations_a.csv',
             'observations_b.jsonl':root/'data/lab2_sources/observations_b.jsonl',
             'lab3/source_batch02.csv':root/'data/changes/source_batch02.csv'}
    hashes={k:sha256_file(p) for k,p in mapping.items()}
    for r in rows:
        if hashes.get(r['source_object'])!=r['source_sha256']:raise ContractError('ROW_SOURCE_BINDING')
    return True

def mutation_tests(rows,registry):
    """Isolated in-memory input-contract probes; no Delta write occurs."""
    cases=[]
    for code,change in [('VALUE',lambda r:r.update(temperature_c='100.00')),
                        ('REQUIRED_VALUE',lambda r:r.update(temperature_c=None)),
                        ('SCHEMA_ALLOWLIST',lambda r:r.update(operator_email='restricted@example.invalid'))]:
        mutated=copy.deepcopy(rows);change(mutated[0])
        try:validate_rows(mutated,registry,False);passed=False;reason='ACCEPTED'
        except (ContractError,ValueError) as e:passed=True;reason=str(e)
        cases.append({'case':code,'passed':passed,'observed':reason,'writes_performed':0})
    return cases

def build_draft(state,author,reviewer,steward,impl_sha):
    return {'release_id':'lab3-'+state['group_id']+'-'+state['run_label'],
      'author':author,'reviewer':reviewer,'steward':steward,
      'handoff_logical_sha256':state['handoff_logical_sha256'],
      'handoff_kind':state['handoff_kind'],
      'trusted_manifest_sha256':state['trusted_manifest_sha256'],
      'code_sha256':impl_sha,
      'silver_path':state['silver_path'],'silver_version':state['silver_final_version'],
      'gold_path':state['gold_path'],'gold_version':state['gold_final_version'],
      'gold_source_silver_version':state['gold_source_silver_version'],
      'as_of':'2026-02-09T00:00:00Z','timezone':'UTC',
      'known_limitations':['Synthetic fixture only.','Inherited QA sample disagreements are not repaired.',
                           'A version tuple is a release protocol, not a cross-table transaction.']}

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',default='config/local.json')
    parser.add_argument('--trusted-manifest',required=True)
    parser.add_argument('--step',choices=['preflight','bootstrap','incremental','benchmark','release'],required=True)
    parser.add_argument('--impl',default='student_impl')
    parser.add_argument('--author',default='');parser.add_argument('--reviewer',default='');parser.add_argument('--steward',default='')
    parser.add_argument('--team-parquet',default='')
    parser.add_argument('--team-data-sha256-file',default='')
    args=parser.parse_args();config=read_json(args.config)
    for key in ('group_id','lakehouse_root','spark_master','evidence_dir','run_label'):
        if key not in config:raise ContractError('CONFIG:'+key)
    if not config['spark_master'].startswith('local') and not config['lakehouse_root'].startswith('s3a://'):
        raise ContractError('CLUSTER_REQUIRES_SHARED_S3A_STORAGE')
    ev=Path(config['evidence_dir']);ev.mkdir(parents=True,exist_ok=True)
    verified=verify_inputs(ROOT,args.trusted_manifest)
    write_json(ev/'input_integrity.json',verified)
    registry=load_registry(ROOT/'data/handoff/sensors.csv')
    baseline=validate_rows(load_csv(ROOT/'data/handoff/curated.csv'),registry,True)
    incoming=validate_rows(load_csv(ROOT/'data/changes/changes_batch02.csv'),registry,False)
    check_provenance(baseline+incoming,ROOT)
    fallback=read_json(ROOT/'data/handoff/handoff_manifest.json')
    if logical_sha256(baseline)!=fallback['logical_sha256']:raise ContractError('HANDOFF_LOGICAL_HASH')
    # Imported implementation is recorded; instructor solution requires explicit PYTHONPATH.
    impl=importlib.import_module(args.impl)
    impl_sha=sha256_file(impl.__file__)
    (ev/'code_versions').mkdir(exist_ok=True)
    shutil.copy2(impl.__file__,ev/'code_versions'/(impl_sha+'.py'))
    root=config['lakehouse_root'].rstrip('/')+'/'+config['group_id']+'/'+config['run_label']
    paths={name:root+'/tables/'+name for name in ('bronze','silver','gold')}
    spark=make_spark(config);run_id=str(uuid.uuid4());job=args.step
    inputs=[('file-fixture',str(ROOT/'data/handoff/curated.csv'))]
    if args.step == 'bootstrap' and args.team_parquet:
        inputs=[(config.get('store_id','local'),args.team_parquet)]
    if args.step in ('incremental','benchmark','release'):
        prior=read_json(ev/'state.json')
        if (prior['group_id']!=config['group_id'] or prior['run_label']!=config['run_label']
            or prior['silver_path']!=paths['silver']
            or prior['trusted_manifest_sha256']!=verified['trusted_manifest_sha256']):
            spark.stop()
            raise ContractError('WORKSPACE_STATE_OR_TRUST_MISMATCH')
        inputs=[(config.get('store_id','local'),prior['silver_path']+'@'+str(prior['silver_final_version']))]
        if args.step == 'incremental':
            inputs.append(('file-fixture',str(ROOT/'data/changes/changes_batch02.csv')))
        elif args.step == 'release':
            inputs.append((config.get('store_id','local'),prior['gold_path']+'@'+str(prior['gold_final_version'])))
    outputs=[];lineage_path=ev/'lineage.jsonl'
    emit_lineage(lineage_path,'START',run_id,job,inputs,[])
    try:
        write_json(ev/f'runtime_{job}.json',runtime_info(spark,config))
        if args.step=='preflight':
            write_json(ev/'worker_probe.json',worker_probe(spark))
            write_json(ev/'input_contract_tests.json',mutation_tests(incoming,registry))
            # A tiny per-run Delta smoke path; no course tables are touched.
            smoke=root+'/preflight-'+run_id
            spark.range(8).write.format('delta').mode('errorifexists').save(smoke)
            if spark.read.format('delta').load(smoke).count()!=8:raise ContractError('DELTA_SMOKE')
            from pyspark.sql import functions as F
            from pyspark.errors import AnalysisException
            rejected=False
            try:
                spark.range(1).withColumn('unexpected',F.lit('x')).write.format('delta').mode('append').save(smoke)
            except AnalysisException:
                rejected=True
            after_count=spark.read.format('delta').load(smoke).count()
            if not rejected or after_count!=8:raise ContractError('NATIVE_SCHEMA_ENFORCEMENT_FAILED')
            write_json(ev/'delta_schema_probe.json',{'rejected_as_analysis_error':rejected,'rows_after':after_count,'passed':True})
            write_json(ev/'preflight.json',{'status':'PASS','delta_smoke_path':smoke,
                       'fixture_rows':len(baseline),'new_batch_rows':len(incoming),'implementation_file':str(Path(impl.__file__).name)})
            outputs=[(config.get('store_id','local-course-fixture'),smoke)]
        elif args.step=='bootstrap':
            if (ev/'state.json').exists():raise ContractError('USE_NEW_RUN_LABEL_OR_RESUME_NEXT_STEP')
            handoff_kind=fallback['handoff_kind']
            if args.team_parquet:
                if not args.team_data_sha256_file:raise ContractError('TEAM_TRUSTED_DATA_SHA_REQUIRED')
                from team_handoff import verify_team_parquet_bytes
                verify_team_parquet_bytes(args.team_parquet,Path(args.team_data_sha256_file).read_text().strip(),config)
                uri=args.team_parquet.replace('s3://','s3a://',1)
                if not uri.startswith('s3a:') and not config['spark_master'].startswith('local'):
                    raise ContractError('TEAM_PARQUET_MUST_BE_SHARED_ON_CLUSTER')
                native=spark.read.parquet(uri)
                actual=validate_rows(small_records(native),registry,True)
                if logical_sha256(actual)!=logical_sha256(baseline):raise ContractError('TEAM_HANDOFF_DIFFERS_NEEDS_TA_RECALIBRATION')
                baseline=actual;handoff_kind='verified_team_lab2_parquet'
                inputs=[(config.get('store_id','local-course-fixture'),args.team_parquet)]
            initial=transport_df(spark,baseline)
            impl.build_gold(initial)  # Fail on an unimplemented TODO before the first write.
            append_receipts(spark,initial,paths['bronze'],'lab2-handoff')
            bv=version(spark,paths['bronze'])
            bronze_initial=snapshot(spark,paths['bronze'],bv).drop('receipt_batch_id')
            write_initial(bronze_initial,paths['silver'])
            sv=version(spark,paths['silver'])
            gv=materialize_gold(spark,impl,paths['silver'],sv,paths['gold'],True)
            state={'group_id':config['group_id'],'run_label':config['run_label'],'handoff_kind':handoff_kind,
               'handoff_logical_sha256':logical_sha256(baseline),'trusted_manifest_sha256':verified['trusted_manifest_sha256'],
               'bronze_path':paths['bronze'],'silver_path':paths['silver'],'gold_path':paths['gold'],
               'bronze_initial_version':bv,'silver_initial_version':sv,'gold_initial_version':gv,
               'silver_final_version':sv,'gold_final_version':gv,'gold_source_silver_version':sv,
               'initial_implementation_sha256':impl_sha,'run_ids':{'bootstrap':run_id}}
            initial_read=rows_from_silver(spark,paths['silver'],sv,registry)
            if logical_sha256(initial_read)!=logical_sha256(baseline):raise ContractError('INITIAL_LOGICAL_DIFFERENCE')
            write_rows(ev/'silver_baseline.jsonl',initial_read)
            save_gold(spark,paths['gold'],gv,ev/'gold_baseline.json')
            write_json(ev/'bootstrap.json',{'rows':len(initial_read),'gold_rows':snapshot(spark,paths['gold'],gv).count(),
              'silver_version':sv,'gold_version':gv,'bronze_rows':spark.read.format('delta').load(paths['bronze']).count()})
            write_json(ev/'state.json',state)
            outputs=[(config.get('store_id','local'),p) for p in paths.values()]
        elif args.step=='incremental':
            from pyspark.sql import functions as F
            state=read_json(ev/'state.json');incoming_df=transport_df(spark,incoming)
            inputs=[(config.get('store_id','local'),state['silver_path']+'@'+str(state['silver_final_version'])),
                    ('file-fixture',str(ROOT/'data/changes/changes_batch02.csv'))]
            tests=mutation_tests(incoming,registry);write_json(ev/'input_contract_tests.json',tests)
            if not all(x['passed'] for x in tests):raise ContractError('MUTATION_TEST_FAILED')
            # All gate checks precede any write in this step. Only a single driver writes each table.
            append_receipts(spark,incoming_df,state['bronze_path'],'batch-02')
            bv=version(spark,state['bronze_path'])
            bronze_batch=snapshot(spark,state['bronze_path'],bv).where("receipt_batch_id = 'batch-02'").drop('receipt_batch_id')
            winners=impl.choose_revisions(bronze_batch)
            if set(winners.columns)!=set(incoming_df.columns):raise ContractError('WINNER_SCHEMA')
            winner_rows=validate_rows(small_records(winners),registry,True,True)
            if len(winner_rows)!=len({r['record_id'] for r in incoming}):raise ContractError('WINNER_CARDINALITY')
            winner_rows_reversed=validate_rows(small_records(impl.choose_revisions(bronze_batch.orderBy(F.col('source_row').desc()))),registry,True,True)
            if logical_sha256(winner_rows)!=logical_sha256(winner_rows_reversed):raise ContractError('WINNER_NOT_DETERMINISTIC')
            impl.merge_silver(spark,state['silver_path'],winners)
            after_v=version(spark,state['silver_path']);after=rows_from_silver(spark,state['silver_path'],after_v,registry)
            gold_v=materialize_gold(spark,impl,state['silver_path'],after_v,state['gold_path'])
            bc1=spark.read.format('delta').load(state['bronze_path']).count()
            # Actual replay, not just a claim or second output from the same DataFrame.
            append_receipts(spark,incoming_df,state['bronze_path'],'batch-02')
            impl.merge_silver(spark,state['silver_path'],winners)
            replay_v=version(spark,state['silver_path']);replay=rows_from_silver(spark,state['silver_path'],replay_v,registry)
            bc2=spark.read.format('delta').load(state['bronze_path']).count()
            if bc1!=bc2 or logical_sha256(after)!=logical_sha256(replay):raise ContractError('REPLAY_NOT_IDEMPOTENT')
            historical=rows_from_silver(spark,state['silver_path'],state['silver_initial_version'],registry)
            if logical_sha256(historical)!=state['handoff_logical_sha256']:raise ContractError('TIME_TRAVEL_CHANGED')
            write_rows(ev/'silver_after_batch02.jsonl',after);write_rows(ev/'silver_replay.jsonl',replay)
            write_rows(ev/'silver_time_travel.jsonl',historical)
            save_gold(spark,state['gold_path'],gold_v,ev/'gold_after_batch02.json')
            state.update(bronze_final_version=bv,silver_final_version=after_v,silver_replay_version=replay_v,
                         gold_final_version=gold_v,gold_source_silver_version=after_v,
                         final_implementation_sha256=impl_sha)
            state['run_ids']['incremental']=run_id;write_json(ev/'state.json',state)
            write_json(ev/'incremental.json',{'incoming_rows':len(incoming),'incoming_winners':len(winner_rows),
                'bronze_before_replay':bc1,'bronze_after_replay':bc2,'silver_rows':len(after),
                'logical_sha256_after':logical_sha256(after),'logical_sha256_replay':logical_sha256(replay),
                'silver_after_version':after_v,'silver_replay_version':replay_v,
                'time_travel_passed':True,'winner_reordering_passed':True,
                'note':'Replay may add a no-op commit. Business-state equivalence, not an unchanged log version, is required.'})
            outputs=[(config.get('store_id','local'),state['silver_path']+'@'+str(after_v)),
                     (config.get('store_id','local'),state['gold_path']+'@'+str(gold_v))]
        elif args.step=='benchmark':
            from pyspark.sql import functions as F
            from delta.tables import DeltaTable
            state=read_json(ev/'state.json');copies=int(config.get('benchmark_copies',100))
            if not 1<=copies<=1000:raise ContractError('BENCHMARK_SIZE')
            src=snapshot(spark,state['silver_path'],state['silver_final_version'])
            base_count=src.count()
            data=src.crossJoin(spark.range(copies).withColumnRenamed('id','replica_id'))
            bench_root=root+'/benchmarks/'+run_id
            locations={'A':bench_root+'/unpartitioned','B':bench_root+'/date_partitioned'}
            prep={}
            for label in ('A','B'):
                start=time.perf_counter()
                if label=='A':data.repartition(32).write.format('delta').mode('errorifexists').save(locations[label])
                else:(data.repartition(5,'event_date').write.format('delta').partitionBy('event_date').mode('errorifexists').save(locations[label]))
                prep[label]=time.perf_counter()-start
            detail={}
            for label,path in locations.items():
                d=DeltaTable.forPath(spark,path).detail().select('numFiles','sizeInBytes','partitionColumns').first()
                detail[label]={'path':path,'version':version(spark,path),'total_table_data_files':d[0],
                   'total_table_bytes':d[1],'partition_columns':d[2]}
            results={};measurements=[];plans={}
            # One warm-up for EACH layout. These are warmed/repeated reads, NOT cold-cache timings.
            for label in ('A','B'):
                frame=snapshot(spark,locations[label],detail[label]['version'])
                if frame.count()!=base_count*copies:raise ContractError('BENCHMARK_ROW_COUNT')
                results[label]=[serial(r.asDict()) for r in impl.benchmark_query(frame).orderBy('site').collect()]
                buf=io.StringIO()
                with contextlib.redirect_stdout(buf):impl.benchmark_query(frame).explain(mode='formatted')
                plans[label]=buf.getvalue();(ev/f'plan_{label}.txt').write_text(plans[label])
            if results['A']!=results['B']:raise ContractError('BENCHMARK_RESULTS_DIFFER')
            for round_index,order in enumerate((('A','B'),('B','A'),('A','B')),1):
                for label in order:
                    spark.catalog.clearCache() # Does not flush OS/object-store/JVM caches.
                    frame=snapshot(spark,locations[label],detail[label]['version'])
                    t=time.perf_counter();answer=[serial(r.asDict()) for r in impl.benchmark_query(frame).orderBy('site').collect()]
                    elapsed=time.perf_counter()-t
                    if answer!=results['A']:raise ContractError('TIMED_QUERY_RESULT_DIFFERENCE')
                    measurements.append({'round':round_index,'layout':label,'elapsed_seconds':elapsed,'result_equal':True})
            med={label:statistics.median(r['elapsed_seconds'] for r in measurements if r['layout']==label) for label in ('A','B')}
            write_json(ev/'benchmark_report.json',{'source_silver_version':state['silver_final_version'],
              'benchmark_copies':copies,'source_rows':base_count,'expanded_rows':base_count*copies,
              'purpose':'Synthetic layout experiment; replicas are not new independent observations.',
              'query':'event_date = 2026-02-06; GROUP BY site; count, sum_temperature_c, n_late',
              'setup_seconds_excluded_from_query_times':prep,'layout_details':detail,'warmups_per_layout':1,
              'measurements':measurements,'median_seconds':med,'A_median_over_B_median':med['A']/med['B'],
              'query_result':results['A'],'table_file_count_is_not_scanned_file_count':True,
              'cache_condition':'Repeated/warmed execution; clearCache does NOT reset OS/object-store caches.'})
            inputs=[(config.get('store_id','local'),state['silver_path']+'@'+str(state['silver_final_version']))]
            outputs=[(config.get('store_id','local'),p) for p in locations.values()]
        elif args.step=='release':
            state=read_json(ev/'state.json')
            if 'final_implementation_sha256' not in state:raise ContractError('RUN_INCREMENTAL_FIRST')
            actual=rows_from_silver(spark,state['silver_path'],state['silver_final_version'],registry)
            # Recompute Gold from the pinned Silver snapshot and compare actual table rows.
            rebuilt=sorted([serial(r.asDict()) for r in impl.build_gold(snapshot(spark,state['silver_path'],state['silver_final_version'])).collect()],key=lambda x:(x['site'],x['event_date']))
            stored=save_gold(spark,state['gold_path'],state['gold_final_version'],ev/'gold_release_snapshot.json')
            if rebuilt!=stored:raise ContractError('GOLD_NOT_FROM_PINNED_SILVER')
            draft=build_draft(state,args.author,args.reviewer,args.steward,state['final_implementation_sha256'])
            draft['release_validator_sha256']=impl_sha
            expected={k:draft[k] for k in ('handoff_logical_sha256','code_sha256','silver_path','silver_version','gold_path','gold_version','gold_source_silver_version')}
            probes=[('valid',copy.deepcopy(draft),True)]
            for name,change in [('mixed_version',lambda x:x.update(gold_source_silver_version=state['silver_initial_version'])),
                 ('self_review',lambda x:x.update(reviewer=x['author'])),
                 ('missing_steward',lambda x:x.pop('steward',None)),
                 ('wrong_code',lambda x:x.update(code_sha256='0'*64))]:
                m=copy.deepcopy(draft);change(m);probes.append((name,m,False))
            test_results=[]
            for name,probe,wanted in probes:
                passed,reasons=impl.validate_release_binding(probe,expected)
                test_results.append({'case':name,'expected_pass':wanted,'observed_pass':bool(passed),'reasons':reasons,'test_passed':bool(passed)==wanted})
            write_json(ev/'release_gate_tests.json',test_results)
            if not all(r['test_passed'] for r in test_results):raise ContractError('RELEASE_GATE_TESTS')
            # Staged only. The separate owner/publisher tool copies AFTER independent review.
            release_dir=ev/'release_candidate';release_dir.mkdir(exist_ok=True)
            write_json(release_dir/'gold.json',stored)
            write_json(release_dir/'snapshot_binding.json',draft)
            catalog={'schema_version':'lab3-catalog-v1','owner':args.reviewer,'steward':args.steward,
              'allowed_use':'Synthetic teaching and assessment only','as_of':'2026-02-09T00:00:00Z',
              'trusted_manifest_sha256':state['trusted_manifest_sha256'],
              'upstream_handoff_kind':state['handoff_kind'],'upstream_logical_sha256':state['handoff_logical_sha256'],
              'datasets':[
                {'dataset_id':'silver_observations','location':state['silver_path'],
                 'version':state['silver_final_version'],'row_count':len(actual),
                 'classification':'RESTRICTED_WORKING_TABLE','columns':read_json(ROOT/'schemas/silver_schema.json')['columns']},
                {'dataset_id':'gold_site_day','location':state['gold_path'],
                 'version':state['gold_final_version'],'source_silver_version':state['silver_final_version'],
                 'row_count':len(stored),'classification':'APPROVED_ONLY_AFTER_OWNER_REVIEW',
                 'columns':read_json(ROOT/'schemas/gold_schema.json')['columns']}],
              'retention':{'published_release_days':90,'time_basis':'actual publication time, not simulated AS_OF',
                'dependency_rule':'Keep referenced table versions/data files while an approved release depends on them. No VACUUM in the core lab.'}}
            write_json(release_dir/'catalog.json',catalog)
            write_json(release_dir/'release_manifest.json',{
                'status':'CANDIDATE_NOT_OWNER_PUBLISHED','release_id':draft['release_id'],'binding':draft,
                'objects':[{'key':'gold.json','bytes':(release_dir/'gold.json').stat().st_size,'sha256':sha256_file(release_dir/'gold.json')},
                           {'key':'snapshot_binding.json','bytes':(release_dir/'snapshot_binding.json').stat().st_size,'sha256':sha256_file(release_dir/'snapshot_binding.json')},
                           {'key':'catalog.json','bytes':(release_dir/'catalog.json').stat().st_size,'sha256':sha256_file(release_dir/'catalog.json')}],
                'publication_protocol':'Independent owner recheck; upload approved artifacts; manifest last. Never overwrite a release prefix.'})
            write_json(ev/'release_check.json',{'silver_rows':len(actual),'gold_rows':len(stored),'snapshot_recompute_equal':True,
                      'status':'CANDIDATE_READY_FOR_INDEPENDENT_OWNER_REVIEW'})
            inputs=[(config.get('store_id','local'),state['silver_path']+'@'+str(state['silver_final_version'])),
                    (config.get('store_id','local'),state['gold_path']+'@'+str(state['gold_final_version']))]
            outputs=[('local-evidence',str(release_dir))]
        emit_lineage(lineage_path,'COMPLETE',run_id,job,inputs,outputs)
        write_json(ev/f'run_{job}.json',{'run_id':run_id,'job':job,'status':'COMPLETE','input_manifest_sha256':verified['trusted_manifest_sha256'],
            'implementation_sha256':impl_sha,'scaffold_sha256':code_digest(ROOT),'outputs':[p for _,p in outputs]})
        print(f'{args.step}: COMPLETE; evidence: {ev}')
    except Exception as e:
        typ='ABORT' if isinstance(e,(ContractError,NotImplementedError)) else 'FAIL'
        emit_lineage(lineage_path,typ,run_id,job,inputs,[])
        write_json(ev/f'run_{job}_error.json',{'run_id':run_id,'job':job,'status':typ,'error_type':type(e).__name__,'message':str(e)[:500]})
        raise
    finally:spark.stop()

if __name__=='__main__':main()
