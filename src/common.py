"""Bounded fixture I/O, integrity, canonical values and lineage helpers.
This module does not implement the five assessed Spark transformations.
"""
from __future__ import annotations
import csv, hashlib, json, re, uuid
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

COLUMNS = ['record_id','sensor_id','site','event_time_utc','ingest_time_utc',
           'temperature_c','is_late','source_object','source_row','source_sha256']
AS_OF = '2026-02-09T00:00:00Z'
FMT = '%Y-%m-%dT%H:%M:%SZ'
MAX_FIXTURE_ROWS = 25000
class ContractError(ValueError):
    pass

def sha256_file(path: str | Path) -> str:
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(1024*1024), b''):h.update(b)
    return h.hexdigest()

def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))

def write_json(path, obj):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(obj,indent=2,ensure_ascii=False,default=str)+'\n',encoding='utf-8')

def utc_text(value: Any) -> str:
    if isinstance(value, datetime):
        value=value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
        if value.microsecond:raise ContractError('TIME_PRECISION')
        return value.strftime(FMT)
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',value):
        raise ContractError('TIME_PARSE')
    datetime.strptime(value,FMT)
    return value

def as_time(value):
    return datetime.strptime(utc_text(value),FMT).replace(tzinfo=timezone.utc)

def canonical_row(row:dict, allow_event_date=False) -> dict:
    expected=set(COLUMNS)|({'event_date'} if allow_event_date else set())
    if set(row)!=expected: raise ContractError('SCHEMA_ALLOWLIST')
    out={k:row[k] for k in COLUMNS}
    for k in ['record_id','sensor_id','site','source_object','source_sha256']:
        if not isinstance(out[k],str) or not out[k].strip():raise ContractError('REQUIRED:'+k)
    if not re.fullmatch(r'R[0-9]{6}',out['record_id']):raise ContractError('KEY')
    if not re.fullmatch(r'[a-f0-9]{64}',out['source_sha256']):raise ContractError('SOURCE_HASH')
    try: value=Decimal(str(out['temperature_c']))
    except Exception as e: raise ContractError('VALUE') from e
    if not value.is_finite() or not Decimal('-30')<=value<=Decimal('60'):raise ContractError('VALUE')
    if value!=value.quantize(Decimal('.01')):raise ContractError('DECIMAL_PRECISION')
    out['temperature_c']=f'{value:.2f}'
    if isinstance(out['is_late'],str):
        if out['is_late'] not in ('true','false'):raise ContractError('BOOLEAN')
        out['is_late']=out['is_late']=='true'
    if not isinstance(out['is_late'],bool):raise ContractError('BOOLEAN')
    sr=out['source_row']
    if isinstance(sr,bool) or not re.fullmatch(r'[1-9][0-9]*',str(sr)):raise ContractError('SOURCE_ROW')
    out['source_row']=int(sr)
    for c in ('event_time_utc','ingest_time_utc'):out[c]=utc_text(out[c])
    event,ingest=as_time(out['event_time_utc']),as_time(out['ingest_time_utc'])
    if not event<=ingest<=as_time(AS_OF):raise ContractError('CHRONOLOGY')
    if out['is_late']!=((ingest-event).total_seconds()>900):raise ContractError('LATE_FLAG')
    if allow_event_date and str(row['event_date'])!=out['event_time_utc'][:10]:raise ContractError('EVENT_DATE')
    return out

def validate_rows(rows, registry:dict, unique_business_ids:bool, allow_event_date=False):
    if not rows:raise ContractError('EMPTY')
    if len(rows)>MAX_FIXTURE_ROWS:raise ContractError('NOT_A_BOUNDED_CORE_FIXTURE')
    result=[canonical_row(r,allow_event_date) for r in rows]
    physical=[(r['source_object'],r['source_row'],r['source_sha256']) for r in result]
    if len(set(physical))!=len(physical):raise ContractError('PHYSICAL_DUPLICATE')
    if unique_business_ids and len({r['record_id'] for r in result})!=len(result):raise ContractError('BUSINESS_DUPLICATE')
    for r in result:
        if registry.get(r['sensor_id'])!=r['site']:raise ContractError('REGISTRY')
    return result

def load_csv(path):
    with Path(path).open(encoding='utf-8',newline='') as f:
        reader=csv.DictReader(f)
        if reader.fieldnames != COLUMNS:raise ContractError('CSV_HEADER')
        return list(reader)

def load_registry(path):
    with Path(path).open(encoding='utf-8',newline='') as f:
        rows=list(csv.DictReader(f))
    if not rows or len({r['sensor_id'] for r in rows})!=len(rows):raise ContractError('REGISTRY_UNIQUENESS')
    return {r['sensor_id']:r['site'] for r in rows}

def canonical_text(rows):
    return ''.join(json.dumps({k:r[k] for k in COLUMNS},ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n'
                   for r in sorted(rows,key=lambda r:r['record_id']))

def logical_sha256(rows):
    return hashlib.sha256(canonical_text(rows).encode('utf-8')).hexdigest()

def write_rows(path,rows):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(canonical_text(rows),encoding='utf-8')

def verify_inputs(root, manifest_path):
    """The caller must obtain manifest_path from an instructor-controlled channel.
    Byte agreement checks integrity, not provenance of the supplied manifest itself.
    """
    root=Path(root).resolve();manifest=read_json(manifest_path);checks=[]
    for entry in manifest['objects']:
        p=(root/entry['path']).resolve()
        if root not in p.parents:raise ContractError('UNSAFE_MANIFEST_PATH')
        if not p.is_file():raise ContractError('MISSING_INPUT:'+entry['path'])
        passed=p.stat().st_size==entry['bytes'] and sha256_file(p)==entry['sha256']
        checks.append({'path':entry['path'],'passed':passed})
        if not passed:raise ContractError('INPUT_INTEGRITY:'+entry['path'])
    return {'checks':checks,'trusted_manifest_sha256':sha256_file(manifest_path),
            'trust_note':'Authenticity of this manifest depends on the independent distribution channel.'}

def code_digest(root):
    root=Path(root);h=hashlib.sha256()
    for p in sorted(list((root/'src').glob('*.py'))+list((root/'templates').glob('*.py'))):
        h.update(p.relative_to(root).as_posix().encode()+b'\0'+p.read_bytes()+b'\0')
    return h.hexdigest()

def emit_lineage(path, event_type, run_id, job, inputs, outputs):
    if event_type not in {'START','COMPLETE','FAIL','ABORT'}:raise ValueError(event_type)
    uuid.UUID(run_id)
    event={'eventType':event_type,'eventTime':datetime.now(timezone.utc).isoformat(),
      'run':{'runId':run_id},'job':{'namespace':'bigdata-lab3','name':job},
      'inputs':[{'namespace':n,'name':name} for n,name in inputs],
      'outputs':[{'namespace':n,'name':name} for n,name in outputs],
      'producer':'urn:bigdata-course:lab3:runner:v1',
      'schemaURL':'https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent'}
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    with p.open('a',encoding='utf-8') as f:f.write(json.dumps(event)+'\n')
    return event
