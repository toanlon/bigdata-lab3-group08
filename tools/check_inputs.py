"""Standard-library-only input check; requires no Spark or cloud installation."""
import argparse,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from common import verify_inputs,load_registry,load_csv,validate_rows,logical_sha256,read_json
p=argparse.ArgumentParser();p.add_argument('--trusted-manifest',required=True);a=p.parse_args()
report=verify_inputs(ROOT,a.trusted_manifest)
reg=load_registry(ROOT/'data/handoff/sensors.csv')
b=validate_rows(load_csv(ROOT/'data/handoff/curated.csv'),reg,True)
c=validate_rows(load_csv(ROOT/'data/changes/changes_batch02.csv'),reg,False)
assert logical_sha256(b)==read_json(ROOT/'data/handoff/handoff_manifest.json')['logical_sha256']
print('PASS: all manifest bytes/hashes and transport contracts match.')
print(f'Handoff rows: {len(b)}; incoming physical revisions: {len(c)}.')
print('This is an input check, NOT a Spark/Delta/Kubernetes execution test.')
