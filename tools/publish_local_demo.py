"""Local publication DEMONSTRATION only: no IAM/S3 claim.
For full deployment the owner uses the private TA live verifier + S3 publisher.
"""
import argparse,sys,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from common import read_json,write_json,sha256_file
p=argparse.ArgumentParser();p.add_argument('--candidate',default='evidence/release_candidate');p.add_argument('--out',required=True)
a=p.parse_args();src=Path(a.candidate);dst=Path(a.out)
if dst.exists():raise SystemExit('REFUSED: choose a new release directory; no overwrites.')
m=read_json(src/'release_manifest.json')
for e in m['objects']:
    path=src/e['key']
    if path.stat().st_size!=e['bytes'] or sha256_file(path)!=e['sha256']:raise SystemExit('ARTIFACT_INTEGRITY')
if m['binding']['author']==m['binding']['reviewer']:raise SystemExit('SELF_REVIEW')
dst.mkdir(parents=True)
for e in m['objects']:shutil.copy2(src/e['key'],dst/e['key'])
m['status']='LOCAL_DEMO_NOT_IAM_ENFORCED';write_json(dst/'release_manifest.json',m)
print('LOCAL DEMO only. Manifest written last; no S3 authorization or independent TA verification established.')
