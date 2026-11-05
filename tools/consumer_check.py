"""Run with read-only analyst credentials after owner publication."""
import argparse,json,hashlib
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--endpoint',required=True);p.add_argument('--bucket',default='research-release')
p.add_argument('--prefix',required=True);p.add_argument('--trusted-manifest-sha256',required=True);p.add_argument('--out',default='evidence/consumer_check.json');a=p.parse_args()
import boto3
from botocore.config import Config
client=boto3.client('s3',endpoint_url=a.endpoint,region_name='us-east-1',config=Config(s3={'addressing_style':'path'}))
prefix=a.prefix.rstrip('/')+'/'
raw=client.get_object(Bucket=a.bucket,Key=prefix+'release_manifest.json')['Body'].read()
if hashlib.sha256(raw).hexdigest()!=a.trusted_manifest_sha256:raise SystemExit('MANIFEST_INTEGRITY')
m=json.loads(raw)
if m['status']!='APPROVED':raise SystemExit('NOT_APPROVED')
checks=[]
for e in m['objects']:
    data=client.get_object(Bucket=a.bucket,Key=prefix+e['key'])['Body'].read()
    ok=len(data)==e['bytes'] and hashlib.sha256(data).hexdigest()==e['sha256'];checks.append({'key':e['key'],'passed':ok})
if not all(c['passed'] for c in checks):raise SystemExit('ARTIFACT_INTEGRITY')
Path(a.out).parent.mkdir(parents=True,exist_ok=True);Path(a.out).write_text(json.dumps({'status':'PASS','release_id':m['release_id'],'checks':checks,'binding':m['binding']},indent=2)+'\n')
print('PASS: approved manifest and all declared release bytes verified.')
