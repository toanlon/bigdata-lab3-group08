"""Verify one Lab 2 DuckDB-exported Parquet OBJECT, not an unmanifested directory.
The expected hash is supplied by the TA from the approved Lab 2 release manifest.
"""
from urllib.parse import urlparse
from pathlib import Path
import hashlib

def verify_team_parquet_bytes(uri,expected_sha256,config):
    if len(expected_sha256)!=64:raise ValueError('EXPECTED_SHA256_REQUIRED')
    h=hashlib.sha256()
    if uri.startswith(('s3://','s3a://')):
        import boto3
        from botocore.config import Config
        p=urlparse(uri)
        client=boto3.client('s3',endpoint_url=config['s3_endpoint'],
            region_name=config.get('s3_region','us-east-1'),config=Config(s3={'addressing_style':'path'}))
        response=client.get_object(Bucket=p.netloc,Key=p.path.lstrip('/'))
        with response['Body'] as stream:
            for chunk in iter(lambda:stream.read(1024*1024),b''):h.update(chunk)
    else:
        with Path(uri).open('rb') as stream:
            for chunk in iter(lambda:stream.read(1024*1024),b''):h.update(chunk)
    if h.hexdigest()!=expected_sha256:raise ValueError('TEAM_PARQUET_INPUT_INTEGRITY')
    return h.hexdigest()
