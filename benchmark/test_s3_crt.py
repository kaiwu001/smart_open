import botocore
import smart_open
import hashlib
import boto3
from smart_open.s3 import CRTclient
import time
import io
crt={}
boto_session = botocore.session.Session()
boto_session.set_config_variable("tcp_keepalive", "true")
crt['session'] = boto_session
client = CRTclient(boto_session.get_config_variable('region'))
crt["client"] = client
url = "s3://waabi-live-training-datasets-us-east-1/staging/pandar_hwy101_curated_v1_human_labelled_train_2023_07_07/00000000-0000-0000-0000-000000000020.tess"
with smart_open.open(
    url, mode="rb", transport_params=crt
) as fp:
        s1 = time.time()

        fp.seek(512)
        t1 = time.time()
        buf = fp.read(19247284)
        t2 = time.time()
        #hash_crt1 = hashlib.md5(buf).hexdigest()
        fp.seek(20247284)
        t3 = time.time()
        buf = fp.read(12247284)
        t4 = time.time()
        print('crt',t1-s1,t2-t1,t3-t2,t4-t1)
        hash_crt2 = hashlib.md5(buf).hexdigest()
s3 = {}
with smart_open.open(
    url, mode="rb", transport_params=s3
) as fp:
        s2 = time.time()

        fp.seek(512)
        t1 = time.time()
        buf = fp.read(19247284)
        t2 = time.time()
        #hash_crt1 = hashlib.md5(buf).hexdigest()
        fp.seek(20247284)
        t3 = time.time()
        buf = fp.read(12247284)
        t4 = time.time()
        print('s3',t1-s2,t2-t1,t3-t2,t4-t1)
        hash_2 = hashlib.md5(buf).hexdigest()
        assert(hash_2==hash_crt2)
s3 = time.time()
print('crt_time_used',s2-s1)
print('original_time_used',s3-s2)
print('all hash is correct',hash_crt2,hash_2)
s3r = boto3.resource('s3')
bucket = s3r.Bucket("waabi-live-training-datasets-us-east-1")
def do_boto3_download(key: str):
    bytes_buffer = io.BytesIO()
    s3r.meta.client.download_fileobj(Bucket="waabi-live-training-datasets-us-east-1", Key=key, Fileobj=bytes_buffer)
    return bytes_buffer
b1 = time.time()
do_boto3_download('staging/pandar_hwy101_curated_v1_human_labelled_train_2023_07_07/00000000-0000-0000-0000-000000000020.tess')
print('boto3',time.time()-b1)

