import botocore
import smart_open
import torch
from torch.utils.data import Dataset, DataLoader
from functools import partial

import boto3
from smart_open.s3 import CRTclient
import time
import io
from multiprocessing.pool import Pool
class S3Dataset(torch.utils.data.IterableDataset):
    def __init__(self, urls,crt):
         self.urls = urls
         self.crt = crt

    def __iter__(self):
         for url in self.urls:
             with smart_open.open(
             url, mode="rb", transport_params=self.crt
             ) as fp:
                s1 = time.time()

                fp.seek(512)
                t1 = time.time()
                buf = fp.read(19247284)
                print(url,'crt',t1-s1)
                yield buf
def torch_test():
    prefix = "s3://waabi-live-training-datasets-us-east-1/staging/pandar_hwy101_curated_v1_human_labelled_train_2023_07_07/00000000-0000-0000-0000-00000000000"
    urls = [prefix+str(i)+".tess" for i in range(10)]
    crt={}
    boto_session = botocore.session.Session()
    boto_session.set_config_variable("tcp_keepalive", "true")

    crt['session'] = boto_session
    client = CRTclient(boto_session.get_config_variable('region'))
    crt["client"] = client
    dataset=S3Dataset(urls,crt)
    loader = DataLoader(dataset,number_workers=2)
    t = time.time()
    for item in loader:
        print(len(item))
    print('time took', time.time()-t)

# def testCRT(url):
#     crt={}
#     prefix = "s3://waabi-live-training-datasets-us-east-1/staging/pandar_hwy101_curated_v1_human_labelled_train_2023_07_07/00000000-0000-0000-0000-00000000000"
#     urls = [prefix+str(i)+".tess" for i in range(10)]
#     boto_session = botocore.session.Session()
#     boto_session.set_config_variable("tcp_keepalive", "true")
#
#     crt['session'] = boto_session
#     client = CRTclient(boto_session.get_config_variable('region'))
#     crt["client"] = client
#     with smart_open.open(
#         url, mode="rb", transport_params=crt
#         ) as fp:
#             s1 = time.time()
#             fp.seek(512)
#             t1 = time.time()
#             buf = fp.read(19247284)
#             t2 = time.time()
#             fp.seek(20247284)
#             t3 = time.time()
#             buf = fp.read(12247284)
#             t4 = time.time()
#             print(url,'crt',t1-s1,t2-t1,t3-t2,t4-t1)
def testGet(key):
    boto_session = botocore.session.Session()
    client = CRTclient(boto_session.get_config_variable('region'))
    client.get_object('waabi-live-training-datasets-us-east-1',key)
def testMultiGet():
    prefix = "/staging/pandar_hwy101_curated_v1_human_labelled_train_2023_07_07/00000000-0000-0000-0000-00000000000"
    urls = [prefix+str(i)+".tess" for i in range(10)]
    s1 = time.time()
    with Pool(5) as p:
        p.map(testGet, urls)
    print('testMultiGet total_time:',time.time()-s1)
# def testMulti():
#     crt={}
#     prefix = "s3://waabi-live-training-datasets-us-east-1/staging/pandar_hwy101_curated_v1_human_labelled_train_2023_07_07/00000000-0000-0000-0000-00000000000"
#     urls = [prefix+str(i)+".tess" for i in range(10)]
#     s1 = time.time()
#     with Pool(5) as p:
#         p.map(testCRT, urls)
#     print('total_time:',time.time()-s1)
def testboto():
    url= "s3://waabi-live-training-datasets-us-east-1/staging/pandar_hwy101_curated_v1_human_labelled_train_2023_07_07/00000000-0000-0000-0000-000000000020.tess"
    s3_config = {}
    with smart_open.open(
        url, mode="rb", transport_params=s3_config
    ) as fp:
            s2 = time.time()

            fp.seek(512)
            t1 = time.time()
            buf = fp.read(19247284)
            t2 = time.time()
            fp.seek(20247284)
            t3 = time.time()
            buf = fp.read(12247284)
            t4 = time.time()
            print('s3',t1-s2,t2-t1,t3-t2,t4-t1)
            s3 = time.time()
    print('original_time_used',s3-s2)

def testRange():
    with smart_open.open(
        url, mode="rb", transport_params=s3_config
        ) as fp:

        s2 = time.time()

        fp.get_object_range(512,512+19247284)
        t1 = time.time()
        out = fp.get_object_range(20247284,20247284+12247284)
        t2 = time.time()
        fp.get_object_range(30247284,30247284+19247284)
        t3 = time.time()
        print('s3 get_object_range',t1-s2,t2-t1,t3-t2)


def testDownload():
    s3r = boto3.resource('s3')
    bucket = s3r.Bucket("waabi-live-training-datasets-us-east-1")
    def do_boto3_download(key: str):
        bytes_buffer = io.BytesIO()
        s3r.meta.client.download_fileobj(Bucket="waabi-live-training-datasets-us-east-1", Key=key, Fileobj=bytes_buffer)
        return bytes_buffer
        b1 = time.time()
        do_boto3_download('staging/pandar_hwy101_curated_v1_human_labelled_train_2023_07_07/00000000-0000-0000-0000-000000000020.tess')
        print('boto3',time.time()-b1)
testMultiGet()

