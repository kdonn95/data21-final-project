import boto3
import json
import pandas as pdb


s3=boto3.client('s3')
list=s3.list_objects(Bucket='s3://data21-final-project/')['Contents']
for s3_key in list:
    s3_object = s3_key['Key']
    if not s3_object.endswith("/"):
        if s3_object.endswith(".json"):
            s3.download_file('bucket', s3_object, s3_object)
    else:
        import os
        if not os.path.exists(s3_object):
            os.makedirs(s3_object)
