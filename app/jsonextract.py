import boto3
import json
import pandas as pd

s3 = boto3.client('s3')

bucket_name = 'data21-final-project'


s3_resource = boto3.resource('s3')
bucket = s3.resource.Bucket(bucket_name)

# download file into current directory
for s3_object in bucket.objects.all():
    if s3_object.endswith(".json"):
        pass

