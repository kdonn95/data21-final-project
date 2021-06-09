import json
from pprint import pprint
import re
import boto3

bucket_name = 'data21-final-project'
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(bucket_name)


def extract_json():
    for s3_object in bucket.objects.all():
        if s3_object.endswith(".json", ):
            contents = s3_object["Body"].read()
            return json.loads(contents)
        else:
            raise Exception("JSON files not found.")




bucket = bucket.get_bucket(bucket_name)

for key in bucket.list():
    print(key.name.encode('utf-8'))