import json
from pprint import pprint
import re
import boto3

bucket_name = 'data21-final-project'
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
bucket = s3_resource.Bucket(bucket_name)


def return_bucket_json_body(bucket_object):
    obj = s3_client.get_object(Bucket=bucket_object.bucket_name,
                               Key=bucket_object.key)
    contents = obj["Body"].read().decode('utf8')
    return json.loads(contents)


def bucket_json_finder():
    for bucket_object in bucket.objects.all():
        if str(bucket_object.key[-5:]) == '.json':
            yield return_bucket_json_body(bucket_object)


def rows_to_list():
    for i in bucket_json_finder():
        print(i)

rows_to_list()



def get_txt_file_key_list(self, bucket_name):
    # Extract all objects in the bucket specified by the user.
    files = self.s3_resource.Bucket(bucket_name).objects.all()

    # Creating an empty list to store the text file objects.
    txt_files = []

    # Looking through every object in the bucket and getting only the txt files.
    for file in files:
        if ".txt" == file.key[-4:]:
            txt_files.append(file.key)
    return txt_files

#pandas SQL