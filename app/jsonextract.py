import json
import boto3
import pandas as pd

#init s3
bucket_name = 'data21-final-project'
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
bucket = s3_resource.Bucket(bucket_name)


def return_bucket_json_body(bucket_name, key):
    """takes bucket_name and key
    returs json row of body"""
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    contents = obj["Body"].read().decode('utf8')
    return json.loads(contents)


def get_bucket_json_key_list():
    """returns a list of pages(another list) of tuples (bucket_name, key)
    page size set using chunk"""
    keylist = []
    for bucket_object in bucket.objects.all():
        if str(bucket_object.key[-5:]) == '.json':
            keylist.append((bucket_object.bucket_name, bucket_object.key))
    outlist = []
    for i in range(0, len(keylist), 300):
        chunk = keylist[i:i + 300]
        outlist.append(chunk)
    return outlist


def json_file_to_dataframe(keylist):
    """takes a list of tuples (bucket_name, key)
    outputs appended dataframe of all rows of files from within list"""
    rows_df = pd.DataFrame(columns=['name', 'date', 'tech_self_score',
                                    'strengths', 'weaknesses',
                                    'self_development', 'geo_flex',
                                    'financial_support_self', 'result',
                                    'course_interest'])
    for tuple in keylist:
        row = return_bucket_json_body(tuple[0], tuple[1])
        rows_df = rows_df.append(row, ignore_index=True)
    return rows_df


def yield_pages():
    """yields each dataframe from s3 pages
    gets list of pages of keys
    creates a dataframe per page"""
    for page in get_bucket_json_key_list():
        yield json_file_to_dataframe(page)


print(type(next(yield_pages())))
#sum(len(i) for i in get_bucket_json_key_list())