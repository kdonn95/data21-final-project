import json
import boto3
import pandas as pd
from app.classes.logger import Logger


class JsonExtract(Logger):
    def __init__(self, used_keylist, logging_level,
                 bucket_name='data21-final-project'):
        Logger.__init__(self, logging_level)
        self.bucket_name = bucket_name
        self.s3_resource = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.used_keylist_in = used_keylist
        self.used_keylist_out = []
        self.keylist = self.get_bucket_json_key_list()

    def get_used_keylist(self):
        return self.used_keylist_out

    def get_bucket_json_key_list(self):
        """returns a list of pages(another list) of tuples (bucket_name, key)
        page size set using chunk"""
        keylist = []
        for bucket_object in self.bucket.objects.all():
            if str(bucket_object.key[-5:]) == '.json':
                keylist.append(bucket_object.key)
        self.log_print(f"get list of json file keys", 'INFO')
        new_keylist = list(set(keylist) ^ set(self.used_keylist_in))
        new_keylist.sort()
        outlist = []
        for i in range(0, len(new_keylist), 20):
            chunk = new_keylist[i:i + 20]
            outlist.append(chunk)
        self.used_keylist_out = new_keylist
        return outlist

    def return_bucket_json_body(self, key):
        """takes bucket_name and key
        returns json row of body"""
        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        contents = obj["Body"].read().decode('utf8')
        self.log_print(json.loads(contents), 'DEBUG')
        return json.loads(contents)

    def json_file_to_dataframe(self, keylist):
        """takes a key
        outputs appended dataframe of all rows of files from within list"""
        rows_df = pd.DataFrame(columns=['name', 'date', 'tech_self_score',
                                        'strengths', 'weaknesses',
                                        'self_development', 'geo_flex',
                                        'financial_support_self', 'result',
                                        'course_interest'])
        for key in keylist:
            row = self.return_bucket_json_body(key)
            rows_df = rows_df.append(row, ignore_index=True)
        self.log_print(f"create and fill json extract datafrme for one page",
                       'INFO')
        return rows_df

    def yield_pages(self):
        """yields each dataframe from s3 pages
        gets list of pages of keys
        creates a dataframe per page"""
        for page in self.keylist:
            self.log_print(f"yield page", 'INFO')
            yield self.json_file_to_dataframe(page)
