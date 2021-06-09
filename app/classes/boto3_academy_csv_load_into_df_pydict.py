import boto3
from pprint import pprint
import pandas as pd


class GetS3AcademyCSVinfo:
    def __init__(self, bucket_name, s3_sub_dir):
        self.s3_client = boto3.client('s3')
        # 'resource' api built into Boto3
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = bucket_name
        self.s3_sub_dir = s3_sub_dir
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.s3_csv_keylist = self.get_list_of_csv_files()

    # Method creates a list of csv file locations with the specified class S3 bucket and subdirectory
    def get_list_of_csv_files(self):
        s3_csv_key_list = []
        filekeys_in_bucket_subdir = self.bucket.objects.filter(Prefix=self.s3_sub_dir)
        csv_counter = 0
        for key_obj in filekeys_in_bucket_subdir:
            # only want CSVs in list!
            if key_obj.key[-4:] == '.csv':
                csv_counter += 1
                print(f'Found file: {key_obj.key}')
                s3_csv_key_list.append(key_obj.key)
        print(csv_counter)
        return s3_csv_key_list

    def create_dict_of_csv_pd_dataframe(self):
        csv_dict_keyed_by_course = {}
        for s3_key in self.s3_csv_keylist:
            csv_s3_object = self.s3_client.get_object(
                Bucket=self.bucket_name,  # bucket_name is a string
                Key=s3_key  # s3_key is a string
            )
            # string splitting to return course name & date from CSV filename, e.g. 'Business_29_2019-11-18'
            # as (string) key for dictionary whose values are Pandas dataframes
            course_name_date = (s3_key.split('/')[-1]).split('.')[0]
            csv_dict_keyed_by_course[course_name_date] = pd.read_csv(csv_s3_object['Body'])
        return csv_dict_keyed_by_course


# s3://data21-final-project/Academy/ is the location of the CSVs we want here
csv_info_getter = GetS3AcademyCSVinfo('data21-final-project', 'Academy/')
# ALW OUTPUT TEST 1
# print('OUTPUT test 1')
# print(csv_info_getter.s3_csv_keylist)

academy_csv_data_dict = csv_info_getter.create_dict_of_csv_pd_dataframe()
# ALW OUTPUT TEST 2
# print('OUTPUT test 2')
# print(len(academy_csv_data_dict))
# for key in academy_csv_data_dict.keys():
#     print(key, academy_csv_data_dict[key])

