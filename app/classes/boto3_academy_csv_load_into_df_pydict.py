import boto3
import pandas as pd
"""
import sqlalchemy
server = 'localhost,1433'
database = 'Northwind'
user = 'SA'
password = 'Passw0rd2018'
driver = 'SQL+Server'
# ODBC+Driver+17+for+
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")
connection = engine.connect()
"""

class GetS3CSVinfo:
    # user manually sets S3 bucket name, sub-directory within that bucket
    def __init__(self, bucket_name, s3_sub_dir):
        # 'resource','client' APIs are built into Boto3
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = bucket_name
        self.s3_sub_dir = s3_sub_dir
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.s3_csv_keylist = self.get_list_of_csv_files()

    # Method creates a list of csv file locations with the specified class S3 bucket and subdirectory
    def get_list_of_csv_files(self):
        s3_csv_key_list = []
        filekeys_in_bucket_subdir = self.bucket.objects.filter(Prefix=self.s3_sub_dir)
        # keep track of the number of CSVs in S3 sub-directory
        csv_counter = 0
        for key_obj in filekeys_in_bucket_subdir:
            # only want CSVs in list!
            if key_obj.key[-4:] == '.csv':
                csv_counter += 1
                print(f'Found CSV file: {key_obj.key}')
                s3_csv_key_list.append(key_obj.key)
        print(f'Number of CSV files found in {self.bucket_name}/{self.s3_sub_dir} = {csv_counter}')
        return s3_csv_key_list

    def create_dict_of_csv_pd_dataframes(self):
        csv_dict_keyed_by_course = {}
        for s3_key in self.s3_csv_keylist:
            csv_s3_object = self.s3_client.get_object(
                Bucket=self.bucket_name,  # bucket_name is a string
                Key=s3_key  # s3_key is a string
            )
            # string splitting to return course name & date from CSV filename, e.g. 'Business_29_2019-11-18',
            # 'Sept2019Applicants' as key for dictionary whose values are Pandas dataframes
            course_name_date = (s3_key.split('/')[-1]).split('.')[0]
            # read csv into pandas dataframe
            csv_dict_keyed_by_course[course_name_date] = pd.read_csv(csv_s3_object['Body'])
        return csv_dict_keyed_by_course


# s3://data21-final-project/ is the location of the CSVs we want here
# 'Academy/' and 'Talent/' are the sub-directories with CSVs
academy_csv_info_getter = GetS3CSVinfo('data21-final-project', 'Academy/')
talent_csv_info_getter = GetS3CSVinfo('data21-final-project', 'Talent/')

academy_csv_df_dict = academy_csv_info_getter.create_dict_of_csv_pd_dataframes()
#print(academy_csv_df_dict)

talent_csv_df_dict = talent_csv_info_getter.create_dict_of_csv_pd_dataframes()
#print(talent_csv_df_dict)
#print(talent_csv_df_dict.keys())

Sept2019_Applicants_df = talent_csv_df_dict['Sept2019Applicants']
Business30_20191230_df = academy_csv_df_dict['Business_30_2019-12-30']
#print(list(Sept2019_Applicants_df.columns))
# strings and integer64 for scores in DF
# DAVID FEEDBACK: USE LOGGING, NOT PRINTING!!!!
#print(list(Business30_20191230_df.columns))
#print(type(Sept2019_Applicants_df['degree'][0]))

for index, row in Sept2019_Applicants_df.iterrows():
    print('test_city', row['city'], len(row.index), type(row.index.values))
# tested: DFs load nicely into SQL, ran a trial row for our big 'candidate' SQL table into local (Northwind) host
#Sept2019_Applicants_df.to_sql('Cand_test', engine)
"""
candidate_test_data = [[1029343, 'john doe'.title(), '19-04-2018', 1, 1, 1, 1, 'Data', 'Male', '05-08-1996',
                        'johndoe@fakeaddress.com', 'Narnia', '123 Fake street', 'AS14 4AN', '+44 1437 583539',
                        'Necromancy', '22-08', 'Trainer Aslan']]
candidate_col_names = ['candidate_id', 'candidate_name', 'date', 'self_development', 'geo_flex', 'financial_support',
                       'result', 'course_interest', 'gender', 'dob', 'email', 'city', 'address', 'postcode',
                       'phone_number', 'uni_degree', 'invited_date', 'invited_by']
candidate_test_df = pd.DataFrame(candidate_test_data, columns=candidate_col_names)
print(candidate_test_df)
candidate_test_df.to_sql('Candidate', engine, if_exists='append')
"""
