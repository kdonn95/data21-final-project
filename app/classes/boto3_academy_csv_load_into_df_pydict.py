import boto3
#from pprint import pprint
import pandas as pd
#from db/db_session import engine


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
print(talent_csv_df_dict.keys())

Sept2019_Applicants_df = talent_csv_df_dict['Sept2019Applicants']
Business30_20191230_df = academy_csv_df_dict['Business_30_2019-12-30']
print(list(Sept2019_Applicants_df.columns))

print(list(Business30_20191230_df.columns))

Sept2019_Applicants_ToCandidate_df = Sept2019_Applicants_df['name', 'gender', 'dob', 'email', 'city', 'address', 'postcode', 'phone_number', 'uni', 'degree', 'invited_date','month', 'invited_by']
Sept2019_Applicants_ToCandidate_df.to_sql('Candidate')
Business30_20191230_df = Business30_20191230_df
Business30_20191230_df.to_sql()
"""
Sept2019Applicants.csv columns => db class.columns
id = --delete, autogenerate new ones--,
name = Candidate.candidate_name,
gender = Candidate.gender,
dob = Candidate.dob,
email = Candidate.email,
city = Candidate.city,
address = Candidate.address,
postcode = Candidate.postcode,
phone_number = Candidate.phone_number,
uni	degree = Candidate.uni_degree,
invited_date = Candidate.invited_date,
month = Candidate.invited_date,
invited_by = Candidate.invited_by

Business_30_2019-12-30.csv columns => db class.columns
'name',
'trainer',
'Analytic_W1' = Scores.analytic and Scores.week_no,
'Independent_W1' = Scores.independent and Scores.week_no,
'Determined_W1' = Scores.determined and Scores.week_no,
'Professional_W1' = Scores.professional and Scores.week_no,
'Studious_W1' = Scores.studious and Scores.week_no,
'Imaginative_W1' = Scores.imaginative and Scores.week_no,
same for all other columns (different Scores.week_no value)
"""
