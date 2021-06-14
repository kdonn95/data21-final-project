from app.classes.boto3_academy_csv_load_into_df_pydict import talent_csv_df_dict
from tabulate import tabulate
from transform_applicants_csv import *
value=df_dict['Sept2019Applicants']['invite_date'][1]
#print(value)
#print(df_dict['April2019Applicants']['email'][1])
#print(df_dict['April2019Applicants']['phone_number'])
df_dict.keys()
# value=[]
for key in df_dict.keys():
    print(df_dict[key]['dob'])
