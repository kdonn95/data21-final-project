from app.classes import boto3_academy_csv_load_into_df_pydict
from app.classes.boto3_academy_csv_load_into_df_pydict import *
from datetime import *
import pandas as pd

Sept2019_Applicants_df['invited_date'] = Sept2019_Applicants_df['invited_date'].fillna(0)
Sept2019_Applicants_df.invited_date = Sept2019_Applicants_df.invited_date.astype(int)

# print(Sept2019_Applicants_df['invited_date'])
Sept2019_Applicants_df['month'] = Sept2019_Applicants_df['month'].fillna(0)


# print(Sept2019_Applicants_df['month'])

def format_date(el):
    if el !='00':
        return datetime.strptime(el, '%d%bT %Y').strftime('%d-%m-%Y')


Sept2019_Applicants_df.invited_date = Sept2019_Applicants_df.invited_date.astype(
    str) + Sept2019_Applicants_df.month.astype(str)
for date in Sept2019_Applicants_df.invited_date:
    if date != '00':
        date=format_date(date)
print(Sept2019_Applicants_df.invited_date)
