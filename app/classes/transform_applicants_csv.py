from string import punctuation
import pandas as pd
import re
from datetime import datetime
from tabulate import tabulate
from pprint import pprint
from fuzzywuzzy import process
import string

from boto3_academy_csv_load_into_df_pydict import GetS3CSVinfo


import logging
# removing logging pointer from root
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

# loggind to file

file_handler = logging.FileHandler('applicants_CSVs_df_transform.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# loggin to console
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)




class transformAppCSV:


      def __init__(self):

         self.talent_csv_info_getter = GetS3CSVinfo('data21-final-project', 'Talent/')
         self.talent_csv_df_dict = self.talent_csv_info_getter.create_dict_of_csv_pd_dataframes()
         
      def transform_dfs(self):

         for key in self.talent_csv_df_dict.keys():
            
            logger.info(f'Transforming the {key} dataframe')


            # cleaning the phone_number
            self.talent_csv_df_dict[key]['phone_number'] = self.talent_csv_df_dict[key]['phone_number'].str.replace(r'[^+\w]','',regex=True)
            
            # logger.info on incorrectly formated phone numbers
            for row in self.talent_csv_df_dict[key].itertuples():
               if type(row.phone_number) != float:
                  if row.phone_number[1:13].isdigit() == False or row.phone_number[0] != '+':
                     logger.debug(f"Wrongly formated phone_number column in {key}")
            #----------------------------------------------------------------------------------------------------------------------------------------------
            # cleaning the name
            self.talent_csv_df_dict[key][['name']] = self.talent_csv_df_dict[key]['name'].str.title()
            self.talent_csv_df_dict[key]['name'] = self.talent_csv_df_dict[key]['name'].str.replace(' - ','-',regex=True)
            self.talent_csv_df_dict[key]['name'] = self.talent_csv_df_dict[key]['name'].str.replace("' ",'',regex=True)
            
            
            #----------------------------------------------------------------------------------------------------------------------------------------------
            # cleaning the staff_names
            self.talent_csv_df_dict[key][['invited_by']] = self.talent_csv_df_dict[key]['invited_by'].str.title()
            self.talent_csv_df_dict[key]['invited_by'] = self.talent_csv_df_dict[key]['invited_by'].str.replace('Bruno Belbrook','Bruno Bellbrook',regex=True)
            self.talent_csv_df_dict[key]['invited_by'] = self.talent_csv_df_dict[key]['invited_by'].str.replace('Fifi Etton','Fifi Eton',regex=True)
            
            
            #----------------------------------------------------------------------------------------------------------------------------------------------
            # making gender title case to keep them constant 
            self.talent_csv_df_dict[key][['gender']] = self.talent_csv_df_dict[key]['gender'].str.title()
       
            # making citytitle case to keep them constant 
            self.talent_csv_df_dict[key][['city']] = self.talent_csv_df_dict[key]['city'].str.title()
            #----------------------------------------------------------------------------------------------------------------------------------------------
            # cleaning the university
            self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.replace('-',',',regex=True)
            self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.replace(' ,',',',regex=True)
            self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.replace('´',"'",regex=True)
            #self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.replace('´S',"'s",regex=True)
            self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.lower()
            self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.title()
            self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.replace("'S","'s",regex=True)
            self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.replace("St\.","St ",regex=True)
            self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.replace("Saint George's","St George's",regex=True)
            # cleaning the email 
            self.talent_csv_df_dict[key]['email'] = self.talent_csv_df_dict[key]['email'].str.replace(';','',regex=True)
            #----------------------------------------------------------------------------------------------------------------------------------------------
            # cleaning the invited_date 
            self.talent_csv_df_dict[key]['invited_date'] = self.talent_csv_df_dict[key]['invited_date'].astype(str).str.replace('.0','',regex=True)
            self.talent_csv_df_dict[key]['month_short'] = self.talent_csv_df_dict[key]['month'].str.slice(0, 3)
            self.talent_csv_df_dict[key]['invite_year'] = self.talent_csv_df_dict[key]['month'].str.extract(r'\b(\w+)$', expand=True)
               # combining the invited date 
            self.talent_csv_df_dict[key]['invite_date'] = (self.talent_csv_df_dict[key]['invite_year']+' '+\
                                                      self.talent_csv_df_dict[key]['month_short']+' '+\
                                                      self.talent_csv_df_dict[key]['invited_date']).astype('datetime64')
            
            #----------------------------------------------------------------------------------------------------------------------------------------------
            # cleaning the date of birth column 
            self.talent_csv_df_dict[key]['dob'] = pd.to_datetime(self.talent_csv_df_dict[key]['dob'], format='%d/%m/%Y')
          


            # dropping unwanted columns to keep only the data we want 
            self.talent_csv_df_dict[key].drop(columns=['id','invited_date','month_short','invite_year','month'],axis=1, inplace=True)


            #renaming columns to align with ERD
            self.talent_csv_df_dict[key].rename(columns={'name': 'candidate_name',
                          'uni': 'uni_name',
                          'degree': 'degree_result',
                          'invite_date': 'date',
                          'invited_by': 'staff_name'},inplace=True)


            # logging duplicates based on name, dob and address
            if self.talent_csv_df_dict[key][self.talent_csv_df_dict[key].duplicated(subset=['candidate_name','dob','address','date'])].empty == False:
               logger.warning(f" Duplicated rows: {self.talent_csv_df_dict[key][self.talent_csv_df_dict[key].duplicated(subset=['candidate_name','dob','address','date'])]}")  

          

            # logging any dataframes with incorectly named columns 
            erd_cols = ['candidate_name', 'gender', 'dob', 'email', 'city', 'address', 'postcode', 'phone_number', 'uni_name', 'degree_result', 'staff_name', 'date']
            if erd_cols.sort() != (list(self.talent_csv_df_dict[key].columns)).sort():
               logger.debug(f'{key} df has incorrect columns: {list(self.talent_csv_df_dict[key].columns)}')
            
            #loggin any rows with no candidate contact information 
            cols = ['candidate_name','phone_number','email']
            if self.talent_csv_df_dict[key][self.talent_csv_df_dict[key][cols].isna().all(1)].empty == False:
               logger.warning(f'Rows with no condidate contact info {self.talent_csv_df_dict[key][self.talent_csv_df_dict[key][cols].isna().all(1)]}')
            


            # logging any staff names that are similar to check for mispellings
            def get_matches(name, column,limit = 500):

               results = process.extract(name,column, limit=limit)
               for result in results:
                  if result[1] < 100  and result[1] > 90:
                     return result

            for name in self.talent_csv_df_dict[key]['staff_name'].unique():
               match = get_matches(str(name),self.talent_csv_df_dict[key]['staff_name'])
               if match != None:
                  logger.warning(f'{name} is {match[1]}% similar to {match[0]} ')

            #logging the dataframes 
            logger.debug(f'\n{list(self.talent_csv_df_dict[key].columns)}')  
            logger.debug(f'{key}\n{tabulate(self.talent_csv_df_dict[key])}')

         return self.talent_csv_df_dict

      

# PRINTING RESULTS
with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # so that console doesn't truncate dataframe results -->> print all 0-n rows! 
   pd.set_option("expand_frame_repr",True)

   df_dict =  transformAppCSV()
   df_dict = df_dict.transform_dfs()
  
   for key in df_dict.keys():
      pass
      pprint(df_dict['Jan2019Applicants'])