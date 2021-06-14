import pandas as pd
import re
from datetime import datetime
from tabulate import tabulate
from pprint import pprint

from boto3_academy_csv_load_into_df_pydict import GetS3CSVinfo


class transformAppCSV:




      def __init__(self):

         self.talent_csv_info_getter = GetS3CSVinfo('data21-final-project', 'Talent/')
         self.talent_csv_df_dict = self.talent_csv_info_getter.create_dict_of_csv_pd_dataframes()
         # print(self.talent_csv_df_dict.keys())


      def transform_dfs(self):

         for key in self.talent_csv_df_dict.keys():
            print(key)
      
            # cleaning the phone_number
            self.talent_csv_df_dict[key]['phone_number'] = self.talent_csv_df_dict[key]['phone_number'].str.replace(r'[/^\s\(\-\)]','',regex=True)
            #----------------------------------------------------------------------------------------------------------------------------------------------
            # cleaning the name
            self.talent_csv_df_dict[key][['name']] = self.talent_csv_df_dict[key]['name'].str.title()
            self.talent_csv_df_dict[key]['name'] = self.talent_csv_df_dict[key]['name'].str.replace(' - ','-',regex=True)
            self.talent_csv_df_dict[key]['name'] = self.talent_csv_df_dict[key]['name'].str.replace("' ",'',regex=True)
            
            # making gender title case to keep them constant 
            self.talent_csv_df_dict[key][['gender']] = self.talent_csv_df_dict[key]['gender'].str.title()
       
            # making citytitle case to keep them constant 
            self.talent_csv_df_dict[key][['city']] = self.talent_csv_df_dict[key]['city'].str.title()
            #----------------------------------------------------------------------------------------------------------------------------------------------
            # cleaning the university
            self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.title()
            self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.replace('-',',',regex=True)
            self.talent_csv_df_dict[key]['uni'] = self.talent_csv_df_dict[key]['uni'].str.replace(' ,',',',regex=True)
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
            
            self.talent_csv_df_dict[key]['dob'] = pd.to_datetime(self.talent_csv_df_dict[key]['dob'], format='%d/%m/%Y')
            # applicants_dataframe[file]['dob'] = pd.to_datetime(applicants_dataframe[file]['dob'], format='%d/%m/%Y')


            # dropping unwanted columns to keep only the data we want 
            self.talent_csv_df_dict[key].drop(columns=['id','invited_date','month_short','invite_year','month'],axis=1, inplace=True)

            # # checking for duplicates based on name, dob and address
            # pprint(self.talent_csv_df_dict[key][self.talent_csv_df_dict[key].duplicated(subset=['name','dob','address'])])  
            # pprint(talent_csv_df_dict[key][talent_csv_df_dict[key].duplicated(subset=['address'])])  

            # dropping duplicates based on name, dob, address and invite_date
            self.talent_csv_df_dict[key] = self.talent_csv_df_dict[key].drop_duplicates(subset=["name","dob",'address','invite_date'])

            #sorting the Dataframe by the name column 
            self.talent_csv_df_dict[key] = self.talent_csv_df_dict[key].sort_values(by = 'name', ascending = True)

            # pprint(talent_csv_df_dict[key].columns)

            # dropping rows where name, phone_number, and email are null
            self.talent_csv_df_dict[key] = self.talent_csv_df_dict[key].dropna(subset=(['name','phone_number','email']),how='all')



         
         return self.talent_csv_df_dict

      # def d_o_b(applicants_dataframe):
      #    for file in applicants_dataframe.keys():
      #    return applicants_dataframe




# PRINTING RESULTS
with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # so that console doesn't truncate dataframe results -->> print all 0-n rows! 
   pd.set_option("expand_frame_repr",True)

   df_dict =  transformAppCSV()
   df_dict = df_dict.transform_dfs()

   for key in df_dict.keys():
      print(key) 
      # print(type(df_dict['Jan2019Applicants'].loc[2,'month'])) #just checking entry types
      print(tabulate(df_dict[key][['name', 'gender', 'dob', 'email', 'city', 'address','degree', 'invite_date']]))
   
      # print(tabulate(df_dict[key][['name','phone_number']]))
      # print("")
