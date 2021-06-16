

from app.classes.json_transform import JsonTransform
# import orm
from app.classes.logger import Logger
from sqlalchemy.orm import sessionmaker
# run from main.py!!!

class JsonLoad(Logger):
    def __init__(self, engine, logging_level):
        # Initialise logging
        Logger.init(self, logging_level)
        # Setting up connection to sql server.
        self.engine = engine
        factory = sessionmaker(bind=self.engine)
        self.session = factory()
        self.session.expire_on_commit = False

    def check_candidate_exists(self, name):
        self.log_print("Checking if candidate exists", "INFO")
        isempty = self.engine.execute(f"SELECT * FROM candidate WHERE "
                                f"candidate_name = '{name}'").fetchall()
        self.log_print(isempty, "INFO")
        if isempty == []:
            return False
        else:
            self.log_print(f'{name} already exists', "FLAG")
            candidate_id = self.engine.execute(f"SELECT candidate_id FROM candidate WHERE "
                                f"candidate_name = '{name}'").fetchone()
            return candidate_id

"""
    def insert_candidate(self, name):

def academy_course_table_newdf_setup(self, dict_of_dfs_to_transform):
        for df_key in dict_of_dfs_to_transform:
            df_to_transform = dict_of_dfs_to_transform[df_key]
            for index, row in df_to_transform.iterrows():  # iterate over dataframe rows
                for index, row in Sept2019_Applicants_df.iterrows():
                    print('test_city', row['city'], len(row.index))  # row.index is a list of column names!!!
                -> use row.index("col_name")
                new_df_cols = ['spartan_name', 'trainer_name', 'week_number'] + scores_table_df_cols
                row_data_new_format = pd.DataFrame(
                    columns=new_df_cols)
                # row.index.values returns a numpy array!!!

                for i in range(len(row.index)):
                    # iterate through
                    if i < 2:
                        row_data_new_format[new_df_cols[i]] = row[row.index[i]]

                    else:  # weekly scores: need to
                        score_type_week = row.index[i]
                        score_type = score_type_week.split('_')[0]
                        week_num = score_type_week.split('_')[-1][-1]
                        row_data_new_format['week_number'] = week_num
                        row_data_new_format[score_type] = row[score_type_week] # this works for the first week, but not for following weeks


Talent/ sub-dir: files represent assessments by month

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
"""


"""
print(y.columns.tolist())
print(y)
print(x.columns.tolist())
print(tabulate(x.head(20)))
"""

#for index, row in Sept2019_Applicants_df.iterrows():
#    print('test_city', row['city'], len(row.index), type(row.index.values))
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

# scores_table_df_cols = ['Analytic', 'Independent', 'Determined', 'Professional', 'Studious', 'Imaginative']
# scores_table_df = pd.DataFrame(columns=['spartan_name', 'trainer_name', 'week_number'] + scores_table_df_cols)

#print(list(Sept2019_Applicants_df.columns))
# strings and integer64 for scores in DF
# DAVID FEEDBACK: USE LOGGING, NOT PRINTING!!!!
#print(academy_csv_df_dict)
#print(talent_csv_df_dict)
#print(talent_csv_df_dict.keys())
Sept2019_Applicants_df = talent_csv_df_dict['Sept2019Applicants']
Business30_20191230_df = academy_csv_df_dict['Business_30_2019-12-30']
#print(list(Business30_20191230_df.columns))
#print(type(Sept2019_Applicants_df['degree'][0]))

