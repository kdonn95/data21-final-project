"""
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

"""

"""
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