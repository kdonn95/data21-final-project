    # transform csv dataframes to be like SQL target schema
    # Academy/ sub-dir: files represent academy courses
    """
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
    def academy_course_table_df_create_empty(self, dict_of_dfs_to_transform):
        # spartan_table_df_cols = ['spartan_name']
        # trainer_table_df_cols = ['trainer_name']
        # split these later via dataframe properties: new = old.filter(['A','B','D'], axis=1)
        #    and df.drop(columns=['B', 'C'])
        course_table_df_cols = ['course_name', 'course_type', 'course_start_date']
        scores_table_df_cols = []
        course_table_df = pd.DataFrame(columns=course_table_df_cols)

        for df_key in dict_of_dfs_to_transform:
            df_to_transform = dict_of_dfs_to_transform[df_key]
            # course data retrieval - simple
            # fields contained in key
            ac_fields = df_key.split('_')
            # for loop in case a course has multiple trainers
            course_table_row = [ac_fields[0]+ac_fields[1],ac_fields[0],ac_fields[-1]]
            course_table_df = course_table_df.append(pd.DataFrame(course_table_row, columns=course_table_df_cols), ignore_index=True)

            for col_name in list(df_to_transform.columns):
                # 'Analytic_W1', 'Independent_W1', 'Determined_W1', 'Professional_W1', 'Studious_W1', 'Imaginative_W1', 'Analytic_W2'
                if '_W' in col_name:
                    if col_name.split('_')[0] not in scores_table_df_cols:
                        scores_table_df_cols.append(col_name.split('_')[0])
            scores_table_df = pd.DataFrame(columns=['spartan_name', 'trainer_name', 'week_number']+scores_table_df_cols)
            for score_type in scores_table_df_cols:


    # Talent/ sub-dir: files represent assessments by month

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
"""
def talent_month_table_df_create_empty(self, df_key, df_to_transform):
    candidate_table_df_cols = []
    candidate_table_df_cols.list(df_to_transform.columns)[1:]

def select_transformation(self):
    if self.s3_sub_dir == 'Academy/':
        academy_course_df_transform()
    elif self.s3_sub_dir == 'Talent/':
        talent_month_df_transform()
