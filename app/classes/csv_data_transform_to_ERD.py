import pandas as pd
from boto3_academy_csv_load_into_df_pydict import academy_csv_info_getter, talent_csv_info_getter
from tabulate import tabulate

class transformCSVdataFrames:
    # transform csv dataframes to be like SQL target schema
    def __init__(self, academy_csv_dfs_dict, talent_csv_dfs_dict):
        # 'resource','client' APIs are built into Boto3
        self.academy_csv_dfs_dict = academy_csv_dfs_dict
        self.talent_csv_dfs_dict = talent_csv_dfs_dict

    """
    Academy/ sub-dir: files represent academy courses
    
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

    def academy_csv_course_names_df_setup(self):
        # course SQL table data retrieval - simple
        course_table_df_cols = ['course_type', 'course_num', 'course_start_date']
        course_table_df = pd.DataFrame(columns=course_table_df_cols)
        for df_key in self.academy_csv_dfs_dict:
            # fields for new df contained in df_key
            ac_fields = df_key.split('_')
            course_table_row = [ac_fields[0], ac_fields[1], ac_fields[-1]]
            # dataframe to return: adding new row per df in dictionary with course info
            # course_table_df = course_table_df.append(pd.DataFrame(course_table_row, columns=course_table_df_cols),
            #                                         ignore_index=True)
            course_table_df = course_table_df.append(course_table_row, ignore_index=True)

        course_table_df['course_start_date'] = pd.to_datetime(course_table_df['course_start_date']).dt.date
        return course_table_df

    def academy_csv_scores_df_setup(self):
        # scores SQL table data retrieval - more difficult
        # scores_table_df_cols = ['Analytic', 'Independent', 'Determined', 'Professional', 'Studious', 'Imaginative']
        # scores_table_df = pd.DataFrame(columns=['spartan_name', 'trainer_name', 'week_number'] + scores_table_df_cols)
        all_courses_new_df_is_empty = True
        all_courses_score_values_df = pd.DataFrame()
        for df_key in self.academy_csv_dfs_dict:
            df_to_transform = self.academy_csv_dfs_dict[df_key]
            ac_fields = df_key.split('_')

            old_columns = list(df_to_transform.columns)
            scores_weeks_dict = {}
            # grab spartan names
            # iterate through columns in old DF format
            for col_name in old_columns:
                # format: 'Analytic_W1', 'Independent_W1', 'Determined_W1', 'Professional_W1',
                # 'Studious_W1', 'Imaginative_W1', 'Analytic_W2', etc.
                if '_W' in col_name:
                    week_num = int(col_name.split('_')[-1][1:])
                    sparta_attribute = col_name.split('_')[0]
                    if week_num not in scores_weeks_dict.keys():
                        # create new df
                        scores_weeks_dict[week_num] = df_to_transform[['name', 'trainer', col_name]].copy()
                        # rename columns
                        scores_weeks_dict[week_num].rename(columns={'name': 'spartan_name', 'trainer': 'trainer_name',
                                                                    col_name: sparta_attribute}, inplace=True)
                        # add week and course columns to comply with ERD format
                        scores_weeks_dict[week_num]['Week'] = week_num
                        scores_weeks_dict[week_num]['Course'] = ac_fields[0] + ac_fields[1]
                    else:  # i.e., if new DF already has data for this week -> add this column to the new DF
                        scores_weeks_dict[week_num][sparta_attribute] = df_to_transform[col_name]
            # concatenate dataframes in dictionary together, output is DF to return
            for week_key in sorted(scores_weeks_dict.keys()):
                #print(f'{ac_fields[0]}-{ac_fields[1]}, week {week_key}')
                if all_courses_new_df_is_empty:  # should be week 1 for first course in df_key's dataframe
                    all_courses_score_values_df = scores_weeks_dict[week_key]
                    # set flag to false: only want 1 dataframe, and want it to have all the data!
                    all_courses_new_df_is_empty = False
                else:
                    all_courses_score_values_df = pd.concat([all_courses_score_values_df,
                                                             scores_weeks_dict[week_key]], ignore_index=True)
        return all_courses_score_values_df

    def talent_csv_new_df_setup(self):
        all_talent_new_df_is_empty = True
        final_big_candidate_df = pd.DataFrame()
        for df_key in self.talent_csv_dfs_dict:
            df_to_transform = self.talent_csv_dfs_dict[df_key]
            # format: 'id', 'name', 'gender', 'dob', 'email', 'city', 'address', 'postcode', 'phone_number', 'uni',
            # 'degree','invited_date','month','invited_by'
            one_key_output_df = df_to_transform.copy()
            one_key_output_df['sparta_day_date'] = one_key_output_df['invited_date'].astype('Int64').apply(str) + ' ' + \
                                                   one_key_output_df['month']
            one_key_output_df.drop(columns=['id', 'invited_date', 'month'], inplace=True)
            one_key_output_df.rename(columns={'invited_by': 'staff_inviter'}, inplace=True)
            #pd.to_datetime
            # new format: 'name', 'gender', 'dob', 'email', 'city', 'address', 'postcode', 'phone_number', 'uni',
            # 'degree','sparta_day_date','staff_inviter'
            if all_talent_new_df_is_empty:  # initialise output DF only in first instance
                final_big_candidate_df = one_key_output_df
                all_talent_new_df_is_empty = False
            else:  # otherwise, simply concat the current DF to to output DF
                final_big_candidate_df = pd.concat([final_big_candidate_df, one_key_output_df], ignore_index=True)
        final_big_candidate_df['sparta_day_date'] = pd.to_datetime(final_big_candidate_df['sparta_day_date']).dt.date
        final_big_candidate_df['dob'] = pd.to_datetime(final_big_candidate_df['dob']).dt.date
        final_big_candidate_df['phone_number'] = final_big_candidate_df['phone_number'].str.replace(r'[^+\w]', '', regex=True)
        return final_big_candidate_df


academy_raw_csv_df_dict = academy_csv_info_getter.create_dict_of_csv_pd_dataframes()
talent_raw_csv_df_dict = talent_csv_info_getter.create_dict_of_csv_pd_dataframes()

x = transformCSVdataFrames(academy_raw_csv_df_dict, talent_raw_csv_df_dict)
courses_table = x.academy_csv_course_names_df_setup()
scores_table = x.academy_csv_scores_df_setup()
candidates_table = x.talent_csv_new_df_setup()
print(tabulate(courses_table.head()))
print(tabulate(candidates_table.head()))
print(scores_table.columns)
print(tabulate(scores_table.head(20)))
#print(pd.to_datetime(candidates_table['sparta_day_date']))
#check_date_format = candidates_table.loc[pd.isnull(pd.to_datetime(candidates_table['sparta_day_date']))]
#print(check_date_format)
#for i in list(check_date_format.columns):
#    print(check_date_format[i].head())
# .dt.month_name() .unique()
#print(scores_table)


# spartan_table_df_cols = ['spartan_name']
# trainer_table_df_cols = ['trainer_name']
# split these later via dataframe properties: new = old.filter(['A','B','D'], axis=1)
#    and df.drop(columns=['B', 'C'])
