import pandas as pd
from boto3_csv_load_pd import academy_csv_info_getter, talent_csv_info_getter
from transform_applicants_csv import candidate_df
from tabulate import tabulate


class transformCSVdataFrames:
    # transform csv dataframes to be like SQL target schema
    def __init__(self, academy_csv_dfs_dict, talent_csv_dfs_dict):
        # 'resource','client' APIs are built into Boto3
        self.academy_csv_dfs_dict = academy_csv_dfs_dict
        self.talent_csv_dfs_dict = talent_csv_dfs_dict

    def academy_csv_scores_and_course_dfs_setup(self):
        # scores SQL table data retrieval - more difficult
        # scores_table_df_cols = ['Analytic', 'Independent', 'Determined', 'Professional', 'Studious', 'Imaginative']
        # scores_table_df = pd.DataFrame(columns=['spartan_name', 'trainer_name', 'week_number'] + scores_table_df_cols)
        course_table_df_cols = ['course_type', 'course_num', 'course_start_date', 'course_duration_weeks']
        course_table_df = pd.DataFrame(columns=course_table_df_cols)
        all_courses_new_df_is_empty = True
        all_courses_score_values_df = pd.DataFrame()
        for df_key in self.academy_csv_dfs_dict:
            df_to_transform = self.academy_csv_dfs_dict[df_key]
            ac_fields = df_key.split('_')

            old_columns = list(df_to_transform.columns)
            scores_weeks_dict = {}
            # grab spartan names
            # iterate through columns in old DF format
            max_week_num = 0
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
                    if week_num > max_week_num:
                        max_week_num = week_num
            # fields for new df contained in df_key
            ac_fields = df_key.split('_')
            course_table_row = [ac_fields[0], ac_fields[1], ac_fields[-1], max_week_num]
            course_row_series = pd.Series(course_table_row, index=course_table_df.columns)
            # dataframe to return: adding new row per df in dictionary with course info
            course_table_df = course_table_df.append(course_row_series, ignore_index=True)

            # concatenate dataframes in dictionary together, output is DF to return
            for week_key in sorted(scores_weeks_dict.keys()):
                # print(f'{ac_fields[0]}-{ac_fields[1]}, week {week_key}')
                if all_courses_new_df_is_empty:  # should be week 1 for first course in df_key's dataframe
                    all_courses_score_values_df = scores_weeks_dict[week_key]
                    # set flag to false: only want 1 dataframe, and want it to have all the data!
                    all_courses_new_df_is_empty = False
                else:
                    all_courses_score_values_df = pd.concat([all_courses_score_values_df,
                                                             scores_weeks_dict[week_key]], ignore_index=True)
        # course start date: convert to pandas date format
        course_table_df['course_start_date'] = pd.to_datetime(course_table_df['course_start_date']).dt.date
        # rearrange columns in all_courses_score_values_df
        current_col_list = all_courses_score_values_df.columns.to_list()
        new_cols_list = current_col_list[:2] + current_col_list[3:5] + [current_col_list[2]] + current_col_list[5:]
        all_courses_score_values_df = all_courses_score_values_df[new_cols_list]
        return all_courses_score_values_df, course_table_df

    def talent_csv_new_df_setup(self):
        
        final_big_candidate_df = candidate_df
        return final_big_candidate_df
      

    def identify_academy_dropout_rows(self, nice_format_df):
        for index, row_data in nice_format_df.iterrows():
            check_nulls = 0
            score_column_values = row_data[-6:]
            for score_field in score_column_values:
                if pd.isnull(score_field):
                    check_nulls += 1

            if check_nulls == len(score_column_values):
                nice_format_df.drop(index, inplace=True)
        nice_format_df.reset_index(drop=True, inplace=True)


academy_raw_csv_df_dict = academy_csv_info_getter.create_dict_of_csv_pd_dataframes()
talent_raw_csv_df_dict = talent_csv_info_getter.create_dict_of_csv_pd_dataframes()

x = transformCSVdataFrames(academy_raw_csv_df_dict, talent_raw_csv_df_dict)
scores_table, courses_table = x.academy_csv_scores_and_course_dfs_setup()
x.identify_academy_dropout_rows(scores_table)
candidates_table = x.talent_csv_new_df_setup()
print(tabulate(courses_table.head()))
print(tabulate(candidates_table.head()))
print(scores_table.columns)
print(tabulate(scores_table.head(20)))
print(pd.to_datetime(candidates_table['sparta_day_date']))

# spartan_table_df_cols = ['spartan_name']
# trainer_table_df_cols = ['trainer_name']
# split these later via dataframe properties: new = old.filter(['A','B','D'], axis=1)
#    and df.drop(columns=['B', 'C'])
