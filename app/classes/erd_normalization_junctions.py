from app.classes.csv_data_transform_erd import TransformCSVdataFrames
import pandas as pd
from app.classes.logger import Logger
from sqlalchemy.orm import sessionmaker
from app.classes.transform_applicants_csv import candidate_df
# 'candidate_df' = Talent directory CSV dataframe with 'Aug2019Applicants.csv', etc.


# need to rearrange csv_data_transform_ERD tables into tables as show in Sparta ERD
class SpartaERDFormat(Logger):
    def __init__(self, engine, logging_level):
        Logger.__init__(self, logging_level)
        self.transformed_df = TransformCSVdataFrames(logging_level)
        self.scores_table, self.courses_table = self.transformed_df.academy_csv_scores_and_course_dfs_setup()
        self.candidates_table = candidate_df

        # Setting up connection to sql server.
        self.engine = engine
        factory = sessionmaker(bind=self.engine)
        self.session = factory()
        self.session.expire_on_commit = False

    # get ERD 'course type' table
    def make_course_type_table(self):
        # need unique course types for this table
        course_type = pd.DataFrame(self.courses_table['course_type'].unique(), columns=['course_type'])
        # get index from DF autogenerated row numbers
        course_type["course_type_id"] = course_type.index + 1
        # put index at front
        cols = ["course_type_id", "course_type"]
        self.log_print(course_type[cols], 'DEBUG')
        self.log_print('Debugging COURSE_TYPE table dataframe', 'INFO')
        return course_type[cols]

    # get ERD 'course' table
    def make_course_table(self):
        course_type = self.make_course_type_table()
        # note: courses_table cols: ['course_type', 'course_name', 'start_date', 'duration']
        course = course_type.merge(self.courses_table, left_on='course_type', right_on='course_type')
        # done with 'course_type' column and it's not in this table per the ERD, so let's drop it
        # course.drop(columns=['course_type'], inplace=True)
        # get index from DF autogenerated row numbers
        course["course_id"] = course.index + 1
        # put index at front
        course_cols = [course.columns.tolist()[-1]]+course.columns.tolist()[0:-1]
        self.log_print(course[course_cols], 'DEBUG')
        self.log_print('Debugging COURSE table dataframe', 'INFO')
        return course[course_cols]

    # get ERD 'weekly performance' table
    def make_weekly_performance_table(self):
        course = self.make_course_table()
        # get candidates' names
        candidates_table_trunc = pd.DataFrame(self.candidates_table['candidate_name'].unique(),
                                              columns=['candidate_name'])
        # get 'candidate_id' column values from SQL row-by-row
        for index, row_data in candidates_table_trunc.iterrows():
            # get 'candidate_name' from local
            candidate_name = row_data['candidate_name'].replace("'","''")
            # get 'candidate_id' from SQL via engine
            candidate_id = self.engine.execute(
                f"SELECT candidate_id FROM candidate WHERE candidate_name = '{candidate_name}'").fetchone()
            # append new column to row in DF
            candidates_table_trunc.loc[index, 'candidate_id'] = candidate_id
        # get score information
        weekly_performance = self.scores_table.merge(course, left_on='course_name', right_on='course_name')
        # test outputs
        weekly_perf_cols = ['spartan_name', 'course_id', 'week_no', 'Analytic', 'Independent', 'Determined',
                            'Professional', 'Studious', 'Imaginative', 'course_name']
        weekly_performance = weekly_performance[weekly_perf_cols]
        weekly_performance.rename(columns={'spartan_name': 'candidate_name'}, inplace=True)
        # remove candidate_name, replace with candidate ID from candidates table
        weekly_performance = weekly_performance.merge(candidates_table_trunc, left_on='candidate_name',
                                                      right_on='candidate_name')
        weekly_perf_cols_final = ['candidate_id', 'course_id', 'week_no', 'Analytic', 'Independent', 'Determined',
                                  'Professional', 'Studious', 'Imaginative', 'course_name']
        self.log_print(weekly_performance[weekly_perf_cols_final], 'DEBUG')
        self.log_print('Debugging WEEKLY_PERFORMANCE table dataframe', 'INFO')
        return weekly_performance[weekly_perf_cols_final]

    def make_staff_table_entries(self):
        staff_table_entries = pd.DataFrame(self.scores_table['trainer_name'].unique(), columns=['staff_name'])
        staff_table_entries['department'] = 'training'
        return staff_table_entries

    def make_staff_course_junc_no_ids(self):
        staff_course_df = pd.DataFrame(self.courses_table['course_name'],
                                       columns=['course_name'])
        staff_course_df['staff_name'] = self.scores_table['trainer_name']
        staff_df = self.make_staff_table_entries()
        course_df = self.make_course_table()
        staff_course_df.merge(staff_df, left_on='staff_name', right_on='staff_name')
        staff_course_df.merge(course_df, left_on='course_name', right_on='course_name')
        # make sure only two columns
        temp_cols = ['course_name', 'staff_name']
        # also, only want unique course-staff combinations, so drop any duplicates
        staff_course_df = staff_course_df[temp_cols]
        staff_course_df = staff_course_df.drop_duplicates()
        return staff_course_df  # DF of unique combos of course names and (training) staff names



    def get_ids_for_staff_course_junc(self):
        staff_id_name_list = list(self.engine.execute("""SELECT staff_id, staff_name FROM staff"""))
        course_id_name_list = list(self.engine.execute("""SELECT course_id, course_name FROM course"""))
        things_to_match_df = self.make_staff_course_junc_no_ids()
        things_to_match_df['course_id'] = [0 for i in range(len(things_to_match_df))]
        things_to_match_df['staff_id'] = [0 for i in range(len(things_to_match_df))]
        for index in things_to_match_df.index():
            course_name = things_to_match_df.loc[index, 'course_name']
            staff_name = things_to_match_df.loc[index, 'staff_name']
            for i in staff_id_name_list:
                if staff_name == i[1]:
                    staff_id = i[0]
                    break
            for j in course_id_name_list:
                if course_name == j[1]:
                    course_id = j[0]
                    break
            things_to_match_df.loc[index, 'staff_id'] = course_id
            things_to_match_df.loc[index, 'course_id'] = staff_id
        things_to_match_df.drop(columns=['staff_name', 'course_name'], inplace=True)
        return things_to_match_df  # should have junction DF of IDs now