from app.classes.csv_data_transform_erd import TransformCSVdataFrames
from app.classes.boto3_csv_load_pd import GetS3CSVinfo
from app.classes.transform_applicants_csv import candidate_df
import pandas as pd
import unittest

academy_csv_info_getter = GetS3CSVinfo('data21-final-project', 'Academy/')
talent_csv_info_getter = GetS3CSVinfo('data21-final-project', 'Talent/')
academy_csv_df_dict = academy_csv_info_getter.create_dict_of_csv_pd_dataframes()
talent_csv_df_dict = talent_csv_info_getter.create_dict_of_csv_pd_dataframes()

scores_df, course_info_df = TransformCSVdataFrames.academy_csv_scores_and_course_dfs_setup(academy_csv_df_dict)
candidate_info_df = candidate_df


transform = TransformCSVdataFrames('INFO')

class DataFrameTests(unittest.TestCase):

    def test_is_dataframe(self):
        scores_df, course_df = transform.academy_csv_scores_and_course_dfs_setup()
        self.assertTrue(type(course_df) == pd.DataFrame)
        self.assertTrue(type(scores_df) == pd.DataFrame)

    def test_shape(self):
        scores_df, course_df = transform.academy_csv_scores_and_course_dfs_setup()
        self.assertEqual(course_df.shape, (36, 4))

    def test_nColumns(self):
        scores_df, course_df = transform.academy_csv_scores_and_course_dfs_setup()
        self.assertEqual(len(scores_df.columns.tolist()), 10)
