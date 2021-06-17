from app.classes.csv_data_transform_erd import TransformCSVdataFrames
from app.classes.transform_applicants_csv import candidate_df
import pandas as pd
import unittest

academy_csv_info_getter = GetS3CSVinfo('data21-final-project', 'Academy/')
talent_csv_info_getter = GetS3CSVinfo('data21-final-project', 'Talent/')
academy_csv_df_dict = academy_csv_info_getter.create_dict_of_csv_pd_dataframes()
talent_csv_df_dict = talent_csv_info_getter.create_dict_of_csv_pd_dataframes()

scores_df, course_info_df = TransformCSVdataFrames.academy_csv_scores_and_course_dfs_setup(academy_csv_df_dict)
candidate_info_dfe = TransformCSVdataFrames.talent_csv_scores_and_course_dfs_setup()


class DataFrameTests(unittest.TestCase):

    def test_is_dataframe(self):
        self.assertTrue(type(course_info_df) == pd.DataFrame)
        self.assertTrue(type(scores_df) == pd.DataFrame)
        # self.assertTrue(type(candidate_info_df) == pd.DataFrame)

    def test_shape(self):
        self.assertEqual(course_info_df.shape, (36, 4))

    def test_nColumns(self):
        self.assertEqual(len(scores_df.columns.tolist()), 10)
