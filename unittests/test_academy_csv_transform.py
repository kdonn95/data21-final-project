from app.classes.csv_data_transform_erd import TransformCSVdataFrames
from app.classes.transform_applicants_csv import candidate_df
import pandas as pd
import unittest

#course_info_df = courses_table
#scores_df = scores_table
#candidate_info_df = candidates_table

scores_df, course_info_df = TransformCSVdataFrames.academy_csv_scores_and_course_dfs_setup()
candidate_info_dfe = TransformCSVdataFrames.talent_csv_scores_and_course_dfs_setup()

class DataFrameTests(unittest.TestCase):

    def test_is_dataframe(self):
        self.assertTrue(type(course_info_df) == pd.DataFrame)
        self.assertTrue(type(scores_df) == pd.DataFrame)
        self.assertTrue(type(candidate_info_df) == pd.DataFrame)

    def test_shape(self):
        self.assertEqual(course_info_df.shape, (36, 4))

    def test_nColumns(self):
        self.assertEqual(len(scores_df.columns.tolist()), 10)
