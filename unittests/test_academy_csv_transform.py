from app.classes.csv_data_transform_erd import TransformCSVdataFrames
import pandas as pd
import unittest



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
