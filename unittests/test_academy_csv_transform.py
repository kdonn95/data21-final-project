from app.classes.csv_data_transform_ERD import courses_table, scores_table, candidates_table
import pandas as pd
import unittest

course_info_df = courses_table
scores_df = scores_table
candidate_info_df = candidates_table


class DataFrameTests(unittest.TestCase):

    def test_is_dataframe(self):
        self.assertTrue(type(course_info_df) == pd.DataFrame)
        self.assertTrue(type(scores_df) == pd.DataFrame)
        self.assertTrue(type(candidate_info_df) == pd.DataFrame)

    def test_shape(self):
        self.assertEqual(course_info_df.shape, (36, 4))

    def test_nColumns(self):
        self.assertEqual(len(scores_df.columns.tolist()), 10)
