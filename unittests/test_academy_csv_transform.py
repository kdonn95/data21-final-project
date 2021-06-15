from app.classes.csv_data_transform_ERD import courses_table, scores_table, candidates_table
import pandas as pd
import pytest

course_info_df = courses_table
scores_df = scores_table


def test_is_dataframe():
    assert type(course_info_df), pd.DataFrame
    assert type(scores_df), pd.DataFrame


def test_shape():
    assert course_info_df.shape == (36, 4)
    assert scores_df.columns == 10