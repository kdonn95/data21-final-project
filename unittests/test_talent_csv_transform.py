from app.classes.transform_applicants_csv import transformAppCSV
import pandas as pd
import pytest

transformation = transformAppCSV()

def test_talent_csv_new_df_setup():
    result = transformation.transform_dfs()
    assert result.shape == (4691, 12)
    assert isinstance(result, pd.DataFrame)