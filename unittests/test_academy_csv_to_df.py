import pytest
from app.classes.boto3_academy_csv_load_into_df_pydict import *

def test_return_academy_csv_df():
    for value in academy_csv_df_dict.values():
        assert isinstance(value, pd.DataFrame)
        
def test_got_all_academy_csv():
    assert len(academy_csv_df_dict) == 36

def test_return_talent_csv_df():
    for value in talent_csv_df_dict.values():
        assert isinstance(value, pd.DataFrame)

def test_got_all_talent_csv():
    assert len(talent_csv_df_dict) == 12