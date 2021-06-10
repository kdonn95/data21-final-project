import pytest
from app import *

def test_return_df():
    for value in academy_csv_data_dict.values():
        assert isinstance(value, pd.DataFrame)

def test_got_all_csv():
    assert len(academy_csv_data_dict) == 36