import pandas as pd
from app.classes.boto3_academy_csv_load_into_df_pydict import GetS3CSVinfo

academy = GetS3CSVinfo('data21-final-project', 'Academy/')
talent = GetS3CSVinfo('data21-final-project', 'Talent/')
academy_df = academy.create_dict_of_csv_pd_dataframes()
talent_df = talent.create_dict_of_csv_pd_dataframes()

def test_return_academy_csv_df():
    for value in academy_df.values():
        assert isinstance(value, pd.DataFrame)
        
def test_got_all_academy_csv():
    assert len(academy_df) == 36

def test_return_talent_csv_df():
    for value in talent_df.values():
        assert isinstance(value, pd.DataFrame)

def test_got_all_talent_csv():
    assert len(talent_df) == 12