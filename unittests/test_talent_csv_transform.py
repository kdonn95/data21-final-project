from app.classes.boto3_academy_csv_load_into_df_pydict import GetS3CSVinfo
from app.classes.csv_data_transform_to_ERD import transformCSVdataFrames
import pandas as pd
import pytest

academy = GetS3CSVinfo('data21-final-project', 'Academy/')
talent = GetS3CSVinfo('data21-final-project', 'Talent/')
academy_df_dict = academy.create_dict_of_csv_pd_dataframes()
talent_df_dict = talent.create_dict_of_csv_pd_dataframes()

transformation = transformCSVdataFrames(academy_df_dict, talent_df_dict)

def test_talent_csv_new_df_setup():
    result = transformation.talent_csv_new_df_setup()
    assert result.shape == (4691, 12)
    assert isinstance(result, pd.DataFrame)