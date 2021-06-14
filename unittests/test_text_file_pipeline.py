from app.classes.text_file_pipeline import TextFilePipeline
from app.classes.db.db_session import global_init
from app.classes.get_config import GetConfig
import pandas as pd
import pandas.api.types as ptypes
from datetime import datetime

config = GetConfig()

conn_str = (
            f'mssql+pyodbc://{config.user}:{config.password}' +
            f'@{config.server}/master?driver={config.driver}'
            )

engine = global_init(conn_str, config.database, "INFO")
con = engine.connect()
pipeline = TextFilePipeline(engine)
bucket_name = "data21-final-project"
key = "Talent/Sparta Day 10 January 2019.txt"

def test_get_txt_file():
    assert isinstance(pipeline.get_txt_file_key_list(bucket_name), list)
    assert pipeline.get_txt_file_key_list(bucket_name) != []
    assert len(pipeline.get_txt_file_key_list(bucket_name)) == 152

def test_text_to_dataframe():
    result = pipeline.text_to_dataframe(bucket_name, key)
    assert isinstance(result, pd.DataFrame)
    date = datetime.strptime('10-01-2019', '%d-%m-%Y')
    date = date.strftime('%Y-%m-%d')
    mock_data = {
        'Name': ['', '', 'MEGGIE SIDSAFF'],
        'candidate_id': [0, 0, 0],
        'date': [date, date, date],
        'location': ['Birmingham Academy', 'Birmingham Academy', 'Birmingham Academy'],
        'psychometrics': ['43', '23', '52'],
        'psychometrics_max': ['100', '100', '100'],
        'presentation': ['12', '10', '18'],
        'presentation_max': ['32', '32', '32']
        }
    mock_result = pd.DataFrame(data=mock_data)
    assert mock_result.iloc[[2]].equals(result.iloc[[2]])

def test_transform_string_to_int():
    df = pipeline.text_to_dataframe(bucket_name, key)
    result = pipeline.transform_string_to_int(df, ['psychometrics', 'psychometrics_max',
                                        'presentation', 'presentation_max'])
    # columns_to_check = ['psychometrics', 'psychometrics_max',
    #                     'presentation', 'presentation_max']
    # assert all(ptypes.is_integer_dtype(result[col]) for col in columns_to_check)
    assert isinstance(result.iloc[2]['presentation'], int)
