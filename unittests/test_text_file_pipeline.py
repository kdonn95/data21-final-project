from app.classes.text_file_pipeline import TextFilePipeline
from app.classes.db.db_session import global_init
from app.classes.get_config import GetConfig
from app.main import engine
import pandas as pd
from datetime import datetime

# config = GetConfig()
# conn_str = (
#             f'mssql+pyodbc://{config.user}:{config.password}' +
#             f'@{config.server}/master?driver={config.driver}'
#             )

# engine = global_init(conn_str, config.database, 'NORMAL')
pipeline = TextFilePipeline(engine, "DEBUG")
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
        'location': [(1,), (1,), (1,)],
        'psychometrics': ['43', '23', '52'],
        'psychometrics_max': ['100', '100', '100'],
        'presentation': ['12', '10', '18'],
        'presentation_max': ['32', '32', '32']
        }
    mock_result = pd.DataFrame(data=mock_data)
    assert result.iloc[2].equals(mock_result.iloc[2])

def test_transform_string_to_int():
    df = pipeline.text_to_dataframe(bucket_name, key)
    result = pipeline.transform_string_to_int(df, ['psychometrics', 'psychometrics_max',
                                        'presentation', 'presentation_max'])
    assert isinstance(result.iloc[2]['presentation'], int)