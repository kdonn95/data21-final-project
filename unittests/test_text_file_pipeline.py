from app.classes.text_file_pipeline import TextFilePipeline
from app.main import engine
import pandas as pdpytest
from datetime import datetime

pipeline = TextFilePipeline(engine, "INFO")
bucket_name = "data21-final-project"
key = "Talent/Sparta Day 10 January 2019.txt"

def test_get_txt_file():
    assert isinstance(pipeline.get_txt_file_key_list(bucket_name), list)
    assert pipeline.get_txt_file_key_list(bucket_name) != []
    assert len(pipeline.get_txt_file_key_list(bucket_name)) == 152

def test_transform_string_to_int():
    df = pipeline.text_to_dataframe(bucket_name, key)
    result = pipeline.transform_string_to_int(df, ['psychometrics', 'psychometrics_max',
                                        'presentation', 'presentation_max'])
    assert isinstance(result.iloc[2]['presentation'], int)