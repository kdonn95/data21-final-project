from app.classes.text_file_pipeline import TextFilePipeline
import pandas as pd

pipeline = TextFilePipeline('Data21Final')
bucket_name = "data21-final-project"
key = "Talent/Sparta Day 10 January 2019.txt"

def test_get_txt_file():
    assert isinstance(pipeline.get_txt_file_key_list(bucket_name), list)
    assert pipeline.get_txt_file_key_list(bucket_name) != []
    assert len(pipeline.get_txt_file_key_list(bucket_name)) == 152

def test_text_to_dataframe():
    result = pipeline.text_to_dataframe(bucket_name, key)
    assert isinstance(result, pd.DataFrame)


