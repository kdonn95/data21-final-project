from app.classes.text_file_pipeline import TextFilePipeline

text_pl = TextFilePipeline("Data21Final")


def test_get_txt_file():
    bucket_name = "data21-final-project"
    assert type(text_pl.get_txt_file_key_list(bucket_name)) is list
    assert text_pl.get_txt_file_key_list(bucket_name) != []
    assert len(text_pl.get_txt_file_key_list(bucket_name)) == 152
