from app.classes.text_file_pipeline import TextFilePipeline

test = TextFilePipeline('Data21Final')

def test_get_txt_file():
    bucket_name = input('Choose which bucket you want the text files from:  ')
    assert bucket_name == 'data21-final-project'
    assert type(test.get_txt_file_key_list(bucket_name)) is list
    assert test.get_txt_file_key_list(bucket_name) != []