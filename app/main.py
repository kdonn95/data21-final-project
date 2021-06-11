from app.classes.db.db_session import global_init
from app.classes.text_file_pipeline import TextFilePipeline
from app.classes.get_config import GetConfig
import logging

config = GetConfig()

conn_str = (
            f'mssql+pyodbc://{config.user}:{config.password}' +
            f'@{config.server}/master?driver={config.driver}'
            )

engine = global_init(conn_str, config.database)


# Uploading Data from text files into sql server.
BUCKET_NAME = "data21-final-project"

# creating an object for the text file pipeline:
txt_file_pipeline = TextFilePipeline(engine)

# Getting a list of keys with all text files in the s3 bucket.
list_of_text_files = txt_file_pipeline.get_txt_file_key_list(BUCKET_NAME)
for text_file in list_of_text_files[0:2]:  # Remove "[0:2]" to input all files in the sql DB. Currently only 2.
    # Converting Text file in a data frame.
    data_frame = txt_file_pipeline.text_to_dataframe(BUCKET_NAME, text_file)

    # Transforming the dataframe by converting numbers to int, and adding the Candidate ID.
    data_frame = txt_file_pipeline.transform_string_to_int(data_frame, ['psychometrics', 'psychometrics_max',
                                                                        'presentation', 'presentation_max'])
    data_frame = txt_file_pipeline.update_candidate_id(data_frame)

    # Loading the dataframe into sql DB.
    txt_file_pipeline.load_data_into_sql(data_frame)
