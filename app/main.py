from app.classes.db.db_session import global_init
from app.classes.get_config import GetConfig
from app.classes.text_file_pipeline import TextFilePipeline
from app.classes.csv_data_transform_ERD import TransformCSVdataFrames
from app.classes.csv_data_transform_ERD import *
from app.classes.load_applicants_csv_to_db import loadApplicantsCSVs

config = GetConfig()

conn_str = (
            f'mssql+pyodbc://{config.user}:{config.password}' +
            f'@{config.server}/master?driver={config.driver}'
            )


engine = global_init(conn_str, config.database, config.logging_level)

# loading the applicants CSV files into sql database
candidates_df = TransformCSVdataFrames(academy_raw_csv_df_dict, talent_raw_csv_df_dict).talent_csv_new_df_setup()
applicants_csv_load = loadApplicantsCSVs(engine,config.logging_level)
applicants_csv_load.upload_applicants_csv_to_db(candidates_df)


# Adding txt file data into sql database.
txt_pipeline = TextFilePipeline(engine, config.logging_level)
txt_pipeline.upload_all_txt_files(config.s3_bucket)

