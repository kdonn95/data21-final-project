from app.classes.db.db_session import global_init
from app.classes.get_config import GetConfig
from app.classes.text_file_pipeline import TextFilePipeline


config = GetConfig()

conn_str = (
            f'mssql+pyodbc://{config.user}:{config.password}' +
            f'@{config.server}/master?driver={config.driver}'
            )


engine = global_init(conn_str, config.database, config.logging_level)


# Adding txt file data into sql database.
# txt_pipeline = TextFilePipeline(engine, logging_level)
# txt_pipeline.upload_all_txt_files("data21-final-project")

#//////////////////////////////////////////////////

from app.classes.json_load import JsonLoad

jl = JsonLoad(engine, config.logging_level)
jl.dev_json_ETL([])