from app.classes.db.db_session import global_init
from app.classes.get_config import GetConfig
from app.classes.text_file_pipeline import TextFilePipeline
from app.classes.json_load import JsonLoad

config = GetConfig()

conn_str = (
            f'mssql+pyodbc://{config.user}:{config.password}' +
            f'@{config.server}/master?driver={config.driver}'
            )


engine = global_init(conn_str, config.database, config.logging_level)


# Adding txt file data into sql database.
# txt_pipeline = TextFilePipeline(engine, logging_level)
# txt_pipeline.upload_all_txt_files("data21-final-project")

#engine.execute("DELETE FROM sparta_day")
jl = JsonLoad(engine, 'INFO')
#jl.insert_sparta_day('Joe Smith', [True, True, True, False], '2015-06-06', 'Data')

