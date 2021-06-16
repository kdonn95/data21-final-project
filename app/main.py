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
# txt_pipeline = TextFilePipeline(engine, config.logging_level)
# txt_pipeline.upload_all_txt_files(config.s3_bucket)

engine.execute(f"DELETE FROM candidate WHERE candidate_name = 'jason bason'")

jl = JsonLoad(engine, config.logging_level)
from tabulate import tabulate
print(jl.insert_candidate_return_id('jason bason'))

query = engine.execute(f"SELECT * FROM candidate WHERE candidate_name = 'jason bason'")
print('AAAA:',query.fetchall())
