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
from app.classes.json_transform import JsonTransform
from app.classes.json_extract import JsonExtract


#
# engine.execute(f"DELETE FROM candidate WHERE candidate_name = 'jason bason'")
#
# jl = JsonLoad(engine, config.logging_level)
# from tabulate import tabulate
# print(jl.insert_candidate_return_id('jason bason'))
#
# query = engine.execute(f"SELECT * FROM candidate WHERE candidate_name = 'jason bason'")
# print('AAAA:',query.fetchall())


je = JsonExtract([], config.logging_level, config.s3_bucket)
page1_df = next(je.yield_pages())

jt = JsonTransform(config.logging_level)
transformed_page1_df = jt.transform_to_df(page1_df)

jl = JsonLoad(engine, config.logging_level)
jl.row_iterator(transformed_page1_df)

#print(engine.execute(f"SELECT * FROM candidate"))
