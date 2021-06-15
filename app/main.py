from app.classes.db.db_session import global_init
from app.classes.get_config import GetConfig
from app.classes.json_transform import JsonTransform
from app.classes.json_extract import JsonExtract

config = GetConfig()

conn_str = (
            f'mssql+pyodbc://{config.user}:{config.password}' +
            f'@{config.server}/master?driver={config.driver}'
            )

engine = global_init(conn_str, config.database, "NORMAL")

#////////// for testing json tyransforms:
# je = JsonExtract([])
# page1_df = next(je.yield_pages())
# jt = JsonTransform(engine)
# from tabulate import tabulate
# print(tabulate(jt.transform_to_df(page1_df)))