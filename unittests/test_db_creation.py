from app.classes.db.db_session import global_init
from app.classes.get_config import GetConfig
import sqlalchemy

config = GetConfig()

conn_str = (
            f'mssql+pyodbc://{config.user}:{config.password}' +
            f'@{config.server}/master?driver={config.driver}'
            )

engine = sqlalchemy.create_engine(conn_str)
connection = engine.connect()


def test_db_creation():
    # drop existing table
    engine.execute(f"""
                    USE master
                    DROP DATABASE IF EXISTS {config.database};
                    """)

    # initialise database
    global_init(conn_str, config.database, "DEBUG")

    # get list of databases
    dbs = engine.execute(f"SELECT name FROM sys.databases")
    dbs = [d[0] for d in dbs]

    assert config.database in dbs
