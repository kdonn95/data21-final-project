from app.classes.db.db_session import global_init
import sqlalchemy

SERVER = 'localhost,1433'
DATABASE = 'Data21Final'
USER = 'SA'
PASSWORD = 'Passw0rd2018'
DRIVER = 'SQL+Server'

conn_str = (
            f'mssql+pyodbc://{USER}:{PASSWORD}' +
            f'@{SERVER}/master?driver={DRIVER}'
            )

engine = sqlalchemy.create_engine(conn_str)
connection = engine.connect()


def test_db_creation():
    # drop existing table
    engine.execute(f"""
                    USE master
                    DROP DATABASE IF EXISTS {DATABASE};
                    """)

    # initialise database
    global_init(conn_str, DATABASE)

    # get list of databases
    dbs = engine.execute(f"SELECT name FROM sys.databases")
    dbs = [d[0] for d in dbs]

    assert DATABASE in dbs
