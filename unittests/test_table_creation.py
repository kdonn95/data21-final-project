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

tablenames = [
            'candidate', 
            'strengths', 
            'tech', 
            'trainer', 
            'weaknesses',
            'course',
            'spartan',
            'strength_junc',
            'tech_junc',
            'test',
            'weaknesses_junc',
            'scores'
            ]


def test_table_creation():
    # drop existing database
    engine.execute(f"""
                    USE master;
                    DROP DATABASE IF EXISTS {config.database};
                    """)
    
    # initialise database
    global_init(conn_str, config.database)
    engine.execute(f'USE {config.database};')
    tables = engine.execute(f"""
                            SELECT name FROM {config.database}.sys.tables;
                            """)
    tables = [t[0] for t in tables]

    result = []

    for table in tables:
        if table in tablenames:
            result.append(True)
        
        else:
            result.append(False)

    assert all(result)
