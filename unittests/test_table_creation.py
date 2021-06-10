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
                    DROP DATABASE IF EXISTS {DATABASE};
                    """)
    
    # initialise database
    global_init(conn_str, DATABASE)
    engine.execute(f'USE {DATABASE};')
    tables = engine.execute(f"""
                            SELECT name FROM {DATABASE}.sys.tables;
                            """)
    tables = [t[0] for t in tables]

    result = []

    for table in tables:
        if table in tablenames:
            result.append(True)
        
        else:
            result.append(False)

    assert all(result)
