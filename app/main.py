from app.classes.db.db_session import global_init

SERVER = 'localhost,1433'
DATABASE = 'Data21Final'
USER = 'SA'
PASSWORD = 'Passw0rd2018'
DRIVER = 'SQL+Server'

conn_str = (
            f'mssql+pyodbc://{USER}:{PASSWORD}' +
            f'@{SERVER}/master?driver={DRIVER}'
            )

global_init(conn_str, DATABASE)
