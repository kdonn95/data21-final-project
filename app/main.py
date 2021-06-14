from app.classes.db.db_session import global_init
from app.classes.get_config import GetConfig
from app.classes.json_transform import JsonTransform

config = GetConfig()

conn_str = (
            f'mssql+pyodbc://{config.user}:{config.password}' +
            f'@{config.server}/master?driver={config.driver}'
            )

engine = global_init(conn_str, config.database, "NORMAL")

jt = JsonTransform(engine)

class JsonTransform:
    def __init__(self, engine):
        # Setting up connection to sql server.
        self.engine = engine
        # Connecting to the sql server.
        connection = self.engine.connect()
        #
        self.je = JsonExtract([])

