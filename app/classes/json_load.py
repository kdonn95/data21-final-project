from app.classes.json_transform import JsonTransform
# import orm
from app.classes.logger import Logger
from sqlalchemy.orm import sessionmaker


class JsonLoad(Logger):
    def init(self, engine, logging_level):
        # Initialise logging
        Logger.init(self, logging_level)
        # Setting up connection to sql server.
        self.engine = engine
        factory = sessionmaker(bind=self.engine)
        self.session = factory()
        self.session.expire_on_commit = False

    def candidate_cols(self):
        return self.engine.execute(f"""SELECT candidate_name FROM 
        candidate""").fetchall()

    def check_candidate_exists(self, name):
        self.log_print("Checking if candidate exists", "INFO")
        x = self.engine.execute(f"SELECT * FROM candidate WHERE "
                                f"candidate_name = '{name}'").fetchall()
        self.log_print(x, "INFO")