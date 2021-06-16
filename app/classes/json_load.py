from app.classes.json_transform import JsonTransform
# import orm
from app.classes.logger import Logger
from sqlalchemy.orm import sessionmaker


class JsonLoad:
    def __init__(self, engine, logging_level):
        # Initialise logging
        Logger.__init__(self, logging_level)
        # Setting up connection to sql server.
        self.engine = engine
        factory = sessionmaker(bind=self.engine)
        self.session = factory()
        self.session.expire_on_commit = False

    def check_candidate_exists(self, name):
        self.log_print("Checking if candidate exists", "INFO")
        isempty = self.engine.execute(f"SELECT * FROM candidate WHERE "
                                f"candidate_name = '{name}'").fetchall()
        self.log_print(isempty, "INFO")
        if isempty == []:
            return False
        else:
            self.log_print(f'{name} already exists', "FLAG")
            candidate_id = self.engine.execute(f"SELECT candidate_id FROM candidate WHERE "
                                f"candidate_name = '{name}'").fetchone()
            return candidate_id

    def insert_new_candidate(self, name):
        self.engine.execute(f"INSERT INTO candidate (candidate_name) VALUES ('{name}')")
        self.log_print(f'insert {name} as new candidate in candidate', "DEBUG")