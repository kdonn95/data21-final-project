from app.classes.json_transform import JsonTransform
# import orm
from app.classes.get_config import GetConfig
from app.classes.logger import Logger
from sqlalchemy.orm import sessionmaker



class JsonLoad(Logger):
    def __init__(self, engine, logging_level):
        # Initialise logging
        Logger.__init__(self, logging_level)
        # Setting up connection to sql server.
        self.engine = engine
        factory = sessionmaker(bind=self.engine)
        self.session = factory()
        self.session.expire_on_commit = False

    def insert_candidate_return_id(self, candidate_name):
        """checks if the candidate exists, if so returns their id, if not
        inserts them as a new row and returns their id"""
        self.log_print("Checking if candidate exists", "INFO")
        isempty = self.engine.execute(f"SELECT * FROM candidate WHERE "
                                f"candidate_name = '{candidate_name}'").fetchall()
        self.log_print(isempty, "INFO")
        #if they are not in the database
        if isempty == []:
            #returns the created candidate id
            return self.insert_new_candidate(candidate_name)
        # if they are already in the database
        else:
            self.log_print(f'{candidate_name} already exists', "FLAG")
            candidate_id = self.engine.execute(f"SELECT candidate_id FROM candidate WHERE "
                                f"candidate_name = '{candidate_name}'").fetchone()
            return candidate_id

    def insert_new_candidate(self, candidate_name):
        """insetrs new candidate into candidate table and returns their id"""
        self.engine.execute(f"INSERT INTO candidate (candidate_name) VALUES ('{candidate_name}')")
        self.log_print(f'insert {candidate_name} as new candidate in candidate', "INFO")
        candidate_id = self.engine.execute(
            f"SELECT candidate_id FROM candidate WHERE candidate_name = '{candidate_name}'")
        return candidate_id
