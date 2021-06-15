<<<<<<< HEAD
import json
from app.classes.json_extract import JsonExtract
from tabulate import tabulate
import datetime
import pandas as pd
from sqlalchemy import orm
from app.classes.json_transform import JsonTransform


class JsonLoad:
    def __init__(self, engine):
=======
from json_transform import JsonTransform
import orm

class JsonLoad:
    def init(self, engine):
>>>>>>> 8199deccfdf84675e82113e0cf8dee9f353e56cb
        # Setting up connection to sql server.
        self.engine = engine
        factory = orm.sessionmaker(bind=self.engine)
        self.session = factory()
        self.session.expire_on_commit = False


    def candidate_cols(self):
        return self.engine.execute(f"""SELECT candidate_name FROM candidate""").fetchall()

    def check_candidate_exists(self, name):
<<<<<<< HEAD
        return self.engine.execute(f"SELECT candidate_name FROM candidate WHERE candidate_name = /'john smith'").fetchone()

=======
        return self.engine.execute(f'SELECT candidate_name FROM candidate WHERE candidate_name = "john smith"').fetchall()
>>>>>>> 8199deccfdf84675e82113e0cf8dee9f353e56cb
