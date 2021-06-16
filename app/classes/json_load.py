from app.classes.json_transform import JsonTransform
from app.classes.json_extract import JsonExtract
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

            #self.log_print(f'{name} already exists', "FLAG")
            candidate_id = self.engine.execute(f"SELECT candidate_id FROM candidate WHERE "
                                f"candidate_name = '{candidate_name}'").fetchall()
            return candidate_id[0][0]

    def check_candidate_exists(self, name):
        return self.engine.execute(f"SELECT * FROM candidate WHERE candidate_name = '{name}'")

    def insert_sparta_day(self, name, bool_lst, date, course_interest):
        if self.check_candidate_exists(name):
            candidate_id = self.check_candidate_exists(name)
            return self.engine.execute(f"INSERT INTO sparta_day "
                                       f"(candidate_id, "
                                       f"location_id, "
                                       f"date, "
                                       f"self_development, "
                                       f"geo_flex, "
                                       f"financial_support, "
                                       f"result,"
                                       f"course_interest) "
                                       f"VALUES ('{candidate_id}', "
                                       f"'1', "
                                       f"CAST('{date}' as datetime),"
                                       f"'{bool_lst[0]}', "
                                       f"'{bool_lst[1]}', "
                                       f"'{bool_lst[2]}', "
                                       f"'{bool_lst[3]}',"
                                       f"'{course_interest}')")
        else:
            self.insert_new_candidate(name)
            self.insert_sparta_day(name, bool_lst, date, course_interest)

    def insert_new_candidate(self, candidate_name):
        """insetrs new candidate into candidate table and returns their id"""
        self.engine.execute(f"INSERT INTO candidate (candidate_name) VALUES ('{candidate_name}')")
        self.log_print(f'insert {candidate_name} as new candidate in candidate', "INFO")
        candidate_id = self.engine.execute(
            f"SELECT candidate_id FROM candidate WHERE candidate_name = '{candidate_name}'")
        return candidate_id

    def load_sql(self, transformed_df):
        for i in range(transformed_df.shape[0]):
            #for row in transformed_df:
            name = self.check_candidate_exists(transformed_df['name'])
            if len(list(name)) > 1:
                ## Flag the name
                pass
            else:
                boolean_lst = []
                boolean_lst.append(transformed_df['self_dev'])
                boolean_lst.append(transformed_df['geo_flex'])
                boolean_lst.append(transformed_df['finance_support'])
                boolean_lst.append(transformed_df['result'])
                self.insert_sparta_day(transformed_df['name'], boolean_lst, transformed_df['date'], transformed_df['course_interest'])
