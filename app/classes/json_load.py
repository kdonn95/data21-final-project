from app.classes.json_transform import JsonTransform
from app.classes.json_extract import JsonExtract
from app.classes.logger import Logger
import sys

from app.classes.get_config import GetConfig

class JsonLoad(JsonExtract, JsonTransform):
    def __init__(self, engine, logging_level):
        # Initialise logging
        Logger.__init__(self, logging_level)
        # Setting up connection to sql server.
        self.engine = engine
        self.config = GetConfig()

    def insert_candidate_return_id(self, candidate_name):
        """checks if the candidate exists, if so returns their id, if not
        inserts them as a new row and returns their id"""
        self.log_print("Checking if candidate exists", "INFO")
        isempty = self.engine.execute(f"SELECT * FROM candidate WHERE "
                                f"""candidate_name = '{candidate_name.replace("'", "''")}'""").fetchall()
        self.log_print(isempty, "INFO")
        #if they are not in the database
        if isempty == []:
            #returns the created candidate id
            return self.insert_new_candidate(candidate_name)
        # if they are already in the database
        else:
            self.log_print(f'{candidate_name} already exists', "FLAG")
            candidate_id = self.engine.execute(f"SELECT candidate_id FROM candidate WHERE "
                                f"""candidate_name = '{candidate_name.replace("'", "''")}'""").fetchone()[0]
            return candidate_id

    def insert_new_candidate(self, candidate_name):
        """insetrs new candidate into candidate table and returns their id"""
        self.engine.execute(f"""INSERT INTO candidate (candidate_name) VALUES ('{candidate_name.replace("'", "''")}')""")
        self.log_print(f'insert {candidate_name} as new candidate in candidate', "INFO")
        candidate_id = self.engine.execute(
            f"""SELECT candidate_id FROM candidate WHERE candidate_name = '{candidate_name.replace("'", "''")}'""").fetchone()[0]
        return candidate_id

    def check_candidate_exists(self, name):
        return self.engine.execute(
            f"""SELECT candidate_id FROM candidate WHERE candidate_name = '{name.replace("'", "''")}'""").fetchone()[0]

    def prep_sparta_day(self, transformed_df, candidate_id):
            boolean_lst = []
            boolean_lst.append(transformed_df['self_dev'])
            boolean_lst.append(transformed_df['geo_flex'])
            boolean_lst.append(transformed_df['finance_support'])
            boolean_lst.append(transformed_df['result'])
            self.insert_sparta_day(candidate_id, boolean_lst,
                                   transformed_df['date'],
                                   transformed_df['course_interest'])

    def insert_sparta_day(self, name, bool_lst, date, course_interest):
        if self.check_candidate_exists(name):
            candidate_id = self.check_candidate_exists(name)
            try:
                self.engine.execute(f"INSERT INTO sparta_day "
                                           f"(candidate_id, "
                                           f"date, "
                                           f"self_development, "
                                           f"geo_flex, "
                                           f"financial_support, "
                                           f"result,"
                                           f"course_interest) "
                                           f"VALUES ({candidate_id}, "
                                           f"'{date}',"
                                           f"'{bool_lst[0]}', "
                                           f"'{bool_lst[1]}', "
                                           f"'{bool_lst[2]}', "
                                           f"'{bool_lst[3]}',"
                                           f"'{course_interest}')")
            except:
                self.engine.execute(f"UPDATE sparta_day SET "
                                    f"candidate_id = {candidate_id}, "
                                    f"date = '{date}', "
                                    f"self_development = '{bool_lst[0]}', "
                                    f"geo_flex = '{bool_lst[1]}', "
                                    f"financial_support = '{bool_lst[2]}', "
                                    f"result = '{bool_lst[3]}', "
                                    f"course_interest = '{course_interest}' "
                                    f"WHERE candidate_id = {candidate_id} "
                                    f"AND date = '{date}'")
        else:
            self.insert_new_candidate(name)
            self.insert_sparta_day(name, bool_lst, date, course_interest)

    def populate_strengths_table(self, strength):
        """Checks whether a strength is already available in the strengths
        table, if not it will insert the strength and assign a primary key"""
        self.log_print("Populating strengths table", "INFO")
        is_empty = self.engine.execute(f"SELECT * FROM strengths WHERE "
                                       f"strength = "
                                       f"'{strength}'").fetchall()
        self.log_print(is_empty, "DEBUG")
        if not is_empty:
            return self.insert_new_strength(strength)
        else:
            self.log_print(f'{strength} already exists', "INFO")
            strength_id = self.engine.execute(
                f"SELECT strength_id FROM strengths WHERE "
                f"strength = '{strength}'").fetchone()[0]
            return strength_id

    def insert_new_strength(self, strength):
        self.engine.execute(f"INSERT INTO strengths (strength) VALUES ('{strength}')")
        self.log_print( f'insert {strength} as new strength in strengths', "INFO")
        strength_id = self.engine.execute(f"SELECT strength_id FROM strengths WHERE strength = '{strength}'").fetchone()[0]
        return strength_id

    def populate_weaknesses_table(self, weakness):
        self.log_print("Populating weaknesses table", "INFO")
        is_empty = self.engine.execute(f"SELECT * FROM weaknesses WHERE "
                                       f"weakness = "
                                       f"'{weakness}'").fetchall()
        self.log_print(is_empty, "DEBUG")
        if not is_empty:
            return self.insert_new_weakness(weakness)
        else:
            self.log_print(f'{weakness} already exists', "INFO")
            weakness_id = self.engine.execute(
                f"SELECT weakness_id FROM weaknesses WHERE "
                f"weakness = '{weakness}'").fetchone()[0]
            return weakness_id

    def insert_new_weakness(self, weakness):
        self.engine.execute(f"INSERT INTO weaknesses (weakness) VALUES ('{weakness}')")
        self.log_print( f'insert {weakness} as new weakness in weaknesses', "INFO")
        weakness_id = self.engine.execute(f"SELECT weakness_id FROM weaknesses WHERE weakness = '{weakness}'").fetchone()[0]
        return weakness_id

    def populate_tech_table(self, tech):
        self.log_print("Populating tech table", "INFO")
        is_empty = self.engine.execute(f"SELECT * FROM tech WHERE "
                                       f"tech = "
                                       f"'{tech}'").fetchall()
        self.log_print(is_empty, "DEBUG")
        if not is_empty:
            return self.insert_new_tech(tech)
        else:
            self.log_print(f'{tech} already exists', "INFO")
            tech_id = self.engine.execute(
                f"SELECT tech_id FROM tech WHERE "
                f"tech = '{tech}'").fetchone()[0]
            return tech_id

    def strength_junc_populate(self, strength_id, candidate_id):
        try:
            self.engine.execute(f"INSERT INTO strength_junc (strength_id, "
                            f"candidate_id) VALUES ({strength_id}, "
                            f"{candidate_id})")
        except:
            self.engine.execute(f"UPDATE strength_junc SET strength_id = {strength_id}, "
                                f"candidate_id = {candidate_id} WHERE strength_id = {strength_id} AND "
                                f"candidate_id = {candidate_id}")

    def weakness_junc_populate(self, weakness_id, candidate_id):
        try:
            self.engine.execute(f"INSERT INTO weaknesses_junc (weakness_id, "
                            f"candidate_id) VALUES ({weakness_id}, "
                            f"{candidate_id})")
        except:
            self.engine.execute(
                f"UPDATE weaknesses_junc SET weakness_id = {weakness_id}, "
                f"candidate_id = {candidate_id} WHERE weakness_id = {weakness_id} AND "
                f"candidate_id = {candidate_id}")

    def insert_new_tech(self, tech):
        self.engine.execute(f"INSERT INTO tech (tech) VALUES ('{tech}')")
        self.log_print( f'insert {tech} as new tech in tech', "INFO")
        tech_id = self.engine.execute(f"SELECT tech_id FROM tech WHERE tech = '{tech}'").fetchone()[0]
        return tech_id

    def insert_tech_junction(self, tech_ids, candidate_id):
        for id in tech_ids:
            tech_id = id[0]
            score = id[1]
            try:
                self.engine.execute(f"INSERT INTO tech_junc (tech_id, candidate_id, score) VALUES ({tech_id}, {candidate_id}, {score})")
                self.log_print(f'insert new tech junction for {candidate_id}', "INFO")
            except:
                self.engine.execute(
                    f"UPDATE tech_junc SET tech_id = {tech_id}, candidate_id = {candidate_id}, score = {score} WHERE tech_id = {tech_id} AND candidate_id = {candidate_id}")



    def row_iterator(self, transformed_df):
        #for row in df

        for index, row in transformed_df.iterrows():

            candidate_id = self.insert_candidate_return_id(row['name'])
            self.prep_sparta_day(row, row['name'])

            strength_ids = []
            for strength in row['strengths']:
                strength_ids.append(self.populate_strengths_table(strength))
            self.log_pprint(strength_ids, "INFO")

            weakness_ids = []
            for weakness in row['weaknesses']:
                weakness_ids.append(self.populate_weaknesses_table(weakness))
            self.log_pprint(weakness_ids, "INFO")

            for strength_id in strength_ids:
                self.log_print(strength_id, "INFO")
                self.strength_junc_populate(strength_id, candidate_id)

            for weakness_id in weakness_ids:
                self.log_print(weakness_id, "INFO")
                self.weakness_junc_populate(weakness_id, candidate_id)

            techs = row['tech_self_score']
            tech_ids = []
            for tech in techs.keys():
                tech_ids.append((self.populate_tech_table(tech), techs[tech]))
            self.log_pprint(weakness_ids, "INFO")

            self.insert_tech_junction(tech_ids, candidate_id)

    def json_ETL(self, used_keys):
        """this function runs the whole json ETL"""
        # create extract and transform classes
        je = JsonExtract(used_keys,
                         self.config.logging_level,
                         self.config.s3_bucket)
        jt = JsonTransform(self.config.logging_level)

        # take items from yield_pages() generator until empty
        iterable = next(je.yield_pages())

        self.log_print(f'load transformed json dataframe into sql', "INFO")

        transformed_page_df = jt.transform_to_df(iterable)
        self.row_iterator(transformed_page_df)