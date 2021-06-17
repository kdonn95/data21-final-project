from app.classes.text_file_pipeline import TextFilePipeline
import pandas as pd

class LoadAcademyTables(TextFilePipeline):
    def __init__(self, engine, logging_level):
        TextFilePipeline.__init__(self, engine, logging_level)
        self.engine = engine

    def load_to_sql_table(self, pd_df_to_load, sql_target_table):
        self.log_pprint(sql_target_table, "INFO")
        self.log_pprint(pd_df_to_load, "INFO")
        if sql_target_table == "course":
            pd_df_to_load = pd_df_to_load.drop(columns=["course_id"])
            pd_df_to_load = self.match_id(pd_df_to_load, "course_type_id", "course_type_id", "course_type", "type", "course_type")
        elif sql_target_table == "course_type":
            pd_df_to_load = pd_df_to_load.drop(columns=["course_type_id"])
            pd_df_to_load = pd.DataFrame(list(pd_df_to_load['course_type']), columns=['type'])
            self.log_pprint(pd_df_to_load, "INFO")
        elif sql_target_table == "weekly_performance":
            pd_df_to_load = self.match_id(pd_df_to_load, "course_id", "course_id", "course", "course_name",
                                          "course_name")
        pd_df_to_load.to_sql(sql_target_table, self.engine, if_exists='append', index=False)
        self.log_pprint("done", "INFO")

    def match_id(self, dataframe, id_name, sql_id_name, sql_table_name, sql_check, df_check):
        for index in list(dataframe.index):
            self.log_pprint(index, "INFO")
            things_to_check = dataframe.loc[index, df_check].replace("'", "''")
            self.log_pprint(things_to_check, "INFO")
            id_from_db = self.engine.execute(f"""
                                            SELECT {sql_id_name}
                                            FROM {sql_table_name}
                                            WHERE {sql_check} = '{things_to_check}'""").fetchone()[0]
            self.log_pprint(id_from_db, "INFO")
            dataframe.loc[index, id_name] = id_from_db
        dataframe = dataframe.drop(columns=[df_check])
        return dataframe
