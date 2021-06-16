from app.classes.text_file_pipeline import TextFilePipeline


class LoadAcademyTables(TextFilePipeline):
    def __init__(self, engine, logging_level):
        TextFilePipeline.__init__(self, engine, logging_level)
        self.engine = engine

    def load_to_sql_table(self, pd_df_to_load, sql_target_table):
        pd_df_to_load.to_sql(sql_target_table, self.engine, if_exists='append', index=False)
