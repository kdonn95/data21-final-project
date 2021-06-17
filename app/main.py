from app.classes.db.db_session import global_init
from app.classes.get_config import GetConfig
from app.classes.text_file_pipeline import TextFilePipeline
from app.classes.load_applicants_csv_to_db import loadApplicantsCSVs
from app.classes.erd_normalization_junctions import SpartaERDFormat
from app.classes.load_academy_tables_to_sql import LoadAcademyTables
from app.classes.transform_applicants_csv import candidate_df

config = GetConfig()

conn_str = (
            f'mssql+pyodbc://{config.user}:{config.password}' +
            f'@{config.server}/master?driver={config.driver}'
            )


engine = global_init(conn_str, config.database, config.logging_level)

# loading the applicants CSV files into sql database
applicants_csv_load = loadApplicantsCSVs(engine, config.logging_level)
applicants_csv_load.upload_applicants_csv_to_db(candidate_df)

# academy CSV data: transformations
dfs_in_erd_format = SpartaERDFormat(engine, config.logging_level)
weekly_performance_df = dfs_in_erd_format.make_weekly_performance_table()
course_df = dfs_in_erd_format.make_course_table()
course_type_df = dfs_in_erd_format.make_course_type_table()

# academy CSV data: loading DFs to SQL database tables
academy_load_to_sql = LoadAcademyTables(engine, config.logging_level)
academy_load_to_sql.load_to_sql_table(weekly_performance_df, 'weekly_performance')
academy_load_to_sql.load_to_sql_table(course_df, 'course')
academy_load_to_sql.load_to_sql_table(course_type_df, 'course_type')

# Adding txt file data into sql database.
txt_pipeline = TextFilePipeline(engine, config.logging_level)
txt_pipeline.upload_all_txt_files(config.s3_bucket)

