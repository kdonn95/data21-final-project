import sqlalchemy
from sqlalchemy import orm
from app.classes.db.__all_class_models import *


class UpdateRow:
    def __init__(self, database):
        # Setting up connection to sql server.
        server = 'localhost,1433'
        user = 'SA'
        password = 'Passw0rd2018'
        driver = 'SQL+Server'

        self.engine = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")
        factory = orm.sessionmaker(bind=self.engine)
        self.session = factory()
        self.session.expire_on_commit = False

    def update_candidate_row(self, list_column_names, list_values):
        """
        To update only the candidate table, **must include candidate_name**
        list_column_values must match the order list_column_names
        """
        c_name_ind = list_column_names.index('candidate_name')
        c_name_val = list_values[c_name_ind]

        # if id exist, set candidate id
        c_id = self.engine.execute(f"""
                                    SELECT candidate_id
                                    FROM candidate
                                    WHERE candidate_name = '{c_name_val}'
                                    """).fetchall()

        t = Candidate()

        if c_id:
            t.candidate_id = c_id[0][0]

        for i in range(len(list_column_names)):
            setattr(t, list_column_names[i], list_values[i])
        
        self.session.merge(t)
        self.session.commit()        
