from json_transform import JsonTransform
import orm

class JsonLoad:
    def init(self, engine):
        # Setting up connection to sql server.
        self.engine = engine
        factory = orm.sessionmaker(bind=self.engine)
        self.session = factory()
        self.session.expire_on_commit = False


    def candidate_cols(self):
        return self.engine.execute(f"""SELECT candidate_name FROM candidate""").fetchall()

    def check_candidate_exists(self, name):
        return self.engine.execute(f'SELECT candidate_name FROM candidate WHERE candidate_name = "john smith"').fetchall()
