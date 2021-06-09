import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.orm import Session
from sqlalchemy.orm.session import sessionmaker

from app.classes.db.modelbase import SqlAlchemyBase

__factory = None

def global_init(db_str: str):
    global __factory

    if __factory:
        return
    
    engine = sa.create_engine(db_str, echo=False, 
                            connect_args={"check_same_thread": False})
    
    __factory = orm.sessionmaker(bind=engine)

    import app.classes.db.__all_models

    SqlAlchemyBase.metadata.create_all(engine)


def create_session() -> Session:
    global __factory

    session = __factory()

    session.expire_on_commit = False

    return session

global_init('mssql+pyodbc://SA:Passw0rd2018@localhost,1433/Data21Final?driver=SQL+Server')
