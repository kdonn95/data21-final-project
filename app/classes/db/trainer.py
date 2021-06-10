from sqlalchemy import Column, Integer, String
from app.classes.db.modelbase import SqlAlchemyBase

# creation of trainer table model

class Trainer(SqlAlchemyBase):
    __tablename__ = 'trainer'

    trainer_id = Column(Integer, primary_key=True)
    trainer_name = Column(String)
