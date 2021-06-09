from sqlalchemy import Column, Integer, Boolean, String, Date
from app.classes.db.modelbase import SqlAlchemyBase


class Candidate(SqlAlchemyBase):
    __tablename__ = 'trainer'

    trainer_id = Column(Integer, primary_key=True)
    trainer_name = Column(String)
