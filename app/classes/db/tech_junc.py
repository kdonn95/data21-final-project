from sqlalchemy import Column, Integer, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


class Tech_Junc(SqlAlchemyBase):
    __tablename__ = 'test_junc'

    tech_id = Column(Integer, ForeignKey['tech.tech_id'])
    candidate_id = Column(Integer, ForeignKey['candidate.candidate_id'])
    score = Column(Integer)