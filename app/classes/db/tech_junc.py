from sqlalchemy import Column, Integer, Boolean, String, Date, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


class Tech_junc(SqlAlchemyBase):
    __tablename__ = 'tech_junc'

    tech_id = Column(Integer, ForeignKey('tech.tech_id'))
    candidate_id = Column(Integer, ForeignKey('candidate.candidate_id'))
    score = Column(Integer)
