from sqlalchemy import Column, Integer, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


class WeaknessesJunc(SqlAlchemyBase):
    __tablename__ = 'weaknesses_junc'

    weakness_id = Column(Integer, ForeignKey('weaknesses.weakness_id'))
    candidate_id = Column(Integer, ForeignKey('candidate.candidate_id'))
