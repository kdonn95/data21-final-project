
from sqlalchemy import Column, Integer, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


class Strength_junc(SqlAlchemyBase):
    __tablename__ = 'strength_junc'

    strength_id = Column(Integer, ForeignKey('strengths.strength_id'))
    candidate_id = Column(Integer, ForeignKey('candidate.candidate_id'))


