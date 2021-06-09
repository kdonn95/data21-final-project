from sqlalchemy import Column, Integer, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


# creation of weaknesses_junc table model
class WeaknessesJunc(SqlAlchemyBase):
    __tablename__ = 'weaknesses_junc'

    weakness_id = Column(Integer, 
                        ForeignKey('weaknesses.weakness_id'), primary_key=True)
    candidate_id = Column(Integer, 
                        ForeignKey('candidate.candidate_id'), primary_key=True)
