from sqlalchemy import Column, Integer, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


# creation of tech_junc table model
class TechJunc(SqlAlchemyBase):
    __tablename__ = 'tech_junc'

    tech_id = Column(Integer, 
                    ForeignKey('tech.tech_id'), primary_key=True)
    candidate_id = Column(Integer, 
                    ForeignKey('candidate.candidate_id'), primary_key=True)
    score = Column(Integer)
