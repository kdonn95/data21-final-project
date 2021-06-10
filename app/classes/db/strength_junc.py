from sqlalchemy import Column, Integer, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase

# creation of strength_junc table model
class StrenghtJunc(SqlAlchemyBase):
    __tablename__ = 'strength_junc'

    strength_id = Column(Integer,
                        ForeignKey('strengths.strength_id'), primary_key=True)
    candidate_id = Column(Integer,
                        ForeignKey('candidate.candidate_id'), primary_key=True)
