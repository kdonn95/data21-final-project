from sqlalchemy import Column, Integer, Boolean, String, Date,ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase

class Candidate(SqlAlchemyBase):
    __tablename__ = 'test'

    candidate_id = Column(Integer, ForeignKey('candidate.candidate_id'))
    date = Column(Date)
    location = Column(String)
    presentation = Column(Integer)
    presentation_max = Column(Integer)
    psychometrics = Column(Integer)
    psychometrics_max = Column(Integer)


