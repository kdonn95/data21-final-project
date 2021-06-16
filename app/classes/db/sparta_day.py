from sqlalchemy import Column, Integer, String, Date, ForeignKey, Boolean
from app.classes.db.modelbase import SqlAlchemyBase


# creation of sparta_day table model
class SpartaDay(SqlAlchemyBase):
    __tablename__ = 'sparta_day'

    candidate_id = Column(Integer, 
                        ForeignKey('candidate.candidate_id'), primary_key=True)
    location_id = Column(Integer,
                          ForeignKey('location.location_id'), primary_key=True)
    date = Column(Date, primary_key=True)
    result = Column(Boolean)
    self_development = Column(Boolean)
    financial_support = Column(Boolean)
    geo_flex = Column(Boolean)
    course_interest = Column(String)
    presentation = Column(Integer)
    presentation_max = Column(Integer)
    psychometrics = Column(Integer)
    psychometrics_max = Column(Integer)
