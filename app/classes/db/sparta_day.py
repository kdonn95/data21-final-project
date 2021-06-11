from sqlalchemy import Column, Integer, String, Date,ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


# creation of test table model
class Test(SqlAlchemyBase):
    __tablename__ = 'sparta_day'

    candidate_id = Column(Integer, 
                        ForeignKey('candidate.candidate_id'), primary_key=True)
    date = Column(Date, primary_key=True)
    location = Column(String)
    presentation = Column(Integer)
    presentation_max = Column(Integer)
    psychometrics = Column(Integer)
    psychometrics_max = Column(Integer)
