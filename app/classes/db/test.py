<<<<<<< HEAD
from sqlalchemy import Column, Integer, Boolean, String, Date, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


=======
from sqlalchemy import Column, Integer, String, Date,ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


# creation of test table model
>>>>>>> b86924fda68a90ed9e915790e6b120e95d073937
class Test(SqlAlchemyBase):
    __tablename__ = 'test'

    candidate_id = Column(Integer, 
                        ForeignKey('candidate.candidate_id'), primary_key=True)
    date = Column(Date, primary_key=True)
    location = Column(String)
    presentation = Column(Integer)
    presentation_max = Column(Integer)
    psychometrics = Column(Integer)
    psychometrics_max = Column(Integer)
