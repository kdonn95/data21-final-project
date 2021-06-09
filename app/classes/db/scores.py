from sqlalchemy import Column, Integer, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


# creation of scores table model
class Scores(SqlAlchemyBase):
    __tablename__ = 'scores'

    spartan_id = Column(Integer, 
                        ForeignKey('spartan.spartan_id'), primary_key=True)
    course_id = Column(Integer, 
                        ForeignKey('course.course_id'), primary_key=True)
    week_no = Column(Integer, primary_key=True)
    analytic = Column(Integer)
    independent = Column(Integer)
    determined = Column(Integer)
    professional = Column(Integer)
    studious = Column(Integer)
    imaginative = Column(Integer)
