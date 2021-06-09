from sqlalchemy import Column, Integer, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


class Scores(SqlAlchemyBase):
    __tablename__ = 'scores'

    spartan_id = Column(Integer, ForeignKey('spartan.spartan_id'))
    course_id = Column(Integer, ForeignKey('course.course_id'))
    week_no = Column(Integer)
    analytic = Column(Integer)
    independent = Column(Integer)
    determined = Column(Integer)
    professional = Column(Integer)
    studious = Column(Integer)
    imaginative = Column(Integer)
