from sqlalchemy import Column, Integer, String, ForeignKey, Date
from app.classes.db.modelbase import SqlAlchemyBase


# creation of course table model
class Course(SqlAlchemyBase):
    __tablename__ = 'course'

    course_id = Column(Integer, primary_key=True)
    course_type_id = Column(Integer, ForeignKey('course_type.course_type_id'))
    course_name = Column(String)
    start_date = Column(Date)
