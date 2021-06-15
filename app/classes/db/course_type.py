from sqlalchemy import Column, Integer, String
from app.classes.db.modelbase import SqlAlchemyBase


# creation of course type table
class CourseType(SqlAlchemyBase):
    __tablename__ = 'course_type'

    course_type_id = Column(Integer, primary_key=True)
    type = Column(String)
    duration = Column(Integer)
