from sqlalchemy import Column, Integer, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


# creation of course staff junction table
class CourseStaffJunc(SqlAlchemyBase):
    __tablename__ = 'course_staff_junc'

    course_id = Column(Integer, 
                        ForeignKey('course.course_id'), primary_key=True)
    staff_id = Column(Integer,
                        ForeignKey('staff.staff_id'), primary_key=True)
