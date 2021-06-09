from sqlalchemy import Column, Integer, String, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


class Course(SqlAlchemyBase):
    __tablename__ = 'course'

    course_id = Column(Integer, primary_key=True)
    trainer_id = Column(Integer, ForeignKey('trainer.trainer_id'))
    course_name = Column(String)
    type = Column(String)
