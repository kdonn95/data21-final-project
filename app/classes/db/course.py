from sqlalchemy import Column, Integer, String, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


# creation of course table model
class Course(SqlAlchemyBase):
    __tablename__ = 'course'

    course_id = Column(Integer, primary_key=True)
    trainer_id = Column(Integer, ForeignKey('trainer.trainer_id'))
    course_name = Column(String)
    type = Column(String)
<<<<<<< HEAD

=======
>>>>>>> b86924fda68a90ed9e915790e6b120e95d073937
