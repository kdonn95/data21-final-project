from sqlalchemy import Column, Integer, String
from app.classes.db.modelbase import SqlAlchemyBase


<<<<<<< HEAD
=======
# creation of trainer table model
>>>>>>> b86924fda68a90ed9e915790e6b120e95d073937
class Trainer(SqlAlchemyBase):
    __tablename__ = 'trainer'

    trainer_id = Column(Integer, primary_key=True)
    trainer_name = Column(String)
