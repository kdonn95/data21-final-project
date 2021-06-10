from sqlalchemy import Column, Integer, String
from app.classes.db.modelbase import SqlAlchemyBase


<<<<<<< HEAD
=======
# creation of strengths table model
>>>>>>> b86924fda68a90ed9e915790e6b120e95d073937
class Strengths(SqlAlchemyBase):
    __tablename__ = 'strengths'

    strength_id = Column(Integer, primary_key=True)
    strength = Column(String)
