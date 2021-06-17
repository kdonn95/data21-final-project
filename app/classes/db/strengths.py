from sqlalchemy import Column, Integer, String
from app.classes.db.modelbase import SqlAlchemyBase


# creation of strengths table model
class Strengths(SqlAlchemyBase):
    __tablename__ = 'strengths'

    strength_id = Column(Integer, primary_key=True)
    strength = Column(String)
