from sqlalchemy import Column, Integer, Boolean, String, Date
from app.classes.db.modelbase import SqlAlchemyBase


class Strengths(SqlAlchemyBase):
    __tablename__ = 'strengths'

    strength_id = Column(Integer, primary_key=True)
    strength = Column(String)
