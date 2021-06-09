from sqlalchemy import Column, Integer, Boolean, String, Date
from app.classes.db.modelbase import SqlAlchemyBase

class Candidate(SqlAlchemyBase):
    __tablename__ = 'course'
