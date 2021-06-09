from sqlalchemy import Column, Integer, Boolean, String, Date, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


class Tech(SqlAlchemyBase):
    __tablename__ = 'tech'

    tech_id = Column(Integer, primary_key=True)
    tech = Column(String)
