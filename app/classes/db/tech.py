from sqlalchemy import Column, Integer, String
from app.classes.db.modelbase import SqlAlchemyBase


# creation of tech table model
class Tech(SqlAlchemyBase):
    __tablename__ = 'tech'

    tech_id = Column(Integer, primary_key=True)
    tech = Column(String)
