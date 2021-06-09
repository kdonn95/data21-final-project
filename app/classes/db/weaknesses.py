from sqlalchemy import Column, Integer, String
from app.classes.db.modelbase import SqlAlchemyBase


# creation of weaknesses table model
class Weaknesses(SqlAlchemyBase):
    __tablename__ = 'weaknesses'

    weakness_id = Column(Integer, primary_key=True)
    weakness = Column(String)
