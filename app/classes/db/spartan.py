from sqlalchemy import Column, Integer, Boolean, String, Date, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


class Spartan(SqlAlchemyBase):
    __tablename__ = 'spartan'

    spartan_id = Column(Integer, primary_key=True)
    candidate_id = Column(Integer, ForeignKey('candidate.candidate_id'))
    spartan_name = Column(String)
