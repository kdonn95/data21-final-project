from sqlalchemy import Column, Integer, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


<<<<<<< HEAD
class Tech_junc(SqlAlchemyBase):
=======
# creation of tech_junc table model
class TechJunc(SqlAlchemyBase):
>>>>>>> b86924fda68a90ed9e915790e6b120e95d073937
    __tablename__ = 'tech_junc'

    tech_id = Column(Integer, 
                    ForeignKey('tech.tech_id'), primary_key=True)
    candidate_id = Column(Integer, 
                    ForeignKey('candidate.candidate_id'), primary_key=True)
    score = Column(Integer)
