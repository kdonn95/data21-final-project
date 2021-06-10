<<<<<<< HEAD
from sqlalchemy import Column, Integer, Boolean, String, Date, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


class Strength_junc(SqlAlchemyBase):
    __tablename__ = 'strength_junc'

    strength_id = Column(Integer, ForeignKey('strengths.strength_id'))
    candidate_id = Column(Integer, ForeignKey('candidate.candidate_id'))
=======
from sqlalchemy import Column, Integer, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


# creation of strength_junc table model
class StrenghtJunc(SqlAlchemyBase):
    __tablename__ = 'strength_junc'

    strength_id = Column(Integer,
                        ForeignKey('strengths.strength_id'), primary_key=True)
    candidate_id = Column(Integer,
                        ForeignKey('candidate.candidate_id'), primary_key=True)


>>>>>>> b86924fda68a90ed9e915790e6b120e95d073937
