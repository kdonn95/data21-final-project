from sqlalchemy import Column, Integer, String, Date, ForeignKey
from app.classes.db.modelbase import SqlAlchemyBase


# creation of candidate table model
class Candidate(SqlAlchemyBase):
    __tablename__ = 'candidate'

    candidate_id = Column(Integer, primary_key=True)
    candidate_name = Column(String)
    gender = Column(String)
    dob = Column(Date)
    email = Column(String)
    city = Column(String)
    address = Column(String)
    postcode = Column(String)
    phone_number = Column(String)
    uni_name = Column(String)
    degree_result = Column(String)
    staff_id = Column(Integer, ForeignKey('staff.staff_id'))
