from sqlalchemy import Column, Integer, Boolean, String, Date
from app.classes.db.modelbase import SqlAlchemyBase


# creation of candidate table model
class Candidate(SqlAlchemyBase):
    __tablename__ = 'candidate'

    candidate_id = Column(Integer, primary_key=True)
    candidate_name = Column(String)
    date = Column(Date)
    self_development = Column(Boolean)
    geo_flex = Column(Boolean)
    financial_support = Column(Boolean)
    result = Column(Boolean)
    course_interest = Column(String)
    # --------- ^ json, csv below -----------
    gender = Column(String)
    dob = Column(Date)
    email = Column(String)
    city = Column(String)
    address = Column(String)
    postcode = Column(String)
    phone_number = Column(String)
    uni_degree = Column(String)
    invited_date = Column(Date)
    invited_by = Column(String)
