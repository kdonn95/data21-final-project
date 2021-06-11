from sqlalchemy import Column, Integer, String
from app.classes.db.modelbase import SqlAlchemyBase


# creation of location table model
class Location(SqlAlchemyBase):
    __tablename__ = 'location'

    location_id = Column(Integer, primary_key=True)
    location = Column(String)
