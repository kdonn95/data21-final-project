from sqlalchemy import Column, Integer, String
from app.classes.db.modelbase import SqlAlchemyBase


# creation of staff table model
class Staff(SqlAlchemyBase):
    __tablename__ = 'staff'

    staff_id = Column(Integer, primary_key=True)
    staff_name = Column(String)
    department = Column(String)
