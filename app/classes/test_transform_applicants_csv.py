import unittest
import ns as ns
from numpy import datetime64
from app.classes.boto3_academy_csv_load_into_df_pydict import talent_csv_df_dict
from tabulate import tabulate
from transform_applicants_csv import *
from datetime import  datetime

class TestTransformApplicantsCsv(unittest.TestCase):
    def test_invite_date(self):
        value=df_dict['Sept2019Applicants']['invite_date'][1]
        assert  value == datetime(2019,9,4,0,0)
        for key in df_dict.keys():
            self.assertTrue(type(df_dict[key]['invite_date']) is datetime64[ns])
    def test_email(self):
        email=df_dict['April2019Applicants']['email'][1]
        assert email == 'maudas1@mapquest.com'
    def test_phone_number(self):
        phone_number=df_dict['April2019Applicants']['phone_number'][1]
        assert phone_number == '+449577280155'
    def test_dob(self):
        for key in df_dict.keys():
            self.assertTrue(type(df_dict[key]['dob']) is datetime64[ns])
