import unittest
import ns as ns
from numpy import datetime64
from app.classes.boto3_academy_csv_load_into_df_pydict import talent_csv_df_dict, talent_csv_info_getter
from tabulate import tabulate
from app.classes.transform_applicants_csv import *
from datetime import datetime
import random

df_dict = candidate_df

class TestTransformApplicantsCsv(unittest.TestCase):

    def test_invite_date(self):
        new_list = []
        for key in df_dict.keys():
            new_list.append(key)
            return new_list
        index = random.randint(1, len(new_list))
        random_row = random.randint(0, len(df_dict[new_list[index]]))
        assert type(df_dict[new_list[index]]['invite_date'][random_row]) is datetime64[ns]

    def test_email(self):
        # email = df_dict['April2019Applicants']['email'][1]
        # assert email == 'maudas1@mapquest.com'
        new_list = []
        for key in df_dict.keys():
            new_list.append(key)
            pprint(new_list)
            return new_list
        index = random.randint(1, len(new_list))
        random_row = random.randint(1, len(df_dict[new_list[index]]))
        assert type(df_dict[new_list[index]]['email'][random_row]) is str

    def test_phone_number(self):
        # phone_number = df_dict['April2019Applicants']['phone_number'][1]
        # assert phone_number == '+449577280155'
        new_list = []
        for key in df_dict.keys():
            new_list.append(key)
            return new_list
        index = random.randint(1, len(new_list))
        random_row = random.randint(1, len(df_dict[new_list[index]]))
        assert type(df_dict[new_list[index]]['phone_number'][random_row]) is str
        assert len(df_dict[new_list[index]]['phone_number'][random_row]) == 13

    def test_dob(self):
        new_list = []
        for key in df_dict.keys():
            new_list.append(key)
            return new_list
        index = random.randint(1, len(new_list))
        random_row = random.randint(1, len(df_dict[new_list[index]]))
        assert type(df_dict[new_list[index]]['dob'][random_row]) is datetime64[ns]

    def test_candidate_name(self):
        new_list = []
        for key in df_dict.keys():
            new_list.append(key)
            return new_list
        index = random.randint(1, len(new_list))
        random_row = random.randint(1, len(df_dict[new_list[index]]))
        assert type(df_dict[new_list[index]]['candidate_name'][random_row]) is str

    def test_gender(self):
        new_list = []
        for key in df_dict.keys():
            new_list.append(key)
            return new_list
        index = random.randint(1, len(new_list))
        random_row = random.randint(1, len(df_dict[new_list[index]]))
        assert type(df_dict[new_list[index]]['gender'][random_row]) is str

    def test_city(self):
        new_list = []
        for key in df_dict.keys():
            new_list.append(key)
            return new_list
        index = random.randint(1, len(new_list))
        random_row = random.randint(1, len(df_dict[new_list[index]]))
        assert type(df_dict[new_list[index]]['city'][random_row]) is str

    def test_postcode(self):
        new_list = []
        for key in df_dict.keys():
            new_list.append(key)
            return new_list
        index = random.randint(1, len(new_list))
        random_row = random.randint(1, len(df_dict[new_list[index]]))
        assert type(df_dict[new_list[index]]['postcode'][random_row]) is str

    def test_address(self):
        new_list = []
        for key in df_dict.keys():
            new_list.append(key)
            return new_list
        index = random.randint(1, len(new_list))
        random_row = random.randint(1, len(df_dict[new_list[index]]))
        assert type(df_dict[new_list[index]]['address'][random_row]) is str

    def test_uni_name(self):
        new_list = []
        for key in df_dict.keys():
            new_list.append(key)
            return new_list
        index = random.randint(1, len(new_list))
        random_row = random.randint(1, len(df_dict[new_list[index]]))
        assert type(df_dict[new_list[index]]['uni_name'][random_row]) is str

    def test_degree_result(self):
        new_list = []
        for key in df_dict.keys():
            new_list.append(key)
            return new_list
        index = random.randint(1, len(new_list))
        random_row = random.randint(1, len(df_dict[new_list[index]]))
        assert type(df_dict[new_list[index]]['degree_result'][random_row]) is str

    def test_staff_name(self):
        new_list = []
        for key in df_dict.keys():
            new_list.append(key)
            return new_list
        index = random.randint(1, len(new_list))
        random_row = random.randint(1, len(df_dict[new_list[index]]))
        assert type(df_dict[new_list[index]]['staff_name'][random_row]) is str
