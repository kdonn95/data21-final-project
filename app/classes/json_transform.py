import json
from app.classes.json_extract import JsonExtract
from tabulate import tabulate
import datetime
import pandas as pd


class JsonTransform:
    def __init__(self, engine):
        # Setting up connection to sql server.
        self.engine = engine
        # Connecting to the sql server.
        connection = self.engine.connect()
        self.je = JsonExtract([])
        self.all_details = {'name': [],
                       'date': [],
                       'tech_self_score': [],
                       'strengths': [],
                       'weaknesses': [],
                       'self_dev': [],
                       'geo_flex': [],
                       'finance_support': [],
                       'result': [],
                       'course_interest': []}


    def to_bool(self, field):
        field = field.lower()
        if field == 'pass' or field == 'yes':
            return True
        elif field == 'fail' or field == 'no':
            return False
        else:
            pass


    def clean_text(self, text):
        return text.replace("'", "").title()


    def convert_date(self, value):
        return datetime.date(int(value[6:11]), int(value[3:5]), int(value[0:2]))


    def transform_to_df(self, page):
        page = page.fillna(0)
        # Cleans each name and then adds to empty dictionary (which will be turned to dataframe later)
        for name in page['name']:
            new_name = self.clean_text(name)
            self.all_details['name'].append(new_name)

        # Converts each date to correct format and then adds to dictionary
        for date in page['date']:
            new_date = self.convert_date(date)
            self.all_details['date'].append(new_date)

        # Adds the tech scores, strengths and weaknesses to dictionary
        for tech_score in page['tech_self_score']:
            if tech_score == 0:
                self.all_details['tech_self_score'].append({})
            else:
                self.all_details['tech_self_score'].append(tech_score)

        for strength in page['strengths']:
            self.all_details['strengths'].append(strength)

        for weakness in page['weaknesses']:
            self.all_details['weaknesses'].append(weakness)

        # Converts self development, geo-flexible, financial support self and result to a boolean and adds to dictionary
        for self_dev in page['self_development']:
            self_dev = self.to_bool(self_dev)
            self.all_details['self_dev'].append(self_dev)

        for geo_flex in page['geo_flex']:
            geo_flex = self.to_bool(geo_flex)
            self.all_details['geo_flex'].append(geo_flex)

        for finance_support in page['financial_support_self']:
            finance_support = self.to_bool(finance_support)
            self.all_details['finance_support'].append(finance_support)

        for result in page['result']:
            result = self.to_bool(result)
            self.all_details['result'].append(result)

        # Adds course onto dictionary
        for course in page['course_interest']:
            self.all_details['course_interest'].append(course)

        # Turns all details from dictionary created into a dataframe
        transformed_df = pd.DataFrame(self.all_details)
        return transformed_df