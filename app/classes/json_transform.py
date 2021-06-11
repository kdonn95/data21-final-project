from app.classes.json_extract import JsonExtract
from tabulate import tabulate
import datetime
import pandas as pd

je = JsonExtract([])
page1_df = next(je.yield_pages())


class JsonTransform:
    def __init__(self):
        self.je = JsonExtract([])

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


jt = JsonTransform()
print(page1_df.columns)

all_details = {'name': [],
               'date': [],
               'tech_self_score': [],
               'strengths': [],
               'weaknesses': [],
               'self_dev': [],
               'geo_flex': [],
               'finance_support': [],
               'result': [],
               'course_interest': []
}

for name in page1_df['name']:
    new_name = jt.clean_text(name)
    all_details['name'].append(new_name)

for date in page1_df['date']:
    new_date = jt.convert_date(date)
    all_details['date'].append(new_date)

for tech_score in page1_df['tech_self_score']:
    all_details['tech_self_score'].append(tech_score)

for strength in page1_df['strengths']:
    all_details['strengths'].append(strength)

for weakness in page1_df['weaknesses']:
    all_details['weaknesses'].append(weakness)

for self_dev in page1_df['self_development']:
    self_dev = jt.to_bool(self_dev)
    all_details['self_dev'].append(self_dev)

for geo_flex in page1_df['geo_flex']:
    geo_flex = jt.to_bool(geo_flex)
    all_details['geo_flex'].append(geo_flex)

for finance_support in page1_df['financial_support_self']:
    finance_support = jt.to_bool(finance_support)
    all_details['finance_support'].append(finance_support)

for result in page1_df['result']:
    result = jt.to_bool(result)
    all_details['result'].append(result)

for course in page1_df['course_interest']:
    all_details['course_interest'].append(course)


df = pd.DataFrame(all_details)

print(tabulate(df))