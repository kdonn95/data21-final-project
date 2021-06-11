from app.classes.json_extract import JsonExtract
from tabulate import tabulate
import datetime

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
#print(tabulate(page1_df))

all_names = []
for name in page1_df['name']:
    all_names.append(name)

print(all_names)




#['name', 'date', 'tech_self_score', 'strengths', 'weaknesses','self_development', 'geo_flex', 'financial_support_self', 'result','course_interest']