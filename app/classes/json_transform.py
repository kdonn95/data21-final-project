from app.classes.json_extract import JsonExtract
from tabulate import tabulate
from datetime import datetime
import datetime
import pandas as pd

je = JsonExtract([])
page1_df = next(je.yield_pages())
print(page1_df.columns)
print(tabulate(page1_df))


class JsonTransform:
    def __init__(self, engine):
        # Setting up connection to sql server.
        self.engine = engine
        # Connecting to the sql server.
        connection = self.engine.connect()
        #
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
        return text.title().replace("'", "")

    def convert_date(self, value):
        return datetime.date(int(value[6:11]), int(value[3:5]), int(value[0:2]))
        # datetime.strptime(value, '%d/%m/%Y').isoformat()[:-9].




#x = datetime.datetime(2018, 9, 15, 12, 45, 35)
#print(page1_df.dtypes())
#print(type(page1_df['strengths'].tolist()))
#print(tabulate(page1_df.filter(len(page1_df[strengths > 3].tolist())>3)))

# ['name', 'date'
# 'self_development', 'geo_flex', 'financial_support_self', 'result'

# 'tech_self_score', 'strengths', 'weaknesses',,'course_interest']