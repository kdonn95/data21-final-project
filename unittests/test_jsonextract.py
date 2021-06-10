from app.classes.jsonextract import JsonExtract
import pandas as pd

je = JsonExtract()


def test_return_bucket_json_body():
    #je.bucket_name = 'data21-final-project'
    body = je.return_bucket_json_body('Talent/10383.json')
    assert body == {'name': 'Stillmann Castano',
                    'date': '22/08/2019',
                    'tech_self_score': {'C#': 6,
                                        'Java': 5,
                                        'R': 2,
                                        'JavaScript': 2},
                    'strengths': ['Charisma'],
                    'weaknesses': ['Distracted', 'Impulsive', 'Introverted'],
                    'self_development': 'Yes',
                    'geo_flex': 'Yes',
                    'financial_support_self': 'Yes',
                    'result': 'Pass',
                    'course_interest': 'Business'}


def test_get_bucket_json_key_list():
    keylist = je.get_bucket_json_key_list()
    assert isinstance(keylist, list)
    assert len(keylist) == 11
    assert sum([len(i) for i in keylist]) == 3105


def test_yield_pages():
   first_page = next(je.yield_pages())
   assert len(first_page) == 300
   assert isinstance(first_page.iloc[0], pd.core.series.Series)
   assert all(first_page.iloc[0] == pd.Series({'name': 'Stillmann Castano',
                    'date': '22/08/2019',
                    'tech_self_score': {'C#': 6,
                                        'Java': 5,
                                        'R': 2,
                                        'JavaScript': 2},
                    'strengths': ['Charisma'],
                    'weaknesses': ['Distracted', 'Impulsive', 'Introverted'],
                    'self_development': 'Yes',
                    'geo_flex': 'Yes',
                    'financial_support_self': 'Yes',
                    'result': 'Pass',
                    'course_interest': 'Business'}))


def test_json_file_to_dataframe():
    df = je.json_file_to_dataframe([('data21-final-project',
                                  'Talent/10383.json')])
    assert all(df.columns == ['name', 'date', 'tech_self_score',
               'strengths', 'weaknesses',
               'self_development', 'geo_flex',
               'financial_support_self', 'result',
               'course_interest'])
    assert all(df.iloc[0] == pd.Series({'name': 'Stillmann Castano',
                    'date': '22/08/2019',
                    'tech_self_score': {'C#': 6,
                                        'Java': 5,
                                        'R': 2,
                                        'JavaScript': 2},
                    'strengths': ['Charisma'],
                    'weaknesses': ['Distracted', 'Impulsive', 'Introverted'],
                    'self_development': 'Yes',
                    'geo_flex': 'Yes',
                    'financial_support_self': 'Yes',
                    'result': 'Pass',
                    'course_interest': 'Business'}))