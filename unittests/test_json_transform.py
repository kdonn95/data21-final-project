import pytest
import boto3
from app.classes.json_transform import *


# These tests checks our cleaning functions work correctly
def test_name():
    df = JsonTransform().clean_text('Juan D\'Arcy')
    assert isinstance(df, str)
    assert "'" not in df


def test_date():
    df = JsonTransform().convert_date('17/03/2023')
    assert isinstance(df, datetime.date)


def test_bools():
    df = JsonTransform().to_bool('Fail')
    assert isinstance(df, bool)
    assert df in (True, False)


# Tests whether each column is in the correct form
def test_columntypes():
    df = assessment_df
    for i in assessment_df['name']:
        assert isinstance(i, str)
    for i in assessment_df['date']:
        assert isinstance(i, datetime.date)
    for i in assessment_df['tech_self_score']:
        assert isinstance(i, dict)
    for i in assessment_df['strengths']:
        assert isinstance(i, list)
    for i in assessment_df['weaknesses']:
        assert isinstance(i, list)
    for i in assessment_df['self_dev']:
        assert isinstance(i, bool)
    for i in assessment_df['geo_flex']:
        assert isinstance(i, bool)
    for i in assessment_df['finance_support']:
        assert isinstance(i, bool)
    for i in assessment_df['result']:
        assert isinstance(i, bool)
    for i in assessment_df['course_interest']:
        assert isinstance(i, str)