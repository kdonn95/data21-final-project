import pytest
import boto3
from app.classes.json_transform import *


def test_name():
    df = JsonTransform.clean_text('self', 'Juan D\'Arcy')
    assert isinstance(df, str)
    assert "'" not in df


def test_date():
    df = JsonTransform.convert_date('self', '17/03/2023')
    assert isinstance(df, datetime.date)


def test_bools():
    df = JsonTransform.to_bool('self', 'Fail')
    assert isinstance(df, bool)
    assert df in (True, False)


def test_course():
    df = JsonTransform.course('self', 'Data')
    assert isinstance(df, str)
    assert df.istitle() == True

