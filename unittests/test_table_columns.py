from app.classes.db.db_session import global_init
from app.classes.get_config import GetConfig
import sqlalchemy

config = GetConfig()

conn_str = (
            f'mssql+pyodbc://{config.user}:{config.password}' +
            f'@{config.server}/master?driver={config.driver}'
            )

engine = sqlalchemy.create_engine(conn_str)
connection = engine.connect()

tables_columns = {
                'candidate': [
                            'candidate_id',
                            'candidate_name',
                            'gender',
                            'dob',
                            'email',
                            'city',
                            'address',
                            'postcode',
                            'phone_number',
                            'uni_name',
                            'degree_result',
                            'staff_id'
                            ],
                'course_staff_junc': [
                                    'course_id',
                                    'staff_id'
                                    ],
                'course_type': [
                                'course_type_id',
                                'type'
                                ],
                'course': [
                            'course_id',
                            'course_type_id',
                            'course_name',
                            ],
                'location': [
                            'location_id',
                            'location'
                            ],
                'sparta_day': [
                            'candidate_id',
                            'location_id', 
                            'date', 
                            'result',
                            'self_development',
                            'financial_support',
                            'geo_flex',
                            'course_interest', 
                            'presentation', 
                            'presentation_max', 
                            'psychometrics', 
                            'psychometrics_max'
                            ],
                'staff': [
                            'staff_id', 
                            'staff_name',
                            'department'
                            ],
                'strength_junc': [
                                'strength_id', 
                                'candidate_id'
                                ],
                'strengths': [
                            'strength_id', 
                            'strength'
                            ],
                'tech_junc': [
                            'tech_id', 
                            'candidate_id',
                            'score'
                            ],
                'tech': [
                        'tech_id', 
                        'tech'
                        ],
                'weaknesses_junc': [
                                'weakness_id', 
                                'candidate_id'
                                ],
                'weaknesses': [
                                'weakness_id', 
                                'weakness'
                                ],    
                'weekly_performance': [
                                        'candidate_id',
                                        'course_id',
                                        'week_no',
                                        'analytic',
                                        'independent',
                                        'determined',
                                        'professional',
                                        'studious',
                                        'imaginative'
                                        ]
                }

# drop database
engine.execute(f"""
                USE master;
                DROP DATABASE IF EXISTS {config.database};
                """)

# initialise database
global_init(conn_str, config.database, "DEBUG")
engine.execute(f'USE {config.database};')


def test_candidate_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('candidate')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['candidate']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_course_staff_junc_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('course_staff_junc')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['course_staff_junc']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_course_type_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('course_type')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['course_type']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_course_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('course')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['course']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_location_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('location')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['location']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_sparta_day_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('sparta_day')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['sparta_day']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_staff_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('staff')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['staff']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_strength_junc_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('strength_junc')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['strength_junc']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_strengths_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('strengths')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['strengths']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_tech_junc_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('tech_junc')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['tech_junc']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_tech_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('tech')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['tech']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_weaknesses_junc_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('weaknesses_junc')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['weaknesses_junc']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_weaknesses_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('weaknesses')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['weaknesses']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_weekly_performance_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('weekly_performance')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['weekly_performance']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)
