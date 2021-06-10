from app.classes.db.db_session import global_init
import sqlalchemy

SERVER = 'localhost,1433'
DATABASE = 'Data21Final'
USER = 'SA'
PASSWORD = 'Passw0rd2018'
DRIVER = 'SQL+Server'

conn_str = (
            f'mssql+pyodbc://{USER}:{PASSWORD}' +
            f'@{SERVER}/master?driver={DRIVER}'
            )

engine = sqlalchemy.create_engine(conn_str)
connection = engine.connect()

tables_columns = {
                'candidate': [
                            'candidate_id',
                            'candidate_name',
                            'date',
                            'self_development',
                            'geo_flex',
                            'financial_support',
                            'result',
                            'course_interest',
                            'gender',
                            'dob',
                            'email',
                            'city',
                            'address',
                            'postcode',
                            'phone_number',
                            'uni_degree',
                            'invited_date',
                            'invited_by'
                            ],
                'weaknesses_junc': [
                                'weakness_id', 
                                'candidate_id'
                                ],
                'weaknesses': [
                                'weakness_id', 
                                'weakness'
                                ],    
                'trainer': [
                            'trainer_id', 
                            'trainer_name'
                            ],
                'course': [
                            'course_id',
                            'trainer_id',
                            'course_name',
                            'type'
                            ],
                'test': [
                        'candidate_id', 
                        'date', 
                        'location', 
                        'presentation', 
                        'presentation_max', 
                        'psychometrics', 
                        'psychometrics_max'
                        ],
                'scores': [
                            'spartan_id',
                            'course_id',
                            'week_no',
                            'analytic',
                            'independent',
                            'determined',
                            'professional',
                            'studious',
                            'imaginative'
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
                'strengths': [
                            'strength_id', 
                            'strength'
                            ],
                'strength_junc': [
                                'strength_id', 
                                'candidate_id'
                                ],
                'spartan': [
                            'spartan_id', 
                            'candidate_id', 
                            'spartan_name'
                            ]
                }

# drop database
engine.execute(f"""
                USE master;
                DROP DATABASE IF EXISTS {DATABASE};
                """)

# initialise database
global_init(conn_str, DATABASE)
engine.execute(f'USE {DATABASE};')

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


def test_scores_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('scores')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['scores']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_spartan_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('spartan')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['spartan']:
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


def test_test_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('test')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['test']:
            result.append(True)
        
        else:
            result.append(False)
    
    assert all(result)


def test_trainer_columns():
    cols = engine.execute("""
                        SELECT name
                        FROM sys.columns
                        WHERE object_id = OBJECT_ID('trainer')
                        """)
    cols = [c[0] for c in cols]

    result = []

    for col in cols:
        if col in tables_columns['trainer']:
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
