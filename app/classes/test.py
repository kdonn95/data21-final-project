import sqlalchemy
import pandas as pd

server = 'localhost,1433'
database = 'Northwind'
user = 'SA'
password = 'Passw0rd2018'
driver = 'SQL+Server'

engine = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")

connection = engine.connect()


result = engine.execute('SELECT ProductID, ProductName, SupplierID FROM Products')
#result = engine.execute('SELECT SupplierID, ProductName FROM Products')
data = []

for x in list(result):
    data.append(list(x))

print(data)

df = pd.DataFrame(data, columns=['ProductID', 'ProductName', 'SupplierID'])

#______________________________
# sql loading:
server = 'localhost,1433'
database = 'testing'
user = 'SA'
password = 'Passw0rd2018'
driver = 'SQL+Server'
engine1 = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")
connection = engine.connect()

df.to_sql("test6", engine1, if_exists='append', index=False)

df = pd.DataFrame(["Snowplows"], columns=['ProductName'])
df.to_sql("test6", engine1, if_exists='append', index=False)


#engine1.execute(f'INSERT INTO test6 (ProductName) VALUES ("Snowplows")')
