import psycopg2
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.orm import sessionmaker


engine = create_engine('postgresql://python_user:qwerty@localhost:5432/questrade_api', )

metadata = MetaData()
users = Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String))

metadata.create_all(engine)

conn = psycopg2.connect(dbname='test', user='python_user', host='localhost', password='qwerty')
create_table_query = '''CREATE TABLE mobile3
          (ID INT PRIMARY KEY     NOT NULL,
          MODEL           TEXT    NOT NULL,
          PRICE         REAL); '''
cursor = conn.cursor()
cursor.execute(create_table_query)
conn.commit()
cursor.close()
conn.close()