import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2

df1 = pd.read_csv("NBA Payroll(1990-2023).csv")

df1['payroll'] = df1['payroll'].str.replace('$','')
df1['payroll'] = df1['payroll'].str.replace(',','')
df1['payroll'] = df1['payroll'].astype(int)

df1['inflationAdjPayroll'] = df1['inflationAdjPayroll'].str.replace('$','')
df1['inflationAdjPayroll'] = df1['inflationAdjPayroll'].str.replace(',','')
df1['inflationAdjPayroll'] = df1['inflationAdjPayroll'].astype(int)

df1['created_at'] = datetime.now()

df1.drop('Unnamed: 0',axis=1,inplace=True)

df1['id'] = df1.index

conn_string = 'postgresql://root:root@localhost:5432/test_db'

db = create_engine(conn_string)
conn = db.connect()

#start_time = time.time()
df1[['id','team','seasonStartYear',	'payroll','inflationAdjPayroll','created_at']].to_sql('NBA_Payroll', schema='desafio1',con=conn, if_exists='replace', index=False)
