# importando pacotes
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
import json

# baixando dados
df1 = pd.read_json("json_data.json")

# como a coluna jobs é um dicionario, achei interessante colocar cada possivel
#  job numa coluna com seu respectivo valor
# talvez fosse interessante fazer o mesmo para as colunas   "tags","locations" e "industries"
# porem optei por nao fazer para a tabela nao ficar muito extensa
df1=pd.concat([df1.drop(['jobs'], axis=1), df1['jobs'].apply(pd.Series)], axis=1)

#criando data de criacao
df1['created_at'] = datetime.now()

#criando conexao e subindo dados para presto
conn_string = 'postgresql://root:root@localhost:5432/test_db'

db = create_engine(conn_string)
conn = db.connect()

#start_time = time.time()
df1.to_sql('startups',schema='desafio1', con=conn, if_exists='replace', index=False)