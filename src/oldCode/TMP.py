from tqdm import tqdm_notebook as tqdm
import pandas as pd
import numpy as np
import sys

import sqlite3 as sql
conn = sql.connect('Blabla.db')

name_fic= "data/donnees_trafic_capteurs_201301.txt"
positions = pd.read_csv(name_fic,delimiter='\t',chunksize=50000,names=["id_arc_trafics", "horodate", "debit", "taux_occ"],parse_dates=['horodate'],decimal=',')

for chunk in tqdm(positions):
    
    #chunk.id_arc_trafics = chunk.id_arc_trafics.apply(lambda x: 0 if np.isnan(x) else int(x))
    #chunk.debit=chunk.debit.apply(lambda x:float(x.replace(',','.')))
    #chunk.taux_occ= chunk.taux_occ.apply(lambda x:float(x))
    chunk=chunk.assign(year=chunk.horodate.apply(lambda x:x.year))
    chunk=chunk.assign(hour=chunk.horodate.apply(lambda x:x.hour))
    chunk=chunk.assign(month=chunk.horodate.apply(lambda x:x.month))
    chunk=chunk.assign(BDay=chunk.horodate.apply(lambda x:x.day))
    chunk=chunk[['id_arc_trafics','year','hour','month','BDay','debit', 'taux_occ']]
    chunk.to_sql('test',con=conn,if_exists='append')


print("Salut")
tqdm(conn.commit())
print("Salut")
cur = conn.cursor()
print("Salut")
#Que faire des capteur qui n'ont pas de position?
cur.execute('SELECT * FROM test where id_arc_trafics=1')
for i in cur.fetchall():
    print(i)

