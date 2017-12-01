'''
    Ce script fait une BD(sqlite) Traffic.db avec une table Capteur qui indique l'emplacement de chaque capteur a Paris.
    Les champs de Capteur sont:index(INT), id_arc_tra(INT), lat(FLOAT), lon(FLOAT)
    Le fichier referentiel-comptages-routiers.csv contenant les donnes doivent être dans le dossier data.
'''

from tqdm import tqdm_notebook as tqdm
import pandas as pd
import numpy as np
import sys

import sqlite3 as sql
conn = sql.connect('Traffic.db')

def c_geo_point_2d(x):
    out = []
    for i in x.split(','):
        try:
            out.append(float(i))
        except ValueError:
            return [np.nan,np.nan]
            raise(ValueError(' x='+str(x)))
    return out
converters = {'geo_point_2d':c_geo_point_2d }

positions = pd.read_csv('data/referentiel-comptages-routiers.csv',delimiter=';',converters=converters,chunksize=50000)
i=0
for chunk in tqdm(positions):
    try:
        chunk=chunk.assign(id_arc_tra=chunk.id_arc_tra.apply(lambda x: 0 if np.isnan(x) else int(x)))
        chunk=chunk.assign(lat=chunk.geo_point_2d.apply(lambda x:x[0]))
        chunk=chunk.assign(lon=chunk.geo_point_2d.apply(lambda x:x[1]))
        chunk=chunk[['id_arc_tra','lat','lon']]
        chunk.to_sql('Capteur',con=conn,if_exists='replace')
    except ValueError:
        print("Erreur:Database")
        break


conn.commit()
cur = conn.cursor()
#Que faire des capteur qui n'ont pas de position?
cur.execute('SELECT * FROM Capteur WHERE lat is NULL')
for i in cur.fetchall():
    print(i)
