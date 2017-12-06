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

def c_geo_point_2d(x):
    out = []
    for i in x.split(','):
        try:
            out.append(float(i))
        except ValueError:
            return [np.nan,np.nan]
            raise(ValueError(' x='+str(x)))
    return out

def modeBD(positions,conn):
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
    #verifBD(conn)

def verifBD(conn):
    cur = conn.cursor()
    #Que faire des capteur qui n'ont pas de position?
    cur.execute('SELECT * FROM Capteur WHERE lat is NULL or lon is NULL')

    #print(cur.description)
    for i in cur.fetchall():
        print(i)
    



def parseFileReferentiel(isModeBD):
    conn = sql.connect('Traffic.db')
    converters = {'geo_point_2d':c_geo_point_2d }
    positions = pd.read_csv('data/referentiel-comptages-routiers.csv',delimiter=';',converters=converters,chunksize=50000)
    
    if(isModeBD):
        modeBD(positions,conn)
    else:
        '''
        positions['lat']=positions['geo_point_2d'].apply(lambda x:x[0])
        positions['lon']=positions['geo_point_2d'].apply(lambda x:x[1]) 
        from collections import defaultdict
        posdict = defaultdict(lambda :{'lat':0,'lon':0})
        for j,i in positions[['id_arc_tra','lat','lon']].iterrows():
            id_arc_tra = float(i.id_arc_tra)
            lat = i.lat
            lon = i.lon
            posdict[id_arc_tra]={'lat':lat,'lon':lon}
        print(positions.head(100))
        '''

def parseFileTrafficYearMounth(name ,year ,month):

    strYearMonth=""
    if(month<10):
        strYearMonth=str(year)+str(0)+str(month)
    else:
        strYearMonth=str(year)+str(month)

    folderName = str(year)+"_paris_donnees_trafic_capteurs"
    fileName = name+strYearMonth+'.txt'

    pathName = 'traffic/'+folderName+'/'+fileName
    print(pathName)

    in_it = pd.read_csv(pathName,delimiter='\t',chunksize=50000,names=['id_arc_trafic','date','debit','taux'])
    
    for chunk in tqdm(in_it):
        try:
            # line by line pre-processing
            #print(chunk)
            #chunk.debit = chunk.debit.apply(lambda x: 0 if np.isnan(x) else x)
            #chunk.taux = chunk.taux.apply(lambda x: 0 if np.isnan(x) else x)
            #chunk=chunk.assign(hour=chunk.horodate.apply(lambda x:x.hour))
            #chunk=chunk.assign(month=chunk.horodate.apply(lambda x:x.month))
            #chunk=chunk.assign(BDay=chunk.horodate.apply(lambda x:x.isoweekday() not in (6,7)))
            # map from counter id to its position
            #chunk = chunk.assign(lat=chunk.id_arc_trafic.apply(lambda x:posdict[float(x)]['lat']))
            #chunk = chunk.assign(lon=chunk.id_arc_trafic.apply(lambda x:posdict[float(x)]['lon']))        
            #chunk = chunk[['hour','month','BDay','debit','taux','id_arc_trafic','lat','lon']]
            print(chunk)
            #chunk.to_sql('traffic',conn,if_exists='append')
        except ValueError:
            break


def parseFolderDataTraffic():

    strNameFolder = "donnees_trafic_capteurs_"
    NumberOfYears=5
    startYear = 2013

    for year in range(startYear,startYear+NumberOfYears):
        for month in range(1,12+1):     
            parseFileTrafficYearMounth(strNameFolder,year,month)
            break


def getWeekAndDay_Number(year,month,day):
    import time
    from datetime import datetime
    datetime = datetime(year,month,day)
    dateNumber=datetime.weekday()
    weekNumber=datetime.isocalendar()[1]

    return (weekNumber,dateNumber)

print(getWeekAndDay_Number(2017,12,6))

parseFileReferentiel(True)

#parseFolderDataTraffic()