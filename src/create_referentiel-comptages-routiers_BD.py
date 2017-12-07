'''
    Ce script fait une BD(sqlite) Traffic.db avec une table Capteur qui indique l'emplacement de chaque capteur a Paris.
    Les champs de Capteur sont:index(INT), id_arc_tra(INT), lat(FLOAT), lon(FLOAT)
    Le fichier referentiel-comptages-routiers.csv contenant les donnes doivent être dans le dossier data.
'''

from tqdm import tqdm
import pandas as pd
import numpy as np
import sys

import sqlite3 as sql

def c_geo_point_2d_FLOAT(x):
	out = []
	for i in x.split(','):
	    try:
	        out.append(float(i))
	    except ValueError:
	        return [np.nan,np.nan]
	        raise(ValueError(' x='+str(x)))
	return out

def modeDict():
	converters = {'geo_point_2d':c_geo_point_2d_FLOAT }
	positions = pd.read_csv('data/referentiel-comptages-routiers.csv',delimiter=';',converters=converters)
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


def modeBD():
	conn = sql.connect('Traffic.db')
	converters = {'geo_point_2d':c_geo_point_2d_FLOAT }
	positions = pd.read_csv('data/referentiel-comptages-routiers.csv',delimiter=';',converters=converters,chunksize=50000)

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
	verifBD(conn)


def verifBD(conn):
	cur = conn.cursor()
	cur.execute('SELECT * FROM Capteur')
	for i in cur.fetchall():
	    print(i)

def verifBD_NULL(conn):
	cur = conn.cursor()
	#Que faire des capteur qui n'ont pas de position?
	cur.execute('SELECT * FROM Capteur WHERE lat is NULL or lon is NULL')

	#print(cur.description)
	for i in cur.fetchall():
	    print(i)


def parseFileReferentiel(isModeBD):

	if(isModeBD):
		modeBD()
	else:
		modeDict()

def testBD_BLABLA(conn):
	cur = conn.cursor()
	cur.execute('SELECT * FROM test where id_arc_trafics=1 LIMIT 100')
	for i in cur.fetchall():
		print(i)



def getPathFile(name ,year ,month):
	strYearMonth=""
	if(month<10):
		strYearMonth=str(year)+str(0)+str(month)
	else:
		strYearMonth=str(year)+str(month)

	folderName = str(year)+"_paris_donnees_trafic_capteurs"
	fileName = name+strYearMonth+'.txt'

	pathName = 'traffic/'+folderName+'/'+fileName

	return pathName


def editChunk(chunk):
	chunk=chunk.assign(hour=chunk.horodate.apply(lambda x:x.hour) ,
		year=chunk.horodate.apply(lambda x:x.year) ,
		dateNumber = chunk.horodate.apply(lambda x: getDay_Number(x.year,x.month,x.day)),
		numberWeek = chunk.horodate.apply(lambda x: getWeek_Number(x.year,x.month,x.day)))
	#chunk=chunk.assign(year=chunk.horodate.apply(lambda x:x.year))
	#chunk=chunk.assign()
	#chunk=chunk.assign()
	return chunk[['id_arc_trafics','year','numberWeek','dateNumber','hour','debit', 'taux_occ']]


def getPositions(pathName):

	return pd.read_csv(pathName,delimiter='\t',
			chunksize=10000,
			names=["id_arc_trafics", "horodate", "debit", "taux_occ"],parse_dates=['horodate'],decimal=',')

from concurrent import futures
def parseFolderDataTraffic():

	strNameFolder = "donnees_trafic_capteurs_"
	startYear = 2013
	numberOfYears=1
	numberOfMonth = 1

	conn = sql.connect('Blabla.db')

	nbThreads = 4
	executor = futures.ProcessPoolExecutor(max_workers=nbThreads)

	listOfFuturs=[]

	for year in tqdm(range(startYear,startYear+numberOfYears)):
		for month in tqdm(range(1,numberOfMonth+1)):

			pathName =getPathFile(strNameFolder ,year ,month)
			try:
				positions = getPositions(pathName)
			except FileNotFoundError :
				print("\nfile not available -> "+pathName+"\n")
				continue

			for chunk in positions:
				listOfFuturs.append(executor.submit(editChunk,chunk))

			for fut in tqdm(futures.as_completed(listOfFuturs), total=len(listOfFuturs)):
				putToDB(fut.result(),conn)
			conn.commit()
	conn.close()
	executor.shutdown()

def putToDB(chunk,conn):
	chunk.to_sql('test',con=conn, if_exists='append')

def getDay_Number(year,month,day):

	'''
	print("year ="+str(year))
	print("month ="+str(month))
	print("day ="+str(day))
	'''
	import time
	from datetime import datetime
	datetime = datetime(year,month,day)
	return datetime.weekday()


def getWeek_Number(year,month,day):

	import time
	from datetime import datetime
	datetime = datetime(year,month,day)
	return datetime.isocalendar()[1]


#print(getWeekAndDay_Number(2017,12,6))

#modeBD()
#modeDict()
print("\n")
parseFolderDataTraffic()

#conn = sql.connect('Blabla.db')
#testBD_BLABLA(conn)
print("FINISH")
#modeDict()
