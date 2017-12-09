'''

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
	print(posdict)
	return posdict

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


def parseManual(line):
	arrayLine = line.split(" ")
	for i in range(len(arrayLine)):
		print(arrayLine[i])


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

import numpy as np
import matplotlib.pyplot as plt
def plotQuery(list):

	
	flagBlack = []
	flagRed = []
	flagYellow = []
	flagGreen = []
	listDays = []
	listHours = []
	
	for i in list:
		valDay = ((i[4] + (i[3]*7) )*24) + i[5]
		listDays.append(valDay)
		
		valFlag= findFlag(i[6],i[7])

		if(valFlag==FLAG_BLACK):
			flagBlack.append(valDay)

		elif(valFlag==FLAG_RED):
			flagRed.append(valDay)

		elif(valFlag==FLAG_YELLOW):
			flagRed.append(valDay)

		else:
			flagGreen.append(valDay)
		

	
	#plt.plot(listDays, 'r--')
	plt.plot(flagRed, 'r--')
	plt.plot(flagGreen, 'g--')
	plt.plot(flagYellow, 'y--')
	plt.plot(flagBlack, 'b--')
	plt.show()


def testBD_BLABLA(conn):
	cur = conn.cursor()
	cur.execute('SELECT * FROM test where id_arc_trafics=1 LIMIT 500')
	plotQuery(cur.fetchall())
	for i in cur.fetchall():
		valFlag= findFlag(i[6],i[7])
		print(str(i)+" -> "+getStrColor(valFlag))




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

FLAG_BLACK =4
FLAG_RED =3
FLAG_YELLOW =2
FLAG_GREEN =1

def getStrColor(color):
	if(color==FLAG_BLACK):
		return "BLACK"
	elif(color==FLAG_RED):
		return "RED"
	elif(color==FLAG_YELLOW):
		return "YELLOW"
	else:
		return "GREEN"

def findFlag(debit,taux):
	
	if(taux>35):
		return FLAG_BLACK
	elif(taux>25):
		return FLAG_RED
	elif(taux>10):
		return FLAG_YELLOW
	else:
		return FLAG_GREEN

def editChunk(chunk):
	chunk=chunk.assign(hour=chunk.horodate.apply(lambda x:x.hour) ,
		year=chunk.horodate.apply(lambda x:x.year) ,
		numberWeek = chunk.horodate.apply(lambda x:x.isocalendar()[1]),
		dateNumber = chunk.horodate.apply(lambda x:x.isoweekday())
		
		#dateNumber = chunk.horodate.apply(lambda x: getDay_Number(x.year,x.month,x.day)),
		#numberWeek = chunk.horodate.apply(lambda x: getWeek_Number(x.year,x.month,x.day)),
		)
	#chunk=chunk.assign(BDay=chunk.horodate.apply(lambda x:x.isoweekday()))
	return chunk[['id_arc_trafics','year','numberWeek','dateNumber','hour','debit', 'taux_occ']]


def getPositions(pathName):

	return pd.read_csv(pathName,delimiter='\t',
			chunksize=50000, infer_datetime_format=True,
			names=["id_arc_trafics", "horodate", "debit", "taux_occ"],parse_dates=['horodate'],decimal=',')

from concurrent import futures
def parseFolderDataTraffic(startYear,numberOfYears,numberOfMonth):

	strNameFolder = "donnees_trafic_capteurs_"

	conn = sql.connect('Blabla.db')
	import multiprocessing

	nbThreads = multiprocessing.cpu_count()
	executor = futures.ProcessPoolExecutor(max_workers=nbThreads)

	for year in tqdm(range(startYear,startYear+numberOfYears)):
		for month in tqdm(range(1,numberOfMonth+1)):
			listOfFuturs=[]
			pathName =getPathFile(strNameFolder ,year ,month)
			try:
				positions = getPositions(pathName)
			except FileNotFoundError :
				print("\nfile not available -> "+pathName+"\n")
				continue

			for chunk in positions:
				listOfFuturs.append(executor.submit(editChunk,chunk))

			for fut in tqdm(futures.as_completed(listOfFuturs), total=len(listOfFuturs), desc=pathName):
				putToDB(fut.result(),conn)
			conn.commit()
	conn.close()
	executor.shutdown()

def putToDB(chunk,conn):
	chunk.to_sql('test',con=conn, if_exists='append')

def getDay_Number(year,month,day):
	import time
	from datetime import datetime
	datetime = datetime(year,month,day)
	return datetime.weekday()


def getWeek_Number(year,month,day):
	import time
	from datetime import datetime
	datetime = datetime(year,month,day)
	return datetime.isocalendar()[1]


#modeBD()
#modeDict()
print("\n")
parseFolderDataTraffic(2017,1,12)

print("\n")
#conn = sql.connect('Blabla.db')
#testBD_BLABLA(conn)
print("FINISH")
#modeDict()
