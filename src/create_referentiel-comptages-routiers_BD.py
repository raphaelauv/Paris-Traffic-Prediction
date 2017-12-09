'''
All the functions necessary to parse and store to the BD
'''

from tqdm import tqdm
import pandas as pd
import numpy as np
import sys

import sqlite3 as sql
import time
from datetime import datetime

def c_geo_point_2d_FLOAT(x):
	out = []
	for i in x.split(','):
	    try:
	        out.append(float(i))
	    except ValueError:
	        return [np.nan,np.nan]
	        raise(ValueError(' x='+str(x)))
	return out

'''
read the file of geo positions of sensors
and return a dictionnary of values
'''
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

'''
read the file of geo positions of sensors
and create a BD SQLite maned Traffic.db
'''
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
	#verifBD(conn)

'''
print the content of the traffic sensors BD
'''
def verifBD(conn):
	cur = conn.cursor()
	cur.execute('SELECT * FROM Capteur')
	for i in cur.fetchall():
	    print(i)
'''
print the content of the traffic sensors BD without nul values
'''
def verifBD_NULL(conn):
	cur = conn.cursor()
	#Que faire des capteur qui n'ont pas de position?
	cur.execute('SELECT * FROM Capteur WHERE lat is NULL or lon is NULL')

	#print(cur.description)
	for i in cur.fetchall():
	    print(i)



import numpy as np
import matplotlib.pyplot as plt

'''
plot the result of a Query
'''
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

'''
print the content of the db of traffic
'''
def testBD_BLABLA(conn):
	cur = conn.cursor()
	cur.execute('SELECT * FROM test where id_arc_trafics=1 LIMIT 500')
	#plotQuery(cur.fetchall())
	for i in cur.fetchall():
		valFlag= findFlag(i[6],i[7])
		print(str(i)+" -> "+getStrColor(valFlag))



'''
return the str of the file correspondig to the Year , month gived
'''
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

'''
cluster values of debit and taux 
'''
def findFlag(debit,taux):
	
	if(taux>35):
		return FLAG_BLACK
	elif(taux>25):
		return FLAG_RED
	elif(taux>10):
		return FLAG_YELLOW
	else:
		return FLAG_GREEN

'''
return the value of the year inside the x :str   | 2017-10-15   -> 2015
'''
def parseYear(x):
	return int(x.split('-')[0])

'''
return the value of the hour inside the x :str   | 23-01-12   -> 23
'''
def parseHour(x):
	return int(x.split(':')[0])
	'''
	tab= x.split(':')
	try:
		return int(tab[0])
	except ValueError:
		return np.nan
	'''


'''
return number of the day in the week , monday=0 ...
'''
def getDay_DATE(date):
	tab= date.split("-")
	year = int(tab[0])
	month = int(tab[1])
	day = int(tab[2])
	return datetime(year,month,day).weekday()

'''
return number of the week in the year
'''
def getWeek_DATE(date):	
	tab= date.split("-")
	year = int(tab[0])
	month = int(tab[1])
	day = int(tab[2])
	return  datetime(year,month,day).isocalendar()[1]

'''
return a tuple of number of day and number of week  | date is a str of date : 2017-04-29
'''
def getTupleWeek_Day(date):
	tab= date.split("-")
	year = int(tab[0])
	month = int(tab[1])
	day = int(tab[2])
	mydate = datetime(year,month,day)
	return (mydate.weekday() , mydate.isocalendar()[1])


'''
Performane is not here, weird
'''
def exemple(x):
	mytuple = getTupleWeek_Day(x['date'])
	x['numberWeek']= mytuple[0]
	x['dateNumber']= mytuple[1]

'''
create new column with desirade data
'''
def editChunk(chunk):
	chunk=chunk.	assign(
		hour=chunk.theHour.apply(lambda x:parseHour(x)) ,
		year=chunk.date.apply(lambda x:parseYear(x)) ,
		dateNumber = chunk.date.apply(lambda x: getDay_DATE(x)),
		numberWeek = chunk.date.apply(lambda x: getWeek_DATE(x)),
		#year=chunk.horodate.apply(lambda x:x.year) ,
		#numberWeek = chunk.horodate.apply(lambda x:x.isocalendar()[1]),
		#dateNumber = chunk.horodate.apply(lambda x:x.isoweekday())
		)
	#chunk=chunk.assign(BDay=chunk.horodate.apply(lambda x:x.isoweekday()))
	#chunk = chunk.apply(exemple,axis=1)
	return chunk[['id_arc_trafics','year','numberWeek','dateNumber','hour','debit', 'taux_occ']]

'''
return a pandas object iterable of the file in argument
'''
def getPositions(pathName):
	return pd.read_csv(pathName, header=None , delimiter=r"\s+",dtype={'date':'str','theHour':'str'},
			chunksize=50000,names=["id_arc_trafics", "date","theHour", "debit", "taux_occ"],decimal=',')


'''
direct parse of data
'''
def getTupleWithData(line):
	line = line[:-1]
	arrayLine =line.split("\t")
	#print(arrayLine)
	id_arc_trafics = int(arrayLine[0])

	strYear = arrayLine[1].split(" ")
	
	year = parseYear(strYear[0])

	typleDate = getTupleWeek_Day(strYear[0])

	dateNumber = typleDate[0]
	numberWeek = typleDate[1]

	hour = parseHour(strYear[1])
	
	debit =-1
	if(len(arrayLine)>3):
		if(len(arrayLine[2])>0):
			#print("str->"+str(arrayLine[2]))
			val=arrayLine[2].replace(",",".")
			debit =float(val)

	taux_occ=-1
	if(len(arrayLine)>4):
		if(len(arrayLine[3])>0):
			val=arrayLine[3].replace(",",".")
			#print("->"+str(arrayLine[3]))
			taux_occ = float(val)
		

	return (id_arc_trafics,year,numberWeek,dateNumber,hour,debit,taux_occ)


'''
test performance of direct parsing in python
'''
def createBulk(namePath,conn):
	with open(namePath) as infile:
		array=[]
		cmp=0
		cmpT=0
		for line in infile:
			#print(cmpT)
			cmpT+=1
			array.append(getTupleWithData(line))
			
			if(cmp==1000):
				cur = conn.cursor()
				cur.execute('BEGIN TRANSACTION')
				for i in array:
					if(len(i)<7):
						cur.execute('INSERT OR IGNORE INTO test  VALUES (?,?,?,?,?,?)', (i[0], i[1] , i[2] ,i[3],i[4],i[5]))
					elif(len(i)<8):
						cur.execute('INSERT OR IGNORE INTO test  VALUES (?,?,?,?,?,?,?)', (i[0], i[1] , i[2] ,i[3],i[4],i[5],i[6]))
					else:
						cur.execute('INSERT OR IGNORE INTO test  VALUES (?,?,?,?,?,?,?,?)', (i[0], i[1] , i[2] ,i[3],i[4],i[5],i[6],i[7]))

				cur.execute('COMMIT')
				cmp=0

		if(cmp>0):
			cur = conn.cursor()
			cur.execute('BEGIN TRANSACTION')
			cur.execute('COMMIT')


from concurrent import futures
'''
parse and store to the db all the folders from year : startYear
exemple : 2013 , 4 , 12

will parse and store the 12 month of year 2013 ,2014 ,2015 and 2016
'''
def parseFolderDataTraffic(startYear,numberOfYears,numberOfMonth):

	strNameFolder = "donnees_trafic_capteurs_"

	conn = sql.connect('Blabla.db')
	cur =conn.cursor()

	cur.execute('CREATE TABLE IF NOT EXISTS test (id_arc_trafics INTEGER, year INTEGER, numberWeek INTEGER, dateNumber INTEGER , hour INTEGER , debit REAL , taux_occ REAL)')

	cursor = conn.execute('select * from test')
	names = list(map(lambda x: x[0], cursor.description))
	print(names)
	return

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

'''
chunk is a pandas series of data 
conn the connector to the SQL bd
'''
def putToDB(chunk,conn):
	chunk.to_sql('test',con=conn, if_exists='append')



conn = sql.connect('Blabla.db')
#createBulk("traffic/2013_paris_donnees_trafic_capteurs/donnees_trafic_capteurs_201301.txt",conn)
print("\n")

parseFolderDataTraffic(2013,1,1)

print("\n")

#testBD_BLABLA(conn)
print("FINISH")

