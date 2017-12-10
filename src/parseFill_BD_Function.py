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


