from parseFill_BD_Function import *


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
print the content of the db of traffic
'''
def testBD_pandas(nameBD):
	conn = sql.connect(nameBD)
	cur = conn.cursor()
	cur.execute('SELECT * FROM test where id_arc_trafics=1 LIMIT 500')
	for i in cur.fetchall():
		valFlag= findFlag(i[6],i[7])
		print(str(i)+" -> "+getStrColor(valFlag))


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
chunk is a pandas series of data 
conn the connector to the SQL bd
'''
def putToDB(chunk,conn):
	chunk.to_sql('test',con=conn, if_exists='append')



'''
parse and store to the db all the folders from year : startYear
exemple : 2013 , 4 , 1 ,12

will parse and store the 12 month of year 2013 ,2014 ,2015 and 2016
'''
def parseFolderDataTraffic(nameBD,startYear,numberOfYears,startMonth,numberOfMonth):

	strNameFolder = "donnees_trafic_capteurs_"

	conn = sql.connect(nameBD)
	cur =conn.cursor()

	import multiprocessing
	from concurrent import futures

	nbThreads = multiprocessing.cpu_count()
	executor = futures.ProcessPoolExecutor(max_workers=nbThreads)

	for year in tqdm(range(startYear,startYear+numberOfYears)):
		for month in tqdm(range(startMonth,startMonth+numberOfMonth)):
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


print("\n")
parseFolderDataTraffic('Blabla.db',2016,1,1,12)
print("\n")
#testBD_pandas('Blabla.db')
