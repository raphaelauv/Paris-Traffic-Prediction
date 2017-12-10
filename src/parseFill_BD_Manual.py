from parseFill_BD_Function import *

'''
print the content of the db of traffic
'''
def testBD_Manual(nameBD):
	conn = sql.connect(nameBD)
	cur = conn.cursor()
	cur.execute('SELECT * FROM test where id_arc_trafics=1 LIMIT 500')
	#plotQuery(cur.fetchall())
	for i in cur.fetchall():
		print(i)
		if(i[5] is not None and i[6] is not None):
			valFlag= findFlag(i[5],i[6])
			print(str(i)+" -> "+getStrColor(valFlag))



'''
direct parse of data
'''
def getTupleWithData(line):
	line = line[:-1]
	arrayLine =line.split("\t")
	id_arc_trafics = int(arrayLine[0])

	strYear = arrayLine[1].split(" ")
	
	year = parseYear(strYear[0])

	typleDate = getTupleWeek_Day(strYear[0])

	dateNumber = typleDate[0]
	numberWeek = typleDate[1]

	hour = parseHour(strYear[1])
	
	debit =-1
	if(len(arrayLine)>2):
		if(len(arrayLine[2])>0):
			#print("str->"+str(arrayLine[2]))
			val=arrayLine[2].replace(",",".")
			debit =float(val)
	else:
		return (id_arc_trafics,year,numberWeek,dateNumber,hour)

	taux_occ=-1
	if(len(arrayLine)>3):
		
		if(len(arrayLine[3])>0):
			val=arrayLine[3].replace(",",".")	
			taux_occ = float(val)
			return (id_arc_trafics,year,numberWeek,dateNumber,hour,debit,taux_occ)
	
	return (id_arc_trafics,year,numberWeek,dateNumber,hour,debit)



def manualPutInBd(array,conn,cur):
	
	for i in array:
		if(len(i)<6):
			cur.execute('INSERT INTO test  VALUES (?,?,?,?,?,?,?)', (i[0], i[1] , i[2] ,i[3],i[4],None,None ))
		elif(len(i)<7):
			#print('insert -> '+str(i[0]))
			cur.execute('INSERT INTO test  VALUES (?,?,?,?,?,?,?)', (i[0], i[1] , i[2] ,i[3],i[4],i[5] ,None))
		else:
			cur.execute('INSERT INTO test  VALUES (?,?,?,?,?,?,?)', (i[0], i[1] , i[2] ,i[3],i[4],i[5],i[6] ))
	
		

'''
test performance of direct parsing in python
'''
def createBulk(pathName,conn,cur):
	
	with open(pathName) as infile:
		array=[]
		cmpT=0
		
		for line in tqdm(infile ,desc=pathName):
			cmpT+=1
			array.append(getTupleWithData(line))
			
			if(cmpT==1000):
				cur.execute('begin')
				manualPutInBd(array,conn,cur)
				conn.commit()
				cmp=0

		if(cmpT>0):
			cur.execute('begin')
			manualPutInBd(array,conn,cur)
			conn.commit()
	#cur.execute('rollback')


def parseFolderDataTrafficManual(nameBD,startYear,numberOfYears,numberOfMonth):

	strNameFolder = "donnees_trafic_capteurs_"
	conn = sql.connect(nameBD)
	cur =conn.cursor()

	cur.execute('CREATE TABLE IF NOT EXISTS test (id_arc_trafics INTEGER, year INTEGER, numberWeek INTEGER, dateNumber INTEGER , hour INTEGER , debit REAL , taux_occ REAL)')
	conn.commit()
	#cursor = conn.execute('select * from test')
	#names = list(map(lambda x: x[0], cursor.description))
	#print(names)

	for year in tqdm(range(startYear,startYear+numberOfYears)):
		for month in tqdm(range(1,numberOfMonth+1)):
			pathName =getPathFile(strNameFolder ,year ,month)
			createBulk(pathName,conn,cur)

	conn.close()


print("\n")
parseFolderDataTrafficManual('Blabla.db',2013,1,1)
print("\n")

testBD_Manual('Blabla.db')
print("FINISH")