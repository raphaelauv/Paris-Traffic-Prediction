from comprehension_DS import *

'''
imperfect solution for Moving average
'''
def testProba(p):
	l = 0
	for i in range(2013,2018):
		l+=computeProba(p,i,2017)
		print(computeProba(p,i,2017))
	print('val -> '+str(l))



def computeProba(p , actualYear, endYear):
	interval = (endYear+1) - actualYear
	div = 2*interval
	proba = p/div
	return  proba


import random
def getRandomizedListeFromBd(cur,p,startYear,endYear):

	listAllYears=[]
	actualList = []

	first=True
	actualIdYear = startYear
	actualProba = computeProba(p,actualIdYear,endYear)
	endedWithBreak=False
	for i in tqdm(cur.fetchall() , desc='getRandomizedListeFromBd'):
		
		#print('i ->' +str(i))
		
		year = i[2]
		
		if(year > actualIdYear):
			
			listAllYears.append(actualList)
			actualList= []
			actualIdYear+=1
			if(actualIdYear==endYear+1):
				endedWithBreak=True
				break
			actualProba = computeProba(p,actualIdYear,endYear)
		else:
			if(random.uniform(0, 1)<actualProba):
				#print('add -> '+str(i))
				#print()
				actualList.append(i)
	
	if(not endedWithBreak):
		listAllYears.append(actualList)

	return listAllYears

def getLisOfList_All_BD(strQuery,p,startYear,endYear):
	
	conn = sql.connect(dataBaseName)
	cur = conn.cursor()
	cur.execute(strQuery)

	listOfList = getRandomizedListeFromBd(cur,p ,startYear,endYear )
	#conn.close()

	return listOfList

strQuery = sensor_with_all_data_AllYearsNot_NULL()
listOfList = getLisOfList_All_BD(strQuery,0.2, 2013,2017)

for i in range(len(listOfList)):
	print(len(listOfList[i]))