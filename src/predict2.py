from comprehension_DS import *
from sklearn.model_selection import train_test_split

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


def get_train_test_sets(pourcent=0.33):
    strQuery = sensor_with_all_data_AllYearsNot_NULL()
    X=getLisOfList_All_BD(strQuery,0.2, 2013,2017)
    flat_X=sum(X, [])
    y=range(len(flat_X))
    return train_test_split(X, y, test_size=pourcent)

X_train, X_test, y_train, y_test=get_train_test_sets()

print(len(X_train))
print(len(y_train))
print(len(X_test))
print(len(y_test))
