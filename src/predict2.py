from comprehension_DS import *
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier

from sklearn.datasets import load_iris
from sklearn import tree
import graphviz
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
	conn.close()

	return listOfList


def printSets(X_train, X_test, y_train, y_test):
	print(len(X_train))
	#print(X_train)
	print(len(y_train))
	#print(y_train)
	print(len(X_test))
	#print(X_test)
	print(len(y_test))
	#print(y_test)

def getColor(value):
	if(value==0):
		return 0
	if(value<5):
		return 1
	elif(value<7):
		return 2
	elif(value<12):
		return 3
	return 4

def getY(X):
	y=[]
	for i in range(len(X)):
		value = (X[i])[7]
		#print(getColor(value))
		y.append(getColor(value))
	return y

def get_train_test_sets(pourcent=0.33):
	
	strQuery = sensor_with_all_data_AllYearsNot_NULL()
	X=getLisOfList_All_BD(strQuery,0.2, 2013,2013)
	#print(X)
	flat_X=sum(X, [])

	print('size SET -> '+str(len(X[0])))
	#print(flat_X)
	#print('size list flatted '+str(len(flat_X)))
	#flat_X = [(3,4),(5,5),(7,8)]

	y=getY(flat_X)
	flat_X = [x[1:-2] for x in flat_X]
	print(flat_X)
	return train_test_split(flat_X, y, test_size=pourcent)

def trainDecisionTree(X_train, X_test, y_train, y_test):
	clf = DecisionTreeClassifier(criterion = "gini", random_state = 100, max_depth=7, min_samples_leaf=5)
	clf = clf.fit(X_train, y_train);
	score = clf.score(X_test, y_test);
	print(score)
	getTreeGraphizc(clf)


def getTreeGraphizc(clf):
	
	dot_data = tree.export_graphviz(clf, out_file="toto.dot", 
                         #feature_names=iris.feature_names,  
                         #class_names=iris.target_names,  
                         filled=True, rounded=True,  
                         special_characters=True) 
	#graph = graphviz.Source(dot_data)  
	

'''
flat_X = [(3,4,10,20),(3,4,10,20),(3,4,10,20)]
flat_X = [x[1:-2] for x in flat_X] 
print(flat_X)
'''

X_train, X_test, y_train, y_test=get_train_test_sets()
printSets(X_train, X_test, y_train, y_test)

trainDecisionTree(X_train, X_test, y_train, y_test)

