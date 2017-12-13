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


'''
compute the probabilty to apply to a gived year depending of the size of the set and the global probability asked: p
'''
def computeProba(p , actualYear, startYear,endYear):
	#return p
	if(startYear==endYear):
		return p

	interval = (endYear+1) - actualYear
	div = 2*interval
	proba = p/div
	return  proba

'''
return a list of values from the cur ( query request)
with the p probability amortized by the interval of years (find with  computeProba)
if startYear = 2013
endYear = 2017
p= 0.5
'''
import random
def getRandomizedListeFromBd(cur,p,startYear,endYear):

	#listAllYears=[]
	actualList = []

	#first=True
	#actualIdYear = startYear
	#actualProba = computeProba(p,actualIdYear,startYear,endYear)

	listOfProba=[]
	for i in range(startYear,endYear+1):
		actualProba = computeProba(p,i,startYear,endYear)
		listOfProba.append(actualProba)
		print('proba '+str(i-startYear)+' '+str(actualProba))

	endedWithBreak=False
	for i in tqdm(cur.fetchall() , desc='getRandomizedListeFromBd'):
		
		#print('i ->' +str(i))
		
		year = i[2]
		'''
		if(year > actualIdYear):
			
			listAllYears.append(actualList)
			actualList= []
			actualIdYear+=1
			if(actualIdYear==endYear+1):
				endedWithBreak=True
				break
			actualProba = computeProba(p,actualIdYear,startYear,endYear)
		else:
		'''
		if(random.uniform(0, 1)<listOfProba[year-startYear]):
				#print('add -> '+str(i))
				#print()
			actualList.append(i)
	'''
	if(not endedWithBreak):
		listAllYears.append(actualList)
	'''
	return actualList

'''
return a list who each part is a list of values of a year
years are in order of time
'''
def getLisOf_All_BD(strQuery,p,startYear,endYear):
	
	conn = sql.connect(dataBaseName)
	cur = conn.cursor()
	cur.execute(strQuery)

	lisOf_All_BD = getRandomizedListeFromBd(cur,p ,startYear,endYear )
	cur.close()
	conn.close()
	
	return lisOf_All_BD

'''
print size of Set of Train and Test
'''
def printSets(X_train, X_test, y_train, y_test):
	print(len(X_train))
	#print(X_train)
	print(len(y_train))
	#print(y_train)
	print(len(X_test))
	#print(X_test)
	print(len(y_test))
	#print(y_test)

'''
arbitrary clusters
'''
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

'''
return list of corresponding clusters of the list X
'''
def getY(X):
	y=[]
	for i in range(len(X)):
		value = (X[i])[7]
		#print(getColor(value))
		y.append(getColor(value))
	return y

'''
return a list of values from a query with a p probability
'''
def getListOfDataFromQuery(query,p=0.5,name='query'):
	conn = sql.connect(dataBaseName)
	cur = conn.cursor()
	cur.execute(query)

	listOfData= []
	for i in tqdm(cur.fetchall() , desc=name):
		if(random.uniform(0, 1)<p):
			listOfData.append(i)
	
	cur.close()
	conn.close()
	return listOfData

'''
get data from the query and produce a train and test set
p is the global probability of piking a raw of a query
pourcent is the repartion between the train and test set
'''
def get_train_test_sets(strQuery ,startYear,endYear,proba=0.5,pourcent=0.33):
	X=getLisOf_All_BD(strQuery,proba, startYear,endYear)

	print('size SET -> '+str(len(X)))
	
	y=getY(X)
	print("clusters finded")

	#remove index BD  , debit , taux_occ
	X = [x[1:-2] for x in X]
	return train_test_split(X, y, test_size=pourcent)


'''
return a decisionTree fited with the arguments
and print a score
outputFile create a graphizv file and a html represention of the differences between the testSet and Predicted values of TestSet
'''
from mapPrinter import *
def trainDecisionTree(X_train, X_test, y_train, y_test,outputFiles=False):
	clf = DecisionTreeClassifier(criterion = "gini", random_state = 100, max_depth=7, min_samples_leaf=5)
	print("start decision")
	clf = clf.fit(X_train, y_train);
	score = clf.score(X_test, y_test);
	print(score)

	if(outputFiles):
		getTreeGraphizc(clf)
		yPredicted = clf.predict(X_test)
		mapDifferences('treeDecision',X_test,y_test , yPredicted)
	
	return clf

'''
create a TreeGraphizc.dot file
'''
def getTreeGraphizc(clf):
	
	dot_data = tree.export_graphviz(clf, out_file="treeGraphizc.dot", 
                         feature_names=['Sensor','Year','Week','Day','Hour'],
                         class_names=['None','Green','Orange','Red','Black'],  
                         filled=True, rounded=True,  
                         special_characters=True) 
	#graph = graphviz.Source(dot_data)  

'''
create an HTML file of the futur prediction of the month of november
'''
def testTreeOnNovembre2017(clf):
	strQuery = sensor_with_all_data_Not_NULL_Nov2017()
	x_listNovember2017 = getListOfDataFromQuery(strQuery,0.5,'november 2017')

	y_listNovember2017=getY(x_listNovember2017)
	x_listNovember2017 = [x[1:-2] for x in x_listNovember2017]

	yPredicted = clf.predict(x_listNovember2017)
	mapDifferences('november',x_listNovember2017,y_listNovember2017 , yPredicted)

'''
Create decision tree fit with all data without month of novemeber of 2017
and call the function testTreeOnNovembre2017()
'''
def analysePredictionNovember():
	strQuery = sensor_with_all_data_AllYearsNot_NULL_notNov2017()
	X_train, X_test, y_train, y_test=get_train_test_sets(strQuery ,2017,2017,0.7)
	clf = trainDecisionTree(X_train, X_test, y_train, y_test)
	testTreeOnNovembre2017(clf)

#strQuery = sensor_with_all_data_AllYearsNot_NULL()
analysePredictionNovember()

