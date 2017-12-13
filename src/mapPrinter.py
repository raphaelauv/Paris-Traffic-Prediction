'''
	Ce script utilise les requetes qu'il y a dans comprehention_DS.
	Se script contruit des maps de paris avec les emplacement des capteurs. 
'''
from mapFunction import *

'''
create an HTML map of a Query 
index is position of Id_arc in the Query result
'''
def make_map_from_request(cur,name,index,color_='crimson',save=True):
	map_osm = make_map_paris()
	sensor_dict=modeDict()
	for i in tqdm(cur.fetchall() , desc=name):
		item=sensor_dict[i[index]]
		if(not np.isnan(item).any()):
			if(len(item)==2):
				folium.Circle(radius=10,location=item,popup='The Waterfront',color=color_,fill=False).add_to(map_osm)
			else:
				print("Sensor without lat and lon -> "+str(i[index]))
	if(save):
		map_osm.save(name)



import dill
def createOSMObjectArray(ArrayOfitem):
	out= []
	for i in ArrayOfitem:
		#print(dill.pickles(folium.Circle(radius=10,location=i,popup='The Waterfront',color='crimson',fill=False)))
		out.append(folium.Circle(radius=10,location=i,popup='The Waterfront',color='crimson',fill=False))
	print(out)
	return out
	

def putToMapOSM(arrayOfItems,map_osm):
	for i in arrayOfItems:
		i.add_to(map_osm)


from concurrent import futures
import multiprocessing
def emptyListFuturs(listOfFuturs,map_osm):
	for fut in futures.as_completed(listOfFuturs):
		putToMapOSM(fut.result(),map_osm)

'''
parallelised version ( not working in processMode because folium.Circle is not pickable)
but working in mode ThreadPoolExecutor
'''
def map_all_sensor():
	
	nbThreads = multiprocessing.cpu_count()
	
	listOfFuturs=[]
	map_osm = make_map_paris()
	sensor_dict=modeDict()
	arrayOfItems=[]
	cmp=0
	cmpJ=0
	#executor = futures.ProcessPoolExecutor(max_workers=nbThreads)
	for id,item in tqdm(sensor_dict.items()):
		if(not np.isnan(item).any()):
			if(len(item)==2):
				arrayOfItems.append(item)
				cmp+=1
				folium.Circle(radius=10,location=item,popup='The Waterfront',color='crimson',fill=False).add_to(map_osm)
				'''
		if(cmp==10):
			listOfFuturs.append(executor.submit(createOSMObjectArray,arrayOfItems))
			arrayOfItems=[]
			cmpJ+=1
			cmp=0
		if(cmpJ>10):
			emptyListFuturs(listOfFuturs,map_osm)
			listOfFuturs=[]

	emptyListFuturs(listOfFuturs,map_osm)
	executor.shutdown()
	if(cmp>1):
		putToMapOSM(createOSMObjectArray(arrayOfItems),map_osm)
	'''
	map_osm.save('sensors_map.html')


def map_sensor_with_taux_occ_bigger_than_100():
	

	conn = sql.connect(dataBaseName)
	cur = conn.cursor()
	strQuery = taux_occ_sup_100()
	cur.execute(strQuery)
	make_map_from_request(cur,'map_sensor_with_taux_occ_bigger_than_100.html',1)

	conn.close()



'''
create a map of the matrix gived , div is the size of the matrix
'''
def make_map_from_matrice(matrix,name,div):
	map_osm = make_map_paris()
	lastItem=[]
	first=True
	for i in tqdm(range(div) , desc=name):
		for j in range(div):
			item=matrix[(i*div)+j]
			folium.Circle(radius=10,location=item,popup=str(item[0])+" : "+str(item[1]),color='red',fill=False).add_to(map_osm)
			if(first):
				first=False
				lastItem=item
			else:
				#folium.PolyLine(locations=[item,lastItem] ,color= 'red' ).add_to(map_osm)
				lastItem=item
	map_osm.save(name)



'''
plot a kmeans of the matrix gived
'''
def plotKMeansMatrix():
	import numpy as np
	import matplotlib.pyplot as plt
	from scipy.cluster.vq import kmeans2, whiten
	div=15
	matrix=getMatrix(48.906254,2.260094 ,48.807316 , 2.426262 ,div)
	arr= np.array(matrix)
	k=10
	x, y = kmeans2(whiten(arr), k, iter = 100)
	
	uniqueY = list(set(y))

	plt.rcParams['figure.figsize'] = (16, 9)
	plt.style.use('ggplot')
	plt.scatter(arr[:,0], arr[:,1], c=y, alpha=1 , marker="s", label='points');
	
	#plt.scatter(x[:,0], x[:,1], c=uniqueY, alpha=0.9 , label='centers');
	plt.show()
	

'''
create an HTML file or plot the kmeans for a gived cursor on a Query
k number of clusters
index posiition of id of sensor in the quuery result
'''
def KmeansFromRequest(cur,name,index,k,modeHTML=True):
	
	
	sensor_dict=modeDict()
	
	from pyproj import Proj
	#transformation should be done here

	allItems=[]
	for i in tqdm(cur.fetchall() , desc=name):
		item=sensor_dict[i[index]]
		if(not np.isnan(item).any()):
			if(len(item)==2):
				if(item[0] !=0 and item[1] !=0):
					allItems.append(item)
				
	
	
	from scipy.cluster.vq import kmeans2, whiten

	npallItems= np.array(allItems)
	x, y = kmeans2(whiten(npallItems), k, iter = 10)
	if(modeHTML):
		createHTMLfromClustering(npallItems,x,y,name)
	else:
		plotFromClustering(npallItems,x,y)

	



def Kmeans_Of_sensor_with_all_data_AllYears():
	conn = sql.connect(dataBaseName)
	cur = conn.cursor()
	strQuery = sensor_with_all_data_AllYears()
	cur.execute(strQuery)
	KmeansFromRequest(cur,'map_sensor_Kmeans.html',1,50,True)
	conn.close()

'''
put inside the gived group all the sensors of the curr (cursor on query)
'''
def put_sensors(sensor_dict,cur,group,index,color='red'):
	for i in tqdm(cur.fetchall()):
		item=sensor_dict[i[index]]
		if(not np.isnan(item).any()):
			if(len(item)==2):
				folium.Circle(radius=10,location=item,popup=str(i[index]),color=color,fill=False).add_to(group)
			else:
				print("Sensor without lat and lon -> "+str(i[index]))


'''
create HTML file of stats of data of the year gived
sensor_broken
sensor_without_taux_occ
sensor_with_all_data
'''
def map_sensors_by_stats(year):
	map_osm = make_map_paris()
	sensor_dict=modeDict()
	conn = sql.connect(dataBaseName)
	cur = conn.cursor()

	strQuery = capteur_broken(year)
	cur.execute(strQuery)
	feat_group1 = folium.FeatureGroup(name="Capteurs hors services(Aucune valeurs sur 70% des example)")
	put_sensors(sensor_dict,cur,feat_group1,0,'red')

	strQuery = capteur_without_taux_occ(year)
	cur.execute(strQuery)
	feat_group2 =  folium.FeatureGroup(name="Capteurs sans taux d'occupation 70% du temps.")
	put_sensors(sensor_dict,cur,feat_group2,0,'purple')

	strQuery = sensor_with_all_data(year)
	cur.execute(strQuery)
	feat_group3 =  folium.FeatureGroup(name="Cateurs avec debit et taux d'occupation 70% du temps")
	put_sensors(sensor_dict,cur,feat_group3,1,'green')

	map_osm.add_child(feat_group1)
	map_osm.add_child(feat_group2)
	map_osm.add_child(feat_group3)
	map_osm.add_child(folium.LayerControl())
	map_osm.save("map_sensors_by_stats_"+year+".html")

	conn.close()

'''
create an HTML file of the traffic of a gived day ( Average Ponderate )
'''
def AverageDayMap(year,numberWeek , dayNumber):
	
	conn = sql.connect(dataBaseName)
	cur = conn.cursor()
	strQuery = allDataFromDate(year,numberWeek,dayNumber)
	cur.execute(strQuery)
	sensor_dict=modeDict()

	id_arc = 0
	first=True

	listValues =[]

	list_Item_AverageValues=[]
	for i in cur.fetchall():
		if(first):
			id_arc = i[0]
			first=False
		else:
			if(id_arc != i[0]):
				item=sensor_dict[id_arc]
				valueItem = getAveragePonderate(listValues)
				list_Item_AverageValues.append((id_arc,item,valueItem))
				id_arc=i[0]
				listValues =[]

		
		listValues.append(i)

	item=sensor_dict[id_arc]
	valueItem = getAveragePonderate(listValues)
	list_Item_AverageValues.append((id_arc,item,valueItem))
	

	map_osm = make_map_paris()
	for i in tqdm(list_Item_AverageValues , desc='AverageDayMap'):
		id_arc = i[0]
		item = i[1]
		if(i[2]==0):
			continue
		color = getColor(i[2])
		if(not np.isnan(item).any()):
			if(len(item)==2):
				folium.Circle(radius=10,location=item,popup=str(id_arc)+' | '+str(i[2]) ,color=color,fill=False).add_to(map_osm)
	map_osm.save('AverageDayMap.html')
	

def map_sensor_with_all_data():
	conn = sql.connect(dataBaseName)
	cur = conn.cursor()
	strQuery = sensor_with_all_data_AllYears()
	cur.execute(strQuery)
	make_map_from_request(cur,'map_sensor_with_all_data.html',1)
	conn.close()

'''
create an html file of the correct and less corred sensors predicted by ta model
'''
def mapDifferences(x,y,predictedY):
	from collections import defaultdict
	dicoFrequencyGoodAndFalse = defaultdict(lambda :(0,0))

	index=0
	cmp=0
	for i in tqdm(x,desc='mapDifferences 1/2'):
		nbSensor = i[index]
		lastTuple =dicoFrequencyGoodAndFalse[nbSensor]
		if(y[cmp]==predictedY[cmp]):
			lastTuple=(lastTuple[0]+1,lastTuple[1])
		else:
			lastTuple=(lastTuple[0],lastTuple[1]+1)
		dicoFrequencyGoodAndFalse[nbSensor]=lastTuple
		cmp+=1



	map_osm = make_map_paris()
	sensor_dict=modeDict()
	feat_group1 =  folium.FeatureGroup(name="Sensor with more than 80% of good predictions")
	feat_group2 =  folium.FeatureGroup(name="Sensor with more than 60% of good predictions")
	feat_group3 =  folium.FeatureGroup(name="Sensor with more than 40% of good predictions")
	feat_group4 =  folium.FeatureGroup(name="Sensor with less than 40% of good predictions")
	cmp=0

	for id,tupleValues in tqdm(dicoFrequencyGoodAndFalse.items(),desc='mapDifferences 2/2'):
		item=sensor_dict[id]

		nbGoodValues = float(tupleValues[0]/(tupleValues[0]+tupleValues[1]))

		if(not np.isnan(item).any()):
			if(len(item)==2):
				if(nbGoodValues>0.8):
					folium.Circle(radius=10,location=item,popup=str(i[index]),color='#008000',fill=False).add_to(feat_group1)
				elif(nbGoodValues>0.6):
					folium.Circle(radius=10,location=item,popup=str(i[index]),color='#FFFF00',fill=False).add_to(feat_group2)
				elif(nbGoodValues>0.4):
					folium.Circle(radius=10,location=item,popup=str(i[index]),color='#FF0000',fill=False).add_to(feat_group3)
				else:
					folium.Circle(radius=10,location=item,popup=str(i[index]),color='#000000',fill=False).add_to(feat_group4)
			else:
				print("Sensor without lat and lon -> "+str(i[index]))

		cmp+=1
	map_osm.add_child(feat_group1)
	map_osm.add_child(feat_group2)
	map_osm.add_child(feat_group3)
	map_osm.add_child(feat_group4)
	map_osm.add_child(folium.LayerControl())
	map_osm.save('DecisionTreeDifference.html')

#AverageDayMap(2013,2,4)
#Kmeans_Of_sensor_with_all_data_AllYears()
#createHTML_MATRIX()
#map_sensor_with_taux_occ_bigger_than_100()
#map_sensors_by_stats('2013')
#map_sensor_with_all_data()

#if __name__ == '__main__':
#map_all_sensor()
