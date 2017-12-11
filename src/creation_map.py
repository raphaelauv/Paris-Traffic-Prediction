'''
	Ce script utilise les requetes qu'il y a dans comprehention_DS.
	Se script contruit des maps de paris avec les emplacement des capteurs. 
'''

import folium
import pandas as pd
import numpy as np
import sys
from tqdm import tqdm
import sqlite3 as sql
import time
from datetime import datetime
from comprehension_DS import *

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
return a dictionnary
'''
def modeDict():
	converters = {'geo_point_2d':c_geo_point_2d_FLOAT }
	positions = pd.read_csv('data/referentiel-comptages-routiers.csv',delimiter=';',converters=converters)
	positions['lat']=positions['geo_point_2d'].apply(lambda x:x[0])
	positions['lon']=positions['geo_point_2d'].apply(lambda x:x[1])

	from collections import defaultdict
	posdict = defaultdict(lambda :(0,0))
	cmp=0
	for j,i in positions[['id_arc_tra','lat','lon']].iterrows():
		if(not np.isnan(i.id_arc_tra)):
			id_arc_tra = int(i.id_arc_tra)
			posdict[id_arc_tra]=(i.lat,i.lon)
		else:
			print("line "+str(cmp)+" do not have id_arc_tra -> "+str(i.lat)+" "+str(i.lon))
		cmp+=1
	return posdict

'''
get image of paris
'''
def make_map_paris():
	#tiles='Stamen Toner' , zoom_start=12
	return folium.Map(location=[48.85, 2.34])

sensor_dict=modeDict()

def make_map_from_request(name,index,color_='crimson',save=True):
    map_osm = make_map_paris()
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

def putToMapOSM(arrayOfItems,map_osm):
	for i in arrayOfItems:
		i.add_to(map_osm)

def map_sensor_with_taux_occ_bigger_than_100():
	taux_occ_sup_100()
	make_map_from_request('map_sensor_with_taux_occ_bigger_than_100.html',1)


def getMatrix(latUpLeft , lonUpLeft , latDownRight , lonDownRight, div):
	diffLat = abs(latUpLeft - latDownRight)
	diffLon = abs(lonUpLeft - lonDownRight)

	curLat=diffLat/div
	curLon=diffLon/div

	latUpLeft -= curLat/3
	lonUpLeft += curLon/3

	#matrix = [[0.0 for x in range(div)] for y in range(div)] 
	#matrix[0][0]= (latUpLeft,lonUpLeft)
	matrix = [0.0 for x in range(div*div)] 
	matrix[0]= (latUpLeft,lonUpLeft)
	first=True
	for i in range(0,div):
		for j in range(0,div):
			if(not first):
				matrix[(i*div)+j]=(latUpLeft-(curLat*i) , lonUpLeft + (curLon*j))
			else:
				first=False

	return matrix

def printMatrix(matrix,div):
	for i in range(0,div):
		for j in range(0,div):
			print (matrix[(i*div)+j] , end='   ')
		print('')

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
create a matrix of paris
'''
def createHTML_MATRIX():
	div=15
	matrix =getMatrix(48.906254,2.260094 ,48.807316 , 2.426262 , div)
	#printMatrix(matrix,div)
	nameFile="matrixMaster.html"
	make_map_from_matrice(matrix,nameFile,div)


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
https://stackoverflow.com/questions/20792445/calculate-rgb-value-for-a-range-of-values-to-create-heat-map
'''
def rgb(minimum, maximum, value):
    minimum, maximum = float(minimum), float(maximum)
    ratio = 2 * (value-minimum) / (maximum - minimum)
    b = int(max(0, 255*(1 - ratio)))
    r = int(max(0, 255*(ratio - 1)))
    g = 255 - b - r
    return (r, g, b)

'''
https://stackoverflow.com/questions/3380726/converting-a-rgb-color-tuple-to-a-six-digit-code-in-python
'''
def getHexaHTML(rgb):
	return '#%02x%02x%02x' % rgb


'''
create a html file of the items of the kmean clustering
'''
def createHTMLfromClustering(npallItems,x,y,name):
	Alllat = npallItems[:,0]
	Alllon = npallItems[:,1]
	max_value = max(y)
	min_value = min(y)
	map_osm = make_map_paris()
	for i in tqdm(range(len(Alllat)),desc=name):
		color=getHexaHTML(rgb(min_value,max_value,y[i]))
		folium.Circle(radius=10,location=(Alllat[i],Alllon[i]),popup=str(Alllat[i])+" : "+str(Alllon[i]),color=str(color),fill=False).add_to(map_osm)
	map_osm.save(name)


def plotFromClustering(npallItems,x,y):

	import matplotlib.pyplot as plt
	

	plt.rcParams['figure.figsize'] = (16, 9)
	plt.style.use('ggplot')
	plt.scatter(npallItems[:,0], npallItems[:,1], c=y, alpha=1 , marker="o", label='points');
	correctedX0 = [x/1000 for x in x[:,0] ]
	correctedX1 = [x for x in x[:,1] ]
	correctedX0= np.array(correctedX0)
	correctedX1= np.array(correctedX1)
	
	#plt.scatter(correctedX0, correctedX1, c=uniqueY, alpha=0.9 , label='centers');
	plt.show()

def KmeansFromRequest(name,index,k):
	import numpy as np
	

	sensor_with_all_data()
	
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
	
	createHTMLfromClustering(npallItems,x,y,name)
	#plotFromClustering(npallItems,x,y)

def put_sensors(map,index,color='red'):
    for i in tqdm(cur.fetchall()):
        item=sensor_dict[i[index]]
        if(not np.isnan(item).any()):
            if(len(item)==2):
                folium.Circle(radius=10,location=item,popup=str(i[index]),color=color,fill=False).add_to(map)
            else:
                print("Sensor without lat and lon -> "+str(i[index]))


def map_sensors_by_stats(year):
    map_osm = make_map_paris()
    capteur_broken(year)
    feat_group1 = folium.FeatureGroup(name="Capteurs hors services(Aucune valeurs sur 70% des example)")
    put_sensors(feat_group1,0,'red')
    capteur_without_taux_occ(year)
    feat_group2 =  folium.FeatureGroup(name="Capteurs sans taux d'occupation 70% du temps.")
    put_sensors(feat_group2,0,'purple')
    sensor_with_all_data(year)
    feat_group3 =  folium.FeatureGroup(name="Cateurs avec debit et taux d'occupation 70% du temps")
    put_sensors(feat_group3,1,'green')
    map_osm.add_child(feat_group1)
    map_osm.add_child(feat_group2)
    map_osm.add_child(feat_group3)
    map_osm.add_child(folium.LayerControl())
    map_osm.save("map_sensors_by_stats_"+year+".html")

	
#KmeansFromRequest('map_sensor_with_all_data.html',1,50)

#createHTML_MATRIX()
#map_sensor_without_taux_occ()
#map_sensor_with_taux_occ_bigger_than_100()
#map_sensor_with_all_data()
#map_sensors_by_stats('2015')


#if __name__ == '__main__':
#map_all_sensor()
