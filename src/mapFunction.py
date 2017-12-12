'''
	Ce script utilise les requetes qu'il y a dans comprehention_DS.
	Se script contruit des maps de paris avec les emplacement des capteurs. 
'''

import folium
import pandas as pd
import sys

from tqdm import tqdm
import time
from datetime import datetime
from comprehension_DS import *

'''
return a tuple of (lat,long) from a string
'''
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
return a dictionnary of key Id_censor  and values (Lat,Log)
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
return the folium Map object on the map of paris
'''
def make_map_paris():
	return folium.Map(location=[48.85, 2.34] , tiles='Stamen Terrain' , zoom_start=12)


'''
create a map from a gived cursor of a query
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

'''
return a matric of size div*div from UpLeft and DownRight filled with GPS positions
'''
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

'''
print the content of the matrix of size div*div
'''
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


'''
plot the list of tuple (lat,long) of cluster y
'''
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

	
'''
return a value of traffic of a gived list who correspond to all data of the day
'''
def getAveragePonderate(list):
	coeff=[1,4,2,4,1]
	coeffLimit=[7,10,17,21,24]
	
	AllAverageValues=[]
	indexCoeff=0

	actualValues=[]
	for i in list:
		
		hour = i[1]
		if(hour==coeffLimit[indexCoeff]):
			indexCoeff+=1
			if(len(actualValues)==0):
				AllAverageValues.append(0)
			else:
				AllAverageValues.append(float(sum(actualValues))/len(actualValues))
			actualValues=[]

		if(i[3] is not None):
			actualValues.append(i[3])

	if(len(actualValues)==0):
		AllAverageValues.append(0)
	else:
		AllAverageValues.append(float(sum(actualValues))/len(actualValues))


	if(len(AllAverageValues)==0):
		return 0
	#print(AllAverageValues)

	finalValue =0
	indexCoeff=0
	for i in AllAverageValues:
		finalValue+= i*coeff[indexCoeff]
		indexCoeff+=1

	finalValue = float(finalValue/sum(coeff))
	return finalValue

'''
return a color of traffic for a gived value of occupation rate
'''
def getColor(value):
	if(value==0):
		return '#ADD8E6'
	if(value<5):
		return '#008000'
	elif(value<7):
		return '#FFFF00'
	elif(value<12):
		return '#FF0000'
	return '#000000'
