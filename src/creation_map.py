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

def make_map_from_request(name,index):
    map_osm = make_map_paris()
    for i in tqdm(cur.fetchall() , desc=name):
        item=sensor_dict[i[index]]
        if(not np.isnan(item).any()):
            if(len(item)==2):
                folium.Circle(radius=10,location=item,popup='The Waterfront',color='crimson',fill=False).add_to(map_osm)
        else:
            print("Sensor without lat and lon -> "+str(i[index]))
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


def map_sensor_without_taux_occ():
    capteur_without_taux_occ()
    make_map_from_request('map_sensor_without_taux_occ.html',0)

def map_sensor_with_taux_occ_bigger_than_100():
    taux_ccc_sup_100()
    make_map_from_request('map_sensor_with_taux_occ_bigger_than_100.html',1)

def map_sensor_with_all_data():
    sensor_with_all_data()
    make_map_from_request('map_sensor_with_all_data.html',1)


def getPositionsBlock(latUpLeft , lonUpLeft , latDownRight , lonDownRight, div):
    diffLat = abs(latUpLeft - latDownRight)
    diffLon = abs(lonUpLeft - lonDownRight)

    curLat=diffLat/div
    curLon=diffLon/div

    latUpLeft -= curLat/3
    lonUpLeft += curLon/3

    matrix = [[0.0 for x in range(div)] for y in range(div)] 
    matrix[0][0]= (latUpLeft,lonUpLeft)
    first=True
    for i in range(0,div):
        for j in range(0,div):
            if(not first):
                matrix[i][j]=(latUpLeft-(curLat*i) , lonUpLeft + (curLon*j))
            else:
                first=False

    return matrix

def printMatrix(matrix,div):
    for i in range(0,div):
        for j in range(0,div):
            print (matrix[i][j] , end='   ')
        print('')

def make_map_from_matrice(matrix,name,div):
    map_osm = make_map_paris()
    for i in tqdm(range(div) , desc=name):
        for j in range(div):
            item=matrix[i][j]
            folium.Circle(radius=10,location=item,popup='The Waterfront',color='crimson',fill=False).add_to(map_osm)
    map_osm.save(name)

def createFileMasterPosition():
    div=15
    matrix =getPositionsBlock(48.906254,2.260094 ,48.807316 , 2.426262 , div)
    #printMatrix(matrix,div)
    nameFile="matrixMaster.html"
    make_map_from_matrice(matrix,nameFile,div)

createFileMasterPosition()

#if __name__ == '__main__':
#map_all_sensor()
'''
map_sensor_without_taux_occ()
map_sensor_with_taux_occ_bigger_than_100()
map_sensor_with_all_data()
'''