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

def modeDict():
    converters = {'geo_point_2d':c_geo_point_2d_FLOAT }
    positions = pd.read_csv('data/referentiel-comptages-routiers.csv',delimiter=';',converters=converters)
    positions['lat']=positions['geo_point_2d'].apply(lambda x:x[0])
    positions['lon']=positions['geo_point_2d'].apply(lambda x:x[1])
    
    from collections import defaultdict
    posdict = defaultdict(lambda :{'lat':0,'lon':0})
    for j,i in positions[['id_arc_tra','lat','lon']].iterrows():
        id_arc_tra = float(i.id_arc_tra)
        lat = i.lat
        lon = i.lon
        posdict[id_arc_tra]={'lat':lat,'lon':lon}
    return posdict

def make_map_paris():
    return folium.Map(location=[48.85, 2.34],tiles='Stamen Toner',zoom_start=12)

def make_map_from_request(name,index):
    map_osm = make_map_paris()
    sensor_dict=modeDict()
    for i in tqdm(cur.fetchall()):
        item=sensor_dict[i[index]]
        lat=item['lat']
        lon =item['lon']
        if(not pd.isnull(lat)):
            folium.Circle(radius=10,location=[lat, lon],popup='The Waterfront',color='crimson',fill=False).add_to(map_osm)
        else:
            print("Sensor without lat and lon")
            print(i[index])

    map_osm.save(name)


def map_all_sensor():
    map_osm = make_map_paris()
    for id,item in tqdm(modeDict().items()):
        lat=item['lat']
        lon =item['lon']
        if(not np.isnan(lat)):
            folium.Circle(radius=10,location=[lat, lon],popup='The Waterfront',color='crimson',fill=False).add_to(map_osm)
    map_osm.save('sensors_map.html')

def map_sensor_without_taux_occ():
    capteur_without_taux_occ()
    make_map_from_request('map_sensor_without_taux_occ.html',0)

def map_sensor_with_taux_occ_bigger_than_100():
    taux_occ_sup_100()
    make_map_from_request('map_sensor_with_taux_occ_bigger_than_100.html',1)

def map_sensor_with_all_data():
    sensor_with_all_data()
    make_map_from_request('map_sensor_with_all_data.html',1)


map_sensor_without_taux_occ()
map_sensor_with_taux_occ_bigger_than_100()
map_sensor_with_all_data()










