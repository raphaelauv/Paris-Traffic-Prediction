
# coding: utf-8

# # Paris live traffic information

# In[1]:


# !ls -lh traffic
# !rm data/aggregates.pd.h5 data/no_commas.h5


# In[3]:


from tqdm import tqdm_notebook as tqdm
import pandas as pd
import numpy as np


# ## Load map from counter id to geo location

# Here we prepare a dictionary mapping from the counter id `id_arc_trafic` to its `lat,lon` position.
# Since the db is tiny, we load everything in memory

# In[4]:

print("bonjour")

#from string import atof
def c_geo_point_2d(x):
    out = []
    for i in x.split(','):
        try:
            out.append(float(i))
        except ValueError:
            return [np.nan,np.nan]
            raise(ValueError(' x='+str(x)))
    return out
converters = {'geo_point_2d':c_geo_point_2d }

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
print(positions.head(100))


# ## Prepare an `sql` db of traffic records
# In contrast to an `hdf` format, an `sqlite` database offers optimized aggregation 
# essentially for free.  Here we copy the original `csv` file to the db operating on chunks of 
# 5e4 items using the `chunk_size` paramers of pandas' `read_csv`. 
# On the fly, we map from counter id `id_arc_trafic` to its gps position and precompute some columns. In particular the `BDay` columns, meaning `business day` is true when the day is mon->fri, false otherwise, holidays are not considered.
# The automatic type detection algorithms fails for 3 columns leading to a `ValueError` when sending the rows to the `db`.  The type of these 3 lines is specified to be 16-bit int or floats, which is enough to store what we need.

# In[5]:


#print np.iinfo(np.int16)
#print np.finfo(np.float16)


# In[6]:


import sqlite3 as sql
conn = sql.connect('traffic.db')


# In[7]:


in_it = pd.read_csv('traffic/no_commas.csv',delimiter=';',parse_dates=['horodate'],chunksize=50000,
                    dtype={'id_arc_trafic':np.int16, # this is NOT bcs I'm pedantic
                           'debit':np.float16,       # but bcs otherwise it won't infer the right columns types
                           'taux':np.float16})       # and will bail out with a misterious "ValueError"
#from itertools import izip
for chunk in tqdm(in_it):
    try:
        # line by line pre-processing
        chunk.debit = chunk.debit.apply(lambda x: 0 if np.isnan(x) else x)
        chunk.taux = chunk.taux.apply(lambda x: 0 if np.isnan(x) else x)
        chunk=chunk.assign(hour=chunk.horodate.apply(lambda x:x.hour))
        chunk=chunk.assign(month=chunk.horodate.apply(lambda x:x.month))
        chunk=chunk.assign(BDay=chunk.horodate.apply(lambda x:x.isoweekday() not in (6,7)))
        # map from counter id to its position
        chunk = chunk.assign(lat=chunk.id_arc_trafic.apply(lambda x:posdict[float(x)]['lat']))
        chunk = chunk.assign(lon=chunk.id_arc_trafic.apply(lambda x:posdict[float(x)]['lon']))        
        chunk = chunk[['hour','month','BDay','debit','taux','id_arc_trafic','lat','lon']]        
        
        chunk.to_sql('traffic',conn,if_exists='append')
    except ValueError:
        break


# In[11]:


#safety checks
conn.commit()
cur = conn.cursor()
cur.execute('SELECT DISTINCT month FROM traffic')
cur.fetchall()


# In[12]:


cur.execute('select count(hour) from traffic')
cur.fetchall()[0]


# ## Aggregate records
# We keep only records of business days (when `BDay==True`) and group by hour and month, meaning that we are averaging over the
# weekdays. The results of the query are read in chunks of 200 records and directly stored in a (this time small) hdf file using `pandas.HDFStore`. In this way the work is done mostly out-of-memory.

# In[13]:


# aggregate by hour, month, and traffic counter (id_arc_trafic)
sql_query = 'SELECT hour,month,id_arc_trafic,AVG(debit),AVG(taux),lat,lon FROM traffic WHERE BDay GROUP BY hour, month, id_arc_trafic;'


# In[14]:


get_ipython().system('rm traffic/aggregates.hdf ')
out = pd.HDFStore('traffic/aggregates.hdf')
for rows in tqdm(pd.read_sql_query(sql_query,conn,chunksize=200)):
    out.append(key = '/aggregates',value = rows)


# In[15]:


agg = out['/aggregates']
out.close()


# In[17]:


agg

