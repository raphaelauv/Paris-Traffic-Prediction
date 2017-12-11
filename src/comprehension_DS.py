'''
    Recapitule toutes les requetes SQL utilisees pour comprendre les donnees qu'on a.
    La table utilisée s'appelle test dans le fichier Blabla.bd
'''

import sqlite3 as sql
import numpy as np
from tqdm import tqdm

dataBaseName = 'Blabla.db'

def allDataFromDate(year , numberWeek , dateNumber):
    return 'SELECT id_arc_trafics,hour ,debit ,taux_occ FROM test WHERE year='+str(year)+' AND numberWeek='+str(numberWeek)+' AND dateNumber='+str(dateNumber)+' ORDER BY id_arc_trafics'
    

def delete_Null():
    conn = sql.connect(dataBaseName)
    cur = conn.cursor()
    cur.execute('DELETE FROM test WHERE taux_occ is NULL OR debit is NULL ')
    conn.commit()
    conn.close()

def calcul_correlation():
    conn = sql.connect(dataBaseName)
    cur = conn.cursor()
    cur.execute('SELECT numberWeek, dateNumber, AVG(debit), AVG(taux_occ) FROM test GROUP BY  numberWeek, dateNumber, year ORDER BY numberWeek, dateNumber')
    taux_occ=[]
    debit=[]
    for i in cur.fetchall():
        taux_occ.append(i[2])
        debit.append(i[3])
    print(np.corrcoef(debit,taux_occ)[0][1])
    conn.close()

def sensor_with_all_data_AllYears():
    return 'SELECT * FROM test WHERE taux_occ IS NOT NULL AND debit IS NOT NULL GROUP BY id_arc_trafics'


#sensors with all the information
def sensor_with_all_data(year):
    return 'SELECT * from (SELECT *,COUNT(id_arc_trafics) AS nb_id FROM test WHERE taux_occ IS NOT NULL AND debit IS NOT NULL AND year='+year+' GROUP BY id_arc_trafics) WHERE nb_id > 6000'

#moyenne des debits et taux_occ par jours
def avg_debit_taux_occ_by_day(year):
    return 'SELECT numberWeek, dateNumber, year, AVG(debit), AVG(taux_occ) FROM test WHERE year='+ year+' GROUP BY  numberWeek, dateNumber ORDER BY numberWeek, dateNumber'

#interpretation des moyennes par jours sur taux d'occupation.
def avg_debit_taux_occ_by_day_order_by_taux_occ():
    return 'SELECT AVG(debit) AS debit_m, AVG(taux_occ) AS taux_m FROM test GROUP BY  numberWeek, dateNumber ORDER BY taux_m'

#interpretation des moyennes par jours sur debit.
def avg_debit_taux_occ_by_day_order_by_debit():
    return 'SELECT AVG(debit) AS debit_m, AVG(taux_occ) AS taux_m FROM test  GROUP BY  numberWeek, dateNumber ORDER BY debit_m'

#MIN des moyennes
def min_avg_debit_taux_occ():
    return 'SELECT MIN(debit_m), MIN(taux_m) FROM  (SELECT numberWeek, dateNumber, AVG(debit) AS debit_m, AVG(taux_occ) AS taux_m FROM test GROUP BY  numberWeek, dateNumber, year ORDER BY numberWeek, dateNumber, year)'

#Max des moyennes
def max_avg_debit_taux_occ():
    return 'SELECT MAX(debit_m), MAX(taux_m) FROM  (SELECT numberWeek, dateNumber, AVG(debit) AS debit_m, AVG(taux_occ) AS taux_m FROM test GROUP BY  numberWeek, dateNumber,year ORDER BY numberWeek, dateNumber, year)'

#moyenne par ans.
def avg_by_year():
    return 'SELECT AVG(debit_m), AVG(taux_m) FROM  (SELECT numberWeek, dateNumber, AVG(debit) AS debit_m, AVG(taux_occ) AS taux_m FROM test GROUP BY  numberWeek, dateNumber,year ORDER BY numberWeek, dateNumber)'

#Capteur avec taux d'occupation > 100%
def taux_occ_sup_100():
    return 'SELECT * FROM test WHERE taux_occ > 100 GROUP BY id_arc_trafics'

#Nb de capteur
def nd_capteur():
    return 'SELECT COUNT(id) FROM (SELECT id_arc_trafics AS id FROM test GROUP BY id_arc_trafics)'

#number line
def nd_line():
    return 'SELECT COUNT(id_arc_trafics) FROM test'

#Capteur qui ne sont pas active
def capteur_broken(year):
    return 'SELECT * FROM (SELECT id_arc_trafics, COUNT(id_arc_trafics) AS nb_null FROM test WHERE taux_occ IS NULL AND debit IS NULL AND year='+year+' GROUP BY id_arc_trafics) WHERE nb_null>6000'

#Capteur sans taux d'occupation
def capteur_without_taux_occ(year):
    return 'SELECT * FROM (SELECT id_arc_trafics, COUNT(id_arc_trafics) AS nb_null FROM test WHERE taux_occ IS NULL AND debit>= 0 AND year='+year+' GROUP BY id_arc_trafics) WHERE nb_null>6000'

#nb capteur sans taux_occupation
def nb_capteur_without_taux_occ():
    return 'SELECT COUNT(nb_null) FROM (SELECT COUNT(id_arc_trafics) AS nb_null FROM test WHERE taux_occ IS NULL GROUP BY id_arc_trafics) WHERE nb_null>6000'

#debit =0 et taux_occ >0 => incoherence
def incoherence_1():
    return 'SELECT * FROM test WHERE debit=0 AND taux_occ > 0'

#debit > 0 et taux_occ =0 ??!!
def incoherence_2():
    return 'SELECT * FROM test WHERE taux_occ=0 AND debit>0'

def make_average_by_year():
    year=2013
    conn = sql.connect(dataBaseName)
    cur = conn.cursor()
    for i in tqdm(range(5)):
        file = open("data/average_"+str(year)+".txt","w")
        strQuery = avg_debit_taux_occ_by_day(str(year))    
        cur.execute(strQuery)
        for j in tqdm(cur.fetchall()):
            file.write(str(j)+"\n")
        year+=1
    conn.close()

#make_average_by_year()
