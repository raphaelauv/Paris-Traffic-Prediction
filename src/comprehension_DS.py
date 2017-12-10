'''
    Recapitule toutes les requetes SQL utilisees pour comprendre les donnees qu'on a.
    La table utilisée s'appelle test dans le fichier Blabla.bd
'''

import sqlite3 as sql
import numpy as np


conn = sql.connect('Blabla.db')
cur = conn.cursor()

def delete_Null():
    cur.execute('DELETE FROM test WHERE taux_occ is NULL OR debit is NULL ')
    conn.commit()

def calcul_correlation():
    cur.execute('SELECT numberWeek, dateNumber, AVG(debit), AVG(taux_occ) FROM test GROUP BY  numberWeek, dateNumber ORDER BY numberWeek, dateNumber ')
    taux_occ=[]
    debit=[]
    for i in cur.fetchall():
        taux_occ.append(i[2])
        debit.append(i[3])
    print(np.corrcoef(debit,taux_occ)[0][1])

#sensors with all the information
def sensor_with_all_data():
    cur.execute('SELECT * FROM test WHERE taux_occ IS NOT NULL AND debit IS NOT NULL GROUP BY id_arc_trafics')

#moyenne des debits et taux_occ par jours
def avg_debit_taux_occ_by_day():
    return cur.execute('SELECT numberWeek, dateNumber, AVG(debit), AVG(taux_occ) FROM test GROUP BY  numberWeek, dateNumber ORDER BY numberWeek, dateNumber ,years ')

#interpretation des moyennes par jours sur taux d'occupation.
def avg_debit_taux_occ_by_day_order_by_taux_occ():
    cur.execute('SELECT AVG(debit) AS debit_m, AVG(taux_occ) AS taux_m FROM test GROUP BY  numberWeek, dateNumber ORDER BY taux_m')

#interpretation des moyennes par jours sur debit.
def avg_debit_taux_occ_by_day_order_by_debit():
    cur.execute('SELECT AVG(debit) AS debit_m, AVG(taux_occ) AS taux_m FROM test  GROUP BY  numberWeek, dateNumber ORDER BY debit_m')


#MIN des moyennes
def min_avg_debit_taux_occ():
    cur.execute('SELECT MIN(debit_m), MIN(taux_m) FROM  (SELECT numberWeek, dateNumber, AVG(debit) AS debit_m, AVG(taux_occ) AS taux_m FROM test GROUP BY  numberWeek, dateNumber ORDER BY numberWeek, dateNumber)')
#Max des moyennes
def min_avg_debit_taux_occ():
    cur.execute('SELECT MAX(debit_m), MAX(taux_m) FROM  (SELECT numberWeek, dateNumber, AVG(debit) AS debit_m, AVG(taux_occ) AS taux_m FROM test GROUP BY  numberWeek, dateNumber ORDER BY numberWeek, dateNumber)')

#moyenne de 2013.
def avg_2013():
    cur.execute('SELECT AVG(debit_m), AVG(taux_m) FROM  (SELECT numberWeek, dateNumber, AVG(debit) AS debit_m, AVG(taux_occ) AS taux_m FROM test GROUP BY  numberWeek, dateNumber ORDER BY numberWeek, dateNumber)')


#Capteur avec taux d'occupation > 100%
def taux_ccc_sup_100():
    cur.execute('SELECT * FROM test WHERE taux_occ > 100 GROUP BY id_arc_trafics')

#Nb de capteur
def nd_capteur():
    cur.execute('SELECT COUNT(id) FROM (SELECT id_arc_trafics AS id FROM test GROUP BY id_arc_trafics)')

#number line
def nd_line():
    cur.execute('SELECT COUNT(id_arc_trafics) FROM test')

#Capteur sans taux d'occupation
def capteur_without_taux_occ():
    cur.execute('SELECT * FROM (SELECT id_arc_trafics, COUNT(id_arc_trafics) AS nb_null FROM test WHERE taux_occ IS NULL GROUP BY id_arc_trafics) WHERE nb_null>6000')

#nb capteur sans taux_occupation
def nb_capteur_without_taux_occ():
    cur.execute('SELECT COUNT(nb_null) FROM (SELECT COUNT(id_arc_trafics) AS nb_null FROM test WHERE taux_occ IS NULL GROUP BY id_arc_trafics) WHERE nb_null>6000')

#debit =0 et taux_occ >0 => incoherence
def incoherence_1():
    cur.execute('SELECT * FROM test WHERE debit=0 AND taux_occ > 0')

#debit > 0 et taux_occ =0 ??!!
def incoherence_2():
    cur.execute('SELECT * FROM test WHERE taux_occ=0 AND debit>0')



