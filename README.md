# Paris road traffic Prediction

thank to data published by OpenData-Paris :
[OpenData-Paris-Traffic](https://opendata.paris.fr/explore/dataset/comptages-routiers-permanents/information/) .
look Pièces jointes rubric


gps localisation of sensors : 
[OpenData-Paris-GPS-sensors](https://opendata.paris.fr/explore/dataset/referentiel-comptages-routiers/) .


<a href="https://vimeo.com/219726521" target="_blank"><img src="https://github.com/raphaelauv/Paris-Traffic-Prediction/blob/master/doc/resume.gif" alt="alt text" width="300" height="whatever"></a>

Full video : https://vimeo.com/248162106

<img src="https://raw.githubusercontent.com/raphaelauv/Paris-Traffic-Prediction/master/doc/images/predictions.png" alt="alt text" width="400" height="whatever">


## Mandarina Project

### To parse and store to SQLite database

	you simply need to personalize the parameters of the only function call at the end of the file , then run :
	python3 parseFill_BD_Pandas.py

### Create an HTML/JavaScript Mapping file:
	you simply need to uncomment function call at the end of the file to get the selected map
	python3 mapPrinter.py

### Create Decision Tree or Random Forest ( predictions )
	
	you can edit the Query of the data train Set who fit the model , by default we predict values of the month of november2017
	pyhton3 predict2.py

### Show the decision tree :

	dot -Tx11 my_dot_file.dot
	dot -Tpng my_dot_file.dot > output.png

### Plot the year X , run :

	python3 plotFileAverage.py 2013
	python3 plotFileAverage.py 2014
	python3 plotFileAverage.py 2015
	python3 plotFileAverage.py 2016
	python3 plotFileAverage.py 2017
