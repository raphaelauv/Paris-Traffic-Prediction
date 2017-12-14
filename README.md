# Fouille Mandarina Project

Auvert Raphael - Andres Quiroz

## Paris road traffic modelisation

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