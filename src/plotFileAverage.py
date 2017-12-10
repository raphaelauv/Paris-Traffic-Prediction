import numpy as np
import matplotlib.pyplot as plt

def two_scales(ax1, ax2,time, data1, data2):
	ax1.plot(time, data1, color='red')
	ax1.set_xlabel('Time (day)')
	ax1.set_ylabel('debit')
	ax2.plot(time, data2, color='green' , alpha=0.4)
	ax2.set_ylabel('taux occ')
	return ax1, ax2

def plotFile():
	lines=[]
	with open("data/average2013.txt") as infile:
		for line in infile:
			line =line.replace("(","")
			line =line.replace(")","")
			lines.append( line.split(",") )

	debit=[]
	taux=[]
	for aLine in lines:
		#print(aLine)
		if(len(aLine)>2):
			debit.append(float(aLine[2]))
			taux.append(float(aLine[3]))


	#fig, ax1 = plt.subplots()
	#ax2 = ax1.twinx()
	fig, (ax1, ax2) = plt.subplots(2)
	t = np.arange(1, 365, 1)
	two_scales(ax1,ax2, t, debit, taux)
	plt.show()

plotFile()