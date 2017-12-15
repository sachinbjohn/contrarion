#!/usr/bin/env python 
import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
file = sys.argv[1]
cops = defaultdict(lambda : [[],[],[],[],[],[],[],[],[]])
eiger = defaultdict(lambda : [[],[],[],[],[],[],[],[],[]])

def mean(l):
	return sum(l)/len(l)
def aggregate(r):
	return [sum(r[0]), mean(r[1]), mean(r[2]), mean(r[3]), mean(r[4]), mean(r[5]), mean(r[6]), mean(r[7]), mean(r[8])]
title=""
outfile=""
with open(file, 'r') as csvfile:
	csvreader = csv.reader(csvfile)
	for row in csvreader:
		if(title==""):
			title="K/Srv="+row[1]+" #Srv="+row[2]+" Val="+row[3]+" K/Rd="+row[4]+" WrFr="+row[5]+" Z="+row[6]
			outfile=file[:-4]+"."+row[1]+"_"+row[2]+"_"+row[3]+"_"+row[4]+"_"+row[5]+"_"+row[6]+"_"
		if(row[0] == 'Eiger') :
			v=eiger[int(row[7])]
			v[0].append(int(row[16]))
			v[1].append(int(row[17]))
			v[2].append(int(row[18]))
			v[3].append(int(row[19]))
			v[4].append(int(row[20]))
			v[5].append(int(row[21]))
			v[6].append(int(row[22]))
			v[7].append(int(row[23]))
			v[8].append(int(row[24]))
		elif (row[0] =='COPS-SNOW'):
			v=cops[int(row[7])]
			v[0].append(int(row[16]))
			v[1].append(int(row[17]))
			v[2].append(int(row[18]))
			v[3].append(int(row[19]))
			v[4].append(int(row[20]))
			v[5].append(int(row[21]))
			v[6].append(int(row[22]))
			v[7].append(int(row[23]))
			v[8].append(int(row[24]))
		else:
			print "Unknown expt ",row[0]


	copsall = []
	eigerall = []
	for k in sorted(cops.keys()):
		copsall.append(aggregate(cops[k]))
		eigerall.append(aggregate(eiger[k]))
	copsTrans = map(list, zip(*copsall))
	eigerTrans = map(list, zip(*eigerall))
	
	copsThr = copsTrans[0]
	copsRavg = copsTrans[1]
	copsR50 = copsTrans[2]
	copsR90 = copsTrans[3]
	copsR99 = copsTrans[4]
	copsWavg = copsTrans[5]
	copsW50 = copsTrans[6]
	copsW90 = copsTrans[7]
	copsW99 = copsTrans[8]

	eigerThr = eigerTrans[0]
	eigerRavg = eigerTrans[1]
	eigerR50 = eigerTrans[2]
	eigerR90 = eigerTrans[3]
	eigerR99 = eigerTrans[4]
	eigerWavg = eigerTrans[5]
	eigerW50 = eigerTrans[6]
	eigerW90 = eigerTrans[7]
	eigerW99 = eigerTrans[8]			
			
	fig,p=plt.subplots()
	p.plot(copsThr, copsRavg, '-o', label='COPS R-avg')
	p.plot(eigerThr, eigerRavg, '-x', label = 'Eiger R-avg')
	plt.xlabel("Throughput (ops/s)")
	plt.ylabel("Latency (us)")
	plt.legend()
	plt.title(title)
	plt.savefig(outfile+"-Ravg.png")
	plt.clf()

	fig,p=plt.subplots()
	p.plot(copsThr, copsR50, '-o', label='COPS R-50')
	p.plot(eigerThr, eigerR50, '-x', label = 'Eiger R-50')
	plt.xlabel("Throughput (ops/s)")
	plt.ylabel("Latency (us)")
	plt.legend()
	plt.title(title)
	plt.savefig(outfile+"-R50.png")
	plt.clf()

	fig,p=plt.subplots()
	p.plot(copsThr, copsR90, '-o', label='COPS R-90')
	p.plot(eigerThr, eigerR90, '-x', label = 'Eiger R-90')
	plt.xlabel("Throughput (ops/s)")
	plt.ylabel("Latency (us)")
	plt.legend()
	plt.title(title)
	plt.savefig(outfile+"-R90.png")
	plt.clf()
	
	fig,p=plt.subplots()
	p.plot(copsThr, copsR99, '-o', label='COPS R-99')
	p.plot(eigerThr, eigerR99, '-x', label = 'Eiger R-99')
	plt.xlabel("Throughput (ops/s)")
	plt.ylabel("Latency (us)")
	plt.legend()
	plt.title(title)
	plt.savefig(outfile+"-R99.png")
	plt.clf()
