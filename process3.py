#!/usr/bin/env python 
import csv
import sys
import matplotlib.pyplot as plt
from textwrap import wrap
from sets import Set

from collections import defaultdict
file = sys.argv[1]
nclients=8
nthreads=5
##  Expt,Key/Serv,#Serv,ValSize,Key/Read,WriteFrac,Zipf,Threads,Client,NumOps,NumKeys,NumColumns,NumBytes,NumReads,NumWrites,Duration,Throughput,Ravg,R50,R90,R99,Wavg,W50,W90,W99,#Tx2R,#K2R,#aggR,#aggW,Lsum,Lavg,P_R,AVG_RD,AVG_W,AVG_OP,Xput,Real Xput
def mean(x):
	assert len(x)==nclients
	return sum(x)/len(x)
def lsum(x):
	assert len(x)==nclients
	return sum(x)


def filterfn(x):
	return   x['Key/Read'] == '4' and x['WriteFrac'] == '0.05' and x['Zipf'] == '0.99'
def keyfn(x):
	return int(x['Threads']),x['Expt'],int(x['ValSize']),int(x['Key/Read']),float(x['WriteFrac']),float(x['Zipf'])
aggfns=[lsum, mean, mean, mean]
valcols=('Throughput', 'R50', 'R99', 'Ravg')
allkeycols=('Threads','Expt','ValSize','Key/Read','WriteFrac','Zipf')
seriesColNum=[1]

assert(len(aggfns) == len(valcols))

def getSeries(key):
	res=map(lambda x: str(key[x]),seriesColNum)
	# print "key={} 0={} type={} res={}".format(key,key[0],type(key),res)
	return "-".join(res);

def getTitle(key):
	title=""
	for k in range(1, len(allkeycols)): #Do not count Threads
		if k not in seriesColNum:
			title += "{} = {} ".format(allkeycols[k],key[k])
	return title

def valfn(row):
	return map(lambda x: int(row[x]), valcols)


def plotFig(data,title,colname):
	fig,p=plt.subplots()
	for x,y,l in data:
		assert len(x)==nthreads
		assert len(y)==nthreads
		c='-o' if l.startswith("Eig") else '-x'
		p.plot(x, y, c,label=l)
	plt.xlabel("NumThreads")
	if(colname == "Throughput"):
		plt.ylabel("Throughput (ops/s)")
	else:
		plt.ylabel("Latency (us)")
	p.set_xscale('log', basex=2)
	plt.legend()
	plt.title("\n".join(wrap(title,60)))
	plt.savefig(file[:-4]+"-"+colname+".png")
	plt.clf()

data = list(csv.DictReader(open(file, 'r')))
groupedData = defaultdict(lambda : [])
for row in filter(filterfn,data):
	groupedData[keyfn(row)].append(valfn(row))

aggData={}
for k,v in groupedData.iteritems():
	aggData[k]= map(lambda x: x[0](x[1]), zip(aggfns,zip(*v)))
title=""	
series=defaultdict(lambda: [])
for key in sorted(aggData.keys()):
	s=getSeries(key)
	if title=="":
		title=getTitle(key)
	vals = aggData[key]
	vals.insert(0, nclients * key[0])
	# print(" key = {}  type = {} val = {} tval = {}".format(thr, type(thr), aggData[key], type(aggData[key])))
	series[s].append(vals) 

def normalize(a, b):
	c= map(lambda x: x[0]/float(x[1]), zip(a,b))
	print "A = {}   B = {}  C = {}".format(a,b,c)
	return c

seriesTr = {}
base=[]
for s,d in series.iteritems():
	seriesTr[s]=zip(*d)
	if(s.startswith('Eiger')):
		base=seriesTr[s]
	assert(len(seriesTr[s]) == (len(valcols)+1)) 

for val in range(0, len(valcols)): 
	print valcols[val]+"::"
	print "Title = "+title
	# for s,d in seriesTr.iteritems():
		# print "{} -> {} {}".format(s, d[0],d[val])
	baseData = base[val+1]
	# print "Base={} ".format(baseData)
	ser_val = map(lambda x: (seriesTr[x][0], normalize(seriesTr[x][val+1], baseData) ,x+" "+valcols[val]),sorted(seriesTr.keys()))
	plotFig(ser_val,title,valcols[val])

# for s in seriesNames:

# # map(keyfn, filter(lambda x: x['Expt']=='Eiger' and filterfn(x), data))
# # cops = map(keyfn,filter(lambda x: x['Expt']=='COPS-SNOW' and filterfn(x), data))
# # print "COPS = {}".format(len(cops))
# # print "Eiger = {}".format(len(eiger))
# for k in sorted(aggData):
# 	print "{} -> {}".format(k,aggData[k])