#!/usr/bin/env python 
import csv
import sys
import matplotlib.pyplot as plt
from textwrap import wrap
from sets import Set

from collections import defaultdict
file = sys.argv[1]
if len(sys.argv) > 2:
	group = sys.argv[2]
	idx = int(sys.argv[3])
else :
	group = 'default'
	idx = 0

wf_values = ['0.05', '0.001', '0.01', '0.1', '0.3']
kpr_values = ['4', '2', '8', '16']
val_values = ['128', '8', '512']
zipf_values = ['0.99', '0.0', '0.8']
customfilterfn = {
	'default': lambda x: True,
	'wf' : lambda x:  x['WriteFrac'] == wf_values[idx],
	'kpr': lambda x:  x['Key/Read'] == kpr_values[idx],
	'zipf' : lambda x: x['Zipf'] == zipf_values[idx],
	'val' : lambda x : x['ValSize'] == val_values[idx],
	'base' : lambda x : x['Key/Read'] == kpr_values[0]  and x['ValSize'] == val_values[0] and  x['Zipf'] == zipf_values[0] and  x['WriteFrac'] == wf_values[0]
}
customSeriesColNum = {
	'wf' : 4,
	'kpr': 3,
	'zipf' : 5,
	'val' : 2
}
nclients=42
nthreads=9
## Expt,NumDCs,Key/Serv,#Serv,ValSize,Key/Read,WriteFrac,Zipf,NumClients,TotalThreads,LocalThreads,Client,NumOps,NumKeys,NumColumns,NumBytes,NumReads,NumWrites,Duration,Throughput,Ravg,R50,R90,R99,Wavg,W50,W90,W99,#Tx2R,#K2R,#aggR,#aggW,Lsum,Lavg,P_R,AVG_RD,AVG_W,AVG_OP,Xput,Real Xput
def mean(x):
	# assert len(x)==nclients,(len(x))
	return sum(x)/len(x)
def lsum(x):
	# assert len(x)==nclients,(len(x))
	return sum(x)


def filterfn(x):
	return  customfilterfn['base'](x) or customfilterfn[group](x) 
def keyfn(x):
	return int(x['TotalThreads']),x['Expt'],int(x['ValSize']),int(x['Key/Read']),float(x['WriteFrac']),float(x['Zipf'])
	
aggfns=[lsum, mean, mean]
valcols=('Throughput','Ravg', 'R99')
allkeycols=('TotalThreads','Expt','ValSize','Key/Read','WriteFrac','Zipf')
seriesColNum=[1]
if(group != 'default'):
	seriesColNum.append(customSeriesColNum[group])

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


def plotFig(data,title,filesuffix):

	fig_size = plt.rcParams["figure.figsize"]
	# Prints: [8.0, 6.0]
	 # fig_size[0] = 20
	fig_size[1] = 3
	fig,p=plt.subplots()
	
	plt.yscale('log')
	for x,y,l in data:
		#assert len(x)==nthreads,(len(x))
		#assert len(y)==nthreads,(len(x))
		c=('-o' if l.startswith("Eig") else '-x') if not l.startswith("Contr") else '-*'
		p.plot(x, y, c,label=l)
	# Get current size

	# fig_size={}
	
	plt.xlabel("Throughput (ops/s)")
	plt.ylabel("Latency (us)")
	plt.legend()
	plt.title("\n".join(wrap(title,60)))
	fig.tight_layout()

	plt.rcParams["figure.figsize"] = fig_size
	plt.savefig(file[:-4]+"__"+group+str(idx)+"__"+filesuffix+".png")
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
	series[s].append(aggData[key])

seriesTr = {}
for s,d in series.iteritems():
	seriesTr[s]=zip(*d)
	assert(len(seriesTr[s]) == len(valcols)) 

for val in range(1, len(valcols)): #do not consider throughput
	print valcols[val]+"::"
	print "Title = "+title
	# for s,d in seriesTr.iteritems():
		# print "{} -> {} {}".format(s, d[0],d[val])
	ser_val = map(lambda x: (seriesTr[x][0], seriesTr[x][val],x+" "+valcols[val]),sorted(seriesTr.keys()))
	print ser_val
	plotFig(ser_val,title,valcols[val])

# for s in seriesNames:

# # map(keyfn, filter(lambda x: x['Expt']=='Eiger' and filterfn(x), data))
# # cops = map(keyfn,filter(lambda x: x['Expt']=='COPS-SNOW' and filterfn(x), data))
# # print "COPS = {}".format(len(cops))
# # print "Eiger = {}".format(len(eiger))
# for k in sorted(aggData):
# 	print "{} -> {}".format(k,aggData[k])