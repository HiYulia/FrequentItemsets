from pyspark import SparkConf, SparkContext
import json
import sys
import time
import itertools
from collections import Counter
from itertools import chain
from collections import defaultdict
midtime1 = time.time()
n_yuan=30
def check_sub(myele, wholelists,check_threshold):
	check_counter = 0
	for eachoriginial in wholelists:
		if set(myele).issubset(eachoriginial): check_counter+=1
	if check_counter>= check_threshold:
		return tuple(sorted(list(myele)))
	else:
		return None

def check_frequent(level_data,wholelists,check_threshold):
	filteredlist=[]
	[filteredlist.append(check_sub(myele,wholelists,check_threshold)) for myele in level_data]
	filteredlist=list(filter(None, filteredlist))
	return filteredlist

def getcandadate1(eachpartition):
	elementslist0=[]
	elementslist_needed=[]
	candidates_list=[]
	element_count_list0=[]
	fornext=[]
	midelements=[]
	elementslist1=[]
	element_count_list=[]
	for k in range(n_yuan):
		elementslist_needed.append([])
		candidates_list.append([])
		fornext.append([])
		element_count_list.append([])	
	for element in eachpartition:
		elementslist0.extend([x for x in itertools.combinations(element, 1)])
		midelements.append(set(element))
	candidates_list[0]=check_frequent(elementslist0,midelements,aj_support_case1)
	fornext[1]=set(chain(*candidates_list[0]))
	for i in range(1,n_yuan-1):
		elementslist_needed[i].extend([x for x in itertools.combinations(fornext[i], i+1)])
		candidates_list[i]=check_frequent(elementslist_needed[i],midelements,aj_support_case1)
		fornext[i+1]=set(chain(*candidates_list[i]))
	return candidates_list

def check_frequent2(level_data,wholelists):
	filteredlist=[]
	check_counter = defaultdict(int)
	for myele in level_data:
		for eachoriginial in wholelists:
			if set(myele).issubset(eachoriginial):check_counter[myele] += 1
	[filteredlist.append((tuple(sorted(list(myitem))),mycount)) for myitem, mycount in check_counter.items()]
	return filteredlist

def getfrequent1(each):
	elementslist0=[]
	midelements=[]
	for element in each:
		elementslist0.extend([tuple(sorted(list(x))) for x in itertools.combinations(element, 1)])
		midelements.append(set(element))
	count_list=[]
	count_list.append([])
	count_list[0]=[(x, elementslist0.count(x)) for x in set(case1_candadates[0])]
	for i in range(1,frequent_iteration):
		count_list.append([])
		count_list[i]=check_frequent2(case1_candadates[i],midelements)
	return count_list


def task2(threshold,support_number,input_file_name,output_file_name):
	conf = SparkConf().setMaster("local").setAppName("HW2")
	sc=SparkContext(conf=conf)
	startTime = time.time()
	data=sc.textFile(input_file_name)
	header = data.first() 
	data = data.filter(lambda row:row != header)
	global case1_candadates,case1_support,aj_support_case1,frequent_iteration
	task2_threshold=int(threshold)
	number_partation=1
	#data.getNumPartitions()
	n_yuan=30
	case1_support=int(support_number)
	aj_support_case1=case1_support*(1/number_partation)	
	case1_basket=data.repartition(int(number_partation)).map(lambda line: line.split(","))\
					 .map(lambda line: (line[0],line[1])).distinct()\
					 .groupByKey().map(lambda x :  list(x[1])).map(lambda x :(x,len(x)))\
					 .filter(lambda x: x[1]>task2_threshold).keys().persist()
	case1_candadates=case1_basket.mapPartitions(getcandadate1).zipWithIndex()\
	.map(lambda e:(e[1]%n_yuan,e[0])).reduceByKey(lambda x,y:x+y).sortByKey(ascending=True).values().collect()
	case1_candadates=[x for x in case1_candadates if x != []]
	frequent_iteration=len(case1_candadates)

	case1=case1_basket.mapPartitions(getfrequent1).zipWithIndex().map(lambda e:(e[1]%len(case1_candadates),e[0]))\
	.flatMap(lambda x: [(x[0], v) for v in x[1]]).map(lambda e: ((e[0],e[1][0]),e[1][1])).reduceByKey(lambda x, y: x + y)\
	.filter(lambda e: e[1]>=case1_support).coalesce(number_partation, False).map(lambda e:(e[0][0],e[0][1])).groupByKey().map(lambda x : (x[0], list(x[1]))).sortByKey(ascending=True)\
	.values().collect()
		
	with open(output_file_name, "w") as outfile1:
		outfile1.write("Candidates:\n")
		for i in range(len(case1_candadates)):
			if i ==0:
				m=','.join(str(v)[:-2]+")" for v in sorted(case1_candadates[i]))
				outfile1.write(m+"\n"+"\n")
			else:
				m=','.join(str(v) for v in sorted(case1_candadates[i]))
				outfile1.write(m+"\n"+"\n")
		outfile1.write("FrequentItemsets:\n")
		for i in range(len(case1)):
			if i ==0:
				m=','.join(str(v)[:-2]+")" for v in sorted(case1[i]))
				outfile1.write(m+"\n"+"\n")
			else:
				m=','.join(str(v) for v in sorted(case1[i]))
				outfile1.write(m+"\n"+"\n")

	case1_endTime = time.time()
	case1_time=case1_endTime-startTime
	print("Duration:"+str(case1_time))

if __name__ == "__main__":
		task2(sys.argv[1], sys.argv[2],sys.argv[3],sys.argv[4])



#spark-submit xinyue_niu_task2.py 70 50 "AZ_yelp.csv" "task2.txt"