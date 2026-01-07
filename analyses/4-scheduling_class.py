import sys
from pyspark import SparkContext
import time
from operator import add


# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
wholeFile = sc.textFile("./dataset/job_events/part-00085-of-00500.csv.gz")

filter_by = "time"

# filter out the first line from the initial RDD
entries = wholeFile.filter(lambda x: not (filter_by in x))

# split each line into an array of items
entries = entries.map(lambda x : x.split(','))

# keep the RDD in memory
entries.cache()

job_ID_index = 2
scheduling_class_index = 5

jobs = entries.map(lambda x: (x[job_ID_index], x[scheduling_class_index])).distinct()
total_jobs = jobs.count()
schedulings = jobs.map(lambda x: (x[1], 1)).reduceByKey(add).mapValues(lambda x: x / total_jobs)

for scheduling_class in sorted(schedulings.collect()):
    print(scheduling_class)


# prevent the program from terminating immediatly
input("Press Enter to continue...")