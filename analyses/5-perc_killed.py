import sys
from pyspark import SparkContext
import time
from operator import add


# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
wholeFile = sc.textFile("./dataset/job_events/part-00089-of-00500.csv.gz")

filter_by = "time"

# filter out the first line from the initial RDD
entries = wholeFile.filter(lambda x: not (filter_by in x))

# split each line into an array of items
entries = entries.map(lambda x : x.split(','))

# keep the RDD in memory
entries.cache()

job_ID_index = 2
event_type_index = 3

job_events = entries.map(lambda x: (x[job_ID_index], x[event_type_index])).groupByKey()
n_jobs = job_events.count()
aborted_jobs = job_events.filter(lambda x: "2" in x[1] or "5" in x[1])
n_aborted = aborted_jobs.count()

print(n_aborted / n_jobs)

# prevent the program from terminating immediatly
input("Press Enter to continue...")