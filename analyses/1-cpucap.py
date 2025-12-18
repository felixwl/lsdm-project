from pyspark import SparkContext
from operator import add


# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
wholeFile = sc.textFile("./dataset/machine_events/part-00000-of-00001.csv.gz")

filter_by = "time"

# filter out the first line from the initial RDD
entries = wholeFile.filter(lambda x: not (filter_by in x))

# split each line into an array of items
entries = entries.map(lambda x : x.split(','))

# keep the RDD in memory
entries.cache()

n_entries = entries.count()

cpus_index = 4
# TODO: Only count every machine once
# Get number of entries for each cpu capacity
cpu_capacities = entries.map(lambda x: (x[cpus_index], 1)).reduceByKey(add)
# Normalize values
cpu_capacities = cpu_capacities.mapValues(lambda x: x/n_entries)

for capacity in sorted(cpu_capacities.collect(), reverse=True):
    print(capacity)

# prevent the program from terminating immediatly
input("Press Enter to continue...")