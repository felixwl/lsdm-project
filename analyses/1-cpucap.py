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

machine_id_index = 1
cpus_index = 4

# Get cpu capacity for each machine
machines = entries.map(lambda x: (x[machine_id_index], x[cpus_index])).distinct()
n_entries = machines.count()
# Get number of entries for each cpu capacity
cpu_capacities = machines.values().map(lambda x: (x, 1)).reduceByKey(add)
# Normalize values
cpu_capacities = cpu_capacities.mapValues(lambda x: x/n_entries)

for capacity in sorted(cpu_capacities.collect(), reverse=True):
    print(capacity)

# prevent the program from terminating immediatly
input("Press Enter to continue...")