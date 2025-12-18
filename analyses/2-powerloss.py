import sys
from pyspark import SparkContext
import time
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

time_index = 0
machine_id_index = 1
event_type_index = 2
cpus_index = 4

last_timestamp = entries.map(lambda x: x[time_index ]).sortBy(lambda x: x, False).take(1)

# Group by machine
machines = entries.map(
    lambda x: (x[machine_id_index], 
               [x[time_index], x[event_type_index], x[cpus_index]]
               )
    )
machines = machines.groupByKey()

def get_maintenance(events):
    added = []
    removed = []
    for event in events:
        if event[1] == "0":
            added.append(event)
        elif event[1] == "1":
            removed.append(event)
    maintenance_cost = 0
    cpu = added[0][2]
    for i in range(len(removed)):
        if i + 1 < len(added):
            start_time = removed[i][0]
            end_time = added[i + 1][0]
            if start_time.isnumeric() and end_time.isnumeric() and cpu.isnumeric():
                maintenance_cost += float(cpu) * (float(end_time) - float(start_time))
    
    start_time = added[0][0]
    if len(removed) > 0 and added[-1][0] < removed[-1][0]:
        end_time = removed[-1][0]
    else:
        end_time = last_timestamp   # approximate the end time to the last detected timestamp
    if start_time.isnumeric() and end_time.isnumeric() and cpu.isnumeric():
        total_cost = float(cpu) * (float(end_time) - float(start_time))
        return maintenance_cost / total_cost
    else: 
        return 0

costs = machines.mapValues(get_maintenance)

print(costs.values().mean())


# prevent the program from terminating immediatly
input("Press Enter to continue...")