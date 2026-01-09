from pyspark import SparkContext
from operator import add


# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
machines_events_file = sc.textFile("./dataset/machine_events/part-00000-of-00001.csv.gz")
task_events_file = sc.textFile("./dataset/task_events/part-00087-of-00500.csv.gz")

filter_by = "time"

# filter out the first line from the initial RDD
machine_events = machines_events_file.filter(lambda x: not (filter_by in x))
task_events = task_events_file.filter(lambda x: not (filter_by in x))


# split each line into an array of items
machine_events = machine_events.map(lambda x : x.split(','))
task_events = task_events.map(lambda x : x.split(','))


# keep the RDD in memory
machine_events.cache()
task_events.cache()

# Indexes used to acces machine event table
me_machine_id_index = 1
me_event_type_index = 2
me_cpu_index = 4
me_memory_index = 5

# Indexes used to acces task event table
te_machine_id_index = 4
te_event_type_index = 5
te_cpu_request_index = 9
te_memory_request_index = 10
    
# Get resources for each machine
machine_resources = machine_events.map(
    lambda x: (
        x[me_machine_id_index],  
        (x[me_event_type_index], x[me_cpu_index], x[me_memory_index])
        )
    ).filter(lambda x: x[1][0] == "0" and x[1][1].isnumeric() and x[1][2].isnumeric())
machine_resources = machine_resources.mapValues(lambda x: (x[1], x[2]))

print(machine_resources.count())

# Get the resources used for each machine
events_resources = task_events.map(
    lambda x: (
        x[te_machine_id_index],
        (x[te_event_type_index], x[te_cpu_request_index], x[te_memory_request_index])
    )
).filter(lambda x: x[1][0] == "1")
used_resources = events_resources.mapValues(lambda x: (x[1], x[2]))
used_resources = events_resources.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# Join the RDDs
available_committed = machine_resources.join(used_resources)
over_committed = available_committed.mapValues(
    lambda x: 1 if x[0][0] >= x[1][0] or x[0][1] >= x[1][1] else 0)

# Print result
print("Rate of over-committals:")
for result in over_committed.sortBy(lambda x: x[1], ascending=False).take(10):
    print(result)


# prevent the program from terminating immediatly
input("Press Enter to continue...")