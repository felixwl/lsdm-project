from pyspark import SparkContext
from operator import add


def resource_metric(cpu, memory, disk):
    return float(cpu) + float(memory) + float(disk)

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
events_file = sc.textFile("./dataset/task_events/part-00085-of-00500.csv.gz")
usage_file = sc.textFile("./dataset/task_usage/part-00088-of-00500.csv.gz")

filter_by = "job ID"

# filter out the first line from the initial RDD
events = events_file.filter(lambda x: not (filter_by in x))
usage = usage_file.filter(lambda x: not (filter_by in x))


# split each line into an array of items
events = events.map(lambda x : x.split(','))
usage = usage.map(lambda x : x.split(','))


# keep the RDD in memory
events.cache()
usage.cache()

# Indexes used to acces task event table
events_job_id_index = 2
events_task_index_index = 3
events_machine_id_index = 4
event_type_index = 5

# Indexes used to acces task usage table
usage_job_id_index = 2
usage_task_index_index = 3
usage_machine_id_index = 4
usage_max_cpu_index = 13
usage_max_memory_index = 10
usage_max_disk_index = 12

# Get machine id for each eviction event
eviction_events = events.map(
    lambda x: (
        "{}_{}".format(x[events_job_id_index], x[events_task_index_index]), 
        (x[event_type_index], x[events_machine_id_index])
        )
    ).filter(lambda x: x[1][0] == "1")
# Get eviction event for each machine
machine_evictions = eviction_events.map(lambda x: (x[1][1], 1)).reduceByKey(add)


# Get the max resource usage for each event
task_peaks = usage.map(
    lambda x: (
        x[usage_machine_id_index],
        (x[usage_max_cpu_index], x[usage_max_memory_index], x[usage_max_disk_index])
    )
)
# machine_max = task_peaks.mapValues(lambda x: resource_metric(x[0], x[1], x[2]))
# for machine in machine_max.collect():
#     print(machine[1])

# Get the number of peaks for each machine
peak_threshold = 0.5

machine_peaks = task_peaks.mapValues(
    lambda x: 1 if resource_metric(x[0], x[1], x[2]) > peak_threshold else 0)
machine_peaks = machine_peaks.reduceByKey(add)

# Join the RDDs
peaks_evictions = machine_peaks.join(machine_evictions)
# Get the number of evictions for each number of peaks
peaks_evictions = peaks_evictions.values().reduceByKey(lambda x, y: (x + y) / 2)

# Print result
print("Number of evictions for each number of peaks:")
for result in peaks_evictions.sortByKey(ascending=False).collect():
    print(f"Peaks: {result[0]}, Evictions: {result[1]}")


# prevent the program from terminating immediatly
input("Press Enter to continue...")