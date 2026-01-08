from pyspark import SparkContext


def resource_metric(cpu, memory, disk):
    return float(cpu) + float(memory) + float(disk)

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
events_file = sc.textFile("./dataset/task_events/part-00085-of-00500.csv.gz")
usage_file = sc.textFile("./dataset/task_usage/part-00085-of-00500.csv.gz")

filter_by = "job ID"

# filter out the first line from the initial RDD
events = events_file.filter(lambda x: not (filter_by in x))
usage = events_file.filter(lambda x: not (filter_by in x))

# split each line into an array of items
events = events.map(lambda x : x.split(','))
usage = usage.map(lambda x : x.split(','))


# keep the RDD in memory
events.cache()
usage.cache()

# Indexes used to acces task event table
events_job_id_index = 2
events_task_index_index = 3
event_type_index = 5
event_cpu_request_index = 9
event_memory_request_index = 10
event_disk_request_index = 11

# Indexes used to acces task usage table
usage_job_id_index = 2
usage_task_index_index = 3
usage_cpu_index = 5
usage_memory_index = 7
usage_disk_index = 12

# Get resource requests for each schedule task event
task_events = events.map(
    lambda x: (
        "{}_{}".format(x[events_job_id_index], x[events_task_index_index]), 
        (x[event_type_index], x[event_cpu_request_index], x[event_memory_request_index], 
         x[event_disk_request_index])
        )
    ).filter(lambda x: x[1][0] == "1")
# Convert the resource requests into a metric
resource_requests = task_events.mapValues(lambda x: resource_metric(x[1], x[2], x[3]))

# Get the resources used for every task
task_usages = usage.map(
    lambda x: (
        "{}_{}".format(x[usage_job_id_index], x[usage_task_index_index]),
        (x[usage_cpu_index], x[usage_memory_index], x[usage_disk_index])
    )
)
# Convert the resource usage to a metric
resource_usage = task_usages.mapValues(lambda x: resource_metric(x[0], x[1], x[2]))

# Join the RDDs
most_requested = resource_requests.join(resource_usage).values().sortByKey(ascending=False)
most_used = resource_usage.join(resource_requests).values().sortByKey(ascending=False)

# Print usage for the most requested and vice versa
print("Usage for most requested:")
for task in most_requested.take(10):
    print(f"Requested: {task[0]}, Used: {task[1]}")
print("Requested for most used:")
for task in most_used.take(10):
    print(f"Used: {task[0]}, Requested: {task[1]}")


# prevent the program from terminating immediatly
input("Press Enter to continue...")