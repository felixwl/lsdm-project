from pyspark import SparkContext


# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
wholeFile = sc.textFile("{},{},{},{},{}".format(
    "./dataset/task_events/part-00085-of-00500.csv.gz",
    "./dataset/task_events/part-00086-of-00500.csv.gz",
    "./dataset/task_events/part-00087-of-00500.csv.gz",
    "./dataset/task_events/part-00088-of-00500.csv.gz",
    "./dataset/task_events/part-00089-of-00500.csv.gz",
    ))

filter_by = "time"

# filter out the first line from the initial RDD
entries = wholeFile.filter(lambda x: not (filter_by in x))

# split each line into an array of items
entries = entries.map(lambda x : x.split(','))

# keep the RDD in memory
entries.cache()

job_ID_index = 2
task_id_index = 3
event_type_index = 5
scheduling_class_index = 7
priority_index = 8


task_events = entries.map(
    lambda x: (
        "{}_{}".format(x[job_ID_index], x[task_id_index]), x[event_type_index]))

task_priority_class = entries.map(
    lambda x: (
        "{}_{}".format(x[job_ID_index], x[task_id_index]), 
        (x[scheduling_class_index], x[priority_index]))
    )

task_evictions = task_events.mapValues(lambda x: 1 if x == "2" else 0).reduceByKey(max)

# Join task -> (scheduling_class, priority) with task eviction flag
priority_class_evictions = task_priority_class.join(task_evictions).values()  # ((sclass, priority), evicted_flag)

# Map to ((sclass, priority), (evicted_count, total_count)) and aggregate
priority_pairs = priority_class_evictions.map(lambda x: (x[0], (int(x[1]), 1)))
priority_aggregated = priority_pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Compute eviction rate per (scheduling_class, priority)
priority_class_eviction_rates = priority_aggregated.mapValues(lambda s_c: s_c[0] / s_c[1] if s_c[1] > 0 else 0.0)
priority_class_eviction_rates = priority_class_eviction_rates.sortBy(lambda x: x[1], ascending=False)

for ((sclass, priority), rate) in priority_class_eviction_rates.collect():
    print(f"Scheduling class: {sclass}, Priority: {priority}, Eviction rate: {rate}")

# prevent the program from terminating immediatly
input("Press Enter to continue...")