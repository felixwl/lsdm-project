from pyspark import SparkContext


# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
machines_events_file = sc.textFile("./dataset/machine_events/part-00000-of-00001.csv.gz")
task_events_file = sc.textFile("{},{},{},{},{}".format(
    "./dataset/task_events/part-00085-of-00500.csv.gz",
    "./dataset/task_events/part-00086-of-00500.csv.gz",
    "./dataset/task_events/part-00087-of-00500.csv.gz",
    "./dataset/task_events/part-00088-of-00500.csv.gz",
    "./dataset/task_events/part-00089-of-00500.csv.gz",
    ))

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
te_job_id_index = 2
te_task_id_index = 3
te_machine_id_index = 4
te_event_type_index = 5

# Get resources for each machine
machine_resources = machine_events.map(
    lambda x: (
        x[me_machine_id_index],  
        (x[me_event_type_index], x[me_cpu_index], x[me_memory_index])
        )
    ).filter(lambda x: x[1][0] == "0" and x[1][1].isnumeric() and x[1][2].isnumeric())
machine_resources = machine_resources.mapValues(lambda x: (float(x[1]), float(x[2])))

# Get the event type and machine id for each task event
events = task_events.map(
    lambda x: (
        "{}_{}".format(x[te_job_id_index], x[te_task_id_index]), x[te_event_type_index]))
task_machines = task_events.map(
    lambda x: (
        "{}_{}".format(x[te_job_id_index], x[te_task_id_index]), x[te_machine_id_index]))

# Mark the tasks that have eviction events (1 if evicted at least once, else 0)
tasks_evicted = events.mapValues(lambda x: 1 if x == "2" else 0).reduceByKey(max)

# Get the events as eviction (1) or other (0) for each machine
machine_evictions = task_machines.join(tasks_evicted).values()  # (machine_id, evicted_flag)

# Join machine resources with machine-level eviction flags
# After .values() we have ((cpu, memory), evicted_flag)
resource_eviction_pairs = machine_resources.join(machine_evictions).values()

# Map to (resource_key, (evicted_count, total_count)) and aggregate
# resource_key is a tuple: (cpu, memory)
resource_pairs = resource_eviction_pairs.map(lambda x: (x[0], (int(x[1]), 1)))
resource_aggregated = resource_pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Compute eviction rate = evicted_count / total_count
resource_eviction_rates = resource_aggregated.mapValues(lambda s_c: s_c[0] / s_c[1] if s_c[1] > 0 else 0.0)

for (cpu_mem, rate) in resource_eviction_rates.collect():
    print(f"CPU: {cpu_mem[0]}, Memory: {cpu_mem[1]}, Eviction rate: {rate}")