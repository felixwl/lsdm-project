from pyspark import SparkContext


def get_maintenance_rate(events):
    """ Get the ratio of downtime to total time between the events. """
    # Split into ADD and REMOVE events
    added = []
    removed = []
    for event in events:
        if event[1] == "0":
            added.append(event)
        elif event[1] == "1":
            removed.append(event)
    # Get the total time spent between REMOVE and ADD events
    maintenance_time = 0
    cpu = added[0][2]
    for i in range(len(removed)):
        # Find the time from the machine being removed to it being reconnected
        if i + 1 < len(added):
            start_time = removed[i][0]
            end_time = added[i + 1][0]
            if start_time.isnumeric() and end_time.isnumeric():
                maintenance_time += float(end_time) - float(start_time)
    
    # Get the total time between the first ADD event and the last REMOVE event
    start_time = added[0][0]
    if len(removed) > 0 and added[-1][0] < removed[-1][0]:
        end_time = removed[-1][0]
    else:
        end_time = last_timestamp   # approximate the end time to the last detected timestamp
    # Get the ratio of downtime to total time
    if start_time.isnumeric() and end_time.isnumeric():
        total_time = float(end_time) - float(start_time)
        return (cpu, maintenance_time / total_time)
    else: 
        return (cpu, 0)


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

# Get the last timestamp in the dataset to use as end time
last_timestamp = entries.map(lambda x: x[time_index ]).sortBy(lambda x: x, False).take(1)[0]

# Group by machine
machines = entries.map(
    lambda x: (x[machine_id_index], 
               [x[time_index], x[event_type_index], x[cpus_index]]
               )
    )
machines = machines.groupByKey()

# Get the rate of maintenance downtime for each machine
rates = machines.mapValues(get_maintenance_rate).values().reduceByKey(lambda x, y: (x + y) / 2)

for rate in sorted(rates.collect(), reverse=True):
    print(rate)


# prevent the program from terminating immediatly
input("Press Enter to continue...")