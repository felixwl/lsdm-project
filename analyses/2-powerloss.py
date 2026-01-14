from pyspark import SparkContext

def get_maintenance(events):
    """ 
    Get the ratio of computational power lost during downtime and computational power
    available during the whole timeframe of the events. 
    """
    # Split the events into ADD and REMOVE events
    added = []
    removed = []
    for event in events:
        if event[1] == "0":
            added.append(event)
        elif event[1] == "1":
            removed.append(event)
    # Get the computational power lost during the time between REMOVE and ADD events
    maintenance_cost = 0
    cpu = added[0][2]
    for i in range(len(removed)):
        # Find the time from the machine being removed to it being reconnected
        if i + 1 < len(added):
            start_time = removed[i][0]
            end_time = added[i + 1][0]
            if start_time.isnumeric() and end_time.isnumeric() and cpu.isnumeric():
                maintenance_cost += float(cpu) * (float(end_time) - float(start_time))
    
    # Get the total computational power between ADD event to the last REMOVE event
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

# Get the computational power loss percentage for each machine
costs = machines.mapValues(get_maintenance)

print(costs.values().mean())


# prevent the program from terminating immediatly
input("Press Enter to continue...")