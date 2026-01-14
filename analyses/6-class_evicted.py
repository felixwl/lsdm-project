from pyspark import SparkContext


def is_evicted(events):
    evicted = 0
    scheduling_class = ""
    for event in events:
        scheduling_class = event[1]
        if event[0] == "2":
            evicted = 1
    return (scheduling_class, evicted)

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
wholeFile = sc.textFile("./dataset/task_events/part-00085-of-00500.csv.gz")

filter_by = "time"

# filter out the first line from the initial RDD
entries = wholeFile.filter(lambda x: not (filter_by in x))

# split each line into an array of items
entries = entries.map(lambda x : x.split(','))

# keep the RDD in memory
entries.cache()

job_ID_index = 2
event_type_index = 5
scheduling_class_index = 7

evictions = entries.map(lambda x: x[event_type_index]).filter(lambda x: x == "2")
print("Total evictions:", evictions.count())

# Get the event tyoe and scheduling class for every event for each job
job_events = entries.map(
    lambda x: (x[job_ID_index], [x[event_type_index], x[scheduling_class_index]])
    )
job_events = job_events.groupByKey()

# Get the percentage of events that are evictions for each scheduling class
evicted = job_events.mapValues(is_evicted).values().reduceByKey(lambda x, y: (x + y) / 2)

for result in sorted(evicted.collect()):
    print(result)

# prevent the program from terminating immediatly
input("Press Enter to continue...")