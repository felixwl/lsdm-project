from pyspark import SparkContext


def all_same(seq):
    if len(seq) <= 1:
        return True
    for e in seq:
        if e != seq[0]:
            return False
    return True


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
task_index_index = 3
machine_id_index = 4
event_type_index = 5

# Get the job id, event type and machine id for each task event
jobs = entries.map(
    lambda x: (x[job_ID_index], [x[task_index_index], x[event_type_index], x[machine_id_index]]
               ))
# Get the machine ids grouped by job id if available for all schedule events
machines = jobs.groupByKey().mapValues(lambda x: [e[2] for e in x if e[1] == '1' and e[2] != ""])
# Get the jobs with tasks scheduled multiple times
machines = machines.filter(lambda x: len(x[1]) > 1)

# for machine in machines.collect():
#     print(machine[0], [id for id in machine[1]])

# Mark the job ids that only use one machine
run_on_same = machines.mapValues(lambda x: 1 if all_same(x) else 0)
machines_reused = machines.filter(lambda x: all_same(x[1]))

# Get the percentage of jobs that use one machine
run_on_same_rate = run_on_same.values().mean()

print(run_on_same_rate)

for machine in machines_reused.collect():
    print(machine)


# prevent the program from terminating immediatly
input("Press Enter to continue...")