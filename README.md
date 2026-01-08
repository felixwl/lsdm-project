# Spark project notes

## Starting the notebook server
Run the following to start the jupyter notebook in docker,

    sudo docker run -it --rm -p 4040:4040 -p 8888:8888 -v .:/home/jovyan/work jupyter/pyspark-notebook

then open the printed localhost url.

## Dataset
### Tables
#### Job events
The state changes of the events.

#### Machine attributes
OBFUSCATED, probably not useful. Attributes for each machine.

#### Machine events
Hardware information for each machine, as well as state changes when they went online and offline.

#### Task constraints
OBFUSCATED, IGNORE. 

#### Task events
Informmation about each task.

#### Task usage
CPU and memory usage during the tasks.


### Download
I have numbers from 85 to 89, meaning files

    part-00085-of-?????.csv.gz
    part-00086-of-?????.csv.gz
    part-00087-of-?????.csv.gz
    part-00088-of-?????.csv.gz
    part-00089-of-?????.csv.gz

The full command for copying a file is

    gsutil -m cp gs://clusterdata-2011-2/TABLE/part-0008[5-9]-of-?????.csv.gz ./dataset/TABLE/

For machine_attributes and machine_events, there only seems to exist one table, so the command is always

    gsutil cp gs://clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz ./dataset/machine_events

and 

    gsutil cp gs://clusterdata-2011-2/machine_attributes/part-00000-of-00001.csv.gz ./dataset/machine_attributes


## Analyses

1. What is the distribution of the machines according to their CPU capacity?
Can you explain (motivate) it?

MOSTLY DONE. TODO: Pretty print?, count distinct machines, not events

Most machines have the same capacity in a cluster, with some weaker outliers.

2. What is the percentage of computational power lost due to maintenance (a machine went
offline and reconnected later)?
[4pt]The computational power is proportional to both the CPU capacity and the unavailability period of machines.

DONE (TODO: Comment)

3. Is there a class of machines, according to their CPU, that stands out with a higher maintenance rate, as compared to other classes ?

The cpu with capacity 0.5 have a higher maintenance rate.

4. What is the distribution of the number of jobs/tasks per scheduling class? Comment on the results.

The latency-sensitive jobs are more rare, and non-production jobs are most common, but the distribution between 0,1 and 2 is fairly even.

5. Would you qualify the percentage of jobs/tasks that got killed or evicted as important?

Yes, around 40%.

6. Do tasks with a low scheduling class have a higher probability of being evicted?

The opposite, actually. The higher the scheduling class, the higher the probability of being evicted.

7. In general, do tasks from the same job run on the same machine? Comment on the observed locality strategy and its pros and cons.

Only in rare cases (0.75% on part-00085 and 0% on part-00087) does it happen. This seems to impl that the locality strategy is to not try to schedule tasks from the same job on the same machine. This makes scheduling faster and reduces overhead, but has the downside of giving less opportunity for caching between tasks.

8. Are the tasks that request the more resources the one that consume the more resources?

No, the top 10 tasks that request the most resources are not in the top 10 of tasks that use the most resources.

9. Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?

10. How often does it happen that the resources of a machine are over-committed ?

11. Your original question 1. Motivate the originality of the question.

12. Your original question 2. Motivate the originality of the question.
