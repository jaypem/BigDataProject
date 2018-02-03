
import os
from pyspark import SparkConf, SparkContext

from lib import *
from options import *

def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

# start spark
confCluster = SparkConf()
confCluster.setAppName('Big Data Project')

sc = SparkContext(conf=confCluster)
quiet_logs(sc)

# set up some options
set_sentiment_levels([[-1, -0.1], [-0.1, 0.1], [0.1, 1]]) # three buckets for sentiments
set_date_levels(generate_date_levels(24)) # split each day up in 24 buckets

# get all files
with open('./files', 'r') as fp:
   txt = fp.readline()

# read the tweets into spark
data_lines = sc.textFile(txt)

# split lines into tuples of the form (date, text)
prepared = data_lines.map(lambda x: split_data_line(x))

# convert the date given as a string into a datetime object
# and filter out invalid dates that were manually set to 1990
converted = prepared.map(lambda x: convert_date(x)).filter(lambda x: x[0].year != 1990)

# analyze the tweets for their sentiment
# filter out tuples with the sentiment 666 as they were manually set for invalid lines
sentiments = converted.map(lambda x: check_sentiment(x)).filter(lambda x: x[1] != 666)

# local save of the global variable with the sentiments
sent_levels = options.sentiment_levels

# cluster similiar sentiments together in buckets
sentiments = sentiments.map(lambda x: cluster_sentiment(x, sent_levels))

# cluster dates in bigger buckets
date_levels = options.date_levels
sentiments = sentiments.map(lambda x: cluster_date(x, date_levels))

# regularization
# for each time period each sentiment is added once (and subtracted later on)
sent_reg = sc.parallelize([])

grouped = sentiments.groupByKey()

# iterate over all sentiments
for i in range(len(sent_levels)):
   sent_reg = sent_reg.union(grouped.map(lambda x: (x[0], i)))

# actually the follwing code should (!) work
# sent_reg = sentiments.union(sent_reg)
# but it does not...
# so the ugly way:

temp_list = sentiments.collect()
temp_list.extend(sent_reg.collect())
s = sc.parallelize(temp_list)

# count the sentiments for each time stamp
final = sc.parallelize([])

for i in range(len(sent_levels)):
   temp = s.filter(lambda x: x[1] == i).groupByKey().map(lambda x: (x[0], (i, len(list(x[1])) - 1)))
   final = final.union(temp)

# output final results into file
result = final.groupByKey().collect()

with open('output.txt', 'w') as fp:
   for r in result:
      line = list(r[1])
      fp.write('{},{},{},{}\n'.format(r[0], line[0][1], line[1][1], line[2][1]))
