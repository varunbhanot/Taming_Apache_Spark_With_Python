# -*- coding: utf-8 -*-
"""
Created on Wed Mar 14 09:33:40 2018

@author: VB
"""

from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

def parse_line(line):
    line = line.split(",")
    age=line[2]
    friends=line[3]
    return (age,int(friends))

lines_rdd = sc.textFile("file:///Github_Projects/Taming_Apache_Spark_With_Python/datasets/fakefriends/fakefriends.csv")
age_friends_pair = lines_rdd.map(parse_line).mapValues(lambda x : (x,1))
age_friends_pair = age_friends_pair.reduceByKey(lambda x , y : (x[0]+y[0],x[1]+y[1]))
average_friends = age_friends_pair.mapValues(lambda x : (x[0]/x[1]))
result = average_friends.collect()

for i in result:
    print("%s => %2f"%(i[0],i[1]))











