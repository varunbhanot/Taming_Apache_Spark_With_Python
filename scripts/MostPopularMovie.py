# -*- coding: utf-8 -*-
"""
Created on Thu Mar 15 10:59:40 2018

@author: VB
"""

from pyspark import SparkContext,SparkConf

def extract_movies(lines):
    return lines.split()[1]

def flip(x):
    return(x[1],x[0])
    
def load_movie_names():
    movie_names={}
    with open('../datasets/ml-100k/u.item') as f:
        for line in f:
            fields = line.split('|')
            movie_names[fields[0]] = fields[1]
    
    return movie_names
    
    

conf = SparkConf().setMaster("local").setAppName("MostPopularMovie")
sc = SparkContext(conf=conf)

#broadcasting movieid moviename dict 
name_dict = sc.broadcast(load_movie_names())

lines_rdd = sc.textFile("file:///Github_Projects/Taming_Apache_Spark_With_Python/datasets/ml-100k/u.data")
movies = lines_rdd.map(extract_movies).map(lambda x : (x,1))
movies_count = movies.reduceByKey(lambda x,y : x+y)
movies_sorted = movies_count.map(flip).sortByKey()

movies_sorted_with_names = movies_sorted.map(lambda x:(name_dict.value[x[1]],x[0]))

results = movies_sorted_with_names.collect()

print(results)








