# -*- coding: utf-8 -*-
"""
Created on Wed Mar 14 17:44:59 2018

@author: VB
"""

from pyspark import SparkConf,SparkContext
import re

def normalize(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

text = sc.textFile("file:///Github_Projects/Taming_Apache_Spark_With_Python/datasets/word-count/book.txt")
word_count = text.flatMap(normalize).map(lambda x : (x,1)).reduceByKey(lambda x,y:(x+y))
word_count_sorted = word_count.map(lambda x: (x[1],x[0])).sortByKey()

results = word_count_sorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)