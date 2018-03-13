from pyspark import SparkContext,SparkConf
import collections


conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

sc = SparkContext(conf=conf)

lines_rdd = sc.textFile("file:///Github_Projects/Taming_Apache_Spark_With_Python/datasets/ml-100k/u.data")

ratings_rdd = lines_rdd.map(lambda x : x.split()[2])

result = ratings_rdd.countByValue()

sorted_result= collections.OrderedDict(sorted(result.items()))

for key,value in sorted_result.items():
    print("%s,%i"%(key,value))

