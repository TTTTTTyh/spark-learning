from pyspark import SparkConf,SparkContext
import Tabu
from ReadData import Reader
import numpy as np
if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    rdd=sc.parallelize([[1,2],[1,3],[2,2],[2,3],[3,4],[3,5]],3)
    rdd=rdd.aggregateByKey(0,lambda x,y:x+y,lambda x,y:x-y)
    print(rdd.collect())