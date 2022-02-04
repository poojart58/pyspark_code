from pyspark import SparkContext
from pyspark.sql import SparkSession

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)


## DF() method is used to create a DataFrame from existing RDD
dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()

#Creating Dataframe usingcreateDataFrame command
dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
