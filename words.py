from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CountWordApp").getOrCreate()
myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split()
words = spark.sparkContext.parallelize(myCollection, 2)
print(workds.count())
