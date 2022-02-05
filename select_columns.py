from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark App").getOrCreate()
data_value = [("James","Smith","USA","CA"),
     ("Michael","Rose","USA","NY"),
     ("Robert","Williams","USA","CA"),
     ("Maria","Jones","USA","FL")
   ]
columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data=data_value, schema = columns)

#display all the columns
df.show()

#display only first and lastname in the Dataframe
df.select("firstname","lastname").show()

#Select all columns
df.select(*columns).show()
df.select("*").show()
