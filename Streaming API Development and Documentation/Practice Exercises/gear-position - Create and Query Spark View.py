from pyspark.sql import SparkSession

# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("gear-position").getOrCreate()
#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')
#TO-DO: read the gear-position kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    

rawsparkstreamDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","gear-position").option("startingOffsets","earliest").load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
sparkstreamDF = rawsparkstreamDF.selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value")

# TO-DO: create a temporary streaming view called "GearPosition" based on the streaming dataframe

sparkstreamDF.createOrReplaceTempView("GearPosition")

# TO-DO: query the temporary view "GearPosition" using spark.sql 
sparkstreamStarDF = spark.sql("select * from GearPosition")

# Write the dataframe from the last query to a kafka broker at localhost:9092, with a topic called gear-position-updates

sparkstreamStarDF.selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value").writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic","gear-position-updates").option("checkpointLocation","/tmp/kafkacheckpoint").start().awaitTermination()


