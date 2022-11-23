from pyspark.sql import SparkSession

#TO-DO: create a Spark Session, and name the app something relevant
spark = SparkSession.builder.appName("balance-updates").getOrCreate()
#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

#TO-DO: read a stream from the kafka topic 'balance-updates', with the bootstrap server localhost:9092, reading from the earliest message

sparkstreamDF = spark.readStream.format("kafka").option("kafka-bootstrap-servers","Localhost:9092").option("subscribe","balance-updates").option("startingoffsets","earliest").load()

#TO-DO: cast the key and value columns as strings and select them using a select expression function

kafkasparkstreamDF = sparkstreamDF.selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value")

#TO-DO: write the dataframe to the console, and keep running indefinitely
kafkasparkstreamDF.writeStream.outputMode("append").format("console").start().awaitTermination()