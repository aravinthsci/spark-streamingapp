package com.streamapp.kafka

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._


object KafkaStructuredStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

    val sc = spark.sparkContext

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    import spark.implicits._
    //Read from kafka

      val df = spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "testing")
                    .option("startingOffsets", "latest").load()

    val data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    val results = data.map(_._2).flatMap(value => value.split("\\s+")).groupByKey(_.toLowerCase).count()
    val query = results.writeStream.format("console").outputMode("complete").start()
    query.awaitTermination()


    val mySchema = StructType(Array(
      StructField("ID", IntegerType),
      StructField("ACCOUNT_NUMBER", StringType)
    ))

    val streamingDataFrame = spark.readStream.schema(mySchema).option("delimiter",",").csv("hdfs:/aravinth/")
    streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
                          .writeStream.format("kafka").option("topic", "testing")
                          .option("kafka.bootstrap.servers", "localhost:9092")
                          .option("checkpointLocation", "file:///opt/jars")
                          .start().awaitTermination()

     streamingDataFrame.writeStream.format("console").start().awaitTermination()





  }


}
