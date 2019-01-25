package com.streamapp.kafka

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.streamapp.sparkhelper.SparkUtils.getSparkSession
import com.streamapp.cassandra.Satement
import com.streamapp.model.ProductSale


object KafkatoCassandra {

  def main(args: Array[String]): Unit = {

    val cassandraIP = "localhost"

    val spark: SparkSession = getSparkSession(cassandraIP)
    import spark.implicits._

    val input: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "product")
      .option("startingOffsets", "earliest")
      .load()

    val df = input.selectExpr("CAST(value AS STRING)").as[String]

    val ds = df.map(r => r.split(",")).map(c => ProductSale(c(0), c(1), c(2), c(3), c(4), c(5), c(6),c(7), c(8), c(9)))

    val connector = Satement.getConnector(spark)

    import org.apache.spark.sql.ForeachWriter
    val writer = new ForeachWriter[ProductSale] {
      override def open(partitionId: Long, version: Long) = true
      override def process(value: ProductSale): Unit = {
        Satement.updateProvinceSale(connector, value)
      }
      override def close(errorOrNull: Throwable): Unit = {}
    }

    val query: StreamingQuery = ds.writeStream
      .queryName("ProductSale")
     // .outputMode("complete")
      .foreach(writer)
      .start()


      //ds.writeStream.format("org.apache.spark.sql.cassandra").option("keyspace", "test").option("table", "test").start()
   // ds.writeStream.format("console").start().awaitTermination()
    query.awaitTermination()
  }



}
