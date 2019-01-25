package com.streamapp.sparkhelper

import com.streamapp.config.Settings.ProjectConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {


  def getSparkSession(cassandraIP: String): SparkSession = {
    //get spark configuration
    val conf = new SparkConf()
      .setAppName("Spark-Kafka")
      .set("spark.cassandra.connection.host", cassandraIP)
      .set("spark.cassandra.auth.username", "")
      .set("spark.cassandra.auth.password", "")
      //.set("spark.local.dir", ProjectConfig.sparkWd)

    val checkpointDirectory = ProjectConfig.checkPointDir

    //init spark session
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir(checkpointDirectory)

    spark
  }


}
