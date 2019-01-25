name := "Spark-StreamingApp"

version := "0.1"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0"


libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.5"

