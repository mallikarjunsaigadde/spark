name := "spark-practice"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.3" //If we want to use Hive tables/HiveContext(HQL) in spark sql

libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"   // No builtin way in spark 1.x to read csv so added these

libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.1"   // No builtin way in spark 1.x to read xml so added these

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.47" // Mysql connector to connect mqsql from spark

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.13"  // Mysql connector to connect cassandra for spark
