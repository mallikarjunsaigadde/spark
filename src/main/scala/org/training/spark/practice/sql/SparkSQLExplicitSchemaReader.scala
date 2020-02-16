package org.training.spark.practice

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

/*Add Explicit Schema to the src file - we use this mainly for Flat files
, for RDBMS and parquet we won't use*/
object SparkSQLExplicitSchemaReader {
  def main(args: Array[String]): Unit = {
    val spkCnf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val spkCxt = new SparkContext(spkCnf)
    val sqlCxt = new HiveContext(spkCxt)
    val customSchema = StructType(Seq(
      StructField("txId", IntegerType, true),
      StructField("custId", IntegerType, true),
      StructField("itmId", IntegerType, true),
      StructField("purchase", DoubleType)))
    val salesDF = sqlCxt.read.format("csv").schema(customSchema).
      option("header", "true").option("mode", "FAILFAST").load("src/main/resources/sales.csv")
    //Databricks spark csv options in GitHub
    /*
    Mode   - "PERMISSIVE"     - Allows Missing Fields Extra Fields fields but not type
           - "FAILFAST"       - Only exact match is allowed (realTime used)
           - "DROPMALFORMED" - Allows Missing Fields Extra Fields fields and type mismatch also , it drops bad records
                               prints bad records with WARN in logs
                               Spark 2.4+ we can give path to store the bad records.
    */
    println(salesDF.schema)
    salesDF.show
  }
}
