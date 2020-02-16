package org.training.spark.practice

import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

object SparkSQLJsonProcessor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val spkCxt = new SparkContext(conf)
    val sqlCxt = new HiveContext(spkCxt)
    //catalyst optimizer for query optimization
    //tungsten engine is special feature in SQLCxt to optimize the memory usage & enhanced CPU performance
    val jsonDF = sqlCxt.read.json("src/main/resources/sales.json") //reading json is not lazy
    jsonDF.printSchema() //it stores column order as alphabetical
    jsonDF.show()

    //spark SQLContext 1.x columnNames are case sensitive but from 2.x it is caseignored and window fun , collect_list
    //collect_set is not present in spark sql 1.x , frm 2.x available

    //1.Get sum of amount paid by Each customer and no.of items purchased & their distinct IDs and aliasing for columns

    //val amountSpentByCustDF = jsonDF.groupBy("customerId").sum("amountPaid")
    //if we use sum - we don't have support for aliasing or multiple aggregating columns we have to use agg
    import sqlCxt.implicits._
    val amountSpentByCustDF = jsonDF.groupBy("customerId").agg(sum($"amountPaid").
      as("totalAmount"), count($"itemId").as("totalItems"), collect_set($"itemId").as("itemsList"))
    //aggregation/groupBy involves shuffling which creates 200 partitions
    //to modify default behaviour we can change with property spark.sql.shuffle.partitions=200

    //rule for groupBy is ,column should be in aggregate or groupBy clause

    amountSpentByCustDF.show()

    buildSQLNative(sqlCxt,jsonDF)
    //Thread.sleep(1000000)
  }

  def buildSQLNative(hc:HiveContext,jsonDF:DataFrame): Unit ={
    jsonDF.registerTempTable("v_custdata_json")
    val sqlDF=hc.sql("select customerId,sum(amountPaid) as totalAmount, count(itemId) as itemCount,collect_set(itemId) as itemsList from v_custdata_json group by customerId")
    println()
    println("showing data from SQL query DF:")

    sqlDF.show()
  }
}
