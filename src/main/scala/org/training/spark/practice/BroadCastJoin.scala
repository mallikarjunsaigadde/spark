package org.training.spark.practice

import org.apache.spark._

/*Here we used broadCast instead of Join so that we can reduce the no.of stages involved
If used Join - 3 stages(transform,join,redByKey)
If broadCast - 2 stages(transform,redByKey)*/

object BroadCastJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc = new SparkContext(conf)
    val salesRDD = sc.textFile(args(0))

    //load customers.csv inoto a map with (k,v) pairs
    val custMap = createCustomerMap(args(1))
    // broadcasts customer map to every executor for easier lookup
    val custMapBrdcst = sc.broadcast(custMap)

    val salesPairRDD = salesRDD.map(eachRec => {
      val recColData = eachRec.split(",")
      (recColData(1), recColData(3).toDouble)
    })
    val totalSalesRDD = salesPairRDD.reduceByKey(_ + _)

    val finalRDD = totalSalesRDD.map(each => {
      val custID = each._1
      val custName = custMapBrdcst.value.getOrElse(custID,"unknown")
      (custName,each._2)
    })
    finalRDD.collect.foreach(println)

    //saving to textFile ,as we using intelliJ it will write to local
    //The no.of text files depends on the no.of partitions it has
    finalRDD.saveAsTextFile("src/main/resources/broadcastjoin_output")
  }

  def createCustomerMap(filePath:String):Map[String,String]={
      val recList=scala.io.Source.fromFile(filePath).getLines().toList
      val recMap: Map[String, String] =recList.map(eachLine=>{
        val colData=eachLine.split(",")
        (colData(0),colData(1))
      }).toMap
    recMap
  }
}
