package org.training.spark.practice

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKey {

  def main(args: Array[String]): Unit = {
    val sparkCf = new SparkConf().setMaster("local").setAppName(getClass.getName)

    val sparkCntxt = new SparkContext(sparkCf)

    val salesRdd = sparkCntxt.textFile("/home/cloudera/projects/spark-core/src/main/resources/sales.csv")

    val salesTupleRDD = salesRdd.map(each => {
      val linSplit = each.split(",")
      (linSplit(1), linSplit(3).toDouble)
    })

    val eachCustPurchaseRDD = salesTupleRDD.aggregateByKey((0, 0.0))((accPrt, eachRec) => (accPrt._1 + 1, eachRec + accPrt._2), (acc, each) => (acc._1 + each._1, acc._2 + each._2))
    eachCustPurchaseRDD.collect().foreach(each=> println(each._1+"  -  "+each._2._2/each._2._1))
  }
}