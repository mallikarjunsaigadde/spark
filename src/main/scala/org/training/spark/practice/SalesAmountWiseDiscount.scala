package org.training.spark.practice

import org.apache.spark._

import org.training.spark.apiexamples.serialization.SalesRecordParser

object SalesAmountWiseDiscount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc = new SparkContext(conf)
    val rddTxtFile = sc.textFile(args(0)) // sales-error.csv,sales.csv
    val salesRecRDD = rddTxtFile.map(rawRec => {
      val eachRec = SalesRecordParser.parse(rawRec)
      //if(eachRec.isRight)
      eachRec.right.get
    })
    val salesRDDTuple = salesRecRDD.map(eachRec => {
      (eachRec.customerId, eachRec.itemValue)
    })
    val custItemValRDD = salesRDDTuple.reduceByKey((accr, itmVal) => accr + itmVal)
    val discAmountRDD = custItemValRDD.map(each => {
      if (each._2 > 2000) (each._1, each._2 * 0.9)
      else each
    })
    //custItemValRDD.collect().foreach(println(_))
    println(custItemValRDD.collect().toList)
    Thread.sleep(30000)
  }

}


