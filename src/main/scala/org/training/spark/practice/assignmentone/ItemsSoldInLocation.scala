package org.training.spark.practice.assignmentone

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object ItemsSoldInLocation {
  def main(args: Array[String]): Unit = {
    val sparkCnf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sparkCxt = new SparkContext(sparkCnf)
    val userInfoRDD = sparkCxt.textFile("src/main/resources/assignmentone/userinfo.txt")
    val userlocRDD = userInfoRDD.map(each => {
      val splits = each.split(" ")
      //println("User Loc -  "+(splits(0),splits(3)))
      (splits(0),splits(3))
    })
    val transInfoRDD = sparkCxt.textFile("src/main/resources/assignmentone/transactioninfo.txt")
    val userItemRDD = transInfoRDD.map(each => {
      val splits = each.split(" ")
      //println((splits(2),splits(4)))
      (splits(2),splits(4))
    })
    val idLocItemJoinRDD=userlocRDD.join(userItemRDD)

    /*val itemLocRDD=idLocItemJoinRDD.map(each=>(each._2._2,each._2._1.trim))
    val itemLocGrpRDD=itemLocRDD.groupByKey()
    val itemLocSetRDD=itemLocGrpRDD.map(each=>(each._1,each._2.toSet))
    itemLocSetRDD.collect().foreach(println)*/

    val itemLocRDD=idLocItemJoinRDD.map(each=>(each._2._2,mutable.HashSet(each._2._1.trim)))
    val itemLocRedRDD=itemLocRDD.reduceByKey(_++_)
    itemLocRedRDD.collect().foreach(println)
    Thread.sleep(300000)


  }
}
