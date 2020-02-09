package org.training.spark.practice.assignmentone

import org.apache.spark.{SparkConf, SparkContext}

/*calculate the average number of friends based on their age.*/
object AverageFriendsPerAge {
  def main(args: Array[String]): Unit = {
    val sparkCnf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sprkCxt = new SparkContext(sparkCnf)
    val socialFrndsRDD = sprkCxt.textFile("src/main/resources/assignmentone/social_friends.csv")
    val ageFrndsRDD = socialFrndsRDD.map(each => {
      val splits = each.split(",")
      // age      //frndsCnt
      (splits(2),splits(3).toInt)
    })
    val ageFrndsCntRDD=ageFrndsRDD.aggregateByKey((0,0))((accr,each)=>(1+accr._1,accr._2+each),((accr,each)=>(accr._1+each._1,accr._2+each._2)))
    ageFrndsCntRDD.collect().foreach(result=> println("Age- "+result._1 +" ,Avg no.of frnds: "+(result._2._2)/result._2._1))

    Thread.sleep(300000)
  }
}
