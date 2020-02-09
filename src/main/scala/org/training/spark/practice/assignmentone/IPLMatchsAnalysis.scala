package org.training.spark.practice.assignmentone

import org.apache.spark.{SparkConf, SparkContext}
/*IPL Match Analysis - Best First Bat win stadium & percentage , Best First Bowl Win Stadium & Percentage*/
object IPLMatchsAnalysis {
  def main(args: Array[String]): Unit = {
    val spCf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val spCxt = new SparkContext(spCf)
    val iplMatchsRDD = spCxt.textFile("src/main/resources/assignmentone/matches.csv")
    val mAlysDataRDD = iplMatchsRDD.map(IPLMatchUtil.extractMatchDetails(_))
    val venuRunWicketRDD = mAlysDataRDD.map(matchDetails => (matchDetails.venue, (matchDetails.wonByRuns, matchDetails.wonByWkts)))
    //val aggrResultRDD=venuRunWicketRDD.reduceByKey((accr,each)=>(accr._1+each._1,accr._2+each._2))
    val aggrResultRDD = venuRunWicketRDD.reduceByKey((accr, each) => (if (each._1 > 0) {
      accr._1 + each._1
    } else {
      accr._1
    }, if (each._2 > 0) {
      accr._2 + each._2
    } else {
      accr._2
    }))
    val batWinPrctRDD = aggrResultRDD.map(each => (each._1, (each._2._1 * 100) / (each._2._1 + each._2._2)))
    val batWinTake=batWinPrctRDD.collect().sortBy(_._2).reverse.take(1);
    println("Batting Win % ")
    println(batWinTake.foreach(println))
    val fieldWinPrctRDD = aggrResultRDD.map(each => (each._1, (each._2._2 * 100) / (each._2._1 + each._2._2)))
    val fieldWinTake=fieldWinPrctRDD.collect().sortBy(_._2).reverse.take(1);
    println("Bowling Win % :")
    println(fieldWinTake.foreach(println))

    Thread.sleep(300000)

  }
}

case class MatchDetails(tossDecision: String, wonByRuns: Int, wonByWkts: Int, venue: String)


object IPLMatchUtil {

  def extractMatchDetails(data: String): MatchDetails = {
    val splits = data.split(",")
    val matchDetails = MatchDetails(splits(7), splits(11).toInt, splits(12).toInt, splits(14))
    matchDetails
  }
}