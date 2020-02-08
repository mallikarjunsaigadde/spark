package org.training.spark.practice.assignmentone

import org.apache.spark.{SparkConf, SparkContext}

/*Print the words in the Text file*/
object PrintWordsInFile {
  def main(args: Array[String]): Unit = {
      val sparkConf=new SparkConf().setMaster("local").setAppName(getClass.getName)
      val sparkCxt=new SparkContext(sparkConf)
      val txtRdd=sparkCxt.textFile("src/main/resources/assignmentone/wordtext.txt")
      val transformRdd=txtRdd.flatMap(_.split(" ").toList)
      transformRdd.collect().foreach(println)
  }
}
