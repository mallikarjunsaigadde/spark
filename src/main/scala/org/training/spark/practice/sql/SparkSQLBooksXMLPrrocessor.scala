package org.training.spark.practice

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext

object SparkSQLBooksXMLPrrocessor {
  def main(args: Array[String]): Unit = {
    val sparkCnf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    val sparkCxt = new SparkContext(sparkCnf)
    //val context = new SQLContext(sparkCxt)
    val context = new HiveContext(sparkCxt)
    val booksDF = context.read.format("xml").option("rowTag", "book").load("src/main/resources/books.xml")
    booksDF.printSchema()
    //booksDF.show() //by default it displays 20 char per column and 20 rows
    booksDF.show(10, true)

    //create different rows based on publish_date and add version to it based on publish_date
    val booksMultiPublishDF = context.read.format("xml").option("rowTag", "book").load("src/main/resources/books-nested-array.xml")
    booksMultiPublishDF.printSchema()
    booksMultiPublishDF.show(10, true)

    //1.we want to store multiple rows based on publish_date
    import context.implicits._
    val explodeDF = booksMultiPublishDF.withColumn("publish_date", explode($"publish_date"))
    explodeDF.printSchema()
    explodeDF.show()

    val castPublishDateDF = explodeDF.withColumn("publish_date", to_date($"publish_date"))

    val windowAggDF = castPublishDateDF.withColumn("version",
      row_number().over(Window.partitionBy("_id").orderBy("publish_date")))
    windowAggDF.printSchema()
    windowAggDF.show()

    //2.we want to retain only latest publish_date book entries

    val explodePubDF = booksMultiPublishDF.withColumn("publish_date", explode(col("publish_date")))

    val castPubDateDF = explodePubDF.withColumn("publish_date", to_date($"publish_date"))
    val windowAggDescDF = castPubDateDF.withColumn("version",
      row_number().over(Window.partitionBy("_id").
        orderBy(desc("publish_date") /*(or) col("publish_date").desc*/))).
      where("version=1") /*(or) .where(col("version")===1)*/

    windowAggDescDF.show()

    //drop the version column as it doesn't have anyother functionality to perform

    /*val finalDF = windowAggDescDF.drop("version").withColumn("publish_date", col("publish_date").
      alias("latest PubDate")) //.alias("latest PubDate")*/
    val finalDF = windowAggDescDF.drop("version").withColumnRenamed("publish_date", "latest_publish_date")
    finalDF.show()

    println("Printing SQL Query Result : ")

    buildSQLQueryRep(context,castPubDateDF)
    //Thread.sleep(300000)
  }

  //Try using nested queries instead of window as it involves shuffling

  def buildSQLQueryRep(hc:HiveContext,castPubDateDF:DataFrame): Unit ={
    castPubDateDF.registerTempTable("v_author_book")
    //val resultDF=hc.sql("select * from v_author_book")
    val resultDF=hc.sql("select max(publish_date) as date from v_author_book")
    //hc.sql("select _id as id ,max(publish_date) as date from v_author_book group by _id")/*order by publish_date DESC*/
    resultDF.show()
  }
}
