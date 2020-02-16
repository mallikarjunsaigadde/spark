package org.training.spark.practice

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/*Read csv File as RDD ,
      1.remove Header if present
            and
      2.convert to DataFrame*/
object RDDCaseClassToDataFrame {

  def main(args: Array[String]): Unit = {
    val spkCnf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val spkCxt = new SparkContext(spkCnf)
    val sqlCxt = new SQLContext(spkCxt)
    val readSalesAsRDD = spkCxt.textFile("src/main/resources/sales.csv")
    //val filteredRec=readSalesAsRDD.filter(!_.startsWith("transactionId")) - Not standard way
    val salesFirstRec = readSalesAsRDD.first()
    val stdFilterRecRDD = readSalesAsRDD.filter(!_.equals(salesFirstRec))
    stdFilterRecRDD.foreach(println)

    val salesRecRDD = stdFilterRecRDD.map(each => {
      val data = each.split(",")
      SalesRecord(data(0).toInt, data(1).toInt, data(2).toInt, data(3).toDouble)
    })

    import sqlCxt.implicits._
    println("Using toDF of implicits sqlContext:")
    val salesRDDToDF = salesRecRDD.toDF()
    salesRDDToDF.printSchema
    salesRDDToDF.show
    //or
    println("Using createDataFrame of sqlContext:")
    val saleRDDCreateRDD = sqlCxt.createDataFrame(salesRecRDD)
    saleRDDCreateRDD.printSchema
    saleRDDCreateRDD.show
    //println("println Data:\n"+salesRecRDD.glom)
  }

  case class SalesRecord(txId: Integer, custId: Integer, itmId: Integer, price: Double)

}
