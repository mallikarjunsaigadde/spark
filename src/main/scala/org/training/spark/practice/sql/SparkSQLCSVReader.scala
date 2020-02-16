package org.training.spark.practice

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/*Reading CSV file and identify the headers(column Names) and Schema*/
object SparkSQLCSVReader {
  def main(args: Array[String]): Unit = {
     val sparkConf=new SparkConf().setMaster("local").setAppName(getClass.getName)
     val sparkCntxt=new SparkContext(sparkConf)
     val sparkSql=new SQLContext(sparkCntxt)
     val cvsDF=sparkSql.read.format("csv").option("header","true").option("inferSchema","true").load("src/main/resources/sales.csv")
     val selectFilter=cvsDF.select("customerId","itemId","amountPaid").where("customerId=1")
     //selectFilter.show
     cvsDF.registerTempTable("v_sales_csv")
     val dFFromView=sparkSql.sql("select customerId,itemId,amountPaid from v_sales_csv where customerId=1")
     dFFromView.show()
  }
}
