package org.training.spark.practice

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/*Display Output DataFrame as,

    itemNum,price,gender,size,colour*/

object SparkSQLApparelXMLParser {

  def main(args: Array[String]): Unit = {
    val spkCnf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val spkCxt = new SparkContext(spkCnf)
    val spkSQL = new SQLContext(spkCxt)
    val catalogDF = spkSQL.read.format("xml").option("rowTag", "product").
      load("src/main/resources/XMLInput.xml")
    catalogDF.printSchema()
    catalogDF.show()
    import spkSQL.implicits._
    val itemDF = catalogDF.select("catalog_item").
      withColumn("catalog_item", explode($"catalog_item")).
      select($"catalog_item.item_number".as("itemNum"),
        $"catalog_item.price".as("price"), $"catalog_item._gender".as("gender"),
        $"catalog_item.size".as("itemsize")).
      withColumn("itemsize", explode($"itemsize"))
    itemDF.printSchema()
    itemDF.show()
    val resultDF = itemDF.select($"itemNum", $"price", $"gender", $"itemsize._description".
      as("size"), $"itemsize.color_swatch".as("colour")).
      withColumn("colour", explode($"colour")).withColumn("colour", $"colour._VALUE")
    resultDF.printSchema()
    resultDF.show()
  }

}
