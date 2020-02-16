package org.training.spark.practice

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/*Reading XML with RowTag with column aliasing*/

object SparkSQLXMLProcessor {
  def main(args: Array[String]): Unit = {
    val sparkCnf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sparkCxt = new SparkContext(sparkCnf)
    val sparkSQL = new SQLContext(sparkCxt)
    val xmlDF = sparkSQL.read.format("xml").option("rowTag", "person").load("src/main/resources/ages.xml")
    xmlDF.printSchema()
    xmlDF.show()
    //val formatedDF=xmlDF.select("age._VALUE","age._birthplace","age._born","name") //Here we can do col. aliasing

    import sparkSQL.implicits._

    val formatedDF = xmlDF.select(xmlDF("age._VALUE").as("Yearsold"),
      col("age._birthplace").as("birthplace"), column("age._born").as("DOB"),
      $"name".as("PersonName"))

    //when & otherwise from(org.apache.spark.sql.functions._) are used for case (in sql)
    println("Using withColumn:-")
    val newColUsingWithColDF = formatedDF.withColumn("bp_flag",
      when(col("birthplace").isNotNull, "Y").otherwise("N")).
      withColumn("DOB", col("DOB").cast("date") /*(or)to_date("DOB")*/)
    newColUsingWithColDF.show()


    println("Using Select:-")
    val newColUsingSelectDF = formatedDF.select(col("Yearsold"), col("birthplace"),
      col("DOB").cast("date").as("DOB"), col("PersonName"),
      when(col("birthplace").isNotNull, "Y").otherwise("N").as("bp_flag"))
    newColUsingSelectDF.show()


    println("Using Native SQl")

    BuildWithNativeSQL.useNativeSQL(xmlDF, sparkSQL)
  }
}


object BuildWithNativeSQL {

  def useNativeSQL(df: DataFrame, slqC: SQLContext): Unit = {
    df.registerTempTable("v_person_details_xml")
    val sqlDF = slqC.sql("select age._VALUE as AgeInYears,age._born as DOB,age._birthplace as BirthPlace," +
      "name as personName , (case when age._birthplace IS NOT NULL then 'Y' else 'N' end) as PlaceOfBirthPresent " +
      "from v_person_details_xml")
    sqlDF.show()
  }

}