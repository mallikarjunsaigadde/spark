package org.training.spark.apiexamples.serialization

object SalesRecordParser {

  def parse(record:String): Either[MalformedRecordException,SalesRecord] = {
    val columns = record.toString.split(",")
    if (columns.length == 4) {
      val transactionId: String = columns(0)
      val customerId: String = columns(1)
      val itemId: String = columns(2)
      val itemValue: Double = columns(3).toDouble
      val s = SalesRecord(transactionId, customerId, itemId, itemValue)
      val r = Right(s)
      r
    }
    else {
      Left(new MalformedRecordException())
    }
  }

}
