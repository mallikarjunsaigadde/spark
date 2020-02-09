package org.training.spark.apiexamples.serialization


case class SalesRecord(val transactionId: String,
                  val customerId: String,
                  val itemId: String,
                  val itemValue: Double)
{

/*
  override def toString: String = {
    transactionId+","+customerId+","+itemId+","+itemValue
  } */
}
