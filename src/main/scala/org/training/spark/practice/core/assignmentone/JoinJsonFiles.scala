package org.training.spark.practice.core.assignmentone

import org.apache.spark.{SparkConf, SparkContext}

/*Join two json files dept and emp , based on their emp_id*/
object JoinJsonFiles {
  def main(args: Array[String]): Unit = {
    val spkCnf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val spkCxt = new SparkContext(spkCnf)
    val empRdd = spkCxt.textFile("src/main/resources/assignmentone/employee.json")

    val empKVRdd = empRdd.map(_.replace("{", "").replace("}", "")).map(each => {
      val splits = each.split(",")
      val empId = splits(0).split(":")
      val empName = splits(1).split(":")
      (empId(1).trim, empName(1).trim.replaceAll("\"", ""))
    })

/*  val deptRdd = spkCxt.textFile("src/main/resources/assignmentone/department.json")
    val deptKVRdd = deptRdd.map(_.replace("{", "").replace("}", "")).map(each => {
        val splits = each.split(",")
        val empId = splits(1).split(":")
        val deptName = splits(0).split(":")
        (empId(1).trim, deptName(1).trim.replaceAll("\"", ""))
    })
    //val brdCastDeptRdd = spkCxt.broadcast(deptKVRdd)
    val joinResult=empKVRdd.join(deptKVRdd)
    joinResult.collect().foreach(each=>println(each._1+"   "+each._2._1+"  "+each._2._2))*/

    val brdCastDeptMap = spkCxt.broadcast(createDeptMap())
    val resultRDD = empKVRdd.map(each => (each._1, each._2, brdCastDeptMap.value.getOrElse(each._1, "N/A")))
    //println("emp_id   " + "  emp_name  " + "  dept_name  ")
    resultRDD.collect().foreach(println)

    Thread.sleep(300000)
  }

  def createDeptMap(): Map[String, String] = {
    val jsonList = scala.io.Source.fromFile("src/main/resources/assignmentone/department.json").getLines().toList
    val deptMap: Map[String, String] = jsonList.map(_.replace("{", "").replace("}", "")).map(each => {
      val splits = each.split(",")
      val empId = splits(1).split(":")
      val deptName = splits(0).split(":")
      (empId(1).trim, deptName(1).trim.replaceAll("\"", ""))
    }).toMap
    deptMap
  }
}
