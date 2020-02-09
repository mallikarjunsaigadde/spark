package org.training.spark.practice.assignmentone

import org.apache.spark.{SparkConf, SparkContext}

/*Find the below,
1. Top ten most viewed movies with their movies Name (Ascending or Descending order) 
2. Top twenty rated movies (Condition: The movie should be rated/viewed by at least 40 users)
3. We wish to know how have the genres ranked by Average Rating, for each profession and age
group. The age groups to be considered are: 18-35, 36-50 and 50+*/

object MovieAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkCnf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val spkCxt = new SparkContext(sparkCnf)
    val ratingRDD = spkCxt.textFile("src/main/resources/assignmentone/moviedata/ratings.dat")
    val movieDetailsBrdCst = spkCxt.broadcast(movieDetailsMap())
    val movieIDKeyRDD = ratingRDD.filter(eachRating => {
      val splits = eachRating.split("::")
      movieDetailsBrdCst.value.getOrElse(splits(1),"N/A")!="N/A"
    }).map(eachRating => {
      val splits = eachRating.split("::")
      (splits(1), (movieDetailsBrdCst.value.get(splits(1)),1))
    })
    val usrDetailsBrdCst = spkCxt.broadcast(userDetailsMap())
    val mostViewedMvsRDD=movieIDKeyRDD.reduceByKey((accr,each)=>(each._1,accr._2+each._2))
    val mostViewTenRDD=mostViewedMvsRDD.collect().sortBy(_._2._2).reverse.take(10)
    mostViewTenRDD.foreach(each=>println("Movie: "+each._2._1+" *****************************views: "+each._2._2))
  }

  def movieDetailsMap():  Map[String,Movie] = {
    val movieFileList = scala.io.Source.fromFile("/home/cloudera/projects/spark-core/src/main/resources/assignmentone/moviedata/movies.dat").getLines().toList
    val movieMap=movieFileList.map(eachMovie => {
      val splits = eachMovie.split("::")
      (splits(0), Movie(splits(1), splits(2)))
    }).toMap
    return movieMap
  }

  case class Ratings(usrID: String, movieID: String, rating: Int)

  case class Movie(name: String, gener: String)

  case class User(age: String, occupation: String)

  def userDetailsMap(): Map[String,User]= {
    val usrFileList = scala.io.Source.fromFile("/home/cloudera/projects/spark-core/src/main/resources/assignmentone/moviedata/users.dat").getLines().toList
    val userMap=usrFileList.map(eachUsr => {
      val splits = eachUsr.split("::")
      val age = splits(2).toInt
      var ageRng = splits(2)
      if (age >= 18 && age <= 35) {
        ageRng = "18-35"
      } else if (age >= 36 && age <= 50) {
        ageRng = "36-50"
      } else if (age > 50) {
        ageRng = "50+"
      }
      (splits(0), User(ageRng, splits(3)))
    }).toMap
    return userMap
  }
}
