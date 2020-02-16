package org.training.spark.practice.core.assignmentone

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
      movieDetailsBrdCst.value.getOrElse(splits(1), "N/A") != "N/A"
    }).map(eachRating => {
      val splits = eachRating.split("::")
      (splits(1), (movieDetailsBrdCst.value.get(splits(1)).get, 1, splits(2).trim.toDouble))
    })
    val mostViewedMvsRDD = movieIDKeyRDD.reduceByKey((accr, each) => (each._1, accr._2 + each._2, accr._3 + each._3)).persist()
    println("\n\n Top ten most viewed movies with their movies Name (Descending) \n\n")
    val mostViewTenRDD = mostViewedMvsRDD.collect().sortBy(_._2._2).reverse.take(10)
    mostViewTenRDD.foreach(each => println("Movie: " + each._2._1.name + " *****************************views: " + each._2._2))
    println("\n\n Top twenty rated movies (Condition: The movie should be rated/viewed by at least 40 users) \n\n")
    val mostRatedTwentyRDD = mostViewedMvsRDD.filter(each => each._2._2 >= 40).collect().sortBy(each => each._2._3 / each._2._2).reverse.take(20)
    mostRatedTwentyRDD.foreach(each => println("Movie: " + each._2._1.name + " *****************************Rating: " + each._2._3 / each._2._2))

    val usrDetailsBrdCst = spkCxt.broadcast(userDetailsMap())

    val userIDkeyRDD = ratingRDD.filter(eachRating => {
      val splits = eachRating.split("::")
      movieDetailsBrdCst.value.getOrElse(splits(1), "N/A") != "N/A" && usrDetailsBrdCst.value.getOrElse(splits(0), "N/A") != "N/A" &&
        (usrDetailsBrdCst.value.get(splits(0)).get.age.contains("18-35") || usrDetailsBrdCst.value.get(splits(0)).get.age.contains("36-50") ||
          usrDetailsBrdCst.value.get(splits(0)).get.age.contains("50+"))
    }).map(eachRating => {
      val occpData = Map(0 -> "other or not specified", 1 -> "academic/educator", 2 -> "artist", 3 -> "clerical/admin", 4 -> "college/grad student", 5 -> "customer service", 6 -> "doctor/health care", 7 -> "executive/managerial", 8 -> "farmer", 9 -> "homemaker", 10 -> "K-12 student", 11 -> "lawyer", 12 -> "programmer", 13 -> "retired", 14 -> "sales/marketing", 15 -> "scientist", 16 -> "self-employed", 17 -> "technician/engineer", 18 -> "tradesman/craftsman", 19 -> "unemployed", 20 -> "writer")
      val splits = eachRating.split("::")
      (occpData.get(usrDetailsBrdCst.value.get(splits(0)).get.occupation.toInt).get + "@" + usrDetailsBrdCst.value.get(splits(0)).get.age,
        scala.collection.mutable.Map() ++ movieDetailsBrdCst.value.get(splits(1)).get.gener.split("""\|""").map(each => each -> (1,splits(2).trim.toDouble)).toMap
      )
    })

    val ageJobRDD = userIDkeyRDD.reduceByKey((accr, rec) => {
      rec.map(each => {
        if (accr.contains(each._1)) {
          accr.put(each._1, (accr.getOrElse(each._1, (0,0))._1.toInt+ 1,accr.getOrElse(each._1, (0.0,0.0))._2.toDouble+each._2._2.toDouble))
        } else {
          accr.put(each._1, (1,each._2._2.toDouble))
        }
        accr
      })
      accr
    })

    println("\n\n the genres ranked by Average Rating, for each profession and age\ngroup. The age groups to be considered are: 18-35, 36-50 and 50+ \n\n")

    ageJobRDD.collect().foreach(each => println(each._1.replace("@", "        ") + "    " + each._2.toSeq.sortBy(each=>each._2._2/each._2._1).reverse.take(5).toMap.keys))

    Thread.sleep(300000)
  }

  def movieDetailsMap(): Map[String, Movie] = {
    val movieFileList = scala.io.Source.fromFile("/home/cloudera/projects/spark-core/src/main/resources/assignmentone/moviedata/movies.dat").getLines().toList
    val movieMap = movieFileList.map(eachMovie => {
      val splits = eachMovie.split("::")
      (splits(0), Movie(splits(1), splits(2)))
    }).toMap
    return movieMap
  }

  case class Ratings(usrID: String, movieID: String, rating: Int)

  case class Movie(name: String, gener: String)

  case class User(age: String, occupation: String)

  def userDetailsMap(): Map[String, User] = {
    val usrFileList = scala.io.Source.fromFile("/home/cloudera/projects/spark-core/src/main/resources/assignmentone/moviedata/users.dat").getLines().toList
    val userMap = usrFileList.map(eachUsr => {
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
