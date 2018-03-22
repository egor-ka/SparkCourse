package task4

import org.apache.spark.sql.SparkSession

object TaskFour extends App {

  val spark = SparkSession.builder
    .master("local[2]")
    .appName("Task_Four")
    .getOrCreate()

  val sc = spark.sparkContext

  val fullText = sc.textFile("/home/ejoya/IdeaProjects/SparkCourse/src/main/resources/data/smsData.txt")

  val strings = fullText
    .flatMap(_.split("\\n"))
    .map(_.toLowerCase)

  val spamRDD = strings
    .filter(_.startsWith("spam"))
    .flatMap(_.split("[^a-zA-Z0-9]").tail)

  val hamRDD = strings
    .filter(_.startsWith("ham"))
    .flatMap(_.split("[^a-zA-Z0-9]").tail)

  val diffRDD = spamRDD.subtract(hamRDD)

  val pairRDD: Array[(String, Int)] = diffRDD
    .map(_ -> 1)
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
    .take(5)

  pairRDD.foreach(println)
}
