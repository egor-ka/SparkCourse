package task1

import org.apache.spark.sql.SparkSession

object TaskOne extends App {

  val spark = SparkSession.builder
    .master("local[2]")
    .appName("Task_One")
    .getOrCreate()

  val sc = spark.sparkContext

  val ints = sc.parallelize(1 to 100, 5)
  val groups = ints.map(x => (x % 3, x))
  groups.groupByKey.collect.foreach(println)
}
