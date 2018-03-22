package task2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object TaskTwo extends App {

  val spark = SparkSession.builder
    .master("local[2]")
    .appName("Task_Two")
    .getOrCreate()

  val dataFile = "/home/ejoya/IdeaProjects/SparkCourse/src/main/resources/data/employees.csv"

  val schema = StructType(Seq(
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("gender", StringType)
  ))

  val employees = spark.read.schema(schema).csv(dataFile)

  employees.createOrReplaceTempView("employees")

  val result = spark.sql("SELECT name FROM employees WHERE age>20")

  result.show()
}