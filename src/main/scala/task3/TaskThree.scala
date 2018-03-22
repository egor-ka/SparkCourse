package task3

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object TaskThree extends App {

  val spark = SparkSession.builder
    .master("local[2]")
    .appName("Task_Three")
    .getOrCreate()

  val sqlContext: SQLContext = spark.sqlContext

  val dataFile = "/home/ejoya/IdeaProjects/SparkCourse/src/main/resources/data/employees.csv"

  val schema = StructType(Seq(
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("gender", StringType)
  ))

  val employees = sqlContext.read.schema(schema).csv(dataFile)

  import sqlContext.implicits._

//  employees.withColumn("surname", employees.)
  val employeesWithSurname = employees.withColumn("surname", split(col("name"), " ").getItem(1))

  employeesWithSurname
    .select("name", "age", "gender")
    .where("age < 70 and gender == 'm'" )
    .orderBy($"surname".asc)
    .show()
}