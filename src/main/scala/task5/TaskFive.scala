package task5

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object TaskFive extends App {

  val spark = SparkSession.builder
    .master("local[2]")
    .appName("Task_Five")
    .getOrCreate()

  val sqlContext: SQLContext = spark.sqlContext

  val schema = StructType(Seq(
    StructField("Id", IntegerType),
    StructField("Name", StringType),
    StructField("Year", IntegerType),
    StructField("Gender", StringType),
    StructField("Count", IntegerType)
  ))

  val dataFile = "/home/ejoya/IdeaProjects/SparkCourse/src/main/resources/data/NationalNames.csv"

  val nationalNames = sqlContext.read
    .option("header", value = true)
    .schema(schema)
    .csv(dataFile)

  private val numOfMaleEmmas = nationalNames
    .where("Name == 'Emma' and Gender == 'M' and Year BETWEEN 1970 AND 2000")
    .agg(sum("Count") / 31)
}
