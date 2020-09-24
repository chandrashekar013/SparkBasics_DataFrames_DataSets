package part3TypesandDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
  val spark = SparkSession.builder().appName("Complex Types").config("spark.master", "local").
    getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  //Dates
  /*val moviesWIthReleaseDates = moviesDF.select(col("Title"),
    to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))

    moviesWIthReleaseDates.withColumn("Today",current_date())
      .withColumn("TimeStamp", current_timestamp()).show()*/
  //.withColumn("Movie_age", datediff(col("Today"), col("Actual_Release"))/365)


  //Structures

  //version -1 with column operators
  moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")).
    select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  //version 2 with expression strings
  moviesDF.selectExpr("Title", "(US_Gross,Worldwide_Gross) as Profit").
    selectExpr("Title", "Profit.US_Gross")

  //Arrays
  val movieWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,")).
    as("Title_words")

  movieWithWords.select(col("Title"),
    expr("Title_words[0]"),
    size(col("Title_words")),
    array_contains(col("Title_words"), "Love")
  ).show()

}
