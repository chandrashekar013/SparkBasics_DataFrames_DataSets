package part3TypesandDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Types extends App {

  val spark = SparkSession.builder().appName("Types").config("spark.master", "local").
    getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  //adding plain value to DF
  moviesDF.select(col("Title"), lit(45)).show() //any types can be added i lit

  //Booleans
  val dramaFilter = col("Major_Genre") === "Drama"
  val ratingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and ratingFilter
  moviesDF.select("Title").where(preferredFilter)

  // TO add condition itself as additional column
  val moviesWithFIlterDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  moviesWithFIlterDF

  moviesWithFIlterDF.where(col("good_movie"))

  //negation
  moviesWithFIlterDF.where(not(col("good_movie")))

  //Numbers
  //math operators(can only be done on numerical fields
  moviesDF.select(col("Title"), (col("US_Gross") / 10 + col("Worldwide_Gross")))

  //Correlation (number bw -1 and 1)
  println(moviesDF.stat.corr("IMDB_Rating", "Rotten_Tomatoes_Rating"))

  //Strings
  val carsDF = spark.read.json("src/main/resources/data/cars.json")
  carsDF.select(initcap(col("Name")))

  //contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  //to have multiple values, we can use regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").show()

  /* exercise
    -
   */
}

