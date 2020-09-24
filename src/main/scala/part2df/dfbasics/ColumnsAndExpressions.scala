package part2df.dfbasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder().
    appName("Columns and Expressions").
    config("spark.master", "local").
    getOrCreate()

  val carsDF = spark.read.json("src/main/resources/data/cars.json")
  carsDF.show()

  // to get sub set of data or build new data frames
  val firstName = carsDF.col("Name")

  //projecttion
  val carNamesDF = carsDF.select(firstName)
  //various ways of select

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    $"Horsepower",
    'Year, //scala symbol auto converted to column
    expr("Origin") //Expression

  )

  // select with plain column names (Either of above or below methods can be used to select)
  carsDF.select("Name", "Acceleration")

  //Expressions
  val simpleExpression = carsDF.col("Name")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWIthWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs /2.2").as("Weight_in_kg")
  )

  val carsWithWeightSimpleExpr = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs /2.2"
  )
  carsWIthWeightDF.show()

  //DF Processing
  //adding a column
  val carsWithKgDF = carsDF.withColumn("Weight_in_lbs", col("Weight_in_lbs") / 2.2)
  //rename column
  val carsWIthCOlumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
  //remove column
  carsWIthCOlumnRenamed.drop("Cylinders", "Displacement")

  //Filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.filter(col("Origin") =!= "USA")
  //Filtering with exp strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  //Chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA") and (col("Horsepower") > 150))
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150 ")

  //unioning
  val moreCarsDF = spark.read.json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)

  allCarsDF.show()

  //distinct
  val distinctCountriesDF = carsDF.select("Origin").distinct()
  distinctCountriesDF.show()

  /*
  1. Read movies DF and select 2 movies of your choice
  2. create another column sum up total profit of movies : US_Gross * WorldWide
  3. Select all comedy movies wih IMDB rating 6 and above
   */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  moviesDF.show()

  val movies2ColumnsDF = moviesDF.select("Title", "Director")
  movies2ColumnsDF.show()

  val moviesGrossDF = moviesDF.withColumn("TotalGross", col("US_Gross") * col("Worldwide_Gross"))
  moviesGrossDF.show()

  val highRatingMovieDF = moviesGrossDF.filter("IMDB_Rating > 6")
  highRatingMovieDF.show()
}
