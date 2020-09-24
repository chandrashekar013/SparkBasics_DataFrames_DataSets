package part2df.dfbasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregation extends App {

  val spark = SparkSession.builder().
    appName("Aggregation and grouping").
    config("spark.master", "local").
    getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  //counting
  //count on single row
  val movieGenresDF = moviesDF.select(count(col("Major_Genre")))
  movieGenresDF.show()
  moviesDF.selectExpr("count(Major_Genre)").show() //same as above(will show only ths=hose which have non nulls)

  //count all rows
  moviesDF.selectExpr("count(*)").show()

  //count distinct values
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  //approx. values(can be used for testing instead of doing on full data scan)
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  //Min, Max, Sum, Avg
  moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  //Same for ,max, sum and avg

  //Data Science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  //Grouping
  val countByGenresDF = moviesDF.groupBy(col("Major_Genre")).count().show()

  val AvgbyGenresDF = moviesDF.
    groupBy(col("Major_Genre")).
    avg("IMDB_Rating").show()

  //multiple aggregations together
  val aggegrationsDF = moviesDF.
    groupBy(col("Major_Genre")).
    agg(
      count("*").as("N_Movies"),
      avg(col("IMDB_Rating")).as("avgIMDB")
    ).
    orderBy(col("avgIMDB"))

  aggegrationsDF.show()

  /*
  Exercises:
    - sum up all profits in movies in DF
    - count how many distinct directors we have
    - Show the mean and std of US gross revenue for the movies
    - compute the avg IMDB rating and avg US gross revenue per director
   */
  // Worldwide_Gross US_Gross Production_Budget
  moviesDF.selectExpr("sum(Worldwide_Gross) + sum (US_Gross) - sum(Production_Budget)").show()
  moviesDF.select((col("Worldwide_Gross") + col("US_Gross")).as("Total_Gross")).
    select(sum(col("Total_Gross"))).show() //same as above

  moviesDF.select(countDistinct(col("Director"))).show()
  moviesDF.agg(
    mean(col("US_Gross")).as("Mean_Gross"),
    stddev(col("US_Gross")).as("Std_Gross")
  ).show()

  moviesDF.groupBy(col("Director")).agg(
    avg(col("IMDB_Rating")).as("IMDB_Avg"),
    avg(col("US_Gross")).as("USGross_Avg")
  ).orderBy(col("IMDB_Avg").desc).show()

}

