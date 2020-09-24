package part3TypesandDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Nulls extends App {

  val spark = SparkSession.builder().appName("Nulls").
    config("spark.master", "local").getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  moviesDF.select(col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating"))
  )

  //checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  //nulls while ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  //removing nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop()

  //replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating")).show()
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10 //will be useful to have all fields with default values
  ))

  //complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(IMDB_Rating,Rotten_Tomatoes_Rating * 10) as ifnull", //same as coalsce
    "nvl(IMDB_Rating,Rotten_Tomatoes_Rating) as nvl", //same as above
    "nullif(IMDB_Rating,Rotten_Tomatoes_Rating) as nullif" // returns null if both equal else returns first
  )

}
