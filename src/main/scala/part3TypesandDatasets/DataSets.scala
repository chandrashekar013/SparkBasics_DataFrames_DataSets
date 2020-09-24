package part3TypesandDatasets

import java.sql.Date
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object DataSets extends App {
  val spark = SparkSession.builder().appName("datasets").config("spark.master", "local").
    getOrCreate()

  val numbersDF = spark.read.format("csv").
    option("header", "true").
    option("sep", ",").
    load("src/main/resources/data/numbers.csv")

  numbersDF.show()

  //convert to DS
  //implicit val IntEncoder =Encoders.scalaInt

  import spark.implicits._
  //val numbersDS : Dataset[Int] = numbersDF.as[Int]

  //dataset of complex type
  // 1. define case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  // 2. read DF from file
  def readDF(FileName: String) =
    spark.read.json(s"src/main/resources/data/$FileName")

  val carsDF = readDF("cars.json")

  //convert DF to DS
  val carsDS = carsDF.as[Car]

  //  numbersDS.filter(_ < 100).show()
  val carCapitalDS = carsDS.map(car => car.Name.toUpperCase())
  carCapitalDS.show()


  /*
  Exercise
    - how many cars
    - how many powerful cars (HP > 140)
    - avg horsepower
   */
  val carsCountDS = carsDS.count()
  println(carsCountDS)

  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count())

  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCountDS)

  carsDS.select(avg(col("Horsepower"))).show() //would get exactly same result as above
  //this can also be done by DF function on DS. Since DS will have access to all DF functions

  /*Note advantage of using DS is it will have everything DF has . Also, we
  can make use of generic functions from scala language*/

  //Joins

  val bandsDF = readDF("bands.json")

  case class band(id: Long, Name: String, homeTown: String, year: Long)

  val bandsDS = bandsDF.as[band]

  val guitarDF = readDF("guitars.json")

  case class guitars(id: Long, model: String, make: String, guitarType: String)

  val guitarDS = guitarDF.as[guitars]

  val guitarPlayersDF = readDF("guitarPlayers.json")

  case class guitarPlayers(id: Long, Name: String, guitars: Seq[Long], band: Long)

  val guitarPlayersDS = guitarPlayersDF.as[guitarPlayers]

  val guitarPlayersBandsDS = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") ===
    bandsDS.col("id"), "inner")
  guitarPlayersBandsDS.show()

  //Grouping DS:
  carsDS.groupByKey(_.Origin).count().show()

}
