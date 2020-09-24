package part2df.dfbasics

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataframeBasics extends App {
  val spark = SparkSession.builder().
    appName("Spark1").config("spark.master", "local").
    getOrCreate()

  //reading a df
  val firstDf = spark.read.format("json").
    option("inferSchema", true).
    load("src/main/resources/data/cars.json")
  //inferschema shouldnt be used in prod since we will give control to spark whihc may or may not infer schema
  // properly

  //showing df
  firstDf.show()
  firstDf.printSchema()

  //get rows
  firstDf.take(10).foreach(println)

  //obtain schema of existing dataset
  val carsSch = firstDf.schema

  //spark types
  val structType = StructType(
    Array(
      StructField("Name", StringType)
    )
  )

  //build our own schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  //read df with own schema
  val carsWithSchema = spark.read.format("json").
    schema(carsSchema).
    load("src/main/resources/data/cars.json")


  //create DF from tuples
  val carsT = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )

  val manualcarsDF = spark.createDataFrame(carsT) //schema auto-inferred

  // note: DFs have schema not rows

  //create DFs with Implicits

  import spark.implicits._

  val manualCarsWithImplicits = carsT.toDF("Name", "MPG", "Cylinder", "Displacement", "HP", "Weight",
    "Acceleration", "Year", "CountryOrigin")


  manualcarsDF.printSchema()
  manualCarsWithImplicits.printSchema()

  //**Exercise
  val smartPhone = Seq(
    ("SAMSUNG", "12", "1200*320", "12"),
    ("APPLE", "32", "233*22", "1"),
    ("MI", "42", "878*12", "3"),
    ("OPPO", "1", "112*11", "1")
  )

  val smartPhoneWithDF = spark.createDataset(smartPhone)
  val smartPhoneWithManual = smartPhone.toDF("Make", "Model", "Dimensions", "Pixels")

  smartPhoneWithManual.printSchema()

  val movies = spark.read.format("json").
    option("inferSchema", true).
    load("src/main/resources/data/movies.json")

  movies.show()
  println(movies.count())

  movies.printSchema()
}
