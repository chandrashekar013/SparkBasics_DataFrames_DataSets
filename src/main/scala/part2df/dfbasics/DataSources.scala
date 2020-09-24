package part2df.dfbasics

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import part2df.dfbasics.DataframeBasics.spark

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("DataSources").
    config("spark.master", "local")
    .getOrCreate()

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


  /*//Reading DF:
    -format
    -Path
    -schema (Optional) but we can enforce one if needed
    -Zero or more options
    -load


  * */

  val carsDF = spark.read.format("json").
    schema(carsSchema)
    .option("mode", "failFast") //dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json").load()
  // or.load("src/main/resources/data/cars.json")

  // same as above instead of using multiple options
  val carsDFWithOptionMap = spark.read.format("json").
    schema(carsSchema).options(Map(
    "mode" -> "failfast",
    "path" -> "src/main/resources/data/cars.json"))

  // writing dataFrames
  /* Things needed for writing df:
    -format
    -Path
    -mode(overwrite, ignore, append, ErrorifExists)
    -Zero or more options
    -Save
  * */

  carsDF.write.format("json").
    mode(SaveMode.Overwrite).
    option("path", "src/main/resources/data/cars_dupl_1.json").save()

  //different formats
  //JSON flags or configurations
  spark.read.format("json").
    schema(carsSchema).
    option("compression", "uncompressed"). // if we want compression, we can use bzip2, gzip, lz4, snappy, deflate
    option("dateFormat", "YYYY-MM-dd").
    option("allowSingleQuotes", "true") //if json has singlequotes instead of doublequotes
    .load("src/main/resources/data/cars.json")


  //CSV flags
  //schema
  val stockSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )

  spark.read.format("csv").
    schema(stockSchema).
    option("header", "true").
    option("sep", ",").
    option("dateFormat", "MMM dd YYYY").
    option("nullValue", "").
    load("src/main/resources/data/stocks.csv")

  //Parquet is the default storing format(stores in binary. compressed in snappy) used by dataframes
  carsDF.write.
    mode(SaveMode.Overwrite).
    save("src/main/resources/data/cars.parquet")

  //text files
  spark.read.text("src/main/resources/data/sampletextFile.txt").show()

  //from DB
  val employeesDF = spark.read.format("jdbc").
    option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /* exercise
    - movies.json to
      -tab separated csv file
      -postgresql table public.movies
      -snappy parquet
   */
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  moviesDF.write.
    mode(SaveMode.Overwrite).
    option("header", "true").
    option("sep", "\t")
    .csv("src/main/resources/data/moviesTab.csv")

  moviesDF.write.save("src/main/resources/data/movies.parquet")

  moviesDF.write.mode(SaveMode.Overwrite).format("jdbc").
    option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees").save()

}
