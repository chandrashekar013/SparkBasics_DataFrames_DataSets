package part2df.dfbasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {

  val spark = SparkSession.builder().appName("Joins").config("spark.master", "local").
    getOrCreate()

  val guitarsDF = spark.read.json("src/main/resources/data/guitars.json")
  val guitarPlayersDF = spark.read.json("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.json("src/main/resources/data/bands.json")

  //inner join
  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")
  val guitaristBandDF = guitarPlayersDF.join(bandsDF, joinCondition, "inner")
  guitaristBandDF.show()

  //similar thing can be done for left outer join,
  val guitaristbandOuterDF = guitarPlayersDF.join(bandsDF, joinCondition, "left_outer")

  //having 2 columns with same name would throw error when we do show. hence, need to make unique
  //To make unique

  val bandRenameDF = bandsDF.withColumnRenamed("id", "bandId")
  guitarPlayersDF.join(bandRenameDF, guitarPlayersDF.col("id") === bandRenameDF.col("bandId"))

  //using complex types
  guitarPlayersDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))


}
