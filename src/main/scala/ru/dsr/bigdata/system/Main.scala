package ru.dsr.bigdata.system

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ru.dsr.bigdata.system.Constants._

object Main extends App{
  private val sparkConf = new SparkConf().setMaster("local")
  implicit val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  import spark.implicits._

  val fm = Tools.loadFromCsv(PATH_POPULATION_FM).filter($"Value" isNotNull)

  val both = Tools.loadFromCsv(PATH_POPULATION_BOTH)
    .filter($"Value" isNotNull)
    .select(
      $"Country or Area".as("country"),
      $"Year".as("year"),
      $"Area".as("area"),
      $"Sex".as("sex"),
      $"City".as("city"),
      $"City type".as("city_type"),
      $"Record Type".as("record_type"),
      $"Reliability".as("reliability"),
      $"Source Year".as("source_year"),
      $"Value".as("value"),
      $"Value Footnotes".as("footnotes")
    )


  val maxYearWindow = Window.partitionBy('country, 'city, 'city_type).orderBy('year.desc)

  val population = both
    .select(
      'country,
      'city,
      'city_type,
      'value,
      dense_rank().over(maxYearWindow).as("dr")
    )
    .filter('dr === 1)
    .groupBy(
      'country
    )
    .agg(
      sum('value).cast(IntegerType).as("population")
    )


  val countMillionCities = both
    .select(
      'country,
      'city,
      'city_type,
      'value,
      dense_rank().over(maxYearWindow).as("dr")
    )
    .filter('dr === 1)
    .groupBy(
      'country,
      'city
    )
    .agg(sum('value).as("total_value"))
    .filter('total_value >= 1000000)
    .groupBy(
      'country
    )
    .agg(count('city).as("count"))


}
