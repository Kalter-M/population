package ru.dsr.bigdata.system

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.dsr.bigdata.system.Constants._


object Main extends App{
  private val sparkConf = new SparkConf().setMaster("local")
  implicit val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/")
    .getOrCreate()

  import spark.implicits._

  val fm = Tools.loadFromCsv(PATH_POPULATION_FM)
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
    .filter(('value isNotNull) and ('sex isNotNull) and ('city isNotNull) and ('area isNotNull) and ('source_year isNotNull))

  val both = Tools.loadFromCsv(PATH_POPULATION_BOTH)
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
    .filter(('value isNotNull) and ('sex isNotNull) and ('city isNotNull) and ('area isNotNull) and ('source_year isNotNull))


  val maxYearWindow = Window.partitionBy('country, 'city, 'city_type).orderBy('year.desc)
  val orderTotalValueWindow = Window.partitionBy('country).orderBy('total_value.desc)

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
    .filter('population isNotNull)
    .orderBy('country)


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
    .filter('total_value isNotNull)
    .filter('total_value >= 1000000)
    .groupBy(
      'country
    )
    .agg(count('city).as("count"))
    .orderBy('country)

  val top5Cities = both
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
    .agg(sum('value).cast(IntegerType).as("total_value"))
    .filter('total_value isNotNull)
    .select(
      'country,
      'city,
      'total_value,
      row_number().over(orderTotalValueWindow).as("rn")
    )
    .filter('rn <= 5)
    .select(
      'country,
      'city,
      'total_value
    )
    .orderBy('country, 'city, 'total_value.desc)

  val ratioPopulation = fm
    .select(
      'country,
      'city,
      'city_type,
      'sex,
      'value,
      dense_rank().over(maxYearWindow).as("dr")
    )
    .filter('dr === 1)
    .groupBy(
      'country,
      'sex
    )
    .agg(
      sum('value).cast(IntegerType).as("population")
    )
    .filter('population isNotNull)
    .select(
      'country,
      when('sex === MALE, 'population).otherwise(0).as("male"),
      when('sex === FEMALE, 'population).otherwise(0).as("female")
    )
    .groupBy(
      'country
    )
    .agg(
      sum('male).as("male"),
      sum('female).as("female")
    )
    .select(
      'country,
      round('male / ('male + 'female), 2).as("male"),
      round('female / ('male + 'female), 2).as("female")
    )
    .orderBy('country)

  val optionsPopulation = Map("host" -> "localhost:27017", "database" -> "population", "collection" -> "population")
  val optionsCountMillionCities = Map("host" -> "localhost:27017", "database" -> "population", "collection" -> "countMillionCities")
  val optionsTop5Cities = Map("host" -> "localhost:27017", "database" -> "population", "collection" -> "top5Cities")
  val optionsRatioPopulation = Map("host" -> "localhost:27017", "database" -> "population", "collection" -> "ratioPopulation")

  population
    .write
    .format("com.mongodb.spark.sql.DefaultSource")
    .mode(SaveMode.Overwrite)
    .options(optionsPopulation)
    .save()
  countMillionCities
    .write
    .format("com.mongodb.spark.sql.DefaultSource")
    .mode(SaveMode.Overwrite)
    .options(optionsCountMillionCities)
    .save()
  top5Cities
    .write
    .format("com.mongodb.spark.sql.DefaultSource")
    .mode(SaveMode.Overwrite)
    .options(optionsTop5Cities)
    .save()
  ratioPopulation
    .write
    .format("com.mongodb.spark.sql.DefaultSource")
    .mode(SaveMode.Overwrite)
    .options(optionsRatioPopulation)
    .save()

}
