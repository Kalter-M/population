package ru.dsr.bigdata

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, Row}
import ru.dsr.bigdata.Constants._
import ru.dsr.bigdata.Main.spark

object Job {

  import spark.implicits._
  val maxYearWindow: WindowSpec = Window.partitionBy('country, 'city, 'city_type).orderBy('year.desc)
  val orderTotalValueWindow: WindowSpec = Window.partitionBy('country).orderBy('total_value.desc)

  def getPopulation(both: Dataset[Row]): Dataset[Row] = {
    both
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
  }

  def getCountMillionCities(both: Dataset[Row]): Dataset[Row] = {
    both
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
  }

  def getTop5Cities(both: Dataset[Row]): Dataset[Row] = {
    both
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
  }

  def getRatioPopulation(fm: Dataset[Row]): Dataset[Row] = {
    fm
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
  }
}
