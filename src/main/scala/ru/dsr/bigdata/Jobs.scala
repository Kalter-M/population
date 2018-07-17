package ru.dsr.bigdata

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import ru.dsr.bigdata.Constants._
import ru.dsr.bigdata.Launcher.spark
import ru.dsr.bigdata.loader.Loader
import ru.dsr.bigdata.saver.Saver
import ru.dsr.bigdata.DataLoad.parseAlias

import scala.language.postfixOps

object Jobs {

  private var fm: Dataset[Row] = _
  private var both: Dataset[Row] = _

  import spark.implicits._
  val maxYearWindow: WindowSpec = Window.partitionBy('country, 'city, 'city_type).orderBy('year.desc)
  val orderTotalValueWindow: WindowSpec = Window.partitionBy('country).orderBy('total_value.desc)
  val yearRangeWindow: WindowSpec = Window.partitionBy('country, 'city, 'city_type, 'sex).orderBy('year)

  def getPopulation: () => Dataset[Row] = () => {
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

  def getCountMillionCities: () => Dataset[Row] = () => {
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

  def getTop5Cities: () => Dataset[Row] = () => {
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

  def getRatioPopulation: () => Dataset[Row] = () => {
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

  def getTop5BestDynamics: (Int, Int) => DataFrame = (from: Int, to: Int) => {
    fm
      .select(
        'country,
        'city,
        'city_type,
        'sex,
        'value,
        'year
      ).filter(
      'year between(from, to)
    )
      .filter('value isNotNull)
      .select(
        'country,
        'city,
        'city_type,
        'sex,
        first('value).over(yearRangeWindow).as("from_value"),
        last('value).over(yearRangeWindow).as("to_value")
      )
      .groupBy(
        'country,
        'sex
      )
      .agg(
        sum('from_value).cast(IntegerType).as("from_value"),
        sum('to_value.cast(IntegerType)).as("to_value")
      )
      .select(
        'country,
        when('sex === MALE, 'from_value)
          .otherwise(0).as("from_male"),
        when('sex === FEMALE, 'from_value)
          .otherwise(0).as("from_female"),
        when('sex === MALE, 'to_value)
          .otherwise(0).as("to_male"),
        when('sex === FEMALE, 'to_value)
          .otherwise(0).as("to_female")
      )
      .groupBy('country)
      .agg(
        sum('from_male).as("from_male"),
        sum('from_female).as("from_female"),
        sum('to_male).as("to_male"),
        sum('to_female).as("to_female")
      )
      .select(
        'country,
        'from_male,
        'from_female,
        'to_male,
        'to_female,
        ('from_male + 'from_female).as("from"),
        ('to_male + 'to_female).as("to")
      )
      .select(
        'country,
        ('to / 'from).as("dynamics"),
        ('to_female / 'from_female).as("dynamics_female"),
        ('to_male / 'from_male).as("dynamics_male")
      )
      .select(
        'country,
        'dynamics,
        'dynamics_female,
        'dynamics_male,
        row_number().over(Window.orderBy('dynamics.desc)).as("rn")
      )
      .filter(
        'rn <= 5 and
          'dynamics >= 1
      )
      .select(
        'country,
        round(('dynamics - 1)*100, 2).as("percents_both"),
        round(('dynamics_male - 1)*100, 2).as("percents_male"),
        round(('dynamics_female - 1)*100, 2).as("percents_female")
      )
  }

  def getTop5WorstDynamics: (Int, Int) => DataFrame = (from: Int, to: Int) => {
    fm
      .select(
        'country,
        'city,
        'city_type,
        'sex,
        'value,
        'year
      ).filter(
      'year between(from, to)
    )
      .filter('value isNotNull)
      .select(
        'country,
        'city,
        'city_type,
        'sex,
        first('value).over(yearRangeWindow).as("from_value"),
        last('value).over(yearRangeWindow).as("to_value")
      )
      .groupBy(
        'country,
        'sex
      )
      .agg(
        sum('from_value).cast(IntegerType).as("from_value"),
        sum('to_value.cast(IntegerType)).as("to_value")
      )
      .select(
        'country,
        when('sex === MALE, 'from_value)
          .otherwise(0).as("from_male"),
        when('sex === FEMALE, 'from_value)
          .otherwise(0).as("from_female"),
        when('sex === MALE, 'to_value)
          .otherwise(0).as("to_male"),
        when('sex === FEMALE, 'to_value)
          .otherwise(0).as("to_female")
      )
      .groupBy('country)
      .agg(
        sum('from_male).as("from_male"),
        sum('from_female).as("from_female"),
        sum('to_male).as("to_male"),
        sum('to_female).as("to_female")
      )
      .select(
        'country,
        'from_male,
        'from_female,
        'to_male,
        'to_female,
        ('from_male + 'from_female).as("from"),
        ('to_male + 'to_female).as("to")
      )
      .select(
        'country,
        ('to / 'from).as("dynamics"),
        ('to_female / 'from_female).as("dynamics_female"),
        ('to_male / 'from_male).as("dynamics_male")
      )
      .select(
        'country,
        'dynamics,
        'dynamics_female,
        'dynamics_male,
        row_number().over(Window.orderBy('dynamics)).as("rn")
      )
      .filter(
        'rn <= 5 and
          'dynamics <= 1
      )
      .select(
        'country,
        round(('dynamics - 1)*100, 2).as("percents_both"),
        round(('dynamics_male - 1)*100, 2).as("percents_male"),
        round(('dynamics_female - 1)*100, 2).as("percents_female")
      )
  }

  implicit class Runner(val job: () => Dataset[Row]) {
    def run(loadStrategy: Loader, saveStrategy: Saver, name: String): Unit = {
      fm = parseAlias(loadStrategy.load(loadStrategy.fm))
      both = parseAlias(loadStrategy.load(loadStrategy.both))

      saveStrategy.save(job.apply, name)
    }
  }

  implicit class RunnerWithPeriod(val job: (Int, Int) => Dataset[Row]) {
    def run(loadStrategy: Loader, saveStrategy: Saver, name: String, from: Int, to: Int): Unit = {
      fm = parseAlias(loadStrategy.load(loadStrategy.fm))
      both = parseAlias(loadStrategy.load(loadStrategy.both))

      saveStrategy.save(job.apply(from, to), name)
    }
  }
}
