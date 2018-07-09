package ru.dsr.bigdata

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Main extends App{
  private val conf = ConfigFactory.load.getConfig("app")

  implicit val parameters: Parameters = Parameters.getInstanceOf(conf)

  private val sparkConf = new SparkConf().setMaster(parameters.spark_master)
  implicit val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .config("spark.mongodb.output.uri", parameters.output_uri)
    .getOrCreate()

  var fm: Dataset[Row] = _
  var both: Dataset[Row] = _
  parameters.load_from match {
    case "path" =>
      fm = DataLoad.loadFromPath(parameters.fm_path)
      both = DataLoad.loadFromPath(parameters.both_path)
    case "url" =>
      fm = DataLoad.loadFromUrl(parameters.fm_url)
      both = DataLoad.loadFromUrl(parameters.both_url)
    case _ =>
      println("Wrong config load_from!")
  }

  parameters.save_to match {
    case "mongodb" =>
      Tools.saveToMongoDB(Job.getPopulation(both), "population")
      Tools.saveToMongoDB(Job.getCountMillionCities(both), "countMillionCities")
      Tools.saveToMongoDB(Job.getTop5Cities(both), "top5Cities")
      Tools.saveToMongoDB(Job.getRatioPopulation(fm), "ratioPopulation")
    case "csv" =>
      Tools.saveToCsv(Job.getPopulation(both), "population.csv")
      Tools.saveToCsv(Job.getCountMillionCities(both), "countMillionCities.csv")
      Tools.saveToCsv(Job.getTop5Cities(both), "top5Cities.csv")
      Tools.saveToCsv(Job.getRatioPopulation(fm), "ratioPopulation.csv")
    case _ =>
      println("Wrong config save_to!")
  }

}
