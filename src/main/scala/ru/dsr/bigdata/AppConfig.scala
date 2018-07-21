package ru.dsr.bigdata

import com.typesafe.config.ConfigFactory

object AppConfig {
  private val conf = ConfigFactory.load.getConfig("app")
  private val data = conf.getConfig("data")
  private val spark = conf.getConfig("spark")
  private val mongodb = conf.getConfig("mongodb")

  val load_from: String = data.getString("load_from")
  val save_to: String = data.getString("save_to")

  val fm_url: String = data.getString("fm_url")
  val both_url: String = data.getString("both_url")
  val fm_path: String = data.getString("fm_path")
  val both_path: String = data.getString("both_path")
  val output_path: String = data.getString("output_path")

  val spark_master: String = spark.getString("master")

  val output_uri: String = mongodb.getString("output_uri")
  val options: Map[String, String] = Map(
    "host" -> mongodb.getString("host"),
    "database" -> mongodb.getString("database")
  )

  val job: String =  data.getString("job")
  val period_start: String =  data.getString("period_start")
  val period_end: String =   data.getString("period_end")
}
