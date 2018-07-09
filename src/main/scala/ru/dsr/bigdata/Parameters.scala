package ru.dsr.bigdata

import com.typesafe.config.Config


object Parameters {
  def getInstanceOf(conf: Config) = new Parameters(conf)
}
class Parameters(conf: Config) {
  val spark_master: String = conf.getString("spark.master")

  val output_uri: String = conf.getString("mongodb.output_uri")
  val host: String = conf.getString("mongodb.host")
  val database: String = conf.getString("mongodb.database")
  val options: Map[String, String] = Map("host" -> host, "database" -> database)

  val load_from: String = conf.getString("data.load_from")
  val save_to: String = conf.getString("data.save_to")

  val fm_url: String = conf.getString("data.fm_url")
  val both_url: String = conf.getString("data.both_url")
  val fm_path: String = conf.getString("data.fm_path")
  val both_path: String = conf.getString("data.both_path")


}