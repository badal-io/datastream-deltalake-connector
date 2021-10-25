package io.badal.databricks.jobs

import io.badal.databricks.config.DatastreamDeltaConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource

/* do not remove */
import eu.timepit.refined.pureconfig._
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._

object DatastreamDeltaStreamingJob {

  val logger = Logger.getLogger(DatastreamDeltaStreamingJob.getClass)

  def main(args: Array[String]): Unit = {

    val jobConf: DatastreamDeltaConf =
      ConfigSource.resources("demo.conf").loadOrThrow[DatastreamDeltaConf]

    // TODO
    Logger.getRootLogger.setLevel(Level.ERROR)

    /** Create a spark session */
    val spark = SparkSession.builder
      .appName("DatastreamReader")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    DatastreamDeltaConnector.run(spark, jobConf)
  }

}
