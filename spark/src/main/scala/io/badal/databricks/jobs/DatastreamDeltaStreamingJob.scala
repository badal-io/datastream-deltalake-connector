package io.badal.databricks.jobs

import io.badal.databricks.config.DatastreamDeltaConf
import org.apache.log4j.Logger
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
      ConfigSource
        .resources("application.conf")
        .loadOrThrow[DatastreamDeltaConf]

    /** Create a spark session */
    val spark = SparkSession.builder
      .appName(jobConf.datastream.name.value)
      .config("spark.sql.streaming.schemaInference", "true")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
              "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .master("local[1]")
      .getOrCreate()

    DatastreamDeltaConnector.run(spark, jobConf)

    spark.streams.awaitAnyTermination()
  }

}
