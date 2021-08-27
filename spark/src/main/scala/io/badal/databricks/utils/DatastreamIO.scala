package io.badal.databricks.utils

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Helper class to read Datastream files and return them as a DataFrame
 * TODO:
 *  1) Add support for JSON
 *  2) Autodiscover tables
 *  3) Turn it into a proper Receiver
 *  4) Add support for maxFileAge
 */
object DatastreamIO {
  def apply(spark: SparkSession, inputBucket: String, fileReadConcurrency: Int, tables: Option[Seq[String]] = None ): DataFrame ={
    spark.readStream
      .format("avro")
      .option("ignoreExtension", true)
      .option("maxFilesPerTrigger", fileReadConcurrency)
      .load(avroFilePaths(inputBucket, tables))
  }

  private def avroFilePaths(inputBucket: String, tables: Option[Seq[String]]): String= {
    // TODO: demo_inventory.voters/*/*/*/*/*
    tables match {
      case None => s"gs://$inputBucket"
      case _    => throw new Exception("specifying tables is not supported yet")
//    case Some(tables) => tables.map(table => s"gs://inputBucket/$table")
    }
  }
}
