package io.badal.databricks.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, TimestampType}

object DataFrameOps {
  implicit class DataFrameOps(val df: DataFrame) extends AnyVal {
    import org.apache.spark.sql.functions._
    def incrementTs(id: String, mins: Integer): DataFrame = {
      df.withColumn(
        "source_timestamp",
        when(col("payload.id") === id,
             col("source_timestamp")
               .cast(TimestampType) + expr(s"INTERVAL ${mins} MINUTES"))
          .otherwise(col("source_timestamp"))
      )
    }

    def changeNameTo(id: String, name: String): DataFrame = {
      val structCols = df
        .select("payload.*")
        .columns
        .filter(_ != "name")
        .map(name => col("payload." + name))

      df.withColumn(
        "payload",
        struct(
          (structCols :+ when(col("payload.id") === id, lit(name))
            .otherwise(col("payload.name"))
            .as("name")): _*)
      )
    }
  }
}
