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
      replaceNestedColumnById(id, "payload", "name", name)
    }
    def markDeleted(id: String): DataFrame = {
      replaceNestedColumnById(id, "source_metadata", "is_deleted", true)
    }
    private def replaceNestedColumnById[T](id: String,
                                           rootField: String,
                                           field: String,
                                           v: T): DataFrame = {
      val structCols = df
        .select(s"$rootField.*")
        .columns
        .filter(_ != field)
        .map(name => col(s"$rootField." + name))

      df.withColumn(
        rootField,
        struct(
          (structCols :+ when(col("payload.id") === id, lit(v))
            .otherwise(col(s"$rootField.$field"))
            .as(field)): _*)
      )
    }

  }
}
