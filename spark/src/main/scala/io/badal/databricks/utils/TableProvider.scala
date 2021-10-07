package io.badal.databricks.utils

import org.apache.spark.sql.SparkSession

trait TableProvider {
  def getTablesFromSource(inputBucket: String): Seq[String]

  def getAllTablesToMerge(inputBucket: String, tables: Option[Seq[String]])(
      implicit ss: SparkSession): Seq[MergeSettings] = {
    val tableList = tables match {
      case Some(tables) => tables
      case None         => getTablesFromSource(inputBucket)
    }
    tableList.map { table =>
      MergeSettings(
        targetTableName = table,
        primaryKeyFields = Seq.empty, //TODO
        orderByFields = Seq.empty,
        //idColName = "id",
        //tsColName = DataStreamSchema.SourceTimestampField,
        spark = ss
      )
    }
  }

  private def avroFilePaths(inputBucket: String,
                            tables: Option[Seq[String]]): String = {
    // TODO: demo_inventory.voters/*/*/*/*/*
    tables match {
      case None => s"gs://$inputBucket"
      case _    => throw new Exception("specifying tables is not supported yet")
      //    case Some(tables) => tables.map(table => s"gs://inputBucket/$table")
    }
  }

}
