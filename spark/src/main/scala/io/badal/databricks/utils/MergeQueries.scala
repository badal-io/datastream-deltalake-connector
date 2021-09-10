package io.badal.databricks.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number}

case class MergeSettings(targetTableName: String, idColName: String, tsColName: String, spark: SparkSession)
case class MergeQueries(settings: MergeSettings) {
  import settings._
  implicit val ss = spark

  /**
   * Upsert a batch of updates to a Delta Table
   *
   * "USING ({stagingViewSql}) AS {stagingAlias} ",
   * "ON {joinCondition} ",
   * "WHEN MATCHED AND {timestampCompareSql} AND {stagingAlias}.{deleteColumn}=True THEN DELETE ",// TODO entire block should be configurably removed
   * "WHEN MATCHED AND {timestampCompareSql} THEN {mergeUpdateSql} ",
   * "WHEN NOT MATCHED BY TARGET AND {stagingAlias}.{deleteColumn}!=True ",
   * "THEN {mergeInsertSql}")
   *
   * @param microBatchOutputDF
   * @param batchId
   */
  def upsertToDelta( microBatchOutputDF: DataFrame, batchId: Long): Unit = {

    val payloadFields: Array[String] = DataStreamSchema.payloadFields(microBatchOutputDF)

    val latestChangeForEachKey: DataFrame = getLatestRow(microBatchOutputDF)

    /** First update the schema of the target table*/
    val targetTable= DeltaSchemaMigration.updateSchema(targetTableName,microBatchOutputDF.schema)

    val updateExp = toFieldMap(payloadFields, "t", "s.payload")
      targetTable.as("t")
        .merge(
          latestChangeForEachKey.as("s"), s"t.${idColName} = s.payload.${idColName}"
        )
        .whenMatched("s.source_metadata.change_type = 'DELETE'").delete()
        .whenMatched().updateExpr(updateExp)
        .whenNotMatched("s.source_metadata.change_type != 'DELETE'").insertExpr(updateExp)
        .execute()
  }

  private def getLatestRow(df: DataFrame): DataFrame =
    df
      .withColumn("row_num", row_number.over(window)).where("row_num == 1")
      .drop("row_num" )

  private lazy val window =
    Window.partitionBy(s"payload.${idColName}").orderBy(desc(tsColName))


  private def toFieldMap(fields: Seq[String], targetTable: String, srcTable: String): Map[String, String] =
    fields.map(f => (f"${targetTable}.$f" -> f"${srcTable}.$f")).toMap
}
