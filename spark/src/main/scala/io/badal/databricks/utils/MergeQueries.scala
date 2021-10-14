package io.badal.databricks.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number}

case class MergeSettings(targetTableName: String,
                         primaryKeyFields: Seq[String],
                         orderByFields: Seq[String])

case class MergeQueries(settings: MergeSettings) {
  import settings._

  /**
    * Upsert a batch of updates to a Delta Table
    *
    * "USING ({stagingViewSql}) AS {stagingAlias} ",
    * "ON {joinCondition} ",
    * "WHEN MATCHED AND {timestampCompareSql} AND {stagingAlias}.{deleteColumn}=True THEN DELETE ",
    * "WHEN MATCHED AND {timestampCompareSql} THEN {mergeUpdateSql} ",
    * "WHEN NOT MATCHED BY TARGET AND {stagingAlias}.{deleteColumn}!=True ",
    * "THEN {mergeInsertSql}")
    *
    * @param microBatchOutputDF
    * @param batchId
    */
  def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long): Unit = {

    implicit val ss = microBatchOutputDF.sparkSession
    val targetTableAlias = s"temp-${settings.targetTableName}"
    val srcPayloadTableAlias = "s.payload"
    val payloadFields: Array[String] =
      DataStreamSchema.payloadFields(microBatchOutputDF)

    val latestChangeForEachKey: DataFrame = getLatestRow(microBatchOutputDF)

    /** First update the schema of the target table*/
    val targetTable = DeltaSchemaMigration.updateSchema(
      targetTableName,
      DataStreamSchema.payloadSchema(microBatchOutputDF))

    val updateExp = toFieldMap(payloadFields, srcPayloadTableAlias)

    val timestampCompareExp = buildTimestampCompareSql(orderByFields.head,
                                                       targetTableAlias,
                                                       srcPayloadTableAlias)

    val isDeleteExp = "s.source_metadata.change_type = 'DELETE'"
    val isNotDeleteExp = "s.source_metadata.change_type != 'DELETE'"

    targetTable
      .as(targetTableAlias)
      .merge(
        latestChangeForEachKey.as("s"),
        buildJoinConditions(primaryKeyFields,
                            targetTableAlias,
                            srcPayloadTableAlias)
      )
      .whenMatched(f"$timestampCompareExp AND $isDeleteExp")
      .delete()
      .whenMatched(timestampCompareExp)
      .updateExpr(updateExp)
      .whenNotMatched(isNotDeleteExp)
      .insertExpr(updateExp)
      .execute()
  }

  private def getLatestRow(df: DataFrame): DataFrame = {
    val pKeys = primaryKeyFields.map(payloadField)

    // TODO: handle deleted field
    val window =
      Window
        .partitionBy(pKeys.head, pKeys.drop(1): _*)
        .orderBy(orderByFields.map(desc): _*)

    df.withColumn("row_num", row_number.over(window))
      .where("row_num == 1")
      .drop("row_num")
  }

  /**Check if target is older than source */
  private def buildTimestampCompareSql(orderingColumn: String,
                                       targetTable: String,
                                       sourceTable: String) = {
    f"$targetTable.$orderingColumn <= $sourceTable.$orderingColumn"
  }

  private def buildJoinConditions(primaryKeyFields: Seq[String],
                                  targetTable: String,
                                  sourceTable: String) =
    primaryKeyFields
      .map(col => f"$targetTable.$col = $sourceTable.$col")
      .mkString(" AND ")

  private def payloadField(field: String) =
    s"payload.${field}"
  private def toFieldMap(fields: Seq[String],
                         srcTable: String): Map[String, String] =
    fields.map(field => (s"$field" -> s"$srcTable.$field")).toMap
}
