package io.badal.databricks.utils

import io.badal.databricks.config.SchemaEvolutionStrategy
import io.badal.databricks.delta.DeltaSchemaMigration
import io.badal.databricks.delta.DeltaSchemaMigration.DatastreamMetadataField
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number}

object MergeQueries {

  private val TargetTableAlias = "t"
  private val SrcTableAlias = "s"
  private val SrcPayloadTableAlias = "s.payload"

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
  def upsertToDelta(microBatchOutputDF: DataFrame,
                    batchId: Long,
                    schemaEvolutionStrategy: SchemaEvolutionStrategy): Unit = {

    implicit val ss = microBatchOutputDF.sparkSession
    implicit val tableMetadata: TableMetadata =
      TableMetadata.fromDf(microBatchOutputDF)

    val targetTableName =
      TableNameFormatter.targetTableName(tableMetadata.table)

    val payloadFields: Array[String] =
      DataStreamSchema.payloadFields(microBatchOutputDF)

    val latestChangeForEachKey: DataFrame = getLatestRow(microBatchOutputDF)

    /** First update the schema of the target table */
    val targetTable =
      DeltaSchemaMigration.updateSchemaByName(targetTableName,
                                              tableMetadata,
                                              schemaEvolutionStrategy)

    val updateExp = toFieldMap(payloadFields, SrcPayloadTableAlias)

    val timestampCompareExp = buildTimestampCompareSql(
      tableMetadata.orderByFields.head,
      TargetTableAlias,
      SrcTableAlias)

    val isDeleteExp = "s.source_metadata.change_type = 'DELETE'"
    val isNotDeleteExp = "s.source_metadata.change_type != 'DELETE'"

    targetTable
      .as(TargetTableAlias)
      .merge(
        latestChangeForEachKey.as(SrcTableAlias),
        buildJoinConditions(tableMetadata.payloadPrimaryKeyFields,
                            TargetTableAlias,
                            SrcPayloadTableAlias)
      )
      .whenMatched(f"$timestampCompareExp AND $isDeleteExp")
      .delete()
      .whenMatched(timestampCompareExp)
      .updateExpr(updateExp)
      .whenNotMatched(isNotDeleteExp)
      .insertExpr(updateExp)
      .execute()
  }

  private def getLatestRow(df: DataFrame)(
      implicit tableMetadata: TableMetadata): DataFrame = {
    val pKeys = tableMetadata.payloadPrimaryKeyFields.map(payloadField)

    // TODO: handle deleted field
    val window =
      Window
        .partitionBy(pKeys.head, pKeys.drop(1): _*)
        .orderBy(tableMetadata.orderByFields.map(desc): _*)

    df.withColumn("row_num", row_number.over(window))
      .where("row_num == 1")
      .drop("row_num")
  }

  /** Check if target is older than source using Datastream row metadata */
  private def buildTimestampCompareSql(orderingColumn: String,
                                       targetTable: String,
                                       sourceTable: String) = {
    f"$targetTable.$DatastreamMetadataField.$orderingColumn <= $sourceTable.$orderingColumn"
  }

  private def buildJoinConditions(primaryKeyFields: Seq[String],
                                  targetTable: String,
                                  sourceTable: String) =
    primaryKeyFields
      .map(col => f"$targetTable.$col = $sourceTable.$col")
      .mkString(" AND ")

  // TODO: Move this logic elsewhere
  private def payloadField(field: String) =
    s"payload.$field"

  private def toFieldMap(fields: Seq[String],
                         srcTable: String): Map[String, String] =
    fields.map(field => (s"$field" -> s"$srcTable.$field")).toMap
}
