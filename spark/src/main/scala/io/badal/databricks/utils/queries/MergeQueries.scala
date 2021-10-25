package io.badal.databricks.utils.queries

import io.badal.databricks.config.SchemaEvolutionStrategy
import io.badal.databricks.utils.DeltaSchemaMigration.DatastreamMetadataField
import io.badal.databricks.utils.{DeltaSchemaMigration, TableNameFormatter}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number}

object MergeQueries {

  private val TargetTableAlias = "t"
  private val SrcTableAlias = "s"
  private val SrcPayloadTableAlias = "s.payload"

  val log = Logger.getLogger(getClass.getName)

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

    val latestChangeForEachKey: DataFrame = getLatestRow(microBatchOutputDF)

    /** First update the schema of the target table*/
    val targetTable =
      DeltaSchemaMigration.updateSchemaByName(targetTableName,
                                              tableMetadata,
                                              schemaEvolutionStrategy)

    val updateExp = buildUpdateExp(tableMetadata, SrcTableAlias)

    val timestampCompareExp =
      buildTimestampCompareSql(tableMetadata, TargetTableAlias, SrcTableAlias)

    val isDeleteExp = "s.source_metadata.change_type = 'DELETE'"
    val isNotDeleteExp = "s.source_metadata.change_type != 'DELETE'"

    log.info(
      s""" run MERGE with
         | timestampCompareExp: ${timestampCompareExp}
         | updateExp: ${updateExp}
         | isNotDeleteExp: ${isNotDeleteExp}
         | """.stripMargin
    )

    targetTable
      .as(TargetTableAlias)
      .merge(
        latestChangeForEachKey.as(SrcTableAlias),
        buildJoinConditions(tableMetadata,
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
        .orderBy(tableMetadata.orderByFields.map(f => desc(f._1)): _*)

    df.withColumn("row_num", row_number.over(window))
      .where("row_num == 1")
      .drop("row_num")
  }

  /** Check if target is older than source using Datastream row metadata*/
  private[queries] def buildTimestampCompareSql(tableMetadata: TableMetadata,
                                                targetTable: String,
                                                sourceTable: String) = {
    val orderingColumn = tableMetadata.orderByFields.head
    f"$targetTable.${DeltaSchemaMigration.datastreamMetadataTargetFieldName(
      orderingColumn._1)} <= $sourceTable.${orderingColumn._1}"
  }

  private[queries] def buildJoinConditions(tableMetadata: TableMetadata,
                                           targetTableAlias: String,
                                           sourceTableAlias: String) =
    tableMetadata.payloadPrimaryKeyFields
      .map(col => f"$targetTableAlias.$col = $sourceTableAlias.$col")
      .mkString(" AND ")

  private[queries] def buildUpdateExp(
      tableMetadata: TableMetadata,
      sourceTableAlias: String): Map[String, String] = {
    val payloadFields: Map[String, String] = tableMetadata.payloadFields
      .map(field => (s"$field" -> s"$sourceTableAlias.payload.$field"))
      .toMap
    val metadataFields: Map[String, String] =
      tableMetadata.orderByFields
        .map(
          field =>
            (s"${DeltaSchemaMigration.datastreamMetadataTargetFieldName(
              field._1)}" -> s"$sourceTableAlias.${field._1}"))
        .toMap
    payloadFields ++ metadataFields
  }

  // TODO: Move this logic elsewhere
  private def payloadField(field: String) =
    s"payload.${field}"

}
