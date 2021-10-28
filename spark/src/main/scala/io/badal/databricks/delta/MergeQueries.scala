package io.badal.databricks.delta

import eu.timepit.refined.types.string.NonEmptyString
import io.badal.databricks.config.SchemaEvolutionStrategy
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
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
                    schemaEvolutionStrategy: SchemaEvolutionStrategy,
                    path: NonEmptyString): Unit = {
    implicit val ss = microBatchOutputDF.sparkSession

    TableMetadata.fromDf(microBatchOutputDF) match {
      case Some(tableMeta) =>
        upsertToDelta(microBatchOutputDF,
                      schemaEvolutionStrategy,
                      path,
                      tableMeta)
      case None =>
        log.debug(s"empty batch for path $path - skipping")
    }
  }

  def upsertToDelta(
      microBatchOutputDF: DataFrame,
      schemaEvolutionStrategy: SchemaEvolutionStrategy,
      path: NonEmptyString,
      tableMetadata: TableMetadata)(implicit ss: SparkSession): Unit = {

    val latestChangeForEachKey: DataFrame =
      getLatestRow(microBatchOutputDF, tableMetadata)

    /** First update the schema of the target table */
    val targetTable = DeltaSchemaMigration.createOrUpdateSchema(
      path,
      tableMetadata,
      schemaEvolutionStrategy,
      ss
    )

    val updateExp = buildUpdateExp(tableMetadata, SrcTableAlias)

    val timestampCompareExp =
      buildTimestampCompareSql(tableMetadata, TargetTableAlias, SrcTableAlias)

    //    val isDeleteExp = "s.source_metadata.change_type = 'DELETE'"
    //    val isNotDeleteExp = "s.source_metadata.change_type != 'DELETE'"

    val isDeleteExp = "s.source_metadata.is_deleted = true"
    val isNotDeleteExp = "s.source_metadata.is_deleted = false"

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

  private def getLatestRow(df: DataFrame,
                           tableMetadata: TableMetadata): DataFrame = {
    val pKeys = tableMetadata.payloadPrimaryKeyFields.map(payloadField)

    // TODO: handle deleted field
    val window =
      Window
        .partitionBy(pKeys.head, pKeys.drop(1): _*)
        .orderBy(tableMetadata.orderByFields.map(f => desc(f.fieldName)): _*)

    df.withColumn("row_num", row_number.over(window))
      .where("row_num == 1")
      .drop("row_num")
  }

  /** Check if target is older than source using Datastream row metadata */
  private[delta] def buildTimestampCompareSql(tableMetadata: TableMetadata,
                                              targetTable: String,
                                              sourceTable: String) = {
    val orderingColumn = tableMetadata.orderByFields.head
    f"$targetTable.${orderingColumn.toTargetFieldName} <= $sourceTable.${orderingColumn.fieldName}"
  }

  private[delta] def buildJoinConditions(tableMetadata: TableMetadata,
                                         targetTableAlias: String,
                                         sourceTableAlias: String) =
    tableMetadata.payloadPrimaryKeyFields
      .map(col => f"$targetTableAlias.$col = $sourceTableAlias.$col")
      .mkString(" AND ")

  private[delta] def buildUpdateExp(
      tableMetadata: TableMetadata,
      sourceTableAlias: String): Map[String, String] = {
    val payloadFields: Map[String, String] = tableMetadata.payloadFields
      .map(field => field -> s"$sourceTableAlias.payload.$field")
      .toMap
    val metadataFields: Map[String, String] = tableMetadata.orderByFields
      .map(field =>
        field.toTargetFieldName -> s"$sourceTableAlias.${field.fieldName}")
      .toMap

    payloadFields ++ metadataFields
  }

  // TODO: Move this logic elsewhere
  private def payloadField(field: String) =
    s"payload.$field"

}
