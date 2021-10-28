package io.badal.databricks.delta

import io.badal.databricks.config.SchemaEvolutionStrategy.{Merge, Overwrite}
import io.badal.databricks.utils.MergeIntoSuiteBase
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterEach

class DeltaSchemaMigrationSpec
    extends MergeIntoSuiteBase
    with BeforeAndAfterEach
    with DeltaSQLCommandTest {

  private val testTable = DatastreamDeltaTable("demo", "inventory.voters")
  private val testTableName = testTable.fullTargetTableName

  test("create initial target schema") {
    withTable(testTableName) {
      DeltaSchemaMigration.createDBIfNotExist(testTable, tempDir.getPath)

      val sourceDf = readJsonRecords("/events/records1.json")

      val tableMetadata = TableMetadata.fromDfUnsafe(sourceDf)

      DeltaSchemaMigration.createOrUpdateSchema(tempPath,
                                                tableMetadata,
                                                Merge,
                                                spark)
      MergeQueries.upsertToDelta(sourceDf, Merge, tempPath)

      val targetSchema = new StructType(
        Array(
          StructField("address", StringType, true),
          StructField("gender", StringType, true),
          StructField("id", StringType, true),
          StructField("name", StringType, true),
          // TODO: nullable should actually be false - need to create the table firsts
          StructField("datastream_metadata_source_timestamp",
                      TimestampType,
                      true),
          StructField("datastream_metadata_source_metadata_log_file",
                      StringType,
                      true),
          StructField("datastream_metadata_source_metadata_log_position",
                      LongType,
                      true)
        ))

      assert(readDeltaTableByName(testTableName).schema == targetSchema)
    }
  }
  test("add a field") {
    withTable(testTableName) {
      DeltaSchemaMigration.createDBIfNotExist(testTable, tempDir.getPath)

      val sourceDf = readJsonRecords("/events/records1.json")

      val tableMetadata = TableMetadata.fromDfUnsafe(sourceDf)

      DeltaSchemaMigration.createOrUpdateSchema(tempPath,
                                                tableMetadata,
                                                Merge,
                                                spark)

      MergeQueries.upsertToDelta(sourceDf, Merge, tempPath)

      val tableMetadataNew = tableMetadata.copy(
        payloadSchema =
          tableMetadata.payloadSchema.add("newField1", LongType, false)
      )

      DeltaSchemaMigration.createOrUpdateSchema(tempPath,
                                                tableMetadataNew,
                                                Merge,
                                                spark)

      val targetSchema = new StructType(
        Array(
          StructField("address", StringType, true),
          StructField("gender", StringType, true),
          StructField("id", StringType, true),
          StructField("name", StringType, true),
          // TODO: nullable should actually be false - need to create the table firsts
          StructField("datastream_metadata_source_timestamp",
                      TimestampType,
                      true),
          StructField("datastream_metadata_source_metadata_log_file",
                      StringType,
                      true),
          StructField("datastream_metadata_source_metadata_log_position",
                      LongType,
                      true),
          StructField("newField1", LongType, true)

          /** new fields are added as nullable */
        ))

      assert(readDeltaTableByName(testTableName).schema == targetSchema)
    }
  }
  test("remove a column") {
    withTable(testTableName) {
      DeltaSchemaMigration.createDBIfNotExist(testTable, tempDir.getPath)

      val sourceDf = readJsonRecords("/events/records1.json")

      val tableMetadata = TableMetadata.fromDfUnsafe(sourceDf)

      val tableMetadataWithExtraColumn = tableMetadata.copy(
        payloadSchema =
          tableMetadata.payloadSchema.add("testColumn", LongType, false)
      )

      DeltaSchemaMigration.createOrUpdateSchema(tempPath,
                                                tableMetadataWithExtraColumn,
                                                Merge,
                                                spark)

      MergeQueries.upsertToDelta(sourceDf, Merge, tempPath)

      val targetSchema = new StructType(
        Array(
          StructField("address", StringType, true),
          StructField("gender", StringType, true),
          StructField("id", StringType, true),
          StructField("name", StringType, true),
          StructField("testColumn", LongType, true), // Became nullable
          // TODO: nullable should actually be false - need to create the table firsts
          StructField("datastream_metadata_source_timestamp",
                      TimestampType,
                      true),
          StructField("datastream_metadata_source_metadata_log_file",
                      StringType,
                      true),
          StructField("datastream_metadata_source_metadata_log_position",
                      LongType,
                      true)
        ))

      assert(readDeltaTableByName(testTableName).schema == targetSchema)
    }
  }

}
