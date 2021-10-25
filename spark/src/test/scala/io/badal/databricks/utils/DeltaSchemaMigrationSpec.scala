package io.badal.databricks.utils

import io.badal.databricks.config.SchemaEvolutionStrategy.Merge
import io.badal.databricks.utils.queries.{MergeQueries, TableMetadata}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.scalatest.BeforeAndAfterEach

class DeltaSchemaMigrationSpec
    extends MergeIntoSuiteBase
    with BeforeAndAfterEach
    with DeltaSQLCommandTest {

  private val testTable: String = "inventory_voters"

  test("target schema") {
    withTable(testTable) {
      withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {

        val sourceDf = readJsonRecords("/events/records1.json")

        val tableMetadata = TableMetadata.fromDf(sourceDf)
        DeltaSchemaMigration.updateSchemaByName(testTable, tableMetadata, Merge)

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

        assert(readDeltaTableByName(testTable).schema == targetSchema)
      }
    }
  }
}
