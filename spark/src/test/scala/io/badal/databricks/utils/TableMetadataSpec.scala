package io.badal.databricks.utils

import io.badal.databricks.utils.queries.TableMetadata
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.scalatest.BeforeAndAfterEach

class TableMetadataSpec
    extends MergeIntoSuiteBase
    with BeforeAndAfterEach
    with DeltaSQLCommandTest {

  private val testTable: String = "inventory_voters"

  test("sample schema ") {

    val sourceDf = readJsonRecords("/events/records1.json")

    val tableMetadata = TableMetadata.fromDf(sourceDf)

    assert(
      tableMetadata.payloadSchema == new StructType(
        Array(StructField("address", StringType, true),
              StructField("gender", StringType, true),
              StructField("id", StringType, true),
              StructField("name", StringType, true))))
  }

}
