package io.badal.databricks.delta

import io.badal.databricks.utils.MergeIntoSuiteBase
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.types.{StringType, StructField, StructType}
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
