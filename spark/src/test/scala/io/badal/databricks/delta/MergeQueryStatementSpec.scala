package io.badal.databricks.delta

import io.badal.databricks.datastream.MySQL
import org.apache.spark.sql.types.{
  LongType,
  StructField,
  StructType,
  TimestampType
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MergeQueryStatementSpec extends AnyFlatSpec with Matchers {
  private val tableMetadata = TableMetadata(
    sourceType = MySQL,
    table = "testT",
    database = "testD",
    payloadPrimaryKeyFields = Seq("id"),
    orderByFields = TableMetadata.MYSQL_ORDER_BY_FIELDS,
    payloadSchema = new StructType(
      Array(
        StructField("field1", TimestampType, false),
        StructField("field2", LongType, false)
      )),
    payloadFields = Seq("field1", "field2")
  )

  "timestamp compare sql" should "user datastream_metadata field in source" in {
    val res = MergeQueries.buildTimestampCompareSql(tableMetadata, "t", "s")
    res should equal(
      "t.datastream_metadata_source_timestamp <= s.source_timestamp")
  }
  "join sql" should "work" in {
    val res = MergeQueries.buildJoinConditions(tableMetadata, "t", "s.payload")
    res should equal("t.id = s.payload.id")
  }
  "update statement" should "work" in {
    val res = MergeQueries.buildUpdateExp(tableMetadata, "t")
    res should equal(
      Map(
        "field1" -> "t.payload.field1",
        "datastream_metadata_source_metadata_log_file" -> "t.source_metadata.log_file",
        "datastream_metadata_source_timestamp" -> "t.source_timestamp",
        "field2" -> "t.payload.field2",
        "datastream_metadata_source_metadata_log_position" -> "t.source_metadata.log_position"
      )
    )
  }

}
