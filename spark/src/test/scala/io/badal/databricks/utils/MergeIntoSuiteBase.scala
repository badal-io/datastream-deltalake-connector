package io.badal.databricks.utils

import java.io.File

import eu.timepit.refined.types.string.NonEmptyString
import io.badal.databricks.utils.DirTestUtils.{createTempDir, deleteRecursively}
import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.functions._

abstract class MergeIntoSuiteBase
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterEach {

  import testImplicits._
  protected var tempDir: File = _

  override def beforeEach() {
    super.beforeEach()
    tempDir = createTempDir()
  }

  override def afterEach() {
    try {
      deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  protected def tempPath: NonEmptyString =
    NonEmptyString.unsafeFrom(tempDir.getCanonicalPath)

  protected def readDeltaTable(path: String): DataFrame = {
    spark.read.format("delta").load(path)
  }
  protected def readDeltaTableByName(table: String): DataFrame = {
    spark.table(table)
  }

  protected def append(df: DataFrame, partitions: Seq[String] = Nil): Unit = {
    val dfw = df.write.format("delta").mode("append")
    if (partitions.nonEmpty) {
      dfw.partitionBy(partitions: _*)
    }
    dfw.save(tempPath.value)
  }

  protected def readJsonRecords(path: String)(
      implicit spark: SparkSession): DataFrame = {

    spark.read
      .option("multiline", "true")
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // 2021-05-16T01:13:02.000Z
      .json(getClass.getResource(path).toString)
      .withColumn("source_timestamp", to_timestamp(col("source_timestamp")))
  }

}
