package io.badal.databricks.utils

import java.io.File

import io.badal.databricks.utils.DirTestUtils.{createTempDir, deleteRecursively}
import io.badal.databricks.utils.TestUtils.getClass
import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.scalatest.BeforeAndAfterEach

abstract class MergeIntoSuiteBase
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterEach {
  // with SQLTestUtils
  //with DeltaTestUtilsForTempViews {

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

  protected def tempPath: String = tempDir.getCanonicalPath

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
    dfw.save(tempPath)
  }

  protected def readJsonRecords(path: String)(
      implicit spark: SparkSession): DataFrame =
    spark.read
      .option("multiline", "true")
      .json(getClass.getResource(path).toString)

}
