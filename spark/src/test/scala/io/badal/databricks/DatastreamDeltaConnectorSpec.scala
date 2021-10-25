package io.badal.databricks

import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import io.badal.databricks.config.{DatastreamConf, DatastreamDeltaConf, DeltalakeConf}
import io.badal.databricks.config.SchemaEvolutionStrategy.Merge
import io.badal.databricks.datastream.LocalDirectory
import io.badal.databricks.jobs.DatastreamDeltaConnector
import io.badal.databricks.utils.DirTestUtils.{createTempDir, deleteRecursively}
import io.badal.databricks.utils.{MergeIntoSuiteBase, TestOps}
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.scalatest.{BeforeAndAfterEach, Ignore}

import java.io.File

/* do not remove */
import eu.timepit.refined.pureconfig._
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._

class DatastreamDeltaConnectorSpec
  extends MergeIntoSuiteBase
  with BeforeAndAfterEach
  with DeltaSQLCommandTest {

  val tablesPath = """
    |/Users/stevendeutscher/git/datastream-deltalake-connector/spark/src/test/resources/tables
    |""".stripMargin.replaceAll("\n", "")

  val datastream = DatastreamConf(
    name = NonEmptyString.unsafeFrom("datastream-delta-connector-test"),
    startDate = Option(NonEmptyString.unsafeFrom("1970-01-01T00:00:00.00Z")),
    fileReadConcurrency = PosInt.unsafeFrom(2),
    tableSource = LocalDirectory(tablesPath),
    readFormat = NonEmptyString.unsafeFrom("json")
  )

  val deltalake = DeltalakeConf(
    tableNamePrefix = "",
    mergeFrequencyMinutes = PosInt.unsafeFrom(1),
    Merge
  )

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

  test("run") {
    withTable("test_accounts", "test_customers") {
      withSQLConf(
        ("spark.sql.streaming.schemaInference", "true"),
        ("spark.databricks.delta.schema.autoMerge.enabled", "true")
      ) {
        val jobConf =
          DatastreamDeltaConf(datastream, deltalake, false, s"$tempPath/checkpoint")
        DatastreamDeltaConnector.run(spark, jobConf)

        checkAnswer(
          TestOps.readDeltaTableByName("test_customers").select("id", "name"),
            Row("1", "Foo1") ::
            Nil
        )
      }
    }
  }
}
