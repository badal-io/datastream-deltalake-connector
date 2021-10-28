package io.badal.databricks.delta

import io.badal.databricks.config.SchemaEvolutionStrategy.Merge
import io.badal.databricks.utils.MergeIntoSuiteBase
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql._
import org.scalatest.BeforeAndAfterEach
class MergeQueriesSpec
    extends MergeIntoSuiteBase
    with BeforeAndAfterEach
    with DeltaSQLCommandTest {
  import testImplicits._
  import io.badal.databricks.utils.DataFrameOps._

  private val testTable: String = "inventory_voters"

  test("insert to an empty table") {
    withTable(testTable) {
      withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {

        val sourceDf = readJsonRecords("/events/records1.json")

        MergeQueries.upsertToDelta(sourceDf, Merge, tempPath)

        spark.sql("show tables").show()

        checkAnswer(
          readDeltaTableByName(s"default.$testTable").select("id", "name"),
          Row("161401245", "Sabrina Ellis") ::
            Row("290819604", "Christopher Bates") ::
            Row("862224591", "Nathan Lowe") ::
            Row("915725144", "Brianna Tucker") ::
            Row("993488433", "Allison Dalton") ::
            Nil
        )
      }
    }
  }

  test("insert records to an existing table") {
    withTable(testTable) {
      withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {

        val sourceDf1 = readJsonRecords("/events/records1.json")

        // Populate table
        MergeQueries.upsertToDelta(sourceDf1, Merge, tempPath)

        checkAnswer(
          readDeltaTableByName(testTable)
            .select("id", "name"),
          Row("161401245", "Sabrina Ellis") ::
            Row("290819604", "Christopher Bates") ::
            Row("862224591", "Nathan Lowe") ::
            Row("915725144", "Brianna Tucker") ::
            Row("993488433", "Allison Dalton") ::
            Nil
        )
      }
    }
  }
  test("update records") {
    withTable(testTable) {
      withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {

        val source1Df = readJsonRecords("/events/records1.json")
        val source2Df = source1Df
          .incrementTs("993488433", 2)
          .changeNameTo("993488433", "Allison Smith")
          .incrementTs("915725144", -2)
          .changeNameTo("915725144", "Brianna Smith")

        MergeQueries.upsertToDelta(source1Df, Merge, tempPath)

        MergeQueries.upsertToDelta(source2Df, Merge, tempPath)

        checkAnswer(
          readDeltaTableByName(testTable).select("id", "name"),
          Row("161401245", "Sabrina Ellis") ::
            Row("290819604", "Christopher Bates") ::
            Row("862224591", "Nathan Lowe") ::
            // not changed since timestamp is the same as target
            Row("915725144", "Brianna Tucker") ::
            // changed
            Row("993488433", "Allison Smith") ::
            Nil
        )
      }
    }
  }
  test("delete records") {
    withTable(testTable) {
      withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {

        val source1Df = readJsonRecords("/events/records1.json")
        val source2Df = source1Df
          .markDeleted("993488433")

        MergeQueries.upsertToDelta(source1Df, Merge, tempPath)

        MergeQueries.upsertToDelta(source2Df, Merge, tempPath)

        checkAnswer(
          readDeltaTableByName(testTable).select("id", "name"),
          Row("161401245", "Sabrina Ellis") ::
            Row("290819604", "Christopher Bates") ::
            Row("862224591", "Nathan Lowe") ::
            // not changed since timestamp is the same as target
            Row("915725144", "Brianna Tucker") ::
            // Deleted
            //  Row("993488433", "Allison Smith") ::
            Nil
        )
      }
    }
  }
}
