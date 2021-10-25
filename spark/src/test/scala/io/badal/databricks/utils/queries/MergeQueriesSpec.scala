package io.badal.databricks.utils.queries

import java.util.Locale

import io.badal.databricks.config.SchemaEvolutionStrategy.Merge
import io.badal.databricks.utils.DirTestUtils.{createTempDir, deleteRecursively}
import io.badal.databricks.utils.{
  DataStreamSchema,
  MergeIntoSuiteBase,
  TestUtils
}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.types.{
  LongType,
  StructField,
  StructType,
  TimestampType
}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterEach
class MergeQueriesSpec
    extends MergeIntoSuiteBase
    with BeforeAndAfterEach
    with DeltaSQLCommandTest {
  import testImplicits._

  private val testTable: String = "inventory_voters"

  test("insert to an empty table") {
    withTable(testTable) {
      withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {

        val sourceDf = readJsonRecords("/events/records1.json")

        MergeQueries.upsertToDelta(sourceDf, 1, Merge)

        checkAnswer(
          readDeltaTableByName(testTable).select("id", "name"),
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
        MergeQueries.upsertToDelta(sourceDf1, 1, Merge)

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
        val source2Df = readJsonRecords("/events/records1_updated.json")

        MergeQueries.upsertToDelta(source1Df, 1, Merge)

        MergeQueries.upsertToDelta(source2Df, 1, Merge)

        checkAnswer(
          readDeltaTableByName(testTable).select("id", "name"),
          Row("161401245", "Sabrina Ellis") ::
            Row("290819604", "Christopher Bates") ::
            Row("862224591", "Nathan Lowe") ::
            Row("915725144", "Brianna Smith") ::
            Row("993488433", "Allison Smith") ::
            Nil
        )
      }
    }
  }
//  /** A simple representative of a any WHEN clause in a MERGE statement */
//  protected case class MergeClause(isMatched: Boolean,
//                                   condition: String,
//                                   action: String = null) {
//    def sql: String = {
//      assert(action != null, "action not specified yet")
//      val matched = if (isMatched) "MATCHED" else "NOT MATCHED"
//      val cond = if (condition != null) s"AND $condition" else ""
//      s"WHEN $matched $cond THEN $action"
//    }
//  }
//  protected def executeMerge(target: String,
//                             source: String,
//                             condition: String,
//                             update: String,
//                             insert: String): Unit = {
//
//    executeMerge(
//      tgt = target,
//      src = source,
//      cond = condition,
//      MergeClause(isMatched = true,
//                  condition = null,
//                  action = s"UPDATE SET $update"),
//      MergeClause(isMatched = false,
//                  condition = null,
//                  action = s"INSERT $insert")
//    )
//  }
//
//  protected def executeMerge(tgt: String,
//                             src: String,
//                             cond: String,
//                             clauses: MergeClause*): Unit = {
//
//    def parseTableAndAlias(
//        tableNameWithAlias: String): (String, Option[String]) = {
//      tableNameWithAlias.split(" ").toList match {
//        case tableName :: Nil =>
//          // 'MERGE INTO tableName' OR `MERGE INTO delta.`path`'
//          tableName -> None
//        case tableName :: alias :: Nil =>
//          // 'MERGE INTO tableName alias' or 'MERGE INTO delta.`path` alias'
//          tableName -> Some(alias)
//        case list
//            if list.size >= 3 && list(list.size - 2)
//              .toLowerCase(Locale.ROOT) == "as" =>
//          // 'MERGE INTO ... AS alias'
//          list.dropRight(2).mkString(" ").trim() -> Some(list.last)
//        case list if list.size >= 2 =>
//          // 'MERGE INTO ... alias'
//          list.dropRight(1).mkString(" ").trim() -> Some(list.last)
//        case _ =>
//          fail(
//            s"Could not build parse '$tableNameWithAlias' for table and optional alias")
//      }
//    }
//  }

}
