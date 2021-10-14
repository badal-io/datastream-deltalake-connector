package io.badal.databricks.utils

import java.io.{File, IOException}
import java.net.URL
import java.util.{Locale, UUID}

import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.test.{
  SQLTestUtils,
  SharedSparkSession,
  TestSparkSession
}
import org.apache.spark.sql.{
  AnalysisException,
  DataFrame,
  QueryTest,
  Row,
  SparkSession
}
import DirTestUtils._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.internal.SQLConf

import scala.io.Source
class MergeQueriesSpec
    extends MergeIntoSuiteBase
    with BeforeAndAfterEach
    with DeltaSQLCommandTest {
  import testImplicits._

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
  test("basic scala API") {
    withTable("source") {

      append(Seq((1, 10), (2, 20)).toDF("key1", "value1"), Nil) // target
      val source = Seq((1, 100), (3, 30)).toDF("key2", "value2") // source

      io.delta.tables.DeltaTable
        .forPath(spark, tempPath)
        .merge(source, "key1 = key2")
        .whenMatched()
        .updateExpr(Map("key1" -> "key2", "value1" -> "value2"))
        .whenNotMatched()
        .insertExpr(Map("key1" -> "key2", "value1" -> "value2"))
        .execute()

      checkAnswer(readDeltaTable(tempPath),
                  Row(1, 100) :: // Update
                    Row(2, 20) :: // No change
                    Row(3, 30) :: // Insert
                    Nil)
    }
  }

  test("upsert to an empty table") {
    val mergeSettings = MergeSettings(
      targetTableName = "target",
      primaryKeyFields = Seq("id"),
      orderByFields = Seq("source_timestamp")
    )
    withTable("target") {
      withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {

        val sourceDf = spark.read
          .option("multiline", "true")
          .json(getClass.getResource("/events/records1.json").toString)

        val emptyDF =
          spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
                                DataStreamSchema.payloadSchema(sourceDf))

        emptyDF.write.format("delta").saveAsTable("target")

        DeltaTable.forName("target")

        MergeQueries(mergeSettings).upsertToDelta(sourceDf, 1)

        checkAnswer(readDeltaTable(tempPath),
                    Row(1, 100) :: // Update
                      Row(2, 20) :: // No change
                      Row(3, 30) :: // Insert
                      Nil)
      }
    }
  }

  protected def readDeltaTable(path: String): DataFrame = {
    spark.read.format("delta").load(path)
  }

  protected def append(df: DataFrame, partitions: Seq[String] = Nil): Unit = {
    val dfw = df.write.format("delta").mode("append")
    if (partitions.nonEmpty) {
      dfw.partitionBy(partitions: _*)
    }
    dfw.save(tempPath)
  }

  /** A simple representative of a any WHEN clause in a MERGE statement */
  protected case class MergeClause(isMatched: Boolean,
                                   condition: String,
                                   action: String = null) {
    def sql: String = {
      assert(action != null, "action not specified yet")
      val matched = if (isMatched) "MATCHED" else "NOT MATCHED"
      val cond = if (condition != null) s"AND $condition" else ""
      s"WHEN $matched $cond THEN $action"
    }
  }
  protected def executeMerge(target: String,
                             source: String,
                             condition: String,
                             update: String,
                             insert: String): Unit = {

    executeMerge(
      tgt = target,
      src = source,
      cond = condition,
      MergeClause(isMatched = true,
                  condition = null,
                  action = s"UPDATE SET $update"),
      MergeClause(isMatched = false,
                  condition = null,
                  action = s"INSERT $insert")
    )
  }

  protected def executeMerge(tgt: String,
                             src: String,
                             cond: String,
                             clauses: MergeClause*): Unit = {

    def parseTableAndAlias(
        tableNameWithAlias: String): (String, Option[String]) = {
      tableNameWithAlias.split(" ").toList match {
        case tableName :: Nil =>
          // 'MERGE INTO tableName' OR `MERGE INTO delta.`path`'
          tableName -> None
        case tableName :: alias :: Nil =>
          // 'MERGE INTO tableName alias' or 'MERGE INTO delta.`path` alias'
          tableName -> Some(alias)
        case list
            if list.size >= 3 && list(list.size - 2)
              .toLowerCase(Locale.ROOT) == "as" =>
          // 'MERGE INTO ... AS alias'
          list.dropRight(2).mkString(" ").trim() -> Some(list.last)
        case list if list.size >= 2 =>
          // 'MERGE INTO ... alias'
          list.dropRight(1).mkString(" ").trim() -> Some(list.last)
        case _ =>
          fail(
            s"Could not build parse '$tableNameWithAlias' for table and optional alias")
      }
    }
  }

}
