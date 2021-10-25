package io.badal.databricks.utils

import java.io.File

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.scalatest.BeforeAndAfterEach

abstract class MergeIntoSuiteBase
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterEach {

  import testImplicits._
  protected var tempDir: File = _

  protected def tempPath: String = tempDir.getCanonicalPath

}
