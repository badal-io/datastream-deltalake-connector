package org.apache.spark.sql.delta.test

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}

// Copied from Delta
/**
  * Because `TestSparkSession` doesn't pick up the conf `spark.sql.extensions` in Spark 2.4.x, we use
  * this class to inject Delta's extension in our tests.
  *
  * @see https://issues.apache.org/jira/browse/SPARK-25003
  */
class DeltaTestSparkSession(sparkConf: SparkConf) extends TestSparkSession(sparkConf) {
  override val extensions: SparkSessionExtensions = {
    val extensions = new SparkSessionExtensions
    new DeltaSparkSessionExtension().apply(extensions)
    extensions
  }
}

/**
  * A trait for tests that are testing a fully set up SparkSession with all of Delta's requirements,
  * such as the configuration of the DeltaCatalog and the addition of all Delta extensions.
  */
trait DeltaSQLCommandTest { self: SharedSparkSession =>

  override protected def createSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new DeltaTestSparkSession(sparkConf)
    session.conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
    session
  }
}
