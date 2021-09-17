package io.badal.databricks.conf

import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import io.badal.databricks.config.Config.{DatastreamConf, DatastreamJobConf, DeltalakeConf, TableConf}
import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

/* do not remove */
import eu.timepit.refined.pureconfig._
import pureconfig._
import pureconfig.generic.auto._

class DatastreamJobConfigSpec extends AnyFlatSpec with Matchers {

  val datastream = DatastreamConf(
    name = NonEmptyString.unsafeFrom("test-name"),
    bucket = NonEmptyString.unsafeFrom("test-bucket"),
    startDate = Option(NonEmptyString.unsafeFrom("1970-01-01T00:00:00.00Z")),
    fileReadConcurrency = PosInt.unsafeFrom(2)
  )

  val tables = Seq(
    TableConf(
      name = NonEmptyString.unsafeFrom("test-table"),
      primaryKey = NonEmptyString.unsafeFrom("test-key"),
      timestamp = NonEmptyString.unsafeFrom("test-timestamp"))
  )

  val deltalake = DeltalakeConf(
    tableNamePrefix = "test-prefix",
    mergeFrequencyMinutes = PosInt.unsafeFrom(1)
  )

  val validConf = DatastreamJobConf(datastream, tables, deltalake)

  "reading a DatastreamJobConf" should "return a DatastreamJobConf for a valid typesafe configuration" in {
    val res = ConfigSource.resources("test.conf").load[DatastreamJobConf]
    res.right.value should be(validConf)
  }
}
