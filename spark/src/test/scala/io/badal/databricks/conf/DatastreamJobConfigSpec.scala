package io.badal.databricks.conf

import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import io.badal.databricks.config.SchemaEvolutionStrategy.Merge
import io.badal.databricks.config.{
  DatastreamConf,
  DatastreamJobConf,
  DeltalakeConf,
  SchemaEvolutionStrategy
}
import io.badal.databricks.datastream.GCSDiscoveryBucket
import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

/* do not remove */
import eu.timepit.refined.pureconfig._
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._

class DatastreamJobConfigSpec extends AnyFlatSpec with Matchers {

  val datastream = DatastreamConf(
    name = NonEmptyString.unsafeFrom("test-name"),
    startDate = Option(NonEmptyString.unsafeFrom("1970-01-01T00:00:00.00Z")),
    fileReadConcurrency = PosInt.unsafeFrom(2),
    tableSource = GCSDiscoveryBucket(
      NonEmptyString.unsafeFrom("test-discovery-bucket"),
      Option(NonEmptyString.unsafeFrom("path/to/test/dir"))
    ),
  )

  val deltalake = DeltalakeConf(
    tableNamePrefix = "test-prefix",
    mergeFrequencyMinutes = PosInt.unsafeFrom(1),
    Merge //SchemaEvolution.Merge
  )

  val validConf =
    DatastreamJobConf(datastream, deltalake, true, "checkpoint")

  "reading a DatastreamJobConf" should "return a DatastreamJobConf for a valid typesafe configuration" in {
    val res = ConfigSource.resources("test.conf").load[DatastreamJobConf]
    res.right.value should be(validConf)
  }
}
