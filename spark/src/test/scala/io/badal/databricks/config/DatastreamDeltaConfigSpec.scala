package io.badal.databricks.config

import eu.timepit.refined.types.all.PosLong
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.types.string.NonEmptyString
import io.badal.databricks.config.SchemaEvolutionStrategy.Merge
import io.badal.databricks.datastream.DiscoveryBucket
import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/* do not remove */
import eu.timepit.refined.pureconfig._
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._

class DatastreamDeltaConfigSpec extends AnyFlatSpec with Matchers {

  val datastream = DatastreamConf(
    name = NonEmptyString.unsafeFrom("test-name"),
    startDate = Option(NonEmptyString.unsafeFrom("1970-01-01T00:00:00.00Z")),
    fileReadConcurrency = PosInt.unsafeFrom(2),
    tableSource = DiscoveryBucket(
      NonEmptyString.unsafeFrom("test-discovery-bucket"),
      Option(NonEmptyString.unsafeFrom("path/to/test/dir"))
    ),
    readFormat = NonEmptyString.unsafeFrom("avro"),
  )

  val compaction = DeltalakeCompactionConf(
    autoCompactionEnabled = true,
    minNumberOfFiles = Option(PosInt.unsafeFrom(20)),
    maxFileSizeBytes = Option(PosLong.unsafeFrom(104857600)),
    targetFileSizeBytes = Option(PosLong.unsafeFrom(1048576))
  )

  val optimize = DeltalakeOptimizeConf(
    autoOptimizeEnabled = true,
    batchInterval = Option(PosInt.unsafeFrom(10)),
    maxFileSizeBytes = Option(PosLong.unsafeFrom(104857600))
  )

  val deltalake = DeltalakeConf(
    tableNamePrefix = "test-prefix",
    schemaEvolution = Merge,
    tablePath = NonEmptyString.unsafeFrom("delta-table-path"),
    mergeFrequency = Option(FiniteDuration(1, TimeUnit.MINUTES)),
    compaction = Option(compaction),
    optimize = Option(optimize)
  )

  val validConf =
    DatastreamDeltaConf(datastream, deltalake, true, "checkpoint")

  "reading a DatastreamJobConf" should
    "return a DatastreamJobConf for a valid typesafe configuration" in {
    val res = ConfigSource.resources("test.conf").load[DatastreamDeltaConf]
    res.right.value should be(validConf)
  }
}
