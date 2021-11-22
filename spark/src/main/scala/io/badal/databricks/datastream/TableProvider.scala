package io.badal.databricks.datastream

import eu.timepit.refined.types.string.NonEmptyString
import io.badal.databricks.google.{GCSClientImpl, GCSOps}
import io.badal.databricks.utils.FileOps

sealed trait TableProvider {
  def list(): Seq[DatastreamTable]

}
final case class DiscoveryBucket(bucket: NonEmptyString,
                                 path: Option[NonEmptyString])
    extends TableProvider {
  override def list(): Seq[DatastreamTable] = {
    GCSOps
      .list(GCSClientImpl, bucket.value, path.map(_.value).getOrElse(""))
      .toSeq
      .map { table =>
        DatastreamTable(
          s"gs://${bucket.value}${path.map("/" + _.value).getOrElse("")}",
          table)
      }
  }
}

final case class LocalDirectory(path: String) extends TableProvider {
  override def list(): Seq[DatastreamTable] =
    FileOps.list(path).toSeq.map(dir => DatastreamTable(path, dir))
}
