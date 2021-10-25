package io.badal.databricks.datastream

import eu.timepit.refined.types.string.NonEmptyString
import io.badal.databricks.google.GCSOps
import io.badal.databricks.utils.FileOps

sealed trait TableProvider {
  def list(): Seq[DatastreamTable]

}
final case class DiscoveryBucket(bucket: NonEmptyString,
                                 path: Option[NonEmptyString])
    extends TableProvider {
  override def list(): Seq[DatastreamTable] = {
    val pathOrEmpty = path.map("/" + _.value).getOrElse("")
    GCSOps
      .list(bucket.value, pathOrEmpty)
      .toSeq
      .map(table => DatastreamTable(s"gs://${bucket.value}$pathOrEmpty", table))
  }
}

final case class LocalDirectory(path: String) extends TableProvider {
  override def list(): Seq[DatastreamTable] =
    FileOps.list(path).toSeq.map(dir => DatastreamTable(path, dir))
}
