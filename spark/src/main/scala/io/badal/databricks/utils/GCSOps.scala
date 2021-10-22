package io.badal.databricks.utils

import com.google.cloud.storage.StorageOptions

import scala.collection.JavaConverters._

object GCSOps {

  def list(bucketName: String, path: String): Set[String] = {
    val storage = StorageOptions.getDefaultInstance.getService()
    val bucket = storage.get(bucketName)

    val targets = bucket
      .list()
      .getValues
      .asScala
      .collect {
        case blob if blob.getName.startsWith(path) =>
          val trimmed =
            if (path.isEmpty) blob.getName
            else blob.getName.substring(path.length + 1)

          trimmed.split("/").headOption.getOrElse("")
      }
      .filter(_.nonEmpty)
      .toSet

    targets
  }

}
