package io.badal.databricks.utils

import java.io.File

object FileOps {

  def list(path: String): Set[String] = {
    new File(path)
      .listFiles()
      .collect {
        case f if f.isDirectory =>
          f.getName
      }
      .toSet
  }
}
