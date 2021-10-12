package io.badal.databricks.utils

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.network.util.JavaUtils

// copied from package org.apache.spark.util
object DirTestUtils {

  /**
    * Create a directory given the abstract pathname
    * @return true, if the directory is successfully created; otherwise, return false.
    */
  def createDirectory(dir: File): Boolean = {
    try {
      // This sporadically fails - not sure why ... !dir.exists() && !dir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      dir.mkdirs()
      if (!dir.exists() || !dir.isDirectory) {
        // logError(s"Failed to create directory " + dir)
      }
      dir.isDirectory
    } catch {
      case e: Exception =>
        // logError(s"Failed to create directory " + dir, e)
        false
    }
  }
  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = 3
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException(
          "Failed to create a temp directory (under " + root + ") after " +
            maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }

    dir.getCanonicalFile
  }
  def createTempDir(root: String = System.getProperty("java.io.tmpdir"),
                    namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    // ShutdownHookManager.registerShutdownDeleteDir(dir)
    dir
  }
  def deleteRecursively(file: File): Unit = {
    if (file != null) {
      JavaUtils.deleteRecursively(file)
      //ShutdownHookManager.removeShutdownDeleteDir(file)
    }
  }
}
