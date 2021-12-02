package io.badal.databricks.google

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalamock.scalatest.MockFactory

class GCSOpsSpec extends AnyFlatSpec with Matchers with MockFactory {

  val client = mock[GCSClient]
  val bucket = "a"
  val path = "b"

  // todo
  "list" should "return all folders above the target bucket + path" in {}

}
