import Versions._

name := "datastream-databricks-connector"
scalaVersion := "2.12.10"

fork in Test := true

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark" %% "spark-sql" % Versions.Spark,
  "org.apache.spark" %% "spark-hive" % "1.5.0",
  "org.apache.spark" %% "spark-hive-thriftserver" % "2.4.1",
  "org.apache.spark" %% "spark-streaming" % Versions.Spark,
  "org.apache.spark" %% "spark-sql" % Versions.Spark,
  "org.apache.spark" %% "spark-streaming" % Versions.Spark,
  "com.google.http-client" % "google-http-client" % "1.40.0",
  "io.delta" %% "delta-core" % Versions.Delta,
)

lazy val commonScalacOptions = Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-unchecked",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-Xlint:_,-missing-interpolator",
  "-Xfatal-warnings",
  "-Xmacro-settings:materialize-derivations",
  "-Yno-adapted-args",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)
