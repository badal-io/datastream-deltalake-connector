import sbt.Keys.libraryDependencies

lazy val commonSettings = Seq(
  organization := "io.badal",
  organizationName := "badal.io",
  version := "0.1",

  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "org.apache.spark" %% "spark-sql" % Versions.Spark,
    //  "org.apache.spark" %% "spark-hive" % "1.5.0",
    //  "org.apache.spark" %% "spark-hive-thriftserver" % "2.4.1",
    "org.apache.spark" %% "spark-streaming" % Versions.Spark,
    "org.apache.spark" %% "spark-sql" % Versions.Spark,
    "org.apache.spark" %% "spark-streaming" % Versions.Spark,
    "com.google.http-client" % "google-http-client" % "1.39.2",
    "io.delta" %% "delta-core" % Versions.Delta,
  )
)

lazy val root = Project("datastream-deltalake-connector", file("spark"))
  .settings(commonSettings)
  .settings(testSettings)
  .settings(scalafmtSettings)

lazy val testSettings = Seq(
  fork in Test := true,
)

lazy val scalafmtSettings = Seq(scalafmtOnCompile := true)

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
