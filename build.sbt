import sbt.Keys.libraryDependencies

lazy val commonSettings = Seq(
  organization := "io.badal",
  organizationName := "badal.io",
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "org.apache.spark" %% "spark-sql" % Versions.Spark % Provided,
    "org.apache.spark" %% "spark-streaming" % Versions.Spark % Provided,
    "com.google.http-client" % "google-http-client" % "1.40.0",
    "io.delta" %% "delta-core" % Versions.Delta,
  ),
)

lazy val root = Project(id = "datastream-deltalake-connector", base = file("."))
  .aggregate(spark)

lazy val spark = Project(id = "spark", base = file("spark"))
  .settings(commonSettings)
  .settings(testSettings)
  .settings(scalafmtSettings)
  .settings(assemblySettings)
  .enablePlugins(AssemblyPlugin)
  .settings(name := "spark")

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

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := s"${name.value.toLowerCase}-assembly.jar",
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  test in assembly := {},
)
