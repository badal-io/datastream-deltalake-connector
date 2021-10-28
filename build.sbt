import sbt.Keys.libraryDependencies

lazy val library = new {

  val enumeratum = "com.beachape" %% "enumeratum" % Versions.Enumeratum
  val refined = "eu.timepit" %% "refined" % Versions.Refined
  val refinedPureConfig = "eu.timepit" %% "refined-pureconfig" % Versions.Refined
  val sparkSql = "org.apache.spark" %% "spark-sql" % Versions.Spark % Provided
  val sparkCore = "org.apache.spark" %% "spark-core" % Versions.Spark % Provided
  val sparkStreaming = "org.apache.spark" %% "spark-streaming" % Versions.Spark % Provided
  val pureConfig = "com.github.pureconfig" %% "pureconfig" % Versions.PureConfig
  val pureConfigEnumeratum = "com.github.pureconfig" %% "pureconfig-enumeratum" % Versions.PureConfig
  val googleCloudStorage = "com.google.cloud" % "google-cloud-storage" % Versions.GoogleCloudStorgage
  val typesafeConfig = "com.typesafe" % "config" % Versions.Typesafe
  val deltaCore = "io.delta" %% "delta-core" % Versions.Delta
  val scalaTest = "org.scalatest" %% "scalatest" % Versions.ScalaTest % "test"
  val sparkSqlTest = "org.apache.spark" %% "spark-sql" % Versions.Spark % Test classifier "tests"
  val sparkCatalystTest = "org.apache.spark" %% "spark-catalyst" % Versions.Spark % Test classifier "tests"
  val sparkCoreTest = "org.apache.spark" %% "spark-core" % Versions.Spark % Test classifier "tests"
  val sparkHiveTest = "org.apache.spark" %% "spark-hive" % Versions.Spark % Test classifier "tests"

  val commonDeps = Seq(
    enumeratum,
    refined,
    refinedPureConfig,
    sparkSql,
    sparkCore,
    sparkStreaming,
    pureConfig,
    pureConfigEnumeratum,
    googleCloudStorage,
    typesafeConfig,
    deltaCore,
  )

  val testDeps = Seq(
    scalaTest,
    sparkSqlTest,
    sparkCatalystTest,
    sparkCoreTest,
    sparkHiveTest,
  )
}


lazy val commonSettings = Seq(
  organization := "io.badal",
  organizationName := "badal.io",
  libraryDependencies ++= library.commonDeps ++ library.testDeps,
  resolvers ++= Seq(
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  ),
)

lazy val root = Project(id = "datastream-deltalake-connector", base = file("."))
  .aggregate(spark)

lazy val spark = Project(id = "spark", base = file("spark"))
  .settings(commonSettings)
  .settings(testSettings)
  .settings(scalafmtSettings)
  .settings(assemblySettings)
  .settings(scalaStyleSettings)
  .enablePlugins(AssemblyPlugin)
  .settings(name := "datastream-deltalake-spark-connector",
    scalaSource in Test := baseDirectory.value / "src/test/scala",
    scalaSource in Compile := baseDirectory.value / "src/main/scala"
  )

lazy val testSettings = Seq(
  fork in Test := true,
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

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := s"${name.value.toLowerCase}-assembly-${version.value}.jar",
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  test in assembly := {},
  assemblyShadeRules in assembly := Seq(
    ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inAll,
    ShadeRule.rename("com.google.common.**" -> "shadegoogle.@1").inAll,
  ),
  assemblyMergeStrategy in assembly := {
    case "module-info.class" => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)

lazy val scalafmtSettings = Seq(scalafmtOnCompile := true)

/*
 ***********************
 * ScalaStyle settings *
 ***********************
 */
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val scalaStyleSettings = Seq(
  compileScalastyle := (Compile / scalastyle).toTask("").value,

  Compile / compile := ((Compile / compile) dependsOn compileScalastyle).value,
)

