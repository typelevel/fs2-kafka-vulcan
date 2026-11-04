val avroVersion = "1.12.2"
val catsEffectVersion = "3.7.1"
val catsVersion = "2.13.0"
val confluentVersion = "8.4.0"
val disciplineVersion = "2.3.0"
val fs2KafkaVersion = "4.1.3"
val logbackVersion = "1.6.6"
val munitVersion = "1.3.6"
val scala212Version = "2.12.21"
val scala213Version = "2.13.18"
val scala3Version = "3.3.9"
val slf4jNopVersion = "2.0.20"
val vulcanVersion = "1.14.1"

inThisBuild(
  Seq(
    crossScalaVersions := Seq(scala212Version, scala213Version, scala3Version),
    developers := List(
      tlGitHubDev("aartigao", "Alan Artigao"),
      tlGitHubDev("bplommer", "Ben Plommer"),
      tlGitHubDev("vlovgr", "Viktor Rudebeck")
    ),
    githubWorkflowBuild += WorkflowStep.Sbt(
      List("docs/mdoc"),
      name = Some("Generate readme"),
      cond = Some("matrix.scala == '3'")
    ),
    githubWorkflowJavaVersions := Seq(JavaSpec.temurin("21")),
    mergifyLabelPaths := Map.empty,
    mergifyStewardConfig ~= (_.map(_.withAuthor("typelevel-steward[bot]"))),
    organization := "org.typelevel",
    resolvers += "confluent".at("https://packages.confluent.io/maven/"),
    scalaVersion := scala3Version,
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    startYear := Some(2018),
    tlBaseVersion := "4.2",
    mimaBinaryIssueFilters ++= {
      import com.typesafe.tools.mima.core.*
      // scalafmt: { maxColumn = 200 }
      Seq()
      // scalafmt: {}
    }
  )
)

lazy val root = tlCrossRootProject
  .aggregate(`fs2-kafka-vulcan`, `fs2-kafka-vulcan-testkit-munit`)

lazy val `fs2-kafka-vulcan` = project
  .in(file("modules/fs2-kafka-vulcan"))
  .settings(
    name := "fs2-kafka-vulcan",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "vulcan" % vulcanVersion,
      "io.confluent" % "kafka-avro-serializer" % confluentVersion,
      "io.confluent" % "kafka-schema-registry-client" % confluentVersion,
      "io.confluent" % "kafka-schema-serializer" % confluentVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect-kernel" % catsEffectVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "org.typelevel" %% "fs2-kafka" % fs2KafkaVersion
    ),
    headerSettings,
    testSettings
  )

lazy val `fs2-kafka-vulcan-testkit-munit` = project
  .in(file("modules/fs2-kafka-vulcan-testkit-munit"))
  .dependsOn(`fs2-kafka-vulcan`)
  .settings(
    name := "fs2-kafka-vulcan-testkit-munit",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "vulcan" % vulcanVersion,
      "io.confluent" % "kafka-schema-registry-client" % confluentVersion,
      "org.apache.avro" % "avro" % avroVersion,
      "org.scalameta" %% "munit-diff" % munitVersion,
      "org.scalameta" %% "munit" % munitVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion
    ),
    headerSettings,
    testSettings,
    tlVersionIntroduced := List("2.12", "2.13", "3").map(_ -> "2.2.0").toMap
  )

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(MdocPlugin, NoPublishPlugin)
  .dependsOn(`fs2-kafka-vulcan`, `fs2-kafka-vulcan-testkit-munit`)
  .settings(
    crossScalaVersions := Seq(scala3Version),
    libraryDependencies += "org.slf4j" % "slf4j-nop" % slf4jNopVersion % Runtime,
    mdocIn := baseDirectory.value / "readme.md",
    mdocOut := (LocalRootProject / baseDirectory).value / "readme.md",
    mdocVariables ++= Map(
      "CONFLUENT_VERSION" -> confluentVersion,
      "FS2_KAFKA_VERSION" -> fs2KafkaVersion,
      "MUNIT_VERSION" -> munitVersion,
      "ORGANIZATION" -> (ThisBuild / organization).value,
      "VERSION" -> tlLatestVersion.value.getOrElse(version.value),
      "VULCAN_CROSS_SCALA_VERSIONS" -> crossScalaVersionsOf(`fs2-kafka-vulcan`).value,
      "VULCAN_MODULE_NAME" -> (`fs2-kafka-vulcan` / moduleName).value,
      "VULCAN_TESTKIT_MUNIT_CROSS_SCALA_VERSIONS" -> crossScalaVersionsOf(
        `fs2-kafka-vulcan-testkit-munit`
      ).value,
      "VULCAN_TESTKIT_MUNIT_MODULE_NAME" -> (`fs2-kafka-vulcan-testkit-munit` / moduleName).value,
      "VULCAN_VERSION" -> vulcanVersion
    )
  )

lazy val headerSettings = Seq(
  headerLicense := Some(
    HeaderLicense.ALv2(
      startYear.value.get.toString,
      "OVO Energy Limited",
      HeaderLicenseStyle.SpdxSyntax
    )
  )
)

lazy val testSettings = Seq(
  libraryDependencies ++= Seq(
    "org.typelevel" %% "discipline-scalatest" % disciplineVersion,
    "org.typelevel" %% "cats-effect-laws" % catsEffectVersion,
    "org.typelevel" %% "cats-effect-testkit" % catsEffectVersion,
    "ch.qos.logback" % "logback-classic" % logbackVersion
  ).map(_ % Test),
  Test / fork := true,
  Test / logBuffered := false,
  Test / parallelExecution := false,
  Test / testOptions += Tests.Argument("-oDF")
)

def scalaVersionOf(version: String): String = {
  if (version.contains("-")) version
  else {
    val (major, minor) =
      CrossVersion.partialVersion(version).get
    s"$major.$minor"
  }
}

def crossScalaVersionsOf(project: Project): Def.Initialize[String] =
  Def.setting {
    val scalaVersions = (project / crossScalaVersions).value.map(scalaVersionOf)
    if (scalaVersions.size <= 2) scalaVersions.mkString(" and ")
    else scalaVersions.init.mkString(", ") ++ " and " ++ scalaVersions.last
  }
