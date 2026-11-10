val catsEffectVersion = "3.7.1"
val catsVersion = "2.13.0"
val confluentVersion = "8.4.0"
val fs2KafkaVersion = "4.3.0-SNAPSHOT"
val http4sSchemaRegistryVersion = "1.0.0-M1"
val http4sVersion = "0.23.38"
val kafkaImage = "apache/kafka:4.4.0"
val karapaceImage = "ghcr.io/aiven-open/karapace:6.2.4"
val munitVersion = "1.3.6"
val scala213Version = "2.13.18"
val scala3Version = "3.3.9"
val schemaRegistryImage = "confluentinc/cp-schema-registry:8.4.0"
val scodecBitsVersion = "1.2.5"
val slf4jNopVersion = "2.0.20"
val testcontainersVersion = "2.0.5"
val vulcan1Version = "1.14.1"
val vulcanVersion = "2.0.0-M2-SNAPSHOT"
val weaverVersion = "0.13.0"

inThisBuild(
  Seq(
    crossScalaVersions := Seq(scala213Version, scala3Version),
    developers := List(tlGitHubDev("vlovgr", "Viktor Rudebeck")),
    githubWorkflowBuild += WorkflowStep.Sbt(
      List("docs/mdoc"),
      name = Some("Generate readme"),
      cond = Some("matrix.scala == '3'")
    ),
    githubWorkflowJavaVersions := Seq(JavaSpec.temurin("21")),
    mergifyLabelPaths := Map.empty,
    mergifyStewardConfig ~= (_.map(_.withAuthor("typelevel-steward[bot]"))),
    organization := "org.typelevel",
    scalaVersion := scala3Version,
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    startYear := Some(2018),
    tlBaseVersion := "5.0",
    mimaBinaryIssueFilters ++= {
      import com.typesafe.tools.mima.core.*
      // scalafmt: { maxColumn = 200 }
      Seq()
      // scalafmt: {}
    }
  )
)

lazy val root = tlCrossRootProject
  .aggregate(`fs2-kafka-vulcan`, `fs2-kafka-vulcan-testkit-munit`, tests)

lazy val `fs2-kafka-vulcan` = project
  .in(file("modules/fs2-kafka-vulcan"))
  .settings(
    name := "fs2-kafka-vulcan",
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-schema-registry" % http4sSchemaRegistryVersion,
      "org.scodec" %% "scodec-bits" % scodecBitsVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect-kernel" % catsEffectVersion,
      "org.typelevel" %% "cats-effect-std" % catsEffectVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "org.typelevel" %% "fs2-kafka" % fs2KafkaVersion,
      "org.typelevel" %% "vulcan" % vulcanVersion
    ),
    headerSettings
  )

lazy val `fs2-kafka-vulcan-testkit-munit` = project
  .in(file("modules/fs2-kafka-vulcan-testkit-munit"))
  .dependsOn(`fs2-kafka-vulcan`)
  .settings(
    name := "fs2-kafka-vulcan-testkit-munit",
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-schema-registry" % http4sSchemaRegistryVersion,
      "org.scalameta" %% "munit-diff" % munitVersion,
      "org.scalameta" %% "munit" % munitVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "org.typelevel" %% "vulcan" % vulcanVersion
    ),
    headerSettings
  )

lazy val tests = project
  .in(file("tests"))
  .enablePlugins(NoPublishPlugin)
  .dependsOn(`fs2-kafka-vulcan`, `fs2-kafka-vulcan-testkit-munit`)
  .settings(
    name := "fs2-kafka-vulcan-tests",
    resolvers += "confluent".at("https://packages.confluent.io/maven/"),
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "vulcan-generic" % vulcan1Version,
      "com.github.fd4s" %% "vulcan" % vulcan1Version,
      "io.confluent" % "kafka-avro-serializer" % confluentVersion,
      "org.http4s" %% "http4s-ember-client" % http4sVersion,
      "org.http4s" %% "http4s-ember-server" % http4sVersion,
      "org.slf4j" % "slf4j-nop" % slf4jNopVersion,
      "org.testcontainers" % "testcontainers-kafka" % testcontainersVersion,
      "org.typelevel" %% "vulcan-generic" % vulcanVersion,
      "org.typelevel" %% "weaver-cats" % weaverVersion,
      "org.typelevel" %% "weaver-scalacheck" % weaverVersion
    ).map(_ % Test),
    headerSettings,
    Test / fork := true,
    Test / javaOptions ++= Seq(
      s"-Dfs2.kafka.vulcan.test.kafka.image=$kafkaImage",
      s"-Dfs2.kafka.vulcan.test.karapace.image=$karapaceImage",
      s"-Dfs2.kafka.vulcan.test.schemaregistry.image=$schemaRegistryImage"
    ),
    Test / parallelExecution := false
  )

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(MdocPlugin, NoPublishPlugin)
  .dependsOn(`fs2-kafka-vulcan`, `fs2-kafka-vulcan-testkit-munit`)
  .settings(
    crossScalaVersions := Seq(scala3Version),
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-ember-client" % http4sVersion,
      "org.slf4j" % "slf4j-nop" % slf4jNopVersion % Runtime
    ),
    mdocIn := baseDirectory.value / "readme.md",
    mdocOut := (LocalRootProject / baseDirectory).value / "readme.md",
    mdocVariables ++= Map(
      "FS2_KAFKA_VERSION" -> fs2KafkaVersion,
      "HTTP4S_SCHEMA_REGISTRY_VERSION" -> http4sSchemaRegistryVersion,
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
