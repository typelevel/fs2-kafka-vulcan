# FS2 Kafka Vulcan

[![Typelevel Organization Project](https://img.shields.io/badge/typelevel-organization%20project-ff6169)](https://typelevel.org)
[![GitHub Actions](https://img.shields.io/github/actions/workflow/status/typelevel/fs2-kafka-vulcan/ci.yml?branch=main)](https://github.com/typelevel/fs2-kafka-vulcan/actions)
[![Discord](https://img.shields.io/discord/632277896739946517?color=5865f2)](https://discord.gg/fjqZMuqzAp)
[![Version](https://img.shields.io/maven-central/v/org.typelevel/fs2-kafka-vulcan_3?label=version&color=orange)](https://index.scala-lang.org/typelevel/fs2-kafka-vulcan)

The `fs2-kafka-vulcan` module provides [Avro](https://avro.apache.org) support for [FS2 Kafka](https://github.com/typelevel/fs2-kafka) using [Vulcan](https://typelevel.org/vulcan).

To get started with [sbt](https://scala-sbt.org), add the following lines to your `build.sbt` file.

```scala
resolvers += "confluent".at("https://packages.confluent.io/maven/")

libraryDependencies += "org.typelevel" %% "fs2-kafka-vulcan" % "4.1.3"
```

Published for Scala 2.12, 2.13 and 3.3.

## Quick Example

We start by defining the type we want to serialize or deserialize, and create a `Codec`.

```scala
import cats.syntax.all._

import vulcan.Codec

final case class Person(name: String, age: Option[Int])

implicit val personCodec: Codec[Person] =
  Codec.record(
    name = "Person",
    namespace = "com.example"
  ) { field =>
    (
      field("name", _.name),
      field("age", _.age)
    ).mapN(Person(_, _))
  }
```

We then define `AvroSettings`, describing the schema registry settings.

```scala
import cats.effect.IO
import fs2.kafka.vulcan.{Auth, AvroSettings, SchemaRegistryClientSettings}

val avroSettings =
  AvroSettings {
    SchemaRegistryClientSettings[IO]("http://localhost:8081").withAuth(
      Auth.Basic("username", "password")
    )
  }
```

We can then create a `Serializer` and `Deserializer` instance for `Person`.

```scala
import cats.effect.Resource
import fs2.kafka.{ValueDeserializer, ValueSerializer}
import fs2.kafka.vulcan.{avroDeserializer, avroSerializer}

implicit val personSerializer: Resource[IO, ValueSerializer[IO, Person]] =
  avroSerializer[Person].forValue(avroSettings)

implicit val personDeserializer: Resource[IO, ValueDeserializer[IO, Person]] =
  avroDeserializer[Person].forValue(avroSettings)
```

Finally, we can create settings, passing the `Serializer`s and `Deserializer`s implicitly.

```scala
import fs2.kafka.{AutoOffsetReset, ConsumerSettings, ProducerSettings}

val consumerSettings =
  ConsumerSettings[IO, String, Person]
    .withAutoOffsetReset(AutoOffsetReset.Earliest)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group")

val producerSettings =
  ProducerSettings[IO, String, Person].withBootstrapServers("localhost:9092")
```

If we prefer, we can instead specify the `Serializer`s and `Deserializer`s explicitly.

```scala
import fs2.kafka.{Deserializer, Serializer}

ConsumerSettings(
  keyDeserializer = Deserializer[IO, String],
  valueDeserializer = personDeserializer
).withAutoOffsetReset(AutoOffsetReset.Earliest)
  .withBootstrapServers("localhost:9092")
  .withGroupId("group")

ProducerSettings(
  keySerializer = Serializer[IO, String],
  valueSerializer = personSerializer
).withBootstrapServers("localhost:9092")
```

By default, a schema will automatically be registered when used to publish a message. We can disable this behaviour by
using `withAutoRegisterSchemas(false)`. We can then use `registerSchema` to manually register the schema with the registry server:

```scala
val avroSettingsWithoutAutoRegister = avroSettings.withAutoRegisterSchemas(false)

avroSettingsWithoutAutoRegister.registerSchema[String]("person-key") *>
  avroSettingsWithoutAutoRegister.registerSchema[Person]("person-value")
```

### Sharing Client

When creating `AvroSettings` with `SchemaRegistryClientSettings`, one schema registry client will be created per `Serializer` or `Deserializer`. For many cases, this is completely fine, but it's possible to reuse a single client for multiple `Serializer`s and `Deserializer`s.

To share a `SchemaRegistryClient`, we first create it and then pass it to `AvroSettings`.

```scala
val avroSettingsSharedClient: IO[AvroSettings[IO]] =
  SchemaRegistryClientSettings[IO]("http://localhost:8081")
    .withAuth(Auth.Basic("username", "password"))
    .createSchemaRegistryClient
    .map(AvroSettings(_))
```

We can then create multiple `Serializer`s and `Deserializer`s using the `AvroSettings`.

```scala
avroSettingsSharedClient.map { avroSettings =>
  val personSerializer: Resource[IO, ValueSerializer[IO, Person]] =
    avroSerializer[Person].forValue(avroSettings)

  val personDeserializer: Resource[IO, ValueDeserializer[IO, Person]] =
    avroDeserializer[Person].forValue(avroSettings)

  val consumerSettings =
    ConsumerSettings(
      keyDeserializer = Deserializer[IO, String],
      valueDeserializer = personDeserializer
    ).withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers("localhost:9092")
      .withGroupId("group")

  val producerSettings =
    ProducerSettings(
      keySerializer = Serializer[IO, String],
      valueSerializer = personSerializer
    ).withBootstrapServers("localhost:9092")

  (consumerSettings, producerSettings)
}
```

## MUnit Testkit

The `fs2-kafka-vulcan-testkit-munit` module provides an [MUnit](https://scalameta.org/munit/) fixture for testing Vulcan
codecs against a [schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html).

To get started with [sbt](https://scala-sbt.org), add the following lines to your `build.sbt` file.

```scala
resolvers += "confluent".at("https://packages.confluent.io/maven/")

libraryDependencies += "org.typelevel" %% "fs2-kafka-vulcan-testkit-munit" % "4.1.3"
```

Following is a quick example showing how the module can be used.

```scala
import cats.effect.unsafe.implicits.global
import fs2.kafka.vulcan.testkit.SchemaSuite
import fs2.kafka.vulcan.SchemaRegistryClientSettings

import vulcan.Codec

class MySpec extends SchemaSuite {

  val checker = compatibilityChecker(
    SchemaRegistryClientSettings("https://some-schema-registry:1234")
  )

  override def munitFixtures = List(checker)

  test("my codec is compatible") {
    val myCodec: Codec[String] = Codec.string

    checker()
      .assertReaderCompatibility(myCodec, "my-schema-subject")
      .unsafeRunSync()
  }

}
```

## Dependencies

Refer to the table below for dependencies and version support across modules.

| Module                               | Dependencies                                                                                                                                                                                                                                    | Scala                                             |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `fs2-kafka-vulcan`               | [FS2-Kafka 4.1.3](https://github.com/typelevel/fs2-kafka), [Vulcan 1.14.1](https://github.com/typelevel/vulcan), [Confluent Kafka Avro Serializer 8.4.0](https://github.com/confluentinc/schema-registry) | Scala 2.12, 2.13 and 3.3               |
| `fs2-kafka-vulcan-testkit-munit` | `fs2-kafka-vulcan`, [MUnit 1.3.6](https://github.com/scalameta/munit)                                                                                                                                                             | Scala 2.12, 2.13 and 3.3 |
