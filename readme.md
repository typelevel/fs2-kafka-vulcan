# FS2 Kafka Vulcan

[![Typelevel Organization Project](https://img.shields.io/badge/typelevel-organization%20project-ff6169)](https://typelevel.org)
[![GitHub Actions](https://img.shields.io/github/actions/workflow/status/typelevel/fs2-kafka-vulcan/ci.yml?branch=main)](https://github.com/typelevel/fs2-kafka-vulcan/actions)
[![Discord](https://img.shields.io/discord/632277896739946517?color=5865f2)](https://discord.gg/fjqZMuqzAp)
[![Version](https://img.shields.io/maven-central/v/org.typelevel/fs2-kafka-vulcan_3?label=version&color=orange)](https://index.scala-lang.org/typelevel/fs2-kafka-vulcan)

The `fs2-kafka-vulcan` module provides [Avro](https://avro.apache.org) support for [FS2 Kafka](https://github.com/typelevel/fs2-kafka) using [Vulcan](https://typelevel.org/vulcan). Schemas are stored in a schema registry implementing the [Confluent Schema Registry REST API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html), like [Confluent Schema Registry](https://github.com/confluentinc/schema-registry) and [Karapace](https://github.com/Aiven-Open/karapace), using [http4s-schema-registry](https://github.com/http4s/http4s-schema-registry). Records use the same wire format as the Confluent serializers.

To get started with [sbt](https://scala-sbt.org), add the following line to your `build.sbt` file.

```scala
libraryDependencies += "org.typelevel" %% "fs2-kafka-vulcan" % "4.2.0"
```

Published for Scala 2.13 and 3.3.

## Quick Example

We start by defining the type we want to serialize or deserialize, and create a `Codec`.

```scala
import cats.syntax.all._
import org.typelevel.vulcan.Codec

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

We then create a `SchemaRegistryClient` for the schema registry.

```scala
import cats.effect.{IO, Resource}
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.schemaregistry.{Auth, SchemaRegistryClient, SchemaRegistryClientBuilder}
import org.http4s.syntax.literals._

val schemaRegistryClient: Resource[IO, SchemaRegistryClient[IO]] =
  EmberClientBuilder.default[IO].build.evalMap { client =>
    SchemaRegistryClientBuilder
      .default(client, uri"http://localhost:8081")
      .withAuth(Auth.Basic("username", "password"))
      .build
  }
```

We can then create a `Serializer` and `Deserializer` for `Person` using `AvroSerializer` and `AvroDeserializer`.

```scala
import fs2.kafka.{ValueDeserializer, ValueSerializer}
import fs2.kafka.vulcan.{AvroDeserializer, AvroSerializer}

def personSerializer(client: SchemaRegistryClient[IO]): Resource[IO, ValueSerializer[IO, Person]] =
  AvroSerializer.default(client).forValue[Person]

def personDeserializer(client: SchemaRegistryClient[IO]): Resource[IO, ValueDeserializer[IO, Person]] =
  AvroDeserializer.default(client).forValue[Person]
```

Finally, we can create settings, passing the `Serializer` and `Deserializer`.

```scala
import fs2.kafka.{AutoOffsetReset, ConsumerSettings, Deserializer, ProducerSettings, Serializer}

val settings =
  schemaRegistryClient.map { client =>
    val consumerSettings =
      ConsumerSettings(
        keyDeserializer = Deserializer[IO, String],
        valueDeserializer = personDeserializer(client)
      ).withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers("localhost:9092")
        .withGroupId("group")

    val producerSettings =
      ProducerSettings(
        keySerializer = Serializer[IO, String],
        valueSerializer = personSerializer(client)
      ).withBootstrapServers("localhost:9092")

    (consumerSettings, producerSettings)
  }
```

The serializers and deserializers share the client, including its schema cache.

## Registering Schemas

By default, the schema of the `Codec` is registered automatically when serializing, unless it is already registered. We can disable this using `withAutoRegisterSchemas(false)`, in which case the schema is looked up instead, and serialization fails if the schema is not registered. We can then use `registerKeySchema` or `registerValueSchema` to register the schema for record keys or values of a topic, or `registerSchema` to register the schema under a subject.

```scala
def registerSchemas(client: SchemaRegistryClient[IO]) = {
  val serializer = AvroSerializer.default(client).withAutoRegisterSchemas(false)

  serializer.registerKeySchema[String]("people") *>
    serializer.registerValueSchema[Person]("people")
}
```

## Testing

For tests, `TestSchemaRegistry` provides an in-memory schema registry.

```scala
import org.http4s.schemaregistry.TestSchemaRegistry

val testSerializer: Resource[IO, ValueSerializer[IO, Person]] =
  Resource.eval(TestSchemaRegistry[IO].flatMap(_.client.build)).flatMap { client =>
    AvroSerializer.default(client).forValue[Person]
  }
```

## MUnit Testkit

The `fs2-kafka-vulcan-testkit-munit` module provides an [MUnit](https://scalameta.org/munit/) fixture for checking whether Vulcan codecs are compatible with subjects in a schema registry.

To get started with [sbt](https://scala-sbt.org), add the following line to your `build.sbt` file.

```scala
libraryDependencies += "org.typelevel" %% "fs2-kafka-vulcan-testkit-munit" % "4.2.0"
```

Following is a quick example showing how the module can be used.

```scala
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.kafka.vulcan.testkit.SchemaSuite
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.schemaregistry.{SchemaRegistryClientBuilder, SubjectName}
import org.http4s.syntax.literals._
import org.typelevel.vulcan.Codec

class MySpec extends SchemaSuite {
  val checker = compatibilityChecker(
    EmberClientBuilder.default[IO].build.evalMap { client =>
      SchemaRegistryClientBuilder.default(client, uri"https://some-schema-registry:1234").build
    }
  )

  override def munitFixtures = List(checker)

  test("my codec is compatible") {
    val myCodec: Codec[String] = Codec.string

    checker()
      .assertCompatibility(myCodec, SubjectName("my-schema-subject"))
      .unsafeRunSync()
  }
}
```

The schema registry checks whether the schema of the codec is compatible with the latest version of the subject, according to the compatibility level configured for the subject.

## Dependencies

Refer to the table below for dependencies and version support across modules.

| Module                               | Dependencies                                                                                                                                                                                                                                         | Scala                                             |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `fs2-kafka-vulcan`               | [fs2-kafka 4.2.0](https://github.com/typelevel/fs2-kafka), [http4s-schema-registry 1.0.0-M1](https://github.com/http4s/http4s-schema-registry), [vulcan 2.0.0-M1](https://github.com/typelevel/vulcan) | Scala 2.13 and 3.3               |
| `fs2-kafka-vulcan-testkit-munit` | fs2-kafka-vulcan, [munit 1.3.6](https://github.com/scalameta/munit)                                                                                                                                                                    | Scala 2.13 and 3.3 |
