/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.IO
import cats.effect.Resource
import fs2.kafka.AutoOffsetReset
import fs2.kafka.ConsumerSettings
import fs2.kafka.Deserializer
import fs2.kafka.KafkaConsumer
import fs2.kafka.KafkaProducer
import fs2.kafka.ProducerRecord
import fs2.kafka.ProducerRecords
import fs2.kafka.ProducerSettings
import fs2.kafka.Serializer
import fs2.kafka.ValueDeserializer
import fs2.kafka.vulcan.testkit.SchemaSuite
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import java.util.concurrent.atomic.AtomicInteger
import munit.FailException
import org.apache.avro.generic.GenericRecord
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.schemaregistry.SchemaRegistryClient
import org.http4s.schemaregistry.SchemaRegistryClientBuilder
import org.http4s.schemaregistry.SubjectName
import org.typelevel.vulcan.Codec
import scala.jdk.CollectionConverters.*
import weaver.IOSuite
import fs2.kafka.vulcan.EndToEndSuite.Context

abstract class EndToEndSuite extends IOSuite {
  override type Res = Context

  def environment: Resource[IO, Containers.Environment]

  override def sharedResource: Resource[IO, Context] =
    for {
      environment <- environment
      http <- EmberClientBuilder.default[IO].build
      client <- Resource.eval(SchemaRegistryClientBuilder.default(http, environment.schemaRegistryUri).build)
    } yield Context(environment, client)

  private val topics: AtomicInteger =
    new AtomicInteger(0)

  def newTopic: IO[String] =
    IO(s"topic-${topics.incrementAndGet()}")

  def produce[A](context: Context, topic: String, serializer: AvroSerializer[IO], value: A)(implicit
    codec: Codec[A]
  ): IO[Unit] = {
    val settings =
      ProducerSettings(Serializer[IO, String], serializer.forValue[A])
        .withBootstrapServers(context.environment.bootstrapServers)

    KafkaProducer.resource(settings).use { producer =>
      producer.produce(ProducerRecords.one(ProducerRecord(topic, "key", value))).flatten.void
    }
  }

  def consume[A](
    context: Context,
    topic: String,
    deserializer: Resource[IO, ValueDeserializer[IO, A]]
  ): IO[A] = {
    val settings =
      ConsumerSettings(Deserializer[IO, String], deserializer)
        .withBootstrapServers(context.environment.bootstrapServers)
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withGroupId(s"$topic-group")

    KafkaConsumer.stream(settings).subscribeTo(topic).records.take(1).compile.lastOrError.map(_.record.value)
  }

  test("consumes values produced with the serializer") { context =>
    for {
      topic <- newTopic
      _ <- produce(context, topic, AvroSerializer.default(context.client), TestRecordV2("test", 42))
      value <- consume(context, topic, AvroDeserializer.default(context.client).forValue[TestRecord])
    } yield expect.same(TestRecord("test"), value)
  }

  test("the Confluent deserializer reads values produced with the serializer") { context =>
    for {
      topic <- newTopic
      _ <- produce(context, topic, AvroSerializer.default(context.client), TestRecord("test"))
      bytes <- consume(context, topic, Resource.pure(Deserializer[IO, Array[Byte]]))
      value <- IO.blocking {
        val uri = context.environment.schemaRegistryUri.renderString
        val deserializer = new KafkaAvroDeserializer(new CachedSchemaRegistryClient(uri, 100))
        deserializer.configure(Map("schema.registry.url" -> uri).asJava, false)
        deserializer.deserialize(topic, bytes)
      }
    } yield matches(value) { case record: GenericRecord =>
      expect.same("test", record.get("name").toString)
    }
  }

  test("serializers with automatic registration disabled use schemas registered before") { context =>
    for {
      topic <- newTopic
      _ <- AvroSerializer.default(context.client).registerValueSchema[TestRecord](topic)
      serializer = AvroSerializer.default(context.client).withAutoRegisterSchemas(false)
      _ <- produce(context, topic, serializer, TestRecord("test"))
      value <- consume(context, topic, AvroDeserializer.default(context.client).forValue[TestRecord])
    } yield expect.same(TestRecord("test"), value)
  }

  test("the testkit checks the compatibility of codecs with the latest version of subjects") { context =>
    val checker = new SchemaSuite {}.newCompatibilityChecker(context.client)
    for {
      topic <- newTopic
      _ <- AvroSerializer.default(context.client).registerValueSchema[TestRecord](topic)
      compatible <- checker.checkCompatibility(Codec[TestRecord], SubjectName(s"$topic-value"))
      incompatible <- checker.checkCompatibility(Codec[TestRecordV2], SubjectName(s"$topic-value"))
      missing <- checker.checkCompatibility(Codec[TestRecord], SubjectName(s"$topic-key")).attempt
    } yield expect(compatible.isCompatible) &&
      expect(!incompatible.isCompatible) &&
      matches(missing) { case Left(error: FailException) =>
        expect(error.getMessage.contains(s"Subject $topic-key was not found in the schema registry"))
      }
  }
}

object EndToEndSuite {
  final case class Context(environment: Containers.Environment, client: SchemaRegistryClient[IO])
}

object ConfluentEndToEndSuite extends EndToEndSuite {
  override def environment: Resource[IO, Containers.Environment] =
    Containers.confluent
}

object KarapaceEndToEndSuite extends EndToEndSuite {
  override def environment: Resource[IO, Containers.Environment] =
    Containers.karapace
}
