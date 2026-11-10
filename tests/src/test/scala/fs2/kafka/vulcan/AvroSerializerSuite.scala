/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.IO
import cats.syntax.all.*
import fs2.kafka.Headers
import fs2.kafka.SerializationException
import fs2.kafka.vulcan.Generators.*
import java.nio.ByteBuffer
import org.http4s.schemaregistry.SchemaId
import org.http4s.schemaregistry.SubjectName
import org.http4s.schemaregistry.TestSchemaRegistry
import org.typelevel.vulcan.AvroException
import org.typelevel.vulcan.Codec
import scodec.bits.ByteVector
import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers

object AvroSerializerSuite extends SimpleIOSuite with Checkers {
  def serialize[A: Codec](serializer: AvroSerializer[IO], topic: String, value: A): IO[Array[Byte]] =
    serializer.forValue[A].use(_.serialize(topic, Headers.empty, value))

  def schemaIdOf(bytes: Array[Byte]): SchemaId =
    SchemaId(ByteBuffer.wrap(bytes, 1, 4).getInt)

  test("serializes values as Avro binary, prefixed by a zero magic byte and the schema id") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      encoder <- IO.fromEither(Codec[TestRecord].binaryEncoder)
      result <- AvroSerializer.default(client).forValue[TestRecord].use { serializer =>
        forall(testRecords) { value =>
          for {
            bytes <- serializer.serialize("topic", Headers.empty, value)
            registered <- registry.registered
            expected <- IO.fromEither(encoder.encode(value))
          } yield expect.same(0.toByte, bytes(0)) &&
            expect.same(registered.map(_.id), List(schemaIdOf(bytes))) &&
            expect.same(expected, ByteVector.view(bytes).drop(5))
        }
      }
    } yield result
  }

  test("registers the schema of the codec under the subject for keys and values of the topic") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      serializer = AvroSerializer.default(client)
      _ <- serializer.forKey[String].use(_.serialize("topic", Headers.empty, "key"))
      _ <- serializer.forValue[TestRecord].use(_.serialize("topic", Headers.empty, TestRecord("test")))
      registered <- registry.registered
    } yield expect.same(
      List(
        "topic-key" -> "\"string\"",
        "topic-value" -> """{"type":"record","name":"Test","namespace":"fs2.kafka.vulcan","fields":[{"name":"name","type":"string"}]}"""
      ),
      registered.map(registered => (registered.subject.value, registered.schema.definition))
    )
  }

  test("registers the schema of the codec for unions") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      _ <- serialize[Either[Int, Boolean]](AvroSerializer.default(client), "topic", Right(true))
      registered <- registry.registered
    } yield expect.same(List("""["int","boolean"]"""), registered.map(_.schema.definition))
  }

  test("registers schemas once for each subject") {
    for {
      registry <- RecordingRegistry()
      client <- registry.client.build
      _ <- AvroSerializer.default(client).forValue[TestRecord].use { serializer =>
        serializer.serialize("first", Headers.empty, TestRecord("a")) >>
          serializer.serialize("first", Headers.empty, TestRecord("b")) >>
          serializer.serialize("second", Headers.empty, TestRecord("c"))
      }
      requests <- registry.requests
      registered <- registry.registered
    } yield expect.same(
      List("POST /subjects/first-value/versions", "POST /subjects/second-value/versions"),
      requests
    ) &&
      expect.same(List("first-value", "second-value"), registered.map(_.subject.value))
  }

  test("looks up schemas when not registering schemas automatically") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      serializer = AvroSerializer.default(client).withAutoRegisterSchemas(false)
      missing <- serialize(serializer, "topic", TestRecord("test")).attempt
      id <- serializer.registerValueSchema[TestRecord]("topic")
      bytes <- serialize(serializer, "topic", TestRecord("test"))
      registered <- registry.registered
    } yield matches(missing) { case Left(error: SerializationException) =>
      expect.same(
        "Schema is not registered under subject topic-value, and registering schemas automatically is disabled",
        error.getMessage
      )
    } &&
      expect.same(id, schemaIdOf(bytes)) &&
      expect.same(List("topic-value"), registered.map(_.subject.value))
  }

  test("uses the subject name strategy") {
    val strategies =
      List(
        SubjectNameStrategy.TopicName -> "topic-value",
        SubjectNameStrategy.RecordName -> "fs2.kafka.vulcan.Test",
        SubjectNameStrategy.TopicRecordName -> "topic-fs2.kafka.vulcan.Test",
        SubjectNameStrategy.custom((topic, isKey, _) => Right(SubjectName(s"$topic-$isKey"))) -> "topic-false"
      )

    strategies
      .traverse { case (strategy, subject) =>
        for {
          registry <- TestSchemaRegistry[IO]
          client <- registry.client.build
          _ <- serialize(
            AvroSerializer.default(client).withSubjectNameStrategy(strategy),
            "topic",
            TestRecord("test")
          )
          registered <- registry.registered
        } yield expect.same(List(subject), registered.map(_.subject.value))
      }
      .map(_.combineAll)
  }

  test("fails serialization for record name strategies with schemas which are not records") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      serializer = AvroSerializer.default(client).withSubjectNameStrategy(SubjectNameStrategy.RecordName)
      result <- serialize(serializer, "topic", 1).attempt
    } yield matches(result) { case Left(error: SerializationException) =>
      expect.same("Subject name strategy RecordName requires a record schema, got \"int\"", error.getMessage)
    }
  }

  test("normalizes schemas when registering and looking up schemas if enabled") {
    for {
      registry <- RecordingRegistry()
      client <- registry.client.build
      serializer = AvroSerializer.default(client).withNormalizeSchemas(true)
      _ <- serialize(serializer, "topic", TestRecord("test"))
      _ <- serialize(serializer.withAutoRegisterSchemas(false), "topic", TestRecord("test"))
      requests <- registry.requests
    } yield expect.same(
      List(
        "POST /subjects/topic-value/versions?normalize=true",
        "POST /subjects/topic-value?normalize=true"
      ),
      requests
    )
  }

  test("fails when acquiring serializers for codecs with invalid schemas") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      codec = Codec.decimal(-1, -1)
      serializer = AvroSerializer.default(client)
      key <- serializer.forKey(codec).use_.attempt
      value <- serializer.forValue(codec).use_.attempt
    } yield forEach(List(key, value)) { result =>
      matches(result) { case Left(error: AvroException) =>
        expect.same("Invalid decimal precision -1: must be positive", error.getMessage)
      }
    }
  }

  test("fails serialization for values which cannot be encoded") {
    val codec = Codec.decimal(4, 2)
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      result <- serialize(AvroSerializer.default(client), "topic", BigDecimal("123.45"))(codec).attempt
    } yield matches(result) { case Left(error: AvroException) =>
      expect.same(
        "Error encoding BigDecimal: Unable to encode decimal with precision 5 exceeding schema precision 4",
        error.getMessage
      )
    }
  }

  test("registerSchema registers the schema of the codec under the subject") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      serializer = AvroSerializer.default(client)
      id <- serializer.registerSchema[TestRecord](SubjectName("subject"))
      keyId <- serializer
        .withSubjectNameStrategy(SubjectNameStrategy.TopicRecordName)
        .registerKeySchema[TestRecord]("topic")
      registered <- registry.registered
    } yield expect.same(
      List("subject" -> id, "topic-fs2.kafka.vulcan.Test" -> keyId),
      registered.map(registered => (registered.subject.value, registered.id))
    )
  }

  test("settings and toString") {
    TestSchemaRegistry[IO].flatMap(_.client.build).map { client =>
      val serializer = AvroSerializer.default(client)
      expect(serializer.autoRegisterSchemas) &&
      expect(!serializer.normalizeSchemas) &&
      expect.same(SubjectNameStrategy.TopicName, serializer.subjectNameStrategy) &&
      expect.same(
        "AvroSerializer(autoRegisterSchemas = false, normalizeSchemas = true, subjectNameStrategy = TopicName, schemaIdHeaders = false)",
        serializer.withAutoRegisterSchemas(false).withNormalizeSchemas(true).toString
      )
    }
  }
}
