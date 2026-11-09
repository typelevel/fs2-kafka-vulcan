/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.IO
import cats.syntax.all.*
import fs2.kafka.DeserializationException
import fs2.kafka.Header
import fs2.kafka.Headers
import fs2.kafka.vulcan.Generators.*
import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID
import org.http4s.schemaregistry
import org.http4s.schemaregistry.SchemaGuid
import org.http4s.schemaregistry.SchemaId
import org.http4s.schemaregistry.SchemaReference
import org.http4s.schemaregistry.SchemaRegistryClient
import org.http4s.schemaregistry.SchemaRegistryError
import org.http4s.schemaregistry.SchemaRegistryErrorCode
import org.http4s.schemaregistry.SchemaType
import org.http4s.schemaregistry.SubjectName
import org.http4s.schemaregistry.SubjectVersion
import org.http4s.schemaregistry.TestSchemaRegistry
import org.http4s.schemaregistry.VersionSelector
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.typelevel.vulcan.AvroException
import org.typelevel.vulcan.Codec
import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers

object AvroDeserializerSuite extends SimpleIOSuite with Checkers {
  def deserialize[A: Codec](client: SchemaRegistryClient[IO], headers: Headers, bytes: Array[Byte]): IO[A] =
    AvroDeserializer.default(client).forValue[A].use(_.deserialize("topic", headers, bytes))

  def register[A](client: SchemaRegistryClient[IO])(implicit codec: Codec[A]): IO[(SchemaId, SchemaGuid)] =
    for {
      id <- AvroSerializer.default(client).registerValueSchema[A]("topic")
      latest <- client.getSubjectVersion(SubjectName("topic-value"), VersionSelector.Latest)
      guid <- IO.fromOption(latest.guid)(new IllegalStateException("Expected GUID"))
    } yield (id, guid)

  def encode[A](value: A)(implicit codec: Codec[A]): IO[Array[Byte]] =
    IO.fromEither(codec.binaryEncoder.flatMap(_.encode(value))).map(_.toArray)

  def prefixed(id: SchemaId, bytes: Array[Byte]): Array[Byte] =
    ByteBuffer.allocate(5 + bytes.length).put(0.toByte).putInt(id.value).put(bytes).array()

  def idHeader(key: String, id: SchemaId): Header =
    Header(key, ByteBuffer.allocate(5).put(0.toByte).putInt(id.value).array())

  def guidHeader(key: String, guid: SchemaGuid): Header = {
    val uuid = UUID.fromString(guid.value)
    Header(
      key,
      ByteBuffer
        .allocate(17)
        .put(1.toByte)
        .putLong(uuid.getMostSignificantBits)
        .putLong(uuid.getLeastSignificantBits)
        .array()
    )
  }

  test("roundtrips values") {
    for {
      client <- TestSchemaRegistry[IO].flatMap(_.client.build)
      result <- (
        AvroSerializer.default(client).forValue[Either[TestRecord, Int]],
        AvroDeserializer.default(client).forValue[Either[TestRecord, Int]]
      ).tupled.use { case (serializer, deserializer) =>
        forall(Gen.either(testRecords, Arbitrary.arbitrary[Int])) { value =>
          serializer
            .serialize("topic", Headers.empty, value)
            .flatMap(deserializer.deserialize("topic", Headers.empty, _))
            .map(expect.same(value, _))
        }
      }
    } yield result
  }

  test("decodes values written with compatible schemas") {
    for {
      client <- TestSchemaRegistry[IO].flatMap(_.client.build)
      result <- (
        AvroSerializer.default(client).forValue[TestRecordV2],
        AvroDeserializer.default(client).forValue[TestRecord]
      ).tupled.use { case (serializer, deserializer) =>
        forall(testRecordsV2) { value =>
          serializer
            .serialize("topic", Headers.empty, value)
            .flatMap(deserializer.deserialize("topic", Headers.empty, _))
            .map(expect.same(TestRecord(value.name), _))
        }
      }
    } yield result
  }

  test("fails to decode values written with mismatching logical types") {
    for {
      client <- TestSchemaRegistry[IO].flatMap(_.client.build)
      bytes <- AvroSerializer.default(client).forValue[Long].use(_.serialize("topic", Headers.empty, 42L))
      result <- deserialize[Instant](client, Headers.empty, bytes).attempt
    } yield matches(result) { case Left(error: AvroException) =>
      expect.same("Error decoding Instant: Got unexpected missing logical type", error.getMessage)
    }
  }

  test("fails when acquiring deserializers for codecs with invalid schemas") {
    for {
      client <- TestSchemaRegistry[IO].flatMap(_.client.build)
      codec = Codec.decimal(-1, -1)
      key <- AvroDeserializer.default(client).forKey(codec).use_.attempt
      value <- AvroDeserializer.default(client).forValue(codec).use_.attempt
    } yield forEach(List(key, value)) { result =>
      matches(result) { case Left(error: AvroException) =>
        expect.same("Invalid decimal precision -1: must be positive", error.getMessage)
      }
    }
  }

  test("fails for values which are not in the wire format") {
    for {
      client <- TestSchemaRegistry[IO].flatMap(_.client.build)
      empty <- deserialize[String](client, Headers.empty, null).attempt
      magic <- deserialize[String](client, Headers.empty, Array[Byte](1, 0, 0, 0, 1, 0)).attempt
    } yield matches(empty) { case Left(error: IllegalArgumentException) =>
      expect.same("Invalid Avro record: bytes is null or empty", error.getMessage)
    } &&
      matches(magic) { case Left(error: DeserializationException) =>
        expect.same("Unknown magic byte 1, expected magic byte 0", error.getMessage)
      }
  }

  test("fails for bytes too short for the value") {
    for {
      client <- TestSchemaRegistry[IO].flatMap(_.client.build)
      ids <- register[TestRecord](client)
      result <- deserialize[TestRecord](client, Headers.empty, prefixed(ids._1, Array.emptyByteArray)).attempt
    } yield matches(result) { case Left(error: AvroException) =>
      expect.same("Unexpected end of input at position 0", error.getMessage)
    }
  }

  test("compiles decoders once for each writer schema") {
    for {
      registry <- RecordingRegistry()
      client <- registry.client.withCaching(false).build
      values <- (
        AvroSerializer.default(client).forValue[TestRecordV2],
        AvroDeserializer.default(client).forValue[TestRecord]
      ).tupled.use { case (serializer, deserializer) =>
        List("a", "b", "c").traverse { name =>
          serializer
            .serialize("topic", Headers.empty, TestRecordV2(name, 1))
            .flatMap(deserializer.deserialize("topic", Headers.empty, _))
        }
      }
      requests <- registry.requests
    } yield expect.same(List(TestRecord("a"), TestRecord("b"), TestRecord("c")), values) &&
      expect.same(1, requests.count(_.startsWith("GET /schemas/")))
  }

  test("does not cache errors from the schema registry") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      bytes <- encode(TestRecord("test")).map(prefixed(SchemaId(1), _))
      results <- AvroDeserializer.default(client).forValue[TestRecord].use { deserializer =>
        for {
          missing <- deserializer.deserialize("topic", Headers.empty, bytes).attempt
          _ <- register[TestRecord](client)
          value <- deserializer.deserialize("topic", Headers.empty, bytes)
        } yield (missing, value)
      }
    } yield matches(results._1) { case Left(error: SchemaRegistryError) =>
      expect.same(Some(SchemaRegistryErrorCode.SchemaNotFound), error.errorCode)
    } &&
      expect.same(TestRecord("test"), results._2)
  }

  test("reads schema GUIDs from headers, where values do not have a prefix") {
    for {
      client <- TestSchemaRegistry[IO].flatMap(_.client.build)
      ids <- register[TestRecord](client)
      bytes <- encode(TestRecord("test"))
      value <- deserialize[TestRecord](client, Headers(guidHeader("__value_schema_id", ids._2)), bytes)
      key <- AvroDeserializer
        .default(client)
        .forKey[TestRecord]
        .use(_.deserialize("topic", Headers(guidHeader("__key_schema_id", ids._2)), bytes))
    } yield expect.same(TestRecord("test"), value) && expect.same(TestRecord("test"), key)
  }

  test("reads schema ids from headers, where values do not have a prefix") {
    for {
      client <- TestSchemaRegistry[IO].flatMap(_.client.build)
      id <- AvroSerializer.default(client).registerValueSchema[Int]("topic")
      bytes <- encode(0)
      value <- deserialize[Int](client, Headers(idHeader("__value_schema_id", id)), bytes)
    } yield expect.same(List(0.toByte), bytes.toList) && expect.same(0, value)
  }

  test("fails for writer schemas which are not Avro, or which have references") {
    for {
      client <- TestSchemaRegistry[IO].flatMap(_.client.build)
      protobuf <- client.registerSchema(
        SubjectName("protobuf"),
        schemaregistry.Schema("syntax = \"proto3\";").withSchemaType(SchemaType.Protobuf)
      )
      references <- client.registerSchema(
        SubjectName("references"),
        schemaregistry
          .Schema("\"fs2.kafka.vulcan.Test\"")
          .withReferences(
            List(SchemaReference("fs2.kafka.vulcan.Test", SubjectName("test"), SubjectVersion(1)))
          )
      )
      bytes <- encode(TestRecord("test"))
      protobufResult <- deserialize[TestRecord](client, Headers.empty, prefixed(protobuf, bytes)).attempt
      referencesResult <- deserialize[TestRecord](client, Headers.empty, prefixed(references, bytes)).attempt
    } yield matches(protobufResult) { case Left(error: DeserializationException) =>
      expect.same(
        s"Expected an Avro schema for schema id ${protobuf.value}, got schema type PROTOBUF",
        error.getMessage
      )
    } &&
      matches(referencesResult) { case Left(error: DeserializationException) =>
        expect.same(
          s"Schema references are not supported, but the schema for schema id ${references.value} has references",
          error.getMessage
        )
      }
  }

  test("toString") {
    TestSchemaRegistry[IO].flatMap(_.client.build).map { client =>
      expect.same("AvroDeserializer(SchemaRegistryClient())", AvroDeserializer.default(client).toString)
    }
  }
}
