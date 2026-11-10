/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.IO
import cats.syntax.all.*
import fs2.kafka.Header
import fs2.kafka.Headers
import fs2.kafka.SerializationException
import fs2.kafka.vulcan.Generators.*
import io.circe.Json
import io.circe.JsonObject
import io.circe.jawn
import java.nio.ByteBuffer
import java.util.UUID
import org.http4s.HttpApp
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.schemaregistry.SchemaGuid
import org.http4s.schemaregistry.SchemaId
import org.http4s.schemaregistry.SchemaRegistryClient
import org.http4s.schemaregistry.SchemaRegistryClientBuilder
import org.http4s.schemaregistry.SubjectName
import org.http4s.schemaregistry.TestSchemaRegistry
import org.http4s.schemaregistry.VersionSelector
import org.typelevel.vulcan.AvroException
import org.typelevel.vulcan.Codec
import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers

/** Tests for writing values using other schemas than the codec schema, and schema identifiers in headers. */
object AvroSerializerWriterSchemaSuite extends SimpleIOSuite with Checkers {

  /** Codec for [[TestRecordV2]] where the number has a default, so [[TestRecord]] can be written as it. */
  val withDefaultCodec: Codec[TestRecordV2] =
    Codec.record(name = "Test", namespace = "fs2.kafka.vulcan") { field =>
      (field("name", _.name), field("number", _.number, default = Some(42))).mapN(TestRecordV2(_, _))
    }

  def serialize[A: Codec](serializer: AvroSerializer[IO], value: A): IO[Array[Byte]] =
    serializer.forValue[A].use(_.serialize("topic", Headers.empty, value))

  def deserialize[A: Codec](client: SchemaRegistryClient[IO], headers: Headers, bytes: Array[Byte]): IO[A] =
    AvroDeserializer.default(client).forValue[A].use(_.deserialize("topic", headers, bytes))

  def schemaIdOf(bytes: Array[Byte]): SchemaId =
    SchemaId(ByteBuffer.wrap(bytes, 1, 4).getInt)

  def guidHeaderValue(guid: SchemaGuid): List[Byte] = {
    val uuid = UUID.fromString(guid.value)
    ByteBuffer
      .allocate(17)
      .put(1.toByte)
      .putLong(uuid.getMostSignificantBits)
      .putLong(uuid.getLeastSignificantBits)
      .array()
      .toList
  }

  def encode[A](value: A)(implicit codec: Codec[A]): IO[List[Byte]] =
    IO.fromEither(codec.binaryEncoder.flatMap(_.encode(value))).map(_.toArray.toList)

  test("writes values using the latest version of the subject") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      _ <- AvroSerializer.default(client).registerSchema[TestRecord](SubjectName("topic-value"))
      latestId <- AvroSerializer.default(client).registerSchema(SubjectName("topic-value"))(withDefaultCodec)
      result <- (
        AvroSerializer.default(client).withLatestVersion.forValue[TestRecord],
        AvroDeserializer.default(client).forValue[TestRecordV2]
      ).tupled.use { case (serializer, deserializer) =>
        forall(testRecords) { value =>
          for {
            bytes <- serializer.serialize("topic", Headers.empty, value)
            read <- deserializer.deserialize("topic", Headers.empty, bytes)
          } yield expect.same(latestId, schemaIdOf(bytes)) && expect.same(TestRecordV2(value.name, 42), read)
        }
      }
      registered <- registry.registered
    } yield result && expect.same(2, registered.size)
  }

  test("fails to write values using latest versions which values cannot be written as") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      _ <- AvroSerializer.default(client).registerSchema[TestRecordV2](SubjectName("topic-value"))
      result <- serialize(AvroSerializer.default(client).withLatestVersion, TestRecord("test")).attempt
    } yield matches(result) { case Left(error: AvroException) =>
      expect.same(
        """Unable to write values using the writer schema: Field "number" of record fs2.kafka.vulcan.Test in writer schema is missing in the schema and has no default""",
        error.getMessage
      )
    }
  }

  test("writes values using the schema with the id") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      id <- AvroSerializer.default(client).registerSchema(SubjectName("other"))(withDefaultCodec)
      result <- (
        AvroSerializer.default(client).withSchemaId(id).forValue[TestRecord],
        AvroDeserializer.default(client).forValue[TestRecordV2]
      ).tupled.use { case (serializer, deserializer) =>
        forall(testRecords) { value =>
          for {
            bytes <- serializer.serialize("topic", Headers.empty, value)
            read <- deserializer.deserialize("topic", Headers.empty, bytes)
          } yield expect.same(id, schemaIdOf(bytes)) && expect.same(TestRecordV2(value.name, 42), read)
        }
      }
      registered <- registry.registered
    } yield result && expect.same(List("other"), registered.map(_.subject.value))
  }

  test("uses the last writer schema setting") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      latest = AvroSerializer.default(client).withAutoRegisterSchemas(true).withLatestVersion
      codec = AvroSerializer.default(client).withLatestVersion.withAutoRegisterSchemas(true)
      _ <- serialize(codec, TestRecord("test"))
      registered <- registry.registered
    } yield expect(!latest.autoRegisterSchemas) &&
      expect(codec.autoRegisterSchemas) &&
      expect.same(List("topic-value"), registered.map(_.subject.value))
  }

  test("writes schema GUIDs in headers, where values do not have a prefix") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      result <- (
        AvroSerializer.default(client).withSchemaIdHeaders(true).forValue[TestRecord],
        AvroDeserializer.default(client).forValue[TestRecord]
      ).tupled.use { case (serializer, deserializer) =>
        forall(testRecords) { value =>
          for {
            written <- serializer.serializeWithHeaders("topic", Headers(Header("other", "value")), value)
            registered <- registry.registered
            guid <- IO.fromOption(registered.headOption.flatMap(_.guid))(
              new IllegalStateException("Expected GUID")
            )
            encoded <- encode(value)
            read <- deserializer.deserialize("topic", written._1, written._2)
          } yield expect.same(
            Some(guidHeaderValue(guid)),
            written._1("__value_schema_id").map(_.value.toList)
          ) &&
            expect.same(Some("value"), written._1("other").map(_.as[String])) &&
            expect.same(encoded, written._2.toList) &&
            expect.same(value, read)
        }
      }
    } yield result
  }

  test("writes schema GUIDs of keys in the key header, replacing existing schema id headers") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      headers = Headers(Header("__key_schema_id", Array[Byte](0, 0, 0, 0, 1)))
      result <- AvroSerializer
        .default(client)
        .withSchemaIdHeaders(true)
        .forKey[TestRecord]
        .use(_.serializeWithHeaders("topic", headers, TestRecord("test")))
      registered <- registry.registered
      guid <- IO.fromOption(registered.headOption.flatMap(_.guid))(new IllegalStateException("Expected GUID"))
      key <- AvroDeserializer
        .default(client)
        .forKey[TestRecord]
        .use(_.deserialize("topic", result._1, result._2))
    } yield expect.same(List("topic-key"), registered.map(_.subject.value)) &&
      expect.same(
        List("__key_schema_id" -> guidHeaderValue(guid)),
        result._1.toChain.toList.map(header => (header.key, header.value.toList))
      ) &&
      expect.same(TestRecord("test"), key)
  }

  test("writes schema GUIDs in headers for the latest version and schema ids") {
    for {
      registry <- TestSchemaRegistry[IO]
      client <- registry.client.build
      id <- AvroSerializer.default(client).registerSchema(SubjectName("topic-value"))(withDefaultCodec)
      latest <- client.getSubjectVersion(SubjectName("topic-value"), VersionSelector.Latest)
      guid <- IO.fromOption(latest.guid)(new IllegalStateException("Expected GUID"))
      serializers = List(
        AvroSerializer.default(client).withLatestVersion.withSchemaIdHeaders(true),
        AvroSerializer.default(client).withSchemaId(id).withSchemaIdHeaders(true)
      )
      results <- serializers.traverse { serializer =>
        serializer
          .forValue[TestRecord]
          .use(_.serializeWithHeaders("topic", Headers.empty, TestRecord("test")))
      }
      values <- results.traverse(result => deserialize[TestRecordV2](client, result._1, result._2))
    } yield expect.same(
      List.fill(2)(Some(guidHeaderValue(guid))),
      results.map(_._1("__value_schema_id").map(_.value.toList))
    ) &&
      expect.same(List.fill(2)(TestRecordV2("test", 42)), values)
  }

  test("fails to write schema identifiers in headers if the schema registry does not return GUIDs") {
    for {
      client <- withGuids(_ => None)
      result <- AvroSerializer
        .default(client)
        .withSchemaIdHeaders(true)
        .forValue[TestRecord]
        .use(_.serializeWithHeaders("topic", Headers.empty, TestRecord("test")))
        .attempt
    } yield matches(result) { case Left(error: SerializationException) =>
      expect.same(
        "Schema registry did not return a GUID for schema id 1, which is required for schema identifiers in headers, so the registry might not support schema GUIDs",
        error.getMessage
      )
    }
  }

  test("fails to write schema identifiers in headers if schema GUIDs are not UUIDs") {
    for {
      client <- withGuids(_ => Some(SchemaGuid("guid")))
      result <- AvroSerializer
        .default(client)
        .withSchemaIdHeaders(true)
        .forValue[TestRecord]
        .use(_.serializeWithHeaders("topic", Headers.empty, TestRecord("test")))
        .attempt
    } yield matches(result) { case Left(error: SerializationException) =>
      expect.same(
        "Schema GUID guid is not a UUID, which is required for schema identifiers in headers",
        error.getMessage
      )
    }
  }

  test("settings and toString") {
    TestSchemaRegistry[IO].flatMap(_.client.build).map { client =>
      val serializer = AvroSerializer.default(client)
      expect(!serializer.schemaIdHeaders) &&
      expect(serializer.withSchemaIdHeaders(true).schemaIdHeaders) &&
      expect.same(
        "AvroSerializer(latestVersion = true, normalizeSchemas = false, subjectNameStrategy = TopicName, schemaIdHeaders = true)",
        serializer.withLatestVersion.withSchemaIdHeaders(true).toString
      ) &&
      expect.same(
        "AvroSerializer(schemaId = 1, normalizeSchemas = false, subjectNameStrategy = TopicName, schemaIdHeaders = false)",
        serializer.withSchemaId(SchemaId(1)).toString
      )
    }
  }

  /**
    * Returns a client of a registry where `f` modifies the schema GUIDs in responses, for example
    * to not return schema GUIDs, like registries without support for them.
    */
  def withGuids(f: SchemaGuid => Option[SchemaGuid]): IO[SchemaRegistryClient[IO]] =
    TestSchemaRegistry[IO].flatMap { registry =>
      def modify(json: JsonObject): JsonObject =
        json("guid").flatMap(_.asString).fold(json) { guid =>
          f(SchemaGuid(guid)).fold(json.remove("guid"))(modified =>
            json.add("guid", Json.fromString(modified.value))
          )
        }

      val httpApp = HttpApp[IO] { request =>
        registry.httpApp(request).flatMap { response =>
          response.as[String].map { body =>
            val modified = jawn.parse(body).fold(_ => body, _.mapObject(modify).noSpaces)
            response.withEntity(modified).withContentTypeOption(response.contentType)
          }
        }
      }

      SchemaRegistryClientBuilder.default(Client.fromHttpApp(httpApp), Uri()).build
    }
}
