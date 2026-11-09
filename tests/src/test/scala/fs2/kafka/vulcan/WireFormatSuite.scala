/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import fs2.kafka.DeserializationException
import fs2.kafka.Header
import fs2.kafka.Headers
import fs2.kafka.vulcan.Generators.*
import fs2.kafka.vulcan.internal.WireFormat
import java.nio.ByteBuffer
import java.util.UUID
import org.http4s.schemaregistry.SchemaGuid
import org.http4s.schemaregistry.SchemaId
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import scodec.bits.ByteVector
import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers

object WireFormatSuite extends SimpleIOSuite with Checkers {
  type SchemaIdentifier = Either[UUID, SchemaId]

  val schemaIdentifiers: Gen[SchemaIdentifier] =
    Gen.either(Gen.uuid, schemaIds)

  val bytes: Gen[Byte] =
    Arbitrary.arbitrary[Byte]

  def headerKey(isKey: Boolean): String =
    if (isKey) WireFormat.KeySchemaIdHeader else WireFormat.ValueSchemaIdHeader

  def headerValue(identifier: SchemaIdentifier): Array[Byte] =
    identifier match {
      case Left(guid) =>
        ByteBuffer
          .allocate(17)
          .put(1.toByte)
          .putLong(guid.getMostSignificantBits)
          .putLong(guid.getLeastSignificantBits)
          .array()
      case Right(id) =>
        ByteBuffer.allocate(5).put(0.toByte).putInt(id.value).array()
    }

  def writerSchemaId(identifier: SchemaIdentifier): WireFormat.WriterSchemaId =
    identifier.fold(guid => WireFormat.Guid(SchemaGuid(guid.toString)), WireFormat.Id(_))

  test("values are prefixed by the zero magic byte and the schema id") {
    forall(Gen.zip(schemaIds, byteVectors)) { case (id, value) =>
      val prefixed = WireFormat.withSchemaId(id, value)
      expect.same(0.toByte, prefixed(0)) &&
      expect.same(id.value, ByteBuffer.wrap(prefixed, 1, 4).getInt) &&
      expect.same(value, ByteVector.view(prefixed).drop(5))
    }
  }

  test("schema ids and values are read from prefixed values") {
    forall(Gen.zip(booleans, schemaIds, byteVectors)) { case (isKey, id, value) =>
      expect.same(
        Right((WireFormat.Id(id), value)),
        WireFormat.read(isKey, Headers.empty, WireFormat.withSchemaId(id, value))
      )
    }
  }

  test("schema identifiers are read from headers, where values are not prefixed") {
    forall(Gen.zip(booleans, schemaIdentifiers, byteVectors)) { case (isKey, identifier, value) =>
      val headers = Headers(Header(headerKey(isKey), headerValue(identifier)))
      expect.same(
        Right((writerSchemaId(identifier), value)),
        WireFormat.read(isKey, headers, value.toArray)
      )
    }
  }

  test("schema identifiers are read from the last header for keys or values, unless it has no value") {
    val headerKeys =
      Gen.oneOf(WireFormat.KeySchemaIdHeader, WireFormat.ValueSchemaIdHeader, "other")

    val headerLists =
      Gen.listOf(Gen.zip(headerKeys, Gen.option(schemaIdentifiers)))

    forall(Gen.zip(booleans, headerLists, schemaIds, byteVectors)) { case (isKey, headerList, id, value) =>
      val headers =
        Headers.fromIterable(headerList.map { case (key, identifier) =>
          Header(key, identifier.map(headerValue).orNull)
        })

      val prefixed = WireFormat.withSchemaId(id, value)
      val read = WireFormat.read(isKey, headers, prefixed)

      headerList.filter(_._1 == headerKey(isKey)).lastOption.flatMap(_._2) match {
        case Some(identifier) =>
          expect.same(Right((writerSchemaId(identifier), ByteVector.view(prefixed))), read)
        case None =>
          expect.same(Right((WireFormat.Id(id), value)), read)
      }
    }
  }

  pureTest("values without schema identifiers in headers fail if they are null or empty") {
    forEach(List[Array[Byte]](null, Array.emptyByteArray)) { value =>
      matches(WireFormat.read(isKey = false, Headers.empty, value)) {
        case Left(error: IllegalArgumentException) =>
          expect.same("Invalid Avro record: bytes is null or empty", error.getMessage)
      }
    }
  }

  test("values without schema identifiers in headers fail for unknown magic bytes") {
    val magicBytes =
      Gen.oneOf(Gen.choose(Byte.MinValue, (-1).toByte), Gen.choose(1.toByte, Byte.MaxValue))

    forall(Gen.zip(booleans, magicBytes, byteVectors)) { case (isKey, magicByte, value) =>
      matches(WireFormat.read(isKey, Headers.empty, (magicByte +: value).toArray)) {
        case Left(error: DeserializationException) =>
          expect.same(s"Unknown magic byte $magicByte, expected magic byte 0", error.getMessage)
      }
    }
  }

  test("values without schema identifiers in headers fail if they are too short for the schema id") {
    forall(Gen.zip(booleans, Gen.choose(0, 3).flatMap(Gen.listOfN(_, bytes)))) { case (isKey, idBytes) =>
      val value = (0.toByte :: idBytes).toArray
      matches(WireFormat.read(isKey, Headers.empty, value)) { case Left(error: DeserializationException) =>
        expect.same(
          s"Invalid Avro record: got ${value.length} bytes, expected at least 5 bytes",
          error.getMessage
        )
      }
    }
  }

  test(
    "schema identifiers in headers fail unless they are 17 bytes with magic byte 1, or 5 bytes with magic byte 0"
  ) {
    val otherSizes =
      Gen.oneOf((0 to 32).filter(size => size != 5 && size != 17)).flatMap(Gen.listOfN(_, bytes))

    def otherMagicBytes(size: Int, magicByte: Byte): Gen[List[Byte]] =
      Gen
        .zip(
          bytes.map(byte => if (byte == magicByte) (byte + 1).toByte else byte),
          Gen.listOfN(size - 1, bytes)
        )
        .map { case (first, rest) =>
          first :: rest
        }

    val invalidValues =
      Gen.oneOf(otherSizes, otherMagicBytes(17, 1), otherMagicBytes(5, 0))

    forall(Gen.zip(booleans, invalidValues, byteVectors)) { case (isKey, invalid, value) =>
      val key = headerKey(isKey)
      matches(WireFormat.read(isKey, Headers(Header(key, invalid.toArray)), value.toArray)) {
        case Left(error: DeserializationException) =>
          expect.same(
            s"Invalid schema identifier in header $key: expected 17 bytes with magic byte 1, or 5 bytes with magic byte 0, got ${invalid.size} bytes",
            error.getMessage
          )
      }
    }
  }
}
