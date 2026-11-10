/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan.internal

import fs2.kafka.DeserializationException
import fs2.kafka.Header
import fs2.kafka.Headers
import fs2.kafka.SerializationException
import java.util.UUID
import org.http4s.schemaregistry.SchemaGuid
import org.http4s.schemaregistry.SchemaId
import scodec.bits.ByteVector

private[vulcan] object WireFormat {
  sealed abstract class WriterSchemaId extends Product with Serializable

  final case class Id(id: SchemaId) extends WriterSchemaId {
    override def toString: String =
      s"schema id ${id.value}"
  }

  final case class Guid(guid: SchemaGuid) extends WriterSchemaId {
    override def toString: String =
      s"schema GUID ${guid.value}"
  }

  final val IdMagicByte: Byte = 0

  final val GuidMagicByte: Byte = 1

  final val KeySchemaIdHeader: String = "__key_schema_id"

  final val ValueSchemaIdHeader: String = "__value_schema_id"

  /** The size of a magic byte followed by a 4-byte schema id. */
  private final val IdSize: Int = 5

  /** The size of a magic byte followed by a 16-byte schema GUID. */
  private final val GuidSize: Int = 17

  /** Returns the zero magic byte, followed by the schema id and the Avro binary. */
  def withSchemaId(id: SchemaId, bytes: ByteVector): Array[Byte] = {
    val result = new Array[Byte](IdSize + bytes.size.toInt)
    result(0) = IdMagicByte
    writeInt(result, 1, id.value)
    bytes.copyToArray(result, IdSize)
    result
  }

  /**
    * Returns the headers with the schema GUID in the `__key_schema_id` or `__value_schema_id` header,
    * as a magic byte of one followed by the 16-byte schema GUID, replacing headers with the same key,
    * like the Confluent `HeaderSchemaIdSerializer`. Fails for schema GUIDs which are not UUIDs.
    */
  def withSchemaGuidHeader(isKey: Boolean, headers: Headers, guid: SchemaGuid): Either[Throwable, Headers] =
    try {
      val uuid = UUID.fromString(guid.value)
      val key = if (isKey) KeySchemaIdHeader else ValueSchemaIdHeader
      val value = new Array[Byte](GuidSize)
      value(0) = GuidMagicByte
      writeLong(value, 1, uuid.getMostSignificantBits)
      writeLong(value, 9, uuid.getLeastSignificantBits)
      Right(Headers.fromChain(headers.toChain.filterNot(_.key == key)).append(Header(key, value)))
    } catch {
      case _: IllegalArgumentException =>
        Left(
          SerializationException(
            s"Schema GUID ${guid.value} is not a UUID, which is required for schema identifiers in headers"
          )
        )
    }

  /** Returns the identifier of the writer schema, and the Avro binary. */
  def read(
    isKey: Boolean,
    headers: Headers,
    bytes: Array[Byte]
  ): Either[Throwable, (WriterSchemaId, ByteVector)] = {
    val headerKey = if (isKey) KeySchemaIdHeader else ValueSchemaIdHeader
    lastHeader(headers, headerKey) match {
      case Some(header) =>
        fromHeader(header).map(id => (id, if (bytes == null) ByteVector.empty else ByteVector.view(bytes)))

      case None if bytes == null || bytes.length == 0 =>
        Left(new IllegalArgumentException("Invalid Avro record: bytes is null or empty"))

      case None if bytes(0) != IdMagicByte =>
        Left(DeserializationException(s"Unknown magic byte ${bytes(0)}, expected magic byte $IdMagicByte"))

      case None if bytes.length < IdSize =>
        Left(
          DeserializationException(
            s"Invalid Avro record: got ${bytes.length} bytes, expected at least $IdSize bytes"
          )
        )

      case None =>
        Right((Id(SchemaId(readInt(bytes, 1))), ByteVector.view(bytes).drop(IdSize.toLong)))
    }
  }

  private def lastHeader(headers: Headers, key: String): Option[Header] =
    headers.toChain.reverseIterator.find(_.key == key).filter(header => header.value != null)

  private def fromHeader(header: Header): Either[Throwable, WriterSchemaId] = {
    val value = header.value
    if (value.length == GuidSize && value(0) == GuidMagicByte)
      Right(Guid(SchemaGuid(new UUID(readLong(value, 1), readLong(value, 9)).toString)))
    else if (value.length == IdSize && value(0) == IdMagicByte)
      Right(Id(SchemaId(readInt(value, 1))))
    else
      Left(
        DeserializationException(
          s"Invalid schema identifier in header ${header.key}: expected $GuidSize bytes with magic byte $GuidMagicByte, " +
            s"or $IdSize bytes with magic byte $IdMagicByte, got ${value.length} bytes"
        )
      )
  }

  private def writeInt(bytes: Array[Byte], offset: Int, value: Int): Unit = {
    bytes(offset) = (value >>> 24).toByte
    bytes(offset + 1) = (value >>> 16).toByte
    bytes(offset + 2) = (value >>> 8).toByte
    bytes(offset + 3) = value.toByte
  }

  private def writeLong(bytes: Array[Byte], offset: Int, value: Long): Unit = {
    writeInt(bytes, offset, (value >>> 32).toInt)
    writeInt(bytes, offset + 4, value.toInt)
  }

  private def readInt(bytes: Array[Byte], offset: Int): Int =
    ((bytes(offset) & 0xff) << 24) |
      ((bytes(offset + 1) & 0xff) << 16) |
      ((bytes(offset + 2) & 0xff) << 8) |
      (bytes(offset + 3) & 0xff)

  private def readLong(bytes: Array[Byte], offset: Int): Long =
    (readInt(bytes, offset).toLong << 32) | (readInt(bytes, offset + 4).toLong & 0xffffffffL)
}
