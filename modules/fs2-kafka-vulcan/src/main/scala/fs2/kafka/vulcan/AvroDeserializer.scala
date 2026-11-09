/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.Resource
import cats.effect.Sync
import cats.effect.std.MapRef
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.kafka.DeserializationException
import fs2.kafka.Deserializer
import fs2.kafka.KeyDeserializer
import fs2.kafka.ValueDeserializer
import fs2.kafka.vulcan.internal.WireFormat
import fs2.kafka.vulcan.internal.WireFormat.WriterSchemaId
import org.http4s.schemaregistry.SchemaRegistryClient
import org.http4s.schemaregistry.SchemaType
import org.typelevel.vulcan.BinaryDecoder
import org.typelevel.vulcan.Codec
import org.typelevel.vulcan.Schema

/**
  * Creates deserializers for record keys and values with Vulcan `Codec`s.
  *
  * Deserializers use the Confluent wire format, which is the Avro binary
  * prefixed by a zero magic byte and the 4-byte schema id. There is also
  * support for the `__key_schema_id` and `__value_schema_id` headers, in
  * which case the Avro binary is not prefixed.
  *
  * Deserialization is done using the writer schema from the schema registry,
  * following Avro's schema resolution, where decoders are compiled once for
  * each writer schema.
  *
  * Use [[AvroDeserializer.default]] to create an instance.
  */
sealed abstract class AvroDeserializer[F[_]] {

  /** The client for the schema registry where writer schemas are looked up. */
  def schemaRegistryClient: SchemaRegistryClient[F]

  /** Returns a deserializer for record keys, failing if the codec schema is invalid. */
  def forKey[A](implicit codec: Codec[A]): Resource[F, KeyDeserializer[F, A]]

  /** Returns a deserializer for record values, failing if the codec schema is invalid. */
  def forValue[A](implicit codec: Codec[A]): Resource[F, ValueDeserializer[F, A]]
}

object AvroDeserializer {
  private final case class AvroDeserializerImpl[F[_]](
    override val schemaRegistryClient: SchemaRegistryClient[F]
  )(implicit F: Sync[F])
    extends AvroDeserializer[F] {
    override def forKey[A](implicit codec: Codec[A]): Resource[F, KeyDeserializer[F, A]] =
      create(isKey = true)

    override def forValue[A](implicit codec: Codec[A]): Resource[F, ValueDeserializer[F, A]] =
      create(isKey = false)

    override def toString: String =
      s"AvroDeserializer($schemaRegistryClient)"

    private def create[A](isKey: Boolean)(implicit codec: Codec[A]): Resource[F, Deserializer[F, A]] =
      for {
        _ <- codec.schema.liftTo[F].toResource
        decoders <- MapRef.ofConcurrentHashMap[F, WriterSchemaId, Decoder[A]]().toResource
        deserializer = Deserializer.instance[F, A] { (_, headers, bytes) =>
          WireFormat.read(isKey, headers, bytes).liftTo[F].flatMap { case (id, payload) =>
            decoder(decoders, id).flatMap(_.decode(payload).liftTo[F])
          }
        }
      } yield deserializer

    private def decoder[A](
      decoders: MapRef[F, WriterSchemaId, Option[Decoder[A]]],
      id: WriterSchemaId
    )(implicit codec: Codec[A]): F[BinaryDecoder[A]] =
      decoders(id).get.flatMap {
        case Some(decoder) =>
          decoder.liftTo[F]
        case None =>
          getWriterSchema(id).flatMap { writerSchema =>
            val decoder = writerSchema.flatMap(codec.binaryDecoder)
            decoders(id).set(Some(decoder)) >> decoder.liftTo[F]
          }
      }

    private def getWriterSchema(id: WriterSchemaId): F[Either[Throwable, Schema]] =
      getSchema(id).map {
        case schema if schema.schemaType != SchemaType.Avro =>
          Left(
            DeserializationException(
              s"Expected an Avro schema for $id, got schema type ${schema.schemaType.name}"
            )
          )
        case schema if schema.references.nonEmpty =>
          Left(
            DeserializationException(
              s"Schema references are not supported, but the schema for $id has references"
            )
          )
        case schema =>
          Schema.parse(schema.definition)
      }

    private def getSchema(id: WriterSchemaId): F[org.http4s.schemaregistry.Schema] =
      id match {
        case WireFormat.Id(id) => schemaRegistryClient.getSchema(id)
        case WireFormat.Guid(guid) => schemaRegistryClient.getSchemaByGuid(guid)
      }
  }

  private type Decoder[A] = Either[Throwable, BinaryDecoder[A]]

  /** Returns an [[AvroDeserializer]] using the specified schema registry client. */
  def default[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient[F]): AvroDeserializer[F] =
    AvroDeserializerImpl(schemaRegistryClient)
}
