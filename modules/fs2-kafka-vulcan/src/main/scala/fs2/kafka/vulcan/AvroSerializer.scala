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
import fs2.kafka.KeySerializer
import fs2.kafka.SerializationException
import fs2.kafka.Serializer
import fs2.kafka.ValueSerializer
import fs2.kafka.vulcan.internal.WireFormat
import org.http4s.schemaregistry
import org.http4s.schemaregistry.SchemaGuid
import org.http4s.schemaregistry.SchemaId
import org.http4s.schemaregistry.SchemaRegistryClient
import org.http4s.schemaregistry.SchemaType
import org.http4s.schemaregistry.SubjectName
import org.http4s.schemaregistry.VersionSelector
import org.typelevel.vulcan.BinaryEncoder
import org.typelevel.vulcan.Codec
import org.typelevel.vulcan.Schema
import scodec.bits.ByteVector

/**
  * Creates serializers for record keys and values with Vulcan `Codec`s.
  *
  * Serializers use the Confluent wire format, which is the Avro binary
  * prefixed by a zero magic byte and the 4-byte schema id.
  *
  * By default, the codec schema is registered under the subject decided
  * by the [[subjectNameStrategy]], unless [[autoRegisterSchemas]] is set
  * to `false`, in which case the schema has to be pre-registered. Use
  * [[withLatestVersion]] or [[withSchemaId]] to write values using another
  * schema, and [[withSchemaIdHeaders]] to write schema identifiers in
  * record headers.
  *
  * Use [[AvroSerializer.default]] to create an instance.
  */
sealed abstract class AvroSerializer[F[_]] {

  /** The client for the schema registry where schemas are registered or looked up. */
  def schemaRegistryClient: SchemaRegistryClient[F]

  /**
    * Whether to register codec schemas which are not already registered.
    *
    * If `false`, schemas are looked up, and serialization fails if they
    * are not already registered.
    *
    * The default value is `true`. Returns `false` when writing values using the latest
    * version of subjects, or using a schema id.
    */
  def autoRegisterSchemas: Boolean

  /**
    * Returns a new [[AvroSerializer]] writing values using the schema of the codec, which is
    * registered automatically if `autoRegisterSchemas` is `true`, and looked up otherwise.
    */
  def withAutoRegisterSchemas(autoRegisterSchemas: Boolean): AvroSerializer[F]

  /**
    * Returns a new [[AvroSerializer]] writing values using the latest version of the subject,
    * rather than the schema of the codec, like `use.latest.version` for the Confluent serializers.
    * Schemas are not registered automatically.
    *
    * Values are resolved into the latest version following Avro's schema resolution, so fields
    * missing in the codec use the defaults of the latest version. Serialization fails if values
    * of the codec cannot be written using the latest version. Latest versions are cached by the
    * schema registry client, which can expire them using `withLatestCacheExpiry`.
    */
  def withLatestVersion: AvroSerializer[F]

  /**
    * Returns a new [[AvroSerializer]] writing values using the schema with the specified id,
    * rather than the schema of the codec, like `use.schema.id` for the Confluent serializers.
    * Schemas are not registered automatically.
    *
    * Values are resolved into the schema following Avro's schema resolution, and serialization
    * fails if values of the codec cannot be written using the schema.
    */
  def withSchemaId(id: SchemaId): AvroSerializer[F]

  /**
    * Whether the schema registry should normalize schemas when registering
    * and looking them up, so that semantically equivalent schemas are the
    * same schema.
    *
    * The default value is `false`.
    */
  def normalizeSchemas: Boolean

  /** Returns a new [[AvroSerializer]] with the specified [[normalizeSchemas]] setting. */
  def withNormalizeSchemas(normalizeSchemas: Boolean): AvroSerializer[F]

  /**
    * The subject name strategy with which schemas are registered and looked up.
    *
    * The default value is [[SubjectNameStrategy.TopicName]].
    */
  def subjectNameStrategy: SubjectNameStrategy

  /** Returns a new [[AvroSerializer]] with the specified [[subjectNameStrategy]]. */
  def withSubjectNameStrategy(subjectNameStrategy: SubjectNameStrategy): AvroSerializer[F]

  /**
    * Whether to write schema identifiers in record headers, rather than before the Avro binary,
    * like the Confluent `HeaderSchemaIdSerializer`. The schema GUID is written in the
    * `__key_schema_id` or `__value_schema_id` header, which requires a schema registry
    * supporting schema GUIDs, like Confluent Schema Registry 8.0 and later.
    *
    * The default value is `false`.
    */
  def schemaIdHeaders: Boolean

  /** Returns a new [[AvroSerializer]] with the specified [[schemaIdHeaders]] setting. */
  def withSchemaIdHeaders(schemaIdHeaders: Boolean): AvroSerializer[F]

  /** Returns a serializer for record keys, failing if the codec schema is invalid. */
  def forKey[A](implicit codec: Codec[A]): Resource[F, KeySerializer[F, A]]

  /** Returns a serializer for record values, failing if the codec schema is invalid. */
  def forValue[A](implicit codec: Codec[A]): Resource[F, ValueSerializer[F, A]]

  /**
    * Registers the codec schema under the specified subject,
    * unless already registered, and returns the schema id.
    */
  def registerSchema[A](subject: SubjectName)(implicit codec: Codec[A]): F[SchemaId]

  /**
    * Registers the codec schema under the subject for record keys
    * of the topic, decided by the [[subjectNameStrategy]], unless
    * already registered, and returns the schema id.
    */
  def registerKeySchema[A](topic: String)(implicit codec: Codec[A]): F[SchemaId]

  /**
    * Registers the codec schema under the subject for record values
    * of the topic, decided by the [[subjectNameStrategy]], unless
    * already registered, and returns the schema id.
    */
  def registerValueSchema[A](topic: String)(implicit codec: Codec[A]): F[SchemaId]
}

object AvroSerializer {
  private sealed abstract class WriterSchema extends Product with Serializable

  private object WriterSchema {

    /** The codec schema, which is registered if `register` is `true`, and looked up otherwise. */
    final case class CodecSchema(register: Boolean) extends WriterSchema

    /** The latest version of the subject. */
    case object LatestVersion extends WriterSchema

    /** The schema with the specified id. */
    final case class WithSchemaId(id: SchemaId) extends WriterSchema
  }

  /** A value written using the schema with the id, and the GUID of the schema, if needed. */
  private final case class Written(id: SchemaId, guid: Option[SchemaGuid], bytes: ByteVector)

  private final case class AvroSerializerImpl[F[_]](
    override val schemaRegistryClient: SchemaRegistryClient[F],
    writerSchema: WriterSchema,
    override val normalizeSchemas: Boolean,
    override val subjectNameStrategy: SubjectNameStrategy,
    override val schemaIdHeaders: Boolean
  )(implicit F: Sync[F])
    extends AvroSerializer[F] {
    override def autoRegisterSchemas: Boolean =
      writerSchema match {
        case WriterSchema.CodecSchema(register) => register
        case _ => false
      }

    override def withAutoRegisterSchemas(autoRegisterSchemas: Boolean): AvroSerializer[F] =
      copy(writerSchema = WriterSchema.CodecSchema(autoRegisterSchemas))

    override def withLatestVersion: AvroSerializer[F] =
      copy(writerSchema = WriterSchema.LatestVersion)

    override def withSchemaId(id: SchemaId): AvroSerializer[F] =
      copy(writerSchema = WriterSchema.WithSchemaId(id))

    override def withNormalizeSchemas(normalizeSchemas: Boolean): AvroSerializer[F] =
      copy(normalizeSchemas = normalizeSchemas)

    override def withSubjectNameStrategy(subjectNameStrategy: SubjectNameStrategy): AvroSerializer[F] =
      copy(subjectNameStrategy = subjectNameStrategy)

    override def withSchemaIdHeaders(schemaIdHeaders: Boolean): AvroSerializer[F] =
      copy(schemaIdHeaders = schemaIdHeaders)

    override def forKey[A](implicit codec: Codec[A]): Resource[F, KeySerializer[F, A]] =
      create(isKey = true)

    override def forValue[A](implicit codec: Codec[A]): Resource[F, ValueSerializer[F, A]] =
      create(isKey = false)

    override def registerSchema[A](subject: SubjectName)(implicit codec: Codec[A]): F[SchemaId] =
      codec.schema.liftTo[F].flatMap(register(subject, _))

    override def registerKeySchema[A](topic: String)(implicit codec: Codec[A]): F[SchemaId] =
      registerForTopic(topic, isKey = true)

    override def registerValueSchema[A](topic: String)(implicit codec: Codec[A]): F[SchemaId] =
      registerForTopic(topic, isKey = false)

    override def toString: String = {
      val schema =
        writerSchema match {
          case WriterSchema.CodecSchema(register) => s"autoRegisterSchemas = $register"
          case WriterSchema.LatestVersion => "latestVersion = true"
          case WriterSchema.WithSchemaId(id) => s"schemaId = ${id.value}"
        }

      s"AvroSerializer($schema, normalizeSchemas = $normalizeSchemas, subjectNameStrategy = $subjectNameStrategy, schemaIdHeaders = $schemaIdHeaders)"
    }

    private def create[A](isKey: Boolean)(implicit codec: Codec[A]): Resource[F, Serializer[F, A]] =
      (
        codec.schema.liftTo[F],
        codec.binaryEncoder.liftTo[F],
        MapRef.ofConcurrentHashMap[F, SchemaId, Either[Throwable, BinaryEncoder[A]]]()
      ).tupled.toResource
        .map { case (schema, encoder, encoders) =>
          def write(topic: String, a: A): F[Written] =
            subjectOf(topic, isKey, schema).flatMap { subject =>
              writerSchema match {
                case WriterSchema.CodecSchema(register) =>
                  for {
                    id <- codecSchemaId(subject, schema, register)
                    guid <- if (schemaIdHeaders) guidOf(subject, registrySchema(schema)) else F.pure(None)
                    bytes <- encoder.encode(a).liftTo[F]
                  } yield Written(id, guid, bytes)

                case WriterSchema.LatestVersion =>
                  for {
                    latest <- schemaRegistryClient.getSubjectVersion(subject, VersionSelector.Latest)
                    encoder <- writerEncoder(encoders, latest.id, latest.schema)
                    bytes <- encoder.encode(a).liftTo[F]
                  } yield Written(latest.id, latest.guid, bytes)

                case WriterSchema.WithSchemaId(id) =>
                  for {
                    writer <- schemaRegistryClient.getSchema(id)
                    encoder <- writerEncoder(encoders, id, writer)
                    guid <- if (schemaIdHeaders) guidOf(subject, writer) else F.pure(None)
                    bytes <- encoder.encode(a).liftTo[F]
                  } yield Written(id, guid, bytes)
              }
            }

          if (schemaIdHeaders)
            Serializer.instanceWithHeaders[F, A] { (topic, headers, a) =>
              write(topic, a).flatMap { written =>
                written.guid match {
                  case Some(guid) =>
                    // Encoders do not reuse arrays after encoding, so the array can be used without copying
                    WireFormat
                      .withSchemaGuidHeader(isKey, headers, guid)
                      .liftTo[F]
                      .map((_, written.bytes.toArrayUnsafe))
                  case None =>
                    F.raiseError(
                      SerializationException(
                        s"Schema registry did not return a GUID for schema id ${written.id.value}, which is " +
                          "required for schema identifiers in headers, so the registry might not support schema GUIDs"
                      )
                    )
                }
              }
            }
          else
            Serializer.instance[F, A] { (topic, _, a) =>
              write(topic, a).map(written => WireFormat.withSchemaId(written.id, written.bytes))
            }
        }

    /**
      * Returns the encoder for values written using the schema with the id, compiling it if it is not
      * cached. Schemas which values cannot be written as are cached as errors, since they do not change.
      */
    private def writerEncoder[A](
      encoders: MapRef[F, SchemaId, Option[Either[Throwable, BinaryEncoder[A]]]],
      id: SchemaId,
      writer: schemaregistry.Schema
    )(implicit codec: Codec[A]): F[BinaryEncoder[A]] =
      encoders(id).get.flatMap {
        case Some(encoder) =>
          encoder.liftTo[F]

        case None =>
          val encoder =
            if (writer.schemaType != SchemaType.Avro)
              Left(
                SerializationException(
                  s"Expected an Avro schema for schema id ${id.value}, got schema type ${writer.schemaType.name}"
                )
              )
            else if (writer.references.nonEmpty)
              Left(
                SerializationException(
                  s"Schema references are not supported, but the schema with id ${id.value} has references"
                )
              )
            else Schema.parse(writer.definition).flatMap(codec.binaryEncoder(_))

          encoders(id).set(Some(encoder)) >> encoder.liftTo[F]
      }

    private def registerForTopic[A](topic: String, isKey: Boolean)(implicit codec: Codec[A]): F[SchemaId] =
      for {
        schema <- codec.schema.liftTo[F]
        subject <- subjectOf(topic, isKey, schema)
        id <- register(subject, schema)
      } yield id

    private def subjectOf(topic: String, isKey: Boolean, schema: Schema): F[SubjectName] =
      subjectNameStrategy.subject(topic, isKey, schema).liftTo[F]

    private def codecSchemaId(subject: SubjectName, schema: Schema, register: Boolean): F[SchemaId] =
      if (register) this.register(subject, schema)
      else
        schemaRegistryClient.lookupSchema(subject, registrySchema(schema), normalizeSchemas).flatMap {
          case Some(registered) =>
            registered.id.pure[F]
          case None =>
            F.raiseError(
              SerializationException(
                s"Schema is not registered under subject ${subject.value}, and registering schemas automatically is disabled"
              )
            )
        }

    /** Returns the GUID of the schema as registered under the subject, if the registry supports GUIDs. */
    private def guidOf(subject: SubjectName, schema: schemaregistry.Schema): F[Option[SchemaGuid]] =
      schemaRegistryClient.lookupSchema(subject, schema, normalizeSchemas).map(_.flatMap(_.guid))

    private def register(subject: SubjectName, schema: Schema): F[SchemaId] =
      schemaRegistryClient.registerSchema(subject, registrySchema(schema), normalizeSchemas)
  }

  private def registrySchema(schema: Schema): org.http4s.schemaregistry.Schema =
    org.http4s.schemaregistry.Schema(schema.toString)

  /** Returns an [[AvroSerializer]] using the specified schema registry client and default settings. */
  def default[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient[F]): AvroSerializer[F] =
    AvroSerializerImpl(
      schemaRegistryClient = schemaRegistryClient,
      writerSchema = WriterSchema.CodecSchema(register = true),
      normalizeSchemas = false,
      subjectNameStrategy = SubjectNameStrategy.TopicName,
      schemaIdHeaders = false
    )
}
