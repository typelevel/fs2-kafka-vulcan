/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.Resource
import cats.effect.Sync
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.kafka.KeySerializer
import fs2.kafka.SerializationException
import fs2.kafka.Serializer
import fs2.kafka.ValueSerializer
import fs2.kafka.vulcan.internal.WireFormat
import org.http4s.schemaregistry.SchemaId
import org.http4s.schemaregistry.SchemaRegistryClient
import org.http4s.schemaregistry.SubjectName
import org.typelevel.vulcan.Codec
import org.typelevel.vulcan.Schema

/**
  * Creates serializers for record keys and values with Vulcan `Codec`s.
  *
  * Serializers use the Confluent wire format, which is the Avro binary
  * prefixed by a zero magic byte and the 4-byte schema id.
  *
  * The codec schema is registered under the subject decided by the
  * [[subjectNameStrategy]], unless [[autoRegisterSchemas]] is set
  * to `false`, in which case the schema has to be pre-registered.
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
    * The default value is `true`.
    */
  def autoRegisterSchemas: Boolean

  /** Returns a new [[AvroSerializer]] with the specified [[autoRegisterSchemas]] setting. */
  def withAutoRegisterSchemas(autoRegisterSchemas: Boolean): AvroSerializer[F]

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
  private final case class AvroSerializerImpl[F[_]](
    override val schemaRegistryClient: SchemaRegistryClient[F],
    override val autoRegisterSchemas: Boolean,
    override val normalizeSchemas: Boolean,
    override val subjectNameStrategy: SubjectNameStrategy
  )(implicit F: Sync[F])
    extends AvroSerializer[F] {
    override def withAutoRegisterSchemas(autoRegisterSchemas: Boolean): AvroSerializer[F] =
      copy(autoRegisterSchemas = autoRegisterSchemas)

    override def withNormalizeSchemas(normalizeSchemas: Boolean): AvroSerializer[F] =
      copy(normalizeSchemas = normalizeSchemas)

    override def withSubjectNameStrategy(subjectNameStrategy: SubjectNameStrategy): AvroSerializer[F] =
      copy(subjectNameStrategy = subjectNameStrategy)

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

    override def toString: String =
      s"AvroSerializer(autoRegisterSchemas = $autoRegisterSchemas, normalizeSchemas = $normalizeSchemas, subjectNameStrategy = $subjectNameStrategy)"

    private def create[A](isKey: Boolean)(implicit codec: Codec[A]): Resource[F, Serializer[F, A]] =
      (codec.schema, codec.binaryEncoder).tupled.liftTo[F].toResource.map { case (schema, encoder) =>
        Serializer.instance[F, A] { (topic, _, a) =>
          for {
            subject <- subjectOf(topic, isKey, schema)
            id <- schemaId(subject, schema)
            bytes <- encoder.encode(a).liftTo[F]
          } yield WireFormat.withSchemaId(id, bytes)
        }
      }

    private def registerForTopic[A](topic: String, isKey: Boolean)(implicit codec: Codec[A]): F[SchemaId] =
      for {
        schema <- codec.schema.liftTo[F]
        subject <- subjectOf(topic, isKey, schema)
        id <- register(subject, schema)
      } yield id

    private def subjectOf(topic: String, isKey: Boolean, schema: Schema): F[SubjectName] =
      subjectNameStrategy.subject(topic, isKey, schema).liftTo[F]

    private def schemaId(subject: SubjectName, schema: Schema): F[SchemaId] =
      if (autoRegisterSchemas) register(subject, schema)
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

    private def register(subject: SubjectName, schema: Schema): F[SchemaId] =
      schemaRegistryClient.registerSchema(subject, registrySchema(schema), normalizeSchemas)
  }

  private def registrySchema(schema: Schema): org.http4s.schemaregistry.Schema =
    org.http4s.schemaregistry.Schema(schema.toString)

  /** Returns an [[AvroSerializer]] using the specified schema registry client and default settings. */
  def default[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient[F]): AvroSerializer[F] =
    AvroSerializerImpl(
      schemaRegistryClient = schemaRegistryClient,
      autoRegisterSchemas = true,
      normalizeSchemas = false,
      subjectNameStrategy = SubjectNameStrategy.TopicName
    )
}
