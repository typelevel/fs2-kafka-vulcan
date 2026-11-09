/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import fs2.kafka.SerializationException
import org.http4s.schemaregistry.SubjectName
import org.typelevel.vulcan.Schema

/**
  * Strategy deciding the subject under which schemas
  * are registered and looked up in a schema registry.
  */
sealed abstract class SubjectNameStrategy {

  /**
    * Returns the subject name to use for the specified topic
    * and schema, and whether the schema is for keys or values.
    */
  def subject(topic: String, isKey: Boolean, schema: Schema): Either[Throwable, SubjectName]
}

object SubjectNameStrategy {
  private final class SubjectNameStrategyImpl(
    name: String,
    subjectName: (String, Boolean, Schema) => Either[Throwable, SubjectName]
  ) extends SubjectNameStrategy {
    override def subject(topic: String, isKey: Boolean, schema: Schema): Either[Throwable, SubjectName] =
      subjectName(topic, isKey, schema)

    override def toString: String =
      name
  }

  /** Uses `<topic>-key` for record keys, and `<topic>-value` for record values. */
  val TopicName: SubjectNameStrategy =
    new SubjectNameStrategyImpl(
      "TopicName",
      (topic, isKey, _) => Right(SubjectName(if (isKey) s"$topic-key" else s"$topic-value"))
    )

  /**
    * Uses the full name of the record, for example `com.example.Person`.
    *
    * Fails serialization for schemas which are not records.
    */
  val RecordName: SubjectNameStrategy =
    new SubjectNameStrategyImpl(
      "RecordName",
      (_, _, schema) => recordName("RecordName", schema).map(SubjectName(_))
    )

  /**
    * Uses `<topic>-<full name of the record>`, for example `people-com.example.Person`.
    *
    * Fails serialization for schemas which are not records.
    */
  val TopicRecordName: SubjectNameStrategy =
    new SubjectNameStrategyImpl(
      "TopicRecordName",
      (topic, _, schema) => recordName("TopicRecordName", schema).map(name => SubjectName(s"$topic-$name"))
    )

  /**
    * Returns a custom [[SubjectNameStrategy]] using the specified function.
    */
  def custom(subjectName: (String, Boolean, Schema) => Either[Throwable, SubjectName]): SubjectNameStrategy =
    new SubjectNameStrategyImpl("SubjectNameStrategy$" + System.identityHashCode(subjectName), subjectName)

  private def recordName(strategy: String, schema: Schema): Either[Throwable, String] =
    schema match {
      case record: Schema.Record =>
        Right(record.fullName)
      case _ =>
        Left(SerializationException(s"Subject name strategy $strategy requires a record schema, got $schema"))
    }
}
