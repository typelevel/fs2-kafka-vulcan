/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import fs2.kafka.vulcan.Generators.*
import org.http4s.schemaregistry.SubjectName
import org.scalacheck.Gen
import org.typelevel.vulcan.Codec
import org.typelevel.vulcan.Schema
import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers

object SubjectNameStrategySuite extends SimpleIOSuite with Checkers {
  val record: Schema =
    Codec[TestRecord].schema.fold(throw _, identity)

  val union: Schema =
    Codec[Either[TestRecord, Int]].schema.fold(throw _, identity)

  test("TopicName uses the topic, and whether the schema is for keys or values") {
    forall(strings) { topic =>
      expect.same(
        Right(SubjectName(s"$topic-key")),
        SubjectNameStrategy.TopicName.subject(topic, isKey = true, union)
      ) &&
      expect.same(
        Right(SubjectName(s"$topic-value")),
        SubjectNameStrategy.TopicName.subject(topic, isKey = false, union)
      )
    }
  }

  test("RecordName uses the full name of records") {
    forall(Gen.zip(strings, booleans)) { case (topic, isKey) =>
      expect.same(
        Right(SubjectName("fs2.kafka.vulcan.Test")),
        SubjectNameStrategy.RecordName.subject(topic, isKey, record)
      )
    }
  }

  test("TopicRecordName uses the topic and the full name of records") {
    forall(Gen.zip(strings, booleans)) { case (topic, isKey) =>
      expect.same(
        Right(SubjectName(s"$topic-fs2.kafka.vulcan.Test")),
        SubjectNameStrategy.TopicRecordName.subject(topic, isKey, record)
      )
    }
  }

  pureTest("RecordName and TopicRecordName fail for schemas which are not records") {
    forEach(List(SubjectNameStrategy.RecordName, SubjectNameStrategy.TopicRecordName)) { strategy =>
      matches(strategy.subject("topic", isKey = false, union)) { case Left(error) =>
        expect.same(
          s"""Subject name strategy $strategy requires a record schema, got [{"type":"record","name":"Test","namespace":"fs2.kafka.vulcan","fields":[{"name":"name","type":"string"}]},"int"]""",
          error.getMessage
        )
      }
    }
  }

  test("custom uses the function") {
    val strategy =
      SubjectNameStrategy.custom((topic, isKey, schema) => Right(SubjectName(s"$topic-$isKey-$schema")))

    forall(Gen.zip(strings, booleans)) { case (topic, isKey) =>
      expect.same(Right(SubjectName(s"$topic-$isKey-\"int\"")), strategy.subject(topic, isKey, Schema.Int))
    }
  }

  pureTest("toString") {
    expect.same("TopicName", SubjectNameStrategy.TopicName.toString) &&
    expect.same("RecordName", SubjectNameStrategy.RecordName.toString) &&
    expect.same("TopicRecordName", SubjectNameStrategy.TopicRecordName.toString) &&
    expect(
      SubjectNameStrategy
        .custom((_, _, _) => Right(SubjectName("")))
        .toString
        .startsWith("SubjectNameStrategy$")
    )
  }
}
