/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan.testkit

import cats.effect.IO
import cats.effect.Resource
import fs2.kafka.vulcan.AvroSerializer
import fs2.kafka.vulcan.TestRecord
import fs2.kafka.vulcan.TestRecordV2
import munit.AnyFixture
import munit.FailException
import org.http4s.schemaregistry.Compatibility
import org.http4s.schemaregistry.SubjectName
import org.http4s.schemaregistry.TestSchemaRegistry
import org.typelevel.vulcan.Codec

import cats.effect.unsafe.implicits.global

final class SchemaSuiteSuite extends SchemaSuite {
  val subject: SubjectName =
    SubjectName("test")

  /** Registry where schemas with a `number` field are incompatible with the `test` subject. */
  val registry: Resource[IO, TestSchemaRegistry[IO]] =
    Resource.eval {
      for {
        registry <- TestSchemaRegistry.withCompatibility[IO] { (_, schema, _) =>
          if (schema.definition.contains("number"))
            Compatibility.Incompatible(
              List("READER_FIELD_MISSING_DEFAULT_VALUE: number", "oldSchemaVersion: 1")
            )
          else Compatibility.Compatible
        }
        client <- registry.client.build
        _ <- AvroSerializer.default(client).registerSchema[TestRecord](subject)
      } yield registry
    }

  val checker: Fixture[AssertableCompatibilityChecker[IO]] =
    compatibilityChecker(registry.evalMap(_.client.build))

  override def munitFixtures: Seq[AnyFixture[?]] =
    List(checker)

  test("checkCompatibility returns the compatibility of the codec with the subject") {
    assertEquals(
      checker().checkCompatibility(Codec[TestRecord], subject).unsafeRunSync(),
      Compatibility.Compatible
    )
    assertEquals(
      checker().checkCompatibility(Codec[TestRecordV2], subject).unsafeRunSync(),
      Compatibility.Incompatible(List("READER_FIELD_MISSING_DEFAULT_VALUE: number", "oldSchemaVersion: 1"))
    )
  }

  test("assertCompatibility succeeds for compatible codecs") {
    checker().assertCompatibility(Codec[TestRecord], subject).unsafeRunSync()
  }

  test("assertCompatibility fails with the incompatibilities for incompatible codecs") {
    val error =
      intercept[FailException](checker().assertCompatibility(Codec[TestRecordV2], subject).unsafeRunSync())
    assert(
      error.getMessage.contains(
        "Schema is incompatible with the latest version of subject test:\n" +
          "1) READER_FIELD_MISSING_DEFAULT_VALUE: number\n" +
          "2) oldSchemaVersion: 1"
      ),
      error.getMessage
    )
  }

  test("checkCompatibility fails for missing subjects") {
    val error = intercept[FailException] {
      checker().checkCompatibility(Codec[TestRecord], SubjectName("missing")).unsafeRunSync()
    }
    assert(
      error.getMessage.contains("Subject missing was not found in the schema registry"),
      error.getMessage
    )
  }

  test("checkCompatibility fails for codecs with invalid schemas") {
    val error = intercept[FailException] {
      checker().checkCompatibility(Codec.decimal(-1, -1), subject).unsafeRunSync()
    }
    assert(
      error.getMessage.contains(
        "Invalid schema for codec checked against subject test: Invalid decimal precision -1: must be positive"
      ),
      error.getMessage
    )
  }
}
