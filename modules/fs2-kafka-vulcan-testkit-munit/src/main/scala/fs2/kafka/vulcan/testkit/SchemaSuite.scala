/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan.testkit

import cats.effect.IO
import cats.effect.Resource
import munit.FunSuite
import org.http4s.schemaregistry.Compatibility
import org.http4s.schemaregistry.SchemaRegistryClient
import org.http4s.schemaregistry.SubjectName
import org.http4s.schemaregistry.VersionSelector
import org.typelevel.vulcan.Codec

import cats.effect.unsafe.implicits.global

/**
  * Checks whether codec schemas are compatible with the latest
  * version of subjects in a schema registry, according to the
  * compatibility level configured for the subjects.
  */
trait CompatibilityChecker[F[_]] {

  /**
    * Checks whether the codec schema is compatible with
    * the latest version of the subject, according to
    * the compatibility level of the subject.
    */
  def checkCompatibility[A](codec: Codec[A], subject: SubjectName): F[Compatibility]
}

/** [[CompatibilityChecker]] which can also assert that schemas are compatible. */
trait AssertableCompatibilityChecker[F[_]] extends CompatibilityChecker[F] {

  /**
    * Asserts the codec schema is compatible with the
    * latest version of the subject, according to the
    * compatibility level of the subject.
    */
  def assertCompatibility[A](codec: Codec[A], subject: SubjectName): F[Unit]
}

/** Provides MUnit fixtures for checking the compatibility of codecs with a schema registry. */
trait SchemaSuite extends FunSuite {

  /**
    * Returns a fixture for a [[AssertableCompatibilityChecker]] using
    * a schema registry client acquired before all tests of the suite,
    * and released after all tests of the suite.
    */
  def compatibilityChecker(
    schemaRegistryClient: Resource[IO, SchemaRegistryClient[IO]],
    name: String = "schema-compatibility-checker"
  ): Fixture[AssertableCompatibilityChecker[IO]] =
    new Fixture[AssertableCompatibilityChecker[IO]](name) {
      private var checker: AssertableCompatibilityChecker[IO] = null

      private var release: IO[Unit] = IO.unit

      override def apply(): AssertableCompatibilityChecker[IO] =
        checker

      override def beforeAll(): Unit = {
        val (client, release) = schemaRegistryClient.allocated.unsafeRunSync()
        this.checker = newCompatibilityChecker(client)
        this.release = release
      }

      override def afterAll(): Unit =
        release.unsafeRunSync()
    }

  /** Returns an [[AssertableCompatibilityChecker]] using the specified schema registry client. */
  def newCompatibilityChecker(
    schemaRegistryClient: SchemaRegistryClient[IO]
  ): AssertableCompatibilityChecker[IO] =
    new AssertableCompatibilityChecker[IO] {
      override def checkCompatibility[A](codec: Codec[A], subject: SubjectName): IO[Compatibility] =
        codec.schema match {
          case Right(schema) =>
            schemaRegistryClient.getSubjectVersionOption(subject, VersionSelector.Latest).flatMap {
              case Some(_) =>
                schemaRegistryClient.checkCompatibility(
                  subject,
                  org.http4s.schemaregistry.Schema(schema.toString)
                )
              case None =>
                IO(fail(s"Subject ${subject.value} was not found in the schema registry"))
            }

          case Left(error) =>
            IO(fail(s"Invalid schema for codec checked against subject ${subject.value}: ${error.message}"))
        }

      override def assertCompatibility[A](codec: Codec[A], subject: SubjectName): IO[Unit] =
        checkCompatibility(codec, subject).flatMap { compatibility =>
          IO(assert(compatibility.isCompatible, renderIncompatibilities(subject, compatibility)))
        }
    }

  private def renderIncompatibilities(subject: SubjectName, compatibility: Compatibility): String =
    if (compatibility.errors.isEmpty)
      s"Schema is incompatible with the latest version of subject ${subject.value}"
    else
      s"Schema is incompatible with the latest version of subject ${subject.value}:\n" +
        compatibility.errors.zipWithIndex
          .map { case (error, index) => s"${index + 1}) $error" }
          .mkString("\n")
}
