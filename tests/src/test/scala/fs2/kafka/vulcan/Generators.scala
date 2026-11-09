/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.Show
import java.util.UUID
import org.http4s.schemaregistry.SchemaId
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import scodec.bits.ByteVector

object Generators {
  val strings: Gen[String] = {
    val codePoints =
      Gen.frequency(
        4 -> Gen.alphaNumChar.map(_.toInt),
        2 -> Gen.choose(0x0, 0x7f),
        1 -> Gen.choose(0x80, 0xd7ff),
        1 -> Gen.choose(0xe000, 0x10ffff)
      )

    Gen.listOf(codePoints).map(_.map(codePoint => new String(Character.toChars(codePoint))).mkString)
  }

  val booleans: Gen[Boolean] =
    Arbitrary.arbitrary[Boolean]

  val schemaIds: Gen[SchemaId] =
    Arbitrary.arbitrary[Int].map(SchemaId(_))

  val byteVectors: Gen[ByteVector] =
    Gen.listOf(Arbitrary.arbitrary[Byte]).map(ByteVector(_))

  val testRecords: Gen[TestRecord] =
    strings.map(TestRecord(_))

  val testRecordsV2: Gen[TestRecordV2] =
    Gen.zip(strings, Arbitrary.arbitrary[Int]).map { case (name, number) => TestRecordV2(name, number) }

  implicit val byteVectorShow: Show[ByteVector] =
    Show.fromToString

  implicit val uuidShow: Show[UUID] =
    Show.fromToString
}
