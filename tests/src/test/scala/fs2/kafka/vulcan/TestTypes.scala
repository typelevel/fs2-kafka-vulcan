/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.Show
import cats.syntax.all.*
import org.typelevel.vulcan.Codec

final case class TestRecord(name: String)

object TestRecord {
  implicit val codec: Codec[TestRecord] =
    Codec.record(
      name = "Test",
      namespace = "fs2.kafka.vulcan"
    ) { field =>
      field("name", _.name).map(TestRecord(_))
    }

  implicit val show: Show[TestRecord] =
    Show.fromToString
}

final case class TestRecordV2(name: String, number: Int)

object TestRecordV2 {
  implicit val codec: Codec[TestRecordV2] =
    Codec.record(
      name = "Test",
      namespace = "fs2.kafka.vulcan"
    ) { field =>
      (
        field("name", _.name),
        field("number", _.number)
      ).mapN(TestRecordV2(_, _))
    }

  implicit val show: Show[TestRecordV2] =
    Show.fromToString
}
