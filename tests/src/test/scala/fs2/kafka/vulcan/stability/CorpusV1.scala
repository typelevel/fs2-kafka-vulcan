/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan.stability

import _root_.vulcan.AvroError
import _root_.vulcan.Codec
import _root_.vulcan.Props
import _root_.vulcan.generic.*
import cats.syntax.all.*
import java.time.Instant
import java.time.LocalDate
import java.util.UUID

/** Codecs defined using Vulcan 1.x, with schemas as registered by fs2-kafka-vulcan 4.x. */
object CorpusV1 {
  implicit val person: Codec[Person] =
    Codec.record(
      name = "Person",
      namespace = "com.example",
      doc = Some("A person"),
      aliases = Seq("Human"),
      props = Props.one("owner", "team")
    ) { field =>
      (
        field("name", _.name, doc = Some("The name")),
        field("age", _.age, default = Some(None)),
        field("tags", _.tags, default = Some(Nil)),
        field("attributes", _.attributes, default = Some(Map.empty[String, String]), aliases = Seq("attrs"))
      ).mapN(Person(_, _, _, _))
    }

  implicit val cat: Codec[Cat] =
    Codec.record(name = "Cat", namespace = "com.example") { field =>
      field("name", _.name).map(Cat(_))
    }

  implicit val dog: Codec[Dog] =
    Codec.record(name = "Dog", namespace = "com.example") { field =>
      (field("name", _.name), field("good", _.good, default = Some(true))).mapN(Dog(_, _))
    }

  implicit val animal: Codec[Animal] =
    Codec.union(alt => alt[Cat] |+| alt[Dog])

  implicit val fruit: Codec[Fruit] =
    Codec.enumeration[Fruit](
      name = "Fruit",
      namespace = "com.example",
      symbols = List("Apple", "Banana"),
      encode = {
        case Apple => "Apple"
        case Banana => "Banana"
      },
      decode = {
        case "Apple" => Right(Apple)
        case "Banana" => Right(Banana)
        case other => Left(AvroError(s"Unknown fruit $other"))
      },
      default = Some(Apple),
      doc = Some("A fruit")
    )

  implicit val checksum: Codec[Checksum] =
    Codec.fixed[Checksum](
      name = "Checksum",
      namespace = "com.example",
      size = 4,
      encode = _.bytes,
      decode = bytes => Right(Checksum(bytes)),
      doc = Some("A checksum")
    )

  implicit val event: Codec[Event] =
    Codec.record(name = "Event", namespace = "com.example.events") { field =>
      (
        field("id", _.id),
        field("at", _.at),
        field("day", _.day),
        field("amount", _.amount)(Codec.decimal(10, 2)),
        field("person", _.person),
        field("previous", _.previous, default = Some(None)),
        field("fruit", _.fruit),
        field("checksum", _.checksum),
        field("animal", _.animal)
      ).mapN(Event(_, _, _, _, _, _, _, _, _))
    }

  implicit val derivedChild: Codec[DerivedChild] =
    Codec.derive

  implicit val derivedKind: Codec[DerivedKind] =
    Codec.derive

  implicit val derived: Codec[Derived] =
    Codec.derive

  private def show[A](codec: Codec[A]): Either[String, String] =
    codec.schema.bimap(_.message, _.toString)

  val schemas: List[(String, Either[String, String])] =
    List(
      "boolean" -> show(Codec[Boolean]),
      "int" -> show(Codec[Int]),
      "long" -> show(Codec[Long]),
      "float" -> show(Codec[Float]),
      "double" -> show(Codec[Double]),
      "string" -> show(Codec[String]),
      "bytes" -> show(Codec[Array[Byte]]),
      "option" -> show(Codec[Option[Int]]),
      "either" -> show(Codec[Either[Int, String]]),
      "list" -> show(Codec[List[String]]),
      "map" -> show(Codec[Map[String, Long]]),
      "instant" -> show(Codec[Instant]),
      "localDate" -> show(Codec[LocalDate]),
      "uuid" -> show(Codec[UUID]),
      "decimal" -> show(Codec.decimal(10, 2)),
      "person" -> show(person),
      "animal" -> show(animal),
      "fruit" -> show(fruit),
      "checksum" -> show(checksum),
      "event" -> show(event),
      "optional person" -> show(Codec[Option[Person]]),
      "derived" -> show(derived)
    )
}
