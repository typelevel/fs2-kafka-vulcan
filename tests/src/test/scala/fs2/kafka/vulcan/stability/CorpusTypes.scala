/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan.stability

import java.time.Instant
import java.time.LocalDate
import java.util.UUID

final case class Person(name: String, age: Option[Int], tags: List[String], attributes: Map[String, String])

sealed trait Animal

final case class Cat(name: String) extends Animal

final case class Dog(name: String, good: Boolean) extends Animal

sealed trait Fruit

case object Apple extends Fruit

case object Banana extends Fruit

final case class Checksum(bytes: Array[Byte])

final case class Event(
  id: UUID,
  at: Instant,
  day: LocalDate,
  amount: BigDecimal,
  person: Person,
  previous: Option[Person],
  fruit: Fruit,
  checksum: Checksum,
  animal: Animal
)

final case class Derived(name: String, count: Int, child: Option[DerivedChild], kind: DerivedKind)

final case class DerivedChild(value: Long)

sealed trait DerivedKind

final case class DerivedA(a: Int) extends DerivedKind

final case class DerivedB(b: String) extends DerivedKind
