/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.IO
import cats.syntax.all.*
import fs2.kafka.Headers
import fs2.kafka.vulcan.Generators.*
import org.apache.avro.generic.GenericRecord
import org.http4s.schemaregistry.SubjectName
import org.typelevel.vulcan.Codec
import weaver.SimpleIOSuite
import weaver.scalacheck.CheckConfig
import weaver.scalacheck.Checkers

/**
  * Tests for interoperability with the Confluent deserializers, for values written using
  * other schemas than the codec schema, and with schema identifiers in headers.
  */
object ConfluentWriterSchemaInteropSuite extends SimpleIOSuite with Checkers {

  /** Checks values one at a time, since each test uses one Confluent deserializer. */
  override def checkConfig: CheckConfig =
    super.checkConfig.withPerPropertyParallelism(1)

  val withDefaultCodec: Codec[TestRecordV2] =
    Codec.record(name = "Test", namespace = "fs2.kafka.vulcan") { field =>
      (field("name", _.name), field("number", _.number, default = Some(42))).mapN(TestRecordV2(_, _))
    }

  test("the Confluent deserializer reads values written with schema GUIDs in headers") {
    InteropRegistry.resource.use { registry =>
      val deserializer = registry.confluentDeserializer
      val avroSerializer = AvroSerializer.default(registry.client).withSchemaIdHeaders(true)
      avroSerializer.forValue[TestRecord].use { serializer =>
        forall(testRecords) { value =>
          for {
            written <- serializer.serializeWithHeaders("topic", Headers.empty, value)
            read <- IO.blocking(deserializer.deserialize("topic", written._1.asJava, written._2))
          } yield expect.same(Some(1.toByte), written._1("__value_schema_id").map(_.value.head)) &&
            matches(read) { case record: GenericRecord =>
              expect.same(value.name, record.get("name").toString)
            }
        }
      }
    }
  }

  test("the Confluent deserializer reads values written using the latest version") {
    InteropRegistry.resource.use { registry =>
      val deserializer = registry.confluentDeserializer
      AvroSerializer.default(registry.client).registerSchema(SubjectName("topic-value"))(withDefaultCodec) >>
        AvroSerializer.default(registry.client).withLatestVersion.forValue[TestRecord].use { serializer =>
          forall(testRecords) { value =>
            for {
              bytes <- serializer.serialize("topic", Headers.empty, value)
              read <- IO.blocking(deserializer.deserialize("topic", bytes))
            } yield matches(read) { case record: GenericRecord =>
              val name = record.get("name").toString
              expect.same(value.name, name) && expect.same(42: Any, record.get("number"))
            }
          }
        }
    }
  }
}
