/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.IO
import fs2.kafka.Header
import fs2.kafka.Headers
import fs2.kafka.vulcan.Generators.*
import java.nio.ByteBuffer
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.typelevel.vulcan.Codec
import scala.jdk.CollectionConverters.*
import weaver.SimpleIOSuite
import weaver.scalacheck.CheckConfig
import weaver.scalacheck.Checkers

object ConfluentInteropSuite extends SimpleIOSuite with Checkers {
  override def checkConfig: CheckConfig =
    super.checkConfig.withPerPropertyParallelism(1)

  def genericRecord(test: TestRecord): GenericRecord = {
    val schema = new org.apache.avro.Schema.Parser().parse(Codec[TestRecord].schema.fold(throw _, _.toString))
    val record = new GenericData.Record(schema)
    record.put("name", test.name)
    record
  }

  def schemaIdOf(bytes: Array[Byte]): Int =
    ByteBuffer.wrap(bytes, 1, 4).getInt

  test("the Confluent deserializer reads values written by the serializer") {
    InteropRegistry.resource.use { registry =>
      val deserializer = registry.confluentDeserializer
      AvroSerializer.default(registry.client).forValue[TestRecord].use { serializer =>
        forall(testRecords) { value =>
          for {
            bytes <- serializer.serialize("topic", Headers.empty, value)
            read <- IO.blocking(deserializer.deserialize("topic", bytes))
          } yield matches(read) { case record: GenericRecord =>
            expect.same(value.name, record.get("name").toString)
          }
        }
      }
    }
  }

  test("the deserializer reads values written by the Confluent serializer") {
    InteropRegistry.resource.use { registry =>
      val serializer = registry.confluentSerializer()
      AvroDeserializer.default(registry.client).forValue[TestRecord].use { deserializer =>
        forall(testRecords) { value =>
          for {
            bytes <- IO.blocking(serializer.serialize("topic", genericRecord(value)))
            read <- deserializer.deserialize("topic", Headers.empty, bytes)
          } yield expect.same(value, read)
        }
      }
    }
  }

  test("the deserializer reads values written by the Confluent serializer with schema GUIDs in headers") {
    val config =
      Map("value.schema.id.serializer" -> "io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer")

    InteropRegistry.resource.use { registry =>
      val serializer = registry.confluentSerializer(config)
      AvroDeserializer.default(registry.client).forValue[TestRecord].use { deserializer =>
        forall(testRecords) { value =>
          for {
            written <- IO.blocking {
              val headers = new RecordHeaders()
              val bytes = serializer.serialize("topic", headers, genericRecord(value))
              val converted =
                Headers.fromIterable(headers.asScala.map(header => Header(header.key, header.value)))
              (converted, bytes)
            }
            read <- deserializer.deserialize("topic", written._1, written._2)
          } yield expect.same(Some(1.toByte), written._1("__value_schema_id").map(_.value.head)) &&
            expect.same(value, read)
        }
      }
    }
  }

  test("the serializer uses the schema registered by the Confluent serializer") {
    InteropRegistry.resource.use { registry =>
      for {
        confluentBytes <- IO.blocking {
          registry.confluentSerializer().serialize("topic", genericRecord(TestRecord("test")))
        }
        bytes <- AvroSerializer
          .default(registry.client)
          .forValue[TestRecord]
          .use(_.serialize("topic", Headers.empty, TestRecord("test")))
        lookupBytes <- AvroSerializer
          .default(registry.client)
          .withAutoRegisterSchemas(false)
          .forValue[TestRecord]
          .use(_.serialize("topic", Headers.empty, TestRecord("test")))
        registered <- registry.registered
      } yield expect.same(
        List("topic-value" -> 1),
        registered.map(registered => (registered.subject.value, registered.version.value))
      ) &&
        expect.same(confluentBytes.toList, bytes.toList) &&
        expect.same(schemaIdOf(confluentBytes), schemaIdOf(lookupBytes))
    }
  }
}
