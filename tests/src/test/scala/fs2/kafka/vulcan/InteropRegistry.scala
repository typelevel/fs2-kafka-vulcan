/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.IO
import cats.effect.Resource
import com.comcast.ip4s.*
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.schemaregistry.RegisteredSchema
import org.http4s.schemaregistry.SchemaRegistryClient
import org.http4s.schemaregistry.TestSchemaRegistry
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*

/**
  * A [[TestSchemaRegistry]] served over HTTP, for interoperability tests
  * with the Confluent serializers, which access the registry using the
  * Confluent schema registry client.
  */
final class InteropRegistry private (
  registry: TestSchemaRegistry[IO],
  val client: SchemaRegistryClient[IO],
  url: String
) {

  /** Returns all subject versions, in the order they were registered. */
  def registered: IO[List[RegisteredSchema]] =
    registry.registered

  /** Returns a Confluent serializer for values, with the specified additional configuration. */
  def confluentSerializer(config: Map[String, String] = Map.empty): KafkaAvroSerializer = {
    val serializer = new KafkaAvroSerializer()
    serializer.configure((Map("schema.registry.url" -> url) ++ config).asJava, false)
    serializer
  }

  /** Returns a Confluent deserializer for values. */
  def confluentDeserializer: KafkaAvroDeserializer = {
    val deserializer = new KafkaAvroDeserializer()
    deserializer.configure(Map("schema.registry.url" -> url).asJava, false)
    deserializer
  }
}

object InteropRegistry {

  /** Returns an empty registry where all schemas are compatible. */
  val resource: Resource[IO, InteropRegistry] =
    for {
      registry <- Resource.eval(TestSchemaRegistry[IO])
      server <- EmberServerBuilder
        .default[IO]
        .withHost(ipv4"127.0.0.1")
        .withPort(port"0")
        .withHttpApp(registry.httpApp)
        .withShutdownTimeout(Duration.Zero)
        .build
      client <- Resource.eval(registry.client.build)
    } yield new InteropRegistry(registry, client, s"http://127.0.0.1:${server.address.getPort}")
}
