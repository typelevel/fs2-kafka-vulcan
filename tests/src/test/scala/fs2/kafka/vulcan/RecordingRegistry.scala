/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.IO
import cats.effect.Ref
import cats.syntax.all.*
import org.http4s.HttpApp
import org.http4s.Request
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.schemaregistry.RegisteredSchema
import org.http4s.schemaregistry.SchemaRegistryClientBuilder
import org.http4s.schemaregistry.TestSchemaRegistry

/**
  * A [[TestSchemaRegistry]] which records the requests it
  * receives, as the method, path and query of the requests.
  */
final class RecordingRegistry private (registry: TestSchemaRegistry[IO], received: Ref[IO, List[String]]) {

  /** Returns a builder for a client of the registry. */
  def client: SchemaRegistryClientBuilder[IO] =
    SchemaRegistryClientBuilder.default(Client.fromHttpApp(httpApp), Uri())

  /** Returns all subject versions, in the order they were registered. */
  def registered: IO[List[RegisteredSchema]] =
    registry.registered

  /** Returns the requests received by the registry, in the order they were received. */
  def requests: IO[List[String]] =
    received.get

  private val httpApp: HttpApp[IO] =
    HttpApp[IO](request => received.update(_ :+ describe(request)) >> registry.httpApp(request))

  private def describe(request: Request[IO]): String = {
    val path = request.uri.path.segments.map(_.decoded()).mkString("/", "/", "")
    val query = request.uri.query.renderString
    if (query.isEmpty) s"${request.method.name} $path" else s"${request.method.name} $path?$query"
  }
}

object RecordingRegistry {

  /** Returns an empty registry where all schemas are compatible. */
  def apply(): IO[RecordingRegistry] =
    (TestSchemaRegistry[IO], IO.ref(List.empty[String])).mapN(new RecordingRegistry(_, _))
}
