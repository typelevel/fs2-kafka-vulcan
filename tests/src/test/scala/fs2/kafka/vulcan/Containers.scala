/*
 * Copyright 2018 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package fs2.kafka.vulcan

import cats.effect.IO
import cats.effect.Resource
import com.github.dockerjava.api.command.CreateContainerCmd
import java.time.Duration
import org.http4s.Uri
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

object Containers {

  /** The bootstrap servers of the Kafka broker, and the base URI of the schema registry. */
  final case class Environment(bootstrapServers: String, schemaRegistryUri: Uri)

  /** Kafka and Confluent schema registry. */
  def confluent: Resource[IO, Environment] =
    environment(Wait.forHttp("/subjects").forStatusCode(200)) { network =>
      new RegistryContainer(image("schemaregistry"))
        .withNetwork(network)
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_LISTENERS", s"http://0.0.0.0:$RegistryPort")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", s"PLAINTEXT://$KafkaListener")
    }

  /** Kafka and Karapace schema registry. */
  def karapace: Resource[IO, Environment] =
    environment(
      Wait
        .forHttp("/_health")
        .forStatusCode(200)
        .forResponsePredicate(_.matches("(?s).*\"schema_registry_is_primary\"\\s*:\\s*true.*"))
    ) { network =>
      new RegistryContainer(image("karapace"))
        .withNetwork(network)
        .withCreateContainerCmdModifier { (cmd: CreateContainerCmd) =>
          val _ = cmd.withEntrypoint("python3", "-m", "karapace")
        }
        .withEnv("KARAPACE_KARAPACE_REGISTRY", "true")
        .withEnv("KARAPACE_BOOTSTRAP_URI", KafkaListener)
        .withEnv("KARAPACE_HOST", "0.0.0.0")
        .withEnv("KARAPACE_PORT", RegistryPort.toString)
        .withEnv("KARAPACE_ADVERTISED_HOSTNAME", "karapace")
        .withEnv("KARAPACE_TOPIC_NAME", "_schemas")
        .withEnv("KARAPACE_GROUP_ID", "karapace")
        .withEnv("KARAPACE_MASTER_ELIGIBILITY", "true")
        .withEnv("KARAPACE_COMPATIBILITY", "BACKWARD")
    }

  final class RegistryContainer(image: DockerImageName) extends GenericContainer[RegistryContainer](image)

  private val KafkaListener: String =
    "kafka:19092"

  private val RegistryPort: Int =
    8081

  /** Starts Kafka and the registry, where the registry is started when `ready` is satisfied. */
  private def environment(
    ready: HttpWaitStrategy
  )(registry: Network => RegistryContainer): Resource[IO, Environment] =
    for {
      network <- Resource.fromAutoCloseable(IO(Network.newNetwork()))
      kafka <- start {
        new KafkaContainer(image("kafka"))
          .withNetwork(network)
          .withNetworkAliases("kafka")
          .withListener(KafkaListener)
      }
      registry <- start {
        registry(network)
          .withExposedPorts(RegistryPort)
          .waitingFor(ready.withStartupTimeout(Duration.ofMinutes(3)))
      }
      uri <- Resource.eval {
        IO.fromEither(Uri.fromString(s"http://${registry.getHost}:${registry.getMappedPort(RegistryPort)}"))
      }
    } yield Environment(kafka.getBootstrapServers, uri)

  private def start[C <: GenericContainer[?]](container: C): Resource[IO, C] =
    Resource.make(IO.blocking { container.start(); container })(container => IO.blocking(container.stop()))

  private def image(name: String): DockerImageName = {
    val property = s"fs2.kafka.vulcan.test.$name.image"
    DockerImageName.parse(
      sys.props.getOrElse(property, sys.error(s"$property is not set, run the tests via sbt"))
    )
  }
}
