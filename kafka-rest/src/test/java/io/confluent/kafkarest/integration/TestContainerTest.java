/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.kafkarest.integration;

import io.confluent.kafkarest.common.CompletableFutures;
import io.confluent.kafkarest.testing.KafkaRestFixture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class TestContainerTest {

  private static final String ZOOKEEPER_IMAGE =
      "368821881613.dkr.ecr.us-west-2.amazonaws.com/confluentinc/cp-zookeeper:master-latest";
  private static final String KAFKA_IMAGE =
      "368821881613.dkr.ecr.us-west-2.amazonaws.com/confluentinc/cp-server:master-latest";

  @Rule public final Network network = Network.newNetwork();

  @Rule
  public final GenericContainer<?> zookeeper =
      new GenericContainer<>(DockerImageName.parse(ZOOKEEPER_IMAGE))
          .withNetwork(network)
          .withNetworkAliases("zookeeper")
          .withExposedPorts(9091)
          .withEnv("ZOOKEEPER_CLIENT_PORT", "9091")
          .withEnv("ZOOKEEPER_SERVER_ID", "1")
          .withLogConsumer(new LogConsumer("zookeeper", "\033[0;33m"));

  public final GenericContainer<?> kafka1 =
      new GenericContainer<>(DockerImageName.parse(KAFKA_IMAGE))
          .withNetwork(network)
          .withNetworkAliases("kafka-1")
          .withExposedPorts(9191, 9192)
          .waitingFor(Wait.forListeningPort())
          .withEnv("KAFKA_NUM_IO_THREADS", "1")
          .withEnv("KAFKA_NUM_NETWORK_THREADS", "1")
          .withEnv("KAFKA_NUM_RECOVERY_THREADS_PER_DATA_DIR", "1")
          .withEnv("KAFKA_NUM_REPLICA_ALTER_LOG_DIRS_THREADS", "1")
          .withEnv("KAFKA_NUM_REPLICA_FETCHERS", "1")
          .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
          .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3")
          .withEnv("KAFKA_AUTO_LEADER_REBALANCE_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_BALANCER_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_SECURITY_EVENT_LOGGER_AUTHENTICATION_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_SECURITY_EVENT_LOGGER_ENABLE", "false")
          .withEnv("KAFKA_KAFKA_REST_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_CLUSTER_LINK_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_TIER_ENABLE", "false")
          .withEnv("KAFKA_CONTROLLED_SHUTDOWN_ENABLE", "false")
          .withEnv("KAFKA_LOG_CLEANER_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_REPORTERS_TELEMETRY_AUTO_ENABLE", "false")
          .withEnv("KAFKA_ADVERTISED_LISTENERS", "INTERNAL://kafka-1:9191")
          .withEnv("KAFKA_BROKER_ID", "1")
          .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
          .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT")
          .withEnv("KAFKA_LISTENERS", "INTERNAL://0.0.0.0:9191,EXTERNAL://0.0.0.0:9192")
          .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:9091")
          .withLogConsumer(new LogConsumer("kafka-1", "\033[0;34m"))
          .withCreateContainerCmdModifier(cmd -> cmd.withStopSignal("SIGKILL"));

  public final GenericContainer<?> kafka2 =
      new GenericContainer<>(DockerImageName.parse(KAFKA_IMAGE))
          .withNetwork(network)
          .withNetworkAliases("kafka-2")
          .withExposedPorts(9191, 9192, 9193)
          .waitingFor(Wait.forListeningPort())
          .withEnv("KAFKA_NUM_IO_THREADS", "1")
          .withEnv("KAFKA_NUM_NETWORK_THREADS", "1")
          .withEnv("KAFKA_NUM_RECOVERY_THREADS_PER_DATA_DIR", "1")
          .withEnv("KAFKA_NUM_REPLICA_ALTER_LOG_DIRS_THREADS", "1")
          .withEnv("KAFKA_NUM_REPLICA_FETCHERS", "1")
          .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
          .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3")
          .withEnv("KAFKA_AUTO_LEADER_REBALANCE_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_BALANCER_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_SECURITY_EVENT_LOGGER_AUTHENTICATION_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_SECURITY_EVENT_LOGGER_ENABLE", "false")
          .withEnv("KAFKA_KAFKA_REST_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_CLUSTER_LINK_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_TIER_ENABLE", "false")
          .withEnv("KAFKA_CONTROLLED_SHUTDOWN_ENABLE", "false")
          .withEnv("KAFKA_LOG_CLEANER_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_REPORTERS_TELEMETRY_AUTO_ENABLE", "false")
          .withEnv("KAFKA_ADVERTISED_LISTENERS", "INTERNAL://kafka-2:9191")
          .withEnv("KAFKA_BROKER_ID", "2")
          .withEnv("KAFKA_CONFLUENT_HTTP_SERVER_LISTENERS", "http://0.0.0.0:9193")
          .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
          .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT")
          .withEnv("KAFKA_LISTENERS", "INTERNAL://0.0.0.0:9191,EXTERNAL://0.0.0.0:9192")
          .withEnv("KAFKA_KAFKA_REST_ADVERTISED_LISTENERS", "http://kafka-2:9193")
          .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:9091")
          .withLogConsumer(new LogConsumer("kafka-2", "\033[0;35m"))
          .withCreateContainerCmdModifier(cmd -> cmd.withStopSignal("SIGKILL"));

  public final GenericContainer<?> kafka3 =
      new GenericContainer<>(DockerImageName.parse(KAFKA_IMAGE))
          .withNetwork(network)
          .withNetworkAliases("kafka-3")
          .withExposedPorts(9191, 9192, 9193)
          .waitingFor(Wait.forListeningPort())
          .withEnv("KAFKA_NUM_IO_THREADS", "1")
          .withEnv("KAFKA_NUM_NETWORK_THREADS", "1")
          .withEnv("KAFKA_NUM_RECOVERY_THREADS_PER_DATA_DIR", "1")
          .withEnv("KAFKA_NUM_REPLICA_ALTER_LOG_DIRS_THREADS", "1")
          .withEnv("KAFKA_NUM_REPLICA_FETCHERS", "1")
          .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
          .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3")
          .withEnv("KAFKA_AUTO_LEADER_REBALANCE_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_BALANCER_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_SECURITY_EVENT_LOGGER_AUTHENTICATION_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_SECURITY_EVENT_LOGGER_ENABLE", "false")
          .withEnv("KAFKA_KAFKA_REST_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_CLUSTER_LINK_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_TIER_ENABLE", "false")
          .withEnv("KAFKA_CONTROLLED_SHUTDOWN_ENABLE", "false")
          .withEnv("KAFKA_LOG_CLEANER_ENABLE", "false")
          .withEnv("KAFKA_CONFLUENT_REPORTERS_TELEMETRY_AUTO_ENABLE", "false")
          .withEnv("KAFKA_ADVERTISED_LISTENERS", "INTERNAL://kafka-3:9191")
          .withEnv("KAFKA_BROKER_ID", "3")
          .withEnv("KAFKA_CONFLUENT_HTTP_SERVER_LISTENERS", "http://0.0.0.0:9193")
          .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
          .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT")
          .withEnv("KAFKA_LISTENERS", "INTERNAL://0.0.0.0:9191,EXTERNAL://0.0.0.0:9192")
          .withEnv("KAFKA_KAFKA_REST_ADVERTISED_LISTENERS", "http://kafka-3:9193")
          .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:9091")
          .withLogConsumer(new LogConsumer("kafka-3", "\033[0;36m"))
          .withCreateContainerCmdModifier(cmd -> cmd.withStopSignal("SIGKILL"));

  private String getKafkaInternalListener(GenericContainer<?> kafka) {
    return kafka.getNetworkAliases().get(1) + ":9191";
  }

  private String getKafkaExternalListener(GenericContainer<?> kafka) {
    return kafka.getHost() + ":" + kafka.getMappedPort(9192);
  }

  private void updateKafkaAdvertisedListeners(GenericContainer<?> kafka) {
    Container.ExecResult result;
    try {
      result =
          kafka.execInContainer(
              "kafka-configs",
              "--alter",
              "--bootstrap-server",
              getKafkaInternalListener(kafka),
              "--entity-type",
              "brokers",
              "--entity-name",
              kafka.getEnvMap().get("KAFKA_BROKER_ID"),
              "--add-config",
              String.format(
                  "advertised.listeners=[INTERNAL://%s,EXTERNAL://%s]",
                  getKafkaInternalListener(kafka), getKafkaExternalListener(kafka)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    if (result.getExitCode() != 0) {
      throw new RuntimeException(result.toString());
    }
  }

  private String getKafkaBootstrapServers() {
    return Stream.of(kafka1, kafka2, kafka3)
        .map(this::getKafkaExternalListener)
        .collect(Collectors.joining(","));
  }

  @Test
  public void foobar() throws Exception {
    CompletableFutures.allAsList(
            Arrays.asList(kafka1, kafka2, kafka3).stream()
                .map(
                    kafka ->
                        CompletableFuture.supplyAsync(
                            () -> {
                              kafka.start();
                              updateKafkaAdvertisedListeners(kafka);
                              return null;
                            }))
                .collect(Collectors.toList()))
        .join();

    KafkaRestFixture kafkaRest =
        KafkaRestFixture.builder()
            .setConfig("bootstrap.servers", getKafkaBootstrapServers())
            .build();
    kafkaRest.before();

    Response response =
        kafkaRest.target().path("/v3/clusters").request().accept(MediaType.APPLICATION_JSON).get();
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    kafkaRest.after();

    CompletableFutures.allAsList(
            Arrays.asList(kafka1, kafka2, kafka3).stream()
                .map(
                    kafka ->
                        CompletableFuture.supplyAsync(
                            () -> {
                              kafka.stop();
                              return null;
                            }))
                .collect(Collectors.toList()))
        .join();
  }

  private static final class LogConsumer extends BaseConsumer<LogConsumer> {
    private final String name;
    private final String color;

    private boolean newLine = true;

    private LogConsumer(String name, String color) {
      this.name = requireNonNull(name);
      this.color = requireNonNull(color);
    }

    @Override
    public void accept(OutputFrame outputFrame) {
      if (OutputFrame.OutputType.END.equals(outputFrame.getType())) {
        return;
      }
      if (newLine) {
        //System.out.print(color + "[" + name + "]\033[0m ");
      }
      String message = outputFrame.getUtf8String();
      //System.out.print(message);
      newLine = message.endsWith("\n");
    }
  }
}
