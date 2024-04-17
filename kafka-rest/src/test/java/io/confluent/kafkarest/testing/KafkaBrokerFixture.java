/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.testing;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafkarest.testing.QuorumControllerFixture.DefaultTestInfo;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import kafka.server.KafkaBroker;
import kafka.server.KafkaConfig;
import kafka.utils.TestInfoUtils;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import scala.Option;

/** An extension that runs a Kafka broker. */
public final class KafkaBrokerFixture implements BeforeEachCallback, AfterEachCallback {

  private static final ImmutableMap<String, String> CONFIG_TEMPLATE =
      ImmutableMap.<String, String>builder()
          .put("auto.create.topics.enable", "false")
          .put("controlled.shutdown.enable", "false")
          .put("default.replication.factor", "1")
          .put("group.initial.rebalance.delay.ms", "0")
          .put("inter.broker.listener.name", "INTERNAL")
          .put("listeners", "INTERNAL://localhost:0,EXTERNAL://localhost:0")
          .put("advertised.listeners", "INTERNAL://localhost:0,EXTERNAL://localhost:0")
          .put("offsets.topic.num.partitions", "1")
          .put("offsets.topic.replication.factor", "1")
          .build();

  private final int brokerId;
  @Nullable private final SslFixture certificates;
  private final ImmutableMap<String, String> configs;
  @Nullable private final String keyName;
  private final SecurityProtocol securityProtocol;
  private final ImmutableMap<String, String> users;
  private final ImmutableSet<String> superUsers;
  private final QuorumControllerFixture quorumController;

  @Nullable private KafkaBroker broker;
  @Nullable private Path logDir;

  public KafkaBrokerFixture(
      int brokerId,
      @Nullable SslFixture certificates,
      Map<String, String> configs,
      @Nullable String keyName,
      SecurityProtocol securityProtocol,
      Map<String, String> users,
      Set<String> superUsers,
      QuorumControllerFixture quorumController) {
    checkArgument(certificates != null ^ keyName == null);
    this.brokerId = brokerId;
    this.certificates = certificates;
    this.configs = ImmutableMap.copyOf(configs);
    this.keyName = keyName;
    this.securityProtocol = requireNonNull(securityProtocol);
    this.users = ImmutableMap.copyOf(users);
    this.superUsers = ImmutableSet.copyOf(superUsers);
    this.quorumController = requireNonNull(quorumController);
    checkArgument(!isSslSecurity() || certificates != null);
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    TestInfo testInfo = new DefaultTestInfo(extensionContext);
    logDir = Files.createTempDirectory(String.format("kafka-%d-", brokerId));
    broker =
        quorumController.createBroker(
            KafkaConfig.fromProps(getBrokerConfigs(TestInfoUtils.isKRaft(testInfo))),
            Time.SYSTEM,
            true,
            Option.empty());
  }

  private Properties getBrokerConfigs(boolean isKraftTest) {
    checkState(logDir != null);
    Properties properties =
        TestUtils.createBrokerConfig(
            brokerId,
            quorumController.zkConnectOrNull(),
            false,
            false,
            TestUtils.RandomPort(),
            Option.apply(null),
            Option.apply(null),
            Option.empty(),
            true,
            false,
            TestUtils.RandomPort(),
            false,
            TestUtils.RandomPort(),
            false,
            TestUtils.RandomPort(),
            Option.empty(),
            1,
            false,
            1,
            (short) 1,
            false);
    properties.putAll(CONFIG_TEMPLATE);
    properties.setProperty("broker.id", String.valueOf(brokerId));
    properties.setProperty("log.dir", logDir.toString());
    properties.putAll(getBrokerSecurityConfigs(isKraftTest));
    properties.putAll(getBrokerSslConfigs());
    properties.putAll(configs);
    return properties;
  }

  private Properties getBrokerSecurityConfigs(boolean isKraftTest) {
    Properties properties = new Properties();
    String listenerSecurityProtocolMapTempl = "EXTERNAL:%s,INTERNAL:%s";
    if (isKraftTest) {
      listenerSecurityProtocolMapTempl += ",CONTROLLER:PLAINTEXT";
    }
    properties.setProperty(
        "listener.security.protocol.map",
        String.format(listenerSecurityProtocolMapTempl, securityProtocol, securityProtocol));
    if (isSaslSecurity()) {
      properties.setProperty(
          "listener.name.external.plain.sasl.jaas.config", getBrokerPlainSaslJaasConfig());
      properties.setProperty(
          "listener.name.internal.plain.sasl.jaas.config", getBrokerPlainSaslJaasConfig());
      properties.setProperty("sasl.enabled.mechanisms", "PLAIN");
      properties.setProperty("sasl.mechanism.inter.broker.protocol", "PLAIN");
      if (isKraftTest) {
        properties.setProperty(
            "authorizer.class.name", "org.apache.kafka.metadata.authorizer.StandardAuthorizer");
      } else {
        properties.setProperty("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
      }
    }
    properties.setProperty("super.users", getSuperUsers());
    return properties;
  }

  private String getBrokerPlainSaslJaasConfig() {
    String userEntries =
        users.entrySet().stream()
            .map(entry -> String.format("user_%s=\"%s\"", entry.getKey(), entry.getValue()))
            .collect(Collectors.joining(" "));
    return "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + "username=\"kafka\" "
        + "password=\"kafka-pass\" "
        + userEntries
        + ";";
  }

  private Properties getBrokerSslConfigs() {
    Properties properties = new Properties();
    if (certificates != null) {
      properties.putAll(certificates.getSslConfigs(keyName));
    }
    return properties;
  }

  private String getSuperUsers() {
    return superUsers.stream()
        .map(user -> String.format("User:%s", user))
        .collect(Collectors.joining(";"));
  }

  public boolean isSaslSecurity() {
    return securityProtocol == SecurityProtocol.SASL_PLAINTEXT
        || securityProtocol == SecurityProtocol.SASL_SSL;
  }

  public boolean isSslSecurity() {
    return securityProtocol == SecurityProtocol.SASL_SSL
        || securityProtocol == SecurityProtocol.SSL;
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    if (broker != null) {
      broker.shutdown();
    }
    if (logDir != null) {
      try {
        Files.walk(logDir)
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
      } catch (IOException e) {
        // Do nothing.
      }
    }
  }

  public String getBootstrapServers() {
    checkState(broker != null);
    int port = broker.boundPort(ListenerName.normalised("EXTERNAL"));
    return String.format("localhost:%d", port);
  }

  public SecurityProtocol getSecurityProtocol() {
    return securityProtocol;
  }

  private Properties getClientConfigs() {
    Properties properties = new Properties();
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    properties.setProperty("security.protocol", securityProtocol.name());
    if (isSaslSecurity()) {
      properties.setProperty("sasl.jaas.config", getClientSaslJaasConfig());
      properties.setProperty("sasl.mechanism", "PLAIN");
    }
    if (certificates != null) {
      properties.putAll(certificates.getSslConfigs(keyName));
    }
    return properties;
  }

  private String getClientSaslJaasConfig() {
    return "org.apache.kafka.common.security.plain.PlainLoginModule required "
        + "username=\"kafka\" "
        + "password=\"kafka-pass\";";
  }

  Properties getAdminConfigs() {
    return getClientConfigs();
  }

  Properties getConsumerConfigs() {
    return getClientConfigs();
  }

  public static Builder builder() {
    return new Builder().addUser("kafka", "kafka-pass").addSuperUser("kafka");
  }

  public static final class Builder {
    private int brokerId = -1;
    private SslFixture certificates = null;
    private final ImmutableMap.Builder<String, String> configs = ImmutableMap.builder();
    private String keyName = null;
    private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
    private final ImmutableMap.Builder<String, String> users = ImmutableMap.builder();
    private final ImmutableSet.Builder<String> superUsers = ImmutableSet.builder();
    private QuorumControllerFixture quorumController = null;

    private Builder() {}

    /** Adds a SASL PLAIN user. */
    public Builder addUser(String username, String password) {
      users.put(username, password);
      return this;
    }

    /** @see #addUser(String, String) */
    public Builder addUsers(Map<String, String> users) {
      this.users.putAll(users);
      return this;
    }

    /** Sets the given SASL PLAIN user as a super-user. */
    public Builder addSuperUser(String username) {
      checkArgument(users.build().containsKey(username));
      superUsers.add(username);
      return this;
    }

    /** @see #addSuperUser(String) */
    public Builder addSuperUsers(Set<String> superUsers) {
      checkArgument(users.build().keySet().containsAll(superUsers));
      this.superUsers.addAll(superUsers);
      return this;
    }

    /** Sets the broker ID. */
    public Builder setBrokerId(int brokerId) {
      checkArgument(brokerId >= 0);
      this.brokerId = brokerId;
      return this;
    }

    /**
     * Sets the SSL certificate store, and the name of the certificate to use as the broker
     * certificate.
     */
    public Builder setCertificate(SslFixture certificates, String keyName) {
      this.certificates = requireNonNull(certificates);
      this.keyName = requireNonNull(keyName);
      return this;
    }

    /** Sets the given broker config. */
    public Builder setConfig(String name, String value) {
      configs.put(name, value);
      return this;
    }

    /** @see #setConfig(String, String) */
    public Builder setConfigs(Map<String, String> configs) {
      this.configs.putAll(configs);
      return this;
    }

    /**
     * Sets the broker security protocol.
     *
     * <p>For the {@link SecurityProtocol#SASL_PLAINTEXT} or {@link SecurityProtocol#SASL_SSL}
     * protocols, only the PLAIN mechanism is supported.
     */
    public Builder setSecurityProtocol(SecurityProtocol securityProtocol) {
      this.securityProtocol = requireNonNull(securityProtocol);
      return this;
    }

    public Builder setQuorumController(QuorumControllerFixture quorumController) {
      this.quorumController = requireNonNull(quorumController);
      return this;
    }

    public KafkaBrokerFixture build() {
      checkState(brokerId >= 0);
      checkState(quorumController != null);
      return new KafkaBrokerFixture(
          brokerId,
          certificates,
          configs.build(),
          keyName,
          securityProtocol,
          users.build(),
          superUsers.build(),
          quorumController);
    }
  }
}
