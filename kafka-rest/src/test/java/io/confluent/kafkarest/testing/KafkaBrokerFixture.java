package io.confluent.kafkarest.testing;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class KafkaBrokerFixture implements BeforeEachCallback, AfterEachCallback {

  private static final ImmutableMap<String, String> CONFIG_TEMPLATE =
      ImmutableMap.<String, String>builder()
          .put(KafkaConfig.AutoCreateTopicsEnableProp(), "false")
          .put(KafkaConfig.ControlledShutdownEnableProp(), "false")
          .put(KafkaConfig.DefaultReplicationFactorProp(), "1")
          .put(KafkaConfig.DeleteTopicEnableProp(), "true")
          .put(KafkaConfig.GroupInitialRebalanceDelayMsProp(), "0")
          .put(KafkaConfig.InterBrokerListenerNameProp(), "INTERNAL")
          .put(KafkaConfig.ListenersProp(), "INTERNAL://localhost:0,EXTERNAL://localhost:0")
          .put(KafkaConfig.OffsetsTopicPartitionsProp(), "1")
          .put(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1")
          .build();

  private static final MockTime MOCK_TIME =
      new MockTime(System.currentTimeMillis(), System.nanoTime());

  private final int brokerId;
  @Nullable private final SslFixture certificates;
  private final ImmutableMap<String, String> configs;
  @Nullable private final String keyName;
  private final SecurityProtocol securityProtocol;
  private final ImmutableMap<String, String> users;
  private final ImmutableSet<String> superUsers;
  private final ZookeeperFixture zookeeper;

  @Nullable private KafkaServer broker;
  @Nullable private Path logDir;

  public KafkaBrokerFixture(
      int brokerId,
      @Nullable SslFixture certificates,
      Map<String, String> configs,
      @Nullable String keyName,
      SecurityProtocol securityProtocol,
      Map<String, String> users,
      Set<String> superUsers,
      ZookeeperFixture zookeeper) {
    checkArgument(certificates != null ^ keyName == null);
    this.brokerId = brokerId;
    this.certificates = certificates;
    this.configs = ImmutableMap.copyOf(configs);
    this.keyName = keyName;
    this.securityProtocol = requireNonNull(securityProtocol);
    this.users = ImmutableMap.copyOf(users);
    this.superUsers = ImmutableSet.copyOf(superUsers);
    this.zookeeper = requireNonNull(zookeeper);
    checkArgument(!isSslSecurity() || certificates != null);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    logDir = Files.createTempDirectory(String.format("kafka-%d-", brokerId));
    broker = TestUtils.createServer(KafkaConfig.fromProps(getBrokerConfigs()), MOCK_TIME);
  }

  private Properties getBrokerConfigs() {
    checkState(logDir != null);
    Properties properties = new Properties();
    properties.putAll(CONFIG_TEMPLATE);
    properties.setProperty(KafkaConfig.BrokerIdProp(), String.valueOf(brokerId));
    properties.setProperty(KafkaConfig.LogDirProp(), logDir.toString());
    properties.setProperty(KafkaConfig.ZkConnectProp(), zookeeper.getZookeeperConnect());
    properties.putAll(getBrokerSecurityConfigs());
    properties.putAll(getBrokerSslConfigs());
    properties.putAll(configs);
    return properties;
  }

  private Properties getBrokerSecurityConfigs() {
    Properties properties = new Properties();
    properties.setProperty(
        KafkaConfig.ListenerSecurityProtocolMapProp(),
        String.format("EXTERNAL:%s,INTERNAL:%s", securityProtocol, securityProtocol));
    if (isSaslSecurity()) {
      properties.setProperty(
          "listener.name.external.plain.sasl.jaas.config", getBrokerPlainSaslJaasConfig());
      properties.setProperty(
          "listener.name.internal.plain.sasl.jaas.config", getBrokerPlainSaslJaasConfig());
      properties.setProperty("sasl.enabled.mechanisms", "PLAIN");
      properties.setProperty("sasl.mechanism.inter.broker.protocol", "PLAIN");
      properties.setProperty("authorizer.class.name", "kafka.security.auth.SimpleAclAuthorizer");
    }
    properties.setProperty("super.users", getSuperUsers());
    return properties;
  }

  private String getBrokerPlainSaslJaasConfig() {
    String userEntries =
        users.entrySet()
            .stream()
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
  public void afterEach(ExtensionContext context) throws Exception {
    if (broker != null) {
      broker.shutdown();
    }
    if (logDir != null) {
      Files.walk(logDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
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
    return new Builder()
        .addUser("kafka", "kafka-pass")
        .addSuperUser("kafka");
  }

  public static final class Builder {
    private int brokerId = -1;
    private SslFixture certificates = null;
    private final ImmutableMap.Builder<String, String> configs = ImmutableMap.builder();
    private String keyName = null;
    private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
    private final ImmutableMap.Builder<String, String> users = ImmutableMap.builder();
    private final ImmutableSet.Builder<String> superUsers = ImmutableSet.builder();
    private ZookeeperFixture zookeeper = null;

    private Builder() {
    }

    public Builder addUser(String username, String password) {
      users.put(username, password);
      return this;
    }

    public Builder addUsers(Map<String, String> users) {
      this.users.putAll(users);
      return this;
    }

    public Builder addSuperUser(String username) {
      checkArgument(users.build().containsKey(username));
      superUsers.add(username);
      return this;
    }

    public Builder addSuperUsers(Set<String> superUsers) {
      checkArgument(users.build().keySet().containsAll(superUsers));
      this.superUsers.addAll(superUsers);
      return this;
    }

    public Builder setBrokerId(int brokerId) {
      checkArgument(brokerId >= 0);
      this.brokerId = brokerId;
      return this;
    }

    public Builder setCertificate(SslFixture certificates, String keyName) {
      this.certificates = requireNonNull(certificates);
      this.keyName = requireNonNull(keyName);
      return this;
    }

    public Builder setConfig(String name, String value) {
      configs.put(name, value);
      return this;
    }

    public Builder setConfigs(Map<String, String> configs) {
      this.configs.putAll(configs);
      return this;
    }

    public Builder setSecurityProtocol(SecurityProtocol securityProtocol) {
      this.securityProtocol = requireNonNull(securityProtocol);
      return this;
    }

    public Builder setZookeeper(ZookeeperFixture zookeeper) {
      this.zookeeper = requireNonNull(zookeeper);
      return this;
    }

    public KafkaBrokerFixture build() {
      checkState(brokerId >= 0);
      checkState(zookeeper != null);
      return new KafkaBrokerFixture(
          brokerId,
          certificates,
          configs.build(),
          keyName,
          securityProtocol,
          users.build(),
          superUsers.build(),
          zookeeper);
    }
  }
}
