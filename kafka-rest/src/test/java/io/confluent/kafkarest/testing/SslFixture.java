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

import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.Files.createTempFile;
import static org.apache.kafka.test.TestSslUtils.createKeyStore;
import static org.apache.kafka.test.TestSslUtils.createTrustStore;
import static org.apache.kafka.test.TestSslUtils.generateCertificate;
import static org.apache.kafka.test.TestSslUtils.generateKeyPair;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.kafka.common.config.types.Password;
import org.glassfish.jersey.SslConfigurator;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class SslFixture implements BeforeEachCallback, AfterEachCallback {
  private static final String SSL_PROTOCOL = "TLSv1.2";
  private static final String SSL_ENABLED_PROTOCOLS = "TLSv1.2";
  private static final String TRUST_STORE_TYPE = "JKS";
  private static final String TRUST_MANAGER_ALGORITHM = TrustManagerFactory.getDefaultAlgorithm();
  private static final String KEY_STORE_TYPE = "JKS";
  private static final String KEY_MANAGER_ALGORITHM = TrustManagerFactory.getDefaultAlgorithm();

  private final ImmutableSet<String> keyNames;

  @Nullable private Path trustStoreLocation;
  @Nullable private String trustStorePassword;
  @Nullable private ImmutableMap<String, Key> keys;

  private SslFixture(Set<String> keyNames) {
    this.keyNames = ImmutableSet.copyOf(keyNames);
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    keys = generateKeys();
    trustStoreLocation = createTempFile("truststore", ".jks");
    trustStorePassword = "truststore-pass";
    createTrustStore(
        trustStoreLocation.toString(),
        new Password(trustStorePassword),
        keys.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getCertificate())));
  }

  private ImmutableMap<String, Key> generateKeys() throws Exception {
    ImmutableMap.Builder<String, Key> keys = ImmutableMap.builder();
    for (String keyName : keyNames) {
      KeyPair keyPair = generateKeyPair("RSA");
      X509Certificate certificate =
          generateCertificate("CN=localhost, O=" + keyName, keyPair, 30, "SHA1withRSA");
      Path keyStoreLocation = createTempFile(keyName + "-keystore", ".jks");
      String keyStorePassword = keyName + "-pass";
      String keyPassword = keyName + "-pass";
      createKeyStore(
          keyStoreLocation.toString(),
          new Password(keyStorePassword),
          new Password(keyPassword),
          keyName,
          keyPair.getPrivate(),
          certificate);
      keys.put(keyName, Key.create(keyStoreLocation, keyStorePassword, certificate, keyPassword));
    }
    return keys.build();
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    if (trustStoreLocation != null) {
      try {
        Files.delete(trustStoreLocation);
      } catch (IOException e) {
        // Do nothing.
      }
    }
    if (keys != null) {
      for (Key key : keys.values()) {
        try {
          Files.delete(key.getKeyStoreLocation());
        } catch (IOException e) {
          // Do nothing.
        }
      }
    }
    trustStoreLocation = null;
    trustStorePassword = null;
    keys = null;
  }

  public Path getTrustStoreLocation() {
    checkState(trustStoreLocation != null);
    return trustStoreLocation;
  }

  public String getTrustStorePassword() {
    checkState(trustStorePassword != null);
    return trustStorePassword;
  }

  public Key getKey(String name) {
    checkState(keys != null);
    return keys.get(name);
  }

  public Map<String, String> getSslConfigs(String keyName) {
    return getSslConfigs(keyName, "");
  }

  public Map<String, String> getSslConfigs(String keyName, String prefix) {
    Key key = getKey(keyName);
    ImmutableMap.Builder<String, String> configs = ImmutableMap.builder();
    configs.put(prefix + "ssl.protocol", SSL_PROTOCOL);
    configs.put(prefix + "ssl.enabled.protocols", SSL_ENABLED_PROTOCOLS);
    configs.put(prefix + "ssl.keystore.location", key.getKeyStoreLocation().toString());
    configs.put(prefix + "ssl.keystore.type", KEY_STORE_TYPE);
    configs.put(prefix + "ssl.keystore.password", key.getKeyStorePassword());
    configs.put(prefix + "ssl.keymanager.algorithm", KEY_MANAGER_ALGORITHM);
    configs.put(prefix + "ssl.key.password", key.getKeyPassword());
    configs.put(prefix + "ssl.truststore.location", getTrustStoreLocation().toString());
    configs.put(prefix + "ssl.truststore.type", TRUST_STORE_TYPE);
    configs.put(prefix + "ssl.truststore.password", getTrustStorePassword());
    configs.put(prefix + "ssl.trustmanager.algorithm", TRUST_MANAGER_ALGORITHM);
    return configs.build();
  }

  public SSLContext getSslContext(String keyName) {
    Key key = getKey(keyName);
    SslConfigurator sslConfig =
        SslConfigurator.newInstance()
            .securityProtocol(SSL_PROTOCOL)
            .keyStoreFile(key.getKeyStoreLocation().toString())
            .keyStoreType(KEY_STORE_TYPE)
            .keyStorePassword(key.getKeyStorePassword())
            .keyManagerFactoryAlgorithm(KEY_MANAGER_ALGORITHM)
            .keyPassword(key.getKeyPassword())
            .trustStoreFile(getTrustStoreLocation().toString())
            .trustStoreType(TRUST_STORE_TYPE)
            .trustStorePassword(getTrustStorePassword())
            .trustManagerFactoryAlgorithm(TRUST_MANAGER_ALGORITHM);
    return sslConfig.createSSLContext();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private final ImmutableSet.Builder<String> keyNames = ImmutableSet.builder();

    private Builder() {}

    public Builder addKey(String name) {
      keyNames.add(name);
      return this;
    }

    public SslFixture build() {
      return new SslFixture(keyNames.build());
    }
  }

  @AutoValue
  public abstract static class Key {

    Key() {}

    public abstract Path getKeyStoreLocation();

    public abstract String getKeyStorePassword();

    public abstract Certificate getCertificate();

    public abstract String getKeyPassword();

    private static Key create(
        Path keyStoreLocation,
        String keyStorePassword,
        Certificate certificate,
        String keyPassword) {
      return new AutoValue_SslFixture_Key(
          keyStoreLocation, keyStorePassword, certificate, keyPassword);
    }
  }
}
