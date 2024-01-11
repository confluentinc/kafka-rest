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

import static java.util.Objects.requireNonNull;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.security.auth.login.Configuration;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * An extension that creates and sets a {@link org.eclipse.jetty.jaas.spi.PropertyFileLoginModule}
 * as the default Java Login Module.
 *
 * <p>This fixture should be used only when you don't have any other option, for example, when
 * configuring Kafka REST or Schema Registry Basic Authentication. Using it might cause unforeseen
 * side-effects. For example, any Kafka client that does not set {@code sasl.jaas.config} will use
 * the JAAS file created instead, and fail since it has no section named {@code KafkaClient}.
 * Instead of using this fixture, you should use the respective {@code sasl.jaas.config} config
 * whenever possible, for example, {@code client.sasl.jaas.config} in Kafka REST.
 */
public final class JvmPropertyFileLoginModuleFixture
    implements BeforeEachCallback, AfterEachCallback {

  private final String name;
  private final ImmutableMap<String, PasswordAndRoles> users;

  @Nullable private Path jaasConfig;
  @Nullable private Path passConfig;

  public JvmPropertyFileLoginModuleFixture(String name, Map<String, PasswordAndRoles> users) {
    this.name = requireNonNull(name);
    this.users = ImmutableMap.copyOf(users);
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    jaasConfig = Files.createTempFile("jaas-config-", ".conf");
    passConfig = Files.createTempFile("pass-config-", ".properties");
    Files.write(
        jaasConfig,
        ImmutableList.of(
            name + " {",
            "  org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required",
            "  file=\"" + passConfig + "\"",
            "  debug=\"true\";",
            "};"));
    Files.write(
        passConfig,
        users.entrySet().stream()
            .map(entry -> entry.getKey() + ": " + entry.getValue().toString())
            .collect(Collectors.toList()));
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasConfig.toString());
    Configuration.setConfiguration(null);
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM);
    Configuration.setConfiguration(null);
    if (jaasConfig != null) {
      try {
        Files.delete(jaasConfig);
      } catch (IOException e) {
        // Do nothing.
      }
      jaasConfig = null;
    }
    if (passConfig != null) {
      try {
        Files.delete(passConfig);
      } catch (IOException e) {
        // Do nothing.
      }
      passConfig = null;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    @Nullable private String name;
    private ImmutableMap.Builder<String, PasswordAndRoles> users = ImmutableMap.builder();

    private Builder() {}

    /** Adds a {@code username: password,role1,role2,...} line in the users properties file. */
    public Builder addUser(String username, String password, String... roles) {
      users.put(username, PasswordAndRoles.create(password, Arrays.asList(roles)));
      return this;
    }

    /** Sets the name of the section in the JAAS file. */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public JvmPropertyFileLoginModuleFixture build() {
      return new JvmPropertyFileLoginModuleFixture(name, users.build());
    }
  }

  @AutoValue
  abstract static class PasswordAndRoles {

    PasswordAndRoles() {}

    abstract String getPassword();

    abstract ImmutableList<String> getRoles();

    @Override
    public final String toString() {
      return getPassword() + (getRoles().isEmpty() ? "" : ("," + String.join(",", getRoles())));
    }

    private static PasswordAndRoles create(String password, List<String> roles) {
      return new AutoValue_JvmPropertyFileLoginModuleFixture_PasswordAndRoles(
          password, ImmutableList.copyOf(roles));
    }
  }
}
