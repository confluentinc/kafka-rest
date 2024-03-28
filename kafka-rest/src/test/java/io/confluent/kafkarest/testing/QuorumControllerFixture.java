/*
 * Copyright 2023 Confluent Inc.
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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import kafka.server.QuorumTestHarness;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.ToStringBuilder;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * An extension that runs a Zookeeper/Kraft controller server.
 *
 * <p>This fixture currently does not support SSL and/or SASL.
 */
public final class QuorumControllerFixture extends QuorumTestHarness
    implements BeforeEachCallback, AfterEachCallback {

  private QuorumControllerFixture() {}

  @Override
  public void beforeEach(ExtensionContext extensionContext) {
    super.setUp(new DefaultTestInfo(extensionContext));
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    super.tearDown();
  }

  @Override
  public Seq<Properties> kraftControllerConfigs() {
    Properties props = new Properties();
    props.put("authorizer.class.name", StandardAuthorizer.class.getName());
    // this setting allows brokers to register to Kraft controller
    props.put(StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, true);
    return JavaConverters.asScalaBuffer(Collections.singletonList(props));
  }

  public static QuorumControllerFixture create() {
    return new QuorumControllerFixture();
  }

  /** Provide a concrete class for TestInfo that can be used with Fixtures */
  static class DefaultTestInfo implements TestInfo {
    private final String displayName;
    private final Set<String> tags;
    private final Optional<Class<?>> testClass;
    private final Optional<Method> testMethod;

    DefaultTestInfo(ExtensionContext extensionContext) {
      this.displayName = extensionContext.getDisplayName();
      this.tags = extensionContext.getTags();
      this.testClass = extensionContext.getTestClass();
      this.testMethod = extensionContext.getTestMethod();
    }

    DefaultTestInfo(
        String displayName,
        Set<String> tags,
        Optional<Class<?>> testClass,
        Optional<Method> testMethod) {
      this.displayName = displayName;
      this.tags = tags;
      this.testClass = testClass;
      this.testMethod = testMethod;
    }

    @Override
    public String getDisplayName() {
      return this.displayName;
    }

    @Override
    public Set<String> getTags() {
      return this.tags;
    }

    @Override
    public Optional<Class<?>> getTestClass() {
      return this.testClass;
    }

    @Override
    public Optional<Method> getTestMethod() {
      return this.testMethod;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this)
          .append("displayName", this.displayName)
          .append("tags", this.tags)
          .append("testClass", nullSafeGet(this.testClass))
          .append("testMethod", nullSafeGet(this.testMethod))
          .toString();
    }

    private static Object nullSafeGet(Optional<?> optional) {
      return optional != null ? optional.orElse(null) : null;
    }
  }
}
