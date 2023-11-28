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
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.ToStringBuilder;

/** Provide a concrete class for TestInfo that can be used with Fixtures */
class DefaultTestInfo implements TestInfo {
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
