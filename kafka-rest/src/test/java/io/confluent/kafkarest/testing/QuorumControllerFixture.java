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

import kafka.server.QuorumTestHarness;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

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

  public static QuorumControllerFixture create() {
    return new QuorumControllerFixture();
  }
}
