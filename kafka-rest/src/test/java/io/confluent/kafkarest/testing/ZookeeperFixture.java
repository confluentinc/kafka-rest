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

import javax.annotation.Nullable;
import kafka.zk.EmbeddedZookeeper;
import org.junit.rules.ExternalResource;

/**
 * An {@link ExternalResource} that runs a Zookeeper server.
 *
 * <p>This fixture currently does not support SSL and/or SASL.
 */
public final class ZookeeperFixture extends ExternalResource {

  @Nullable private EmbeddedZookeeper zookeeper;

  private ZookeeperFixture() {
  }

  @Override
  public void before() {
    zookeeper = new EmbeddedZookeeper();
  }

  @Override
  public void after() {
    if (zookeeper != null) {
      zookeeper.shutdown();
    }
    zookeeper = null;
  }

  String getZookeeperConnect() {
    checkState(zookeeper != null);
    return String.format("localhost:%d", zookeeper.port());
  }

  public static ZookeeperFixture create() {
    return new ZookeeperFixture();
  }
}
