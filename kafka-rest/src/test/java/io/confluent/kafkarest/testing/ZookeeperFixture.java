package io.confluent.kafkarest.testing;

import static com.google.common.base.Preconditions.checkState;

import javax.annotation.Nullable;
import kafka.zk.EmbeddedZookeeper;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class ZookeeperFixture implements BeforeEachCallback, AfterEachCallback {

  @Nullable
  private EmbeddedZookeeper zookeeper;

  private ZookeeperFixture() {
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) {
    zookeeper = new EmbeddedZookeeper();
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
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
