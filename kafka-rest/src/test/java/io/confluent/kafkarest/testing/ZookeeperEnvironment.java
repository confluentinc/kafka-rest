package io.confluent.kafkarest.testing;

import static com.google.common.base.Preconditions.checkState;

import javax.annotation.Nullable;
import kafka.zk.EmbeddedZookeeper;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class ZookeeperEnvironment implements BeforeEachCallback, AfterEachCallback {

  @Nullable
  private EmbeddedZookeeper zookeeper;

  private ZookeeperEnvironment() {
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    checkState(zookeeper == null, "Starting environment that already started.");
    zookeeper = new EmbeddedZookeeper();
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) throws Exception {
    checkState(zookeeper != null, "Stopping environment that never started.");
    zookeeper.shutdown();
    zookeeper = null;
  }

  String getZookeeperConnect() {
    checkState(zookeeper != null);
    return String.format("localhost:%d", zookeeper.port());
  }

  public static ZookeeperEnvironment create() {
    return new ZookeeperEnvironment();
  }
}
