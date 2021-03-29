package io.confluent.kafkarest.testing;

import static com.google.common.base.Preconditions.checkState;

import javax.annotation.Nullable;
import kafka.zk.EmbeddedZookeeper;
import org.junit.rules.ExternalResource;

public final class ZookeeperFixture extends ExternalResource {

  @Nullable
  private EmbeddedZookeeper zookeeper;

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
