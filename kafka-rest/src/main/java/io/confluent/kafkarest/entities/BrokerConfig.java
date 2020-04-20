package io.confluent.kafkarest.entities;

import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;

/**
 * A Kafka Broker Config
 */
public class BrokerConfig {

  private final String clusterId;

  private final String brokerId;

  private final String name;

  @Nullable
  private final String value;

  private final boolean isDefault;

  private final boolean isReadOnly;

  private final boolean isSensitive;

  public BrokerConfig(
      String clusterId,
      String brokerId,
      String name,
      @Nullable String value,
      boolean isDefault,
      boolean isReadOnly,
      boolean isSensitive) {
    this.clusterId = Objects.requireNonNull(clusterId);
    this.brokerId = Objects.requireNonNull(brokerId);
    this.name = Objects.requireNonNull(name);
    this.value = value;
    this.isDefault = isDefault;
    this.isReadOnly = isReadOnly;
    this.isSensitive = isSensitive;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getBrokerId() {
    return brokerId;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getValue() {
    return value;
  }

  public boolean isDefault() {
    return isDefault;
  }

  public boolean isReadOnly() {
    return isReadOnly;
  }

  public boolean isSensitive() {
    return isSensitive;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BrokerConfig that = (BrokerConfig) o;
    return isDefault == that.isDefault
        && isReadOnly == that.isReadOnly
        && isSensitive == that.isSensitive
        && Objects.equals(clusterId, that.clusterId)
        && Objects.equals(brokerId, that.brokerId)
        && Objects.equals(name, that.name)
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterId, brokerId, name, value, isDefault, isReadOnly, isSensitive);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", BrokerConfig.class.getSimpleName() + "[", "]")
        .add("clusterId='" + clusterId + "'")
        .add("brokerId='" + brokerId + "'")
        .add("name='" + name + "'")
        .add("value='" + value + "'")
        .add("isDefault=" + isDefault)
        .add("isSensitive=" + isSensitive)
        .toString();
  }
}
