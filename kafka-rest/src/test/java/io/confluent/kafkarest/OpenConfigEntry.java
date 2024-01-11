package io.confluent.kafkarest;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.entities.AbstractConfig;
import org.apache.kafka.clients.admin.ConfigEntry;

/**
 * A {@link ConfigEntry} that works around an unavailable public constructor for some fields that we
 * expect to be able to configure in tests. We need to be able to do that as there are accessors for
 * these fields in {@link ConfigEntry} and we use these to build {@link AbstractConfig} entities. So
 * to set up tests correctly, it's most convenient to be able to set these fields too.
 */
public final class OpenConfigEntry extends ConfigEntry {

  private final ConfigEntry.ConfigSource source;
  private final boolean isSensitive;
  private final boolean isReadOnly;

  public OpenConfigEntry(
      String name,
      String value,
      ConfigEntry.ConfigSource source,
      boolean isSensitive,
      boolean isReadOnly) {
    super(name, value);
    this.source = requireNonNull(source);
    this.isSensitive = isSensitive;
    this.isReadOnly = isReadOnly;
  }

  @Override
  public ConfigSource source() {
    return source;
  }

  @Override
  public boolean isDefault() {
    return source == ConfigSource.DEFAULT_CONFIG;
  }

  @Override
  public boolean isSensitive() {
    return isSensitive;
  }

  @Override
  public boolean isReadOnly() {
    return isReadOnly;
  }
}
