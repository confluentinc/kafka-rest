package io.confluent.kafkarest.resources;

import io.confluent.kafkarest.resources.v3.V3ResourcesConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class ResourcesConfig {

  private ResourcesConfig() {
  }

  public static ConfigDef defineConfigs(ConfigDef baseConfigDef) {
    ConfigDef configs = baseConfigDef;
    configs = V3ResourcesConfig.defineConfigs(baseConfigDef);
    return configs;
  }
}
