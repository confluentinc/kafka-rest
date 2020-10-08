package io.confluent.kafkarest.resources;

import io.confluent.kafkarest.resources.v3.V3ResourcesConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class ResourcesConfig {

  private ResourcesConfig() {
  }

  public static void defineConfigs(ConfigDef baseConfigDef) {
    V3ResourcesConfig.defineConfigs(baseConfigDef);
  }
}
