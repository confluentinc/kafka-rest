package io.confluent.kafkarest.resources.v3;

import org.apache.kafka.common.config.ConfigDef;

public final class V3ResourcesConfig {

  private V3ResourcesConfig() {
  }

  public static ConfigDef defineConfigs(ConfigDef baseConfigDef) {
    ConfigDef configs = baseConfigDef;
    configs = AclsResource.defineConfigs(baseConfigDef);
    return configs;
  }
}
