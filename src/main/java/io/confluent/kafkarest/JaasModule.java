/*
  Copyright 2015 Confluent Inc.
  <p>
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

package io.confluent.kafkarest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class JaasModule {

  private static final Logger log = LoggerFactory.getLogger(JaasModule.class);
  private String moduleName;
  private boolean debug = false;
  private Map<String, String> entries = new HashMap<>();

  public JaasModule(String moduleName, boolean debug, Map<String, String> entries) {
    this.moduleName = moduleName;
    this.debug = debug;
    this.entries = entries;
  }

  public String getModuleName() {
    return moduleName;
  }

  public boolean isDebug() {
    return debug;
  }

  public Map<String, String> getEntries() {
    return entries;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Iterator<Map.Entry<String, String>> entries = getEntries().entrySet().iterator();
    if (entries.hasNext()) {
      Map.Entry<String, String> entry = entries.next();
      sb.append(entry.getKey()).append("=\"").append(entry.getValue()).append("\"");
      while (entries.hasNext()) {
        entry = entries.next();
        sb.append("\n    ")
            .append(entry.getKey()).append("=\"")
            .append(entry.getValue()).append("\"");
      }
      sb.append(";");
    }
    String result = String.format("%s required\n    debug=%s\n    %s",
                                  getModuleName(), isDebug(), sb.toString());
    log.info("JaasModule: " + result);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JaasModule)) {
      return false;
    }
    JaasModule that = (JaasModule) o;
    return isDebug() == that.isDebug()
        && Objects.equals(getModuleName(), that.getModuleName())
        && Objects.equals(getEntries(), that.getEntries());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getModuleName(), isDebug(), getEntries());
  }
}
