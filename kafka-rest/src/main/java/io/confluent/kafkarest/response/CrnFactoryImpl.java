/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafkarest.response;

import io.confluent.kafkarest.config.ConfigModule.CrnAuthorityConfig;
import java.util.StringJoiner;
import javax.inject.Inject;

/**
 * A {@link CrnFactory} that uses the given {@code crnAuthorityConfig} as the Confluent Resource
 * Authority for the generated CRNs.
 */
public final class CrnFactoryImpl implements CrnFactory {

  private static final char SEPARATOR = '/';

  private final String baseCrn;

  @Inject
  public CrnFactoryImpl(@CrnAuthorityConfig String crnAuthorityConfig) {
    baseCrn = computeBaseCrn(crnAuthorityConfig);
  }

  @Override
  public String create(String... components) {
    if (components.length % 2 != 0) {
      throw new IllegalArgumentException(
          "Expected an even number of components on the form: type1, id1, type2, id2, etc.");
    }
    StringJoiner joiner = new StringJoiner(String.valueOf(SEPARATOR)).add(baseCrn);
    for (int i = 0; i < components.length; i += 2) {
      if (components[i + 1] != null) {
        joiner.add(
            String.format("%s=%s", trimSeparator(components[i]), trimSeparator(components[i + 1])));
      } else {
        joiner.add(String.format("%s", trimSeparator(components[i])));
      }
    }
    return joiner.toString();
  }

  private static String computeBaseCrn(String crnAuthorityConfig) {
    return String.format("crn://%s", trimSeparator(crnAuthorityConfig));
  }

  private static String trimSeparator(String component) {
    int beginning = 0;
    while (beginning < component.length() && component.charAt(beginning) == SEPARATOR) {
      beginning++;
    }
    int end = component.length() - 1;
    while (end >= 0 && component.charAt(end) == SEPARATOR) {
      end--;
    }
    return beginning <= end ? component.substring(beginning, end + 1) : "";
  }
}
