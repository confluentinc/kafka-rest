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

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;

public final class UrlBuilder {

  private final String baseUrl;
  private final List<String> pathSegments;
  private final List<QueryParameter> queryParameters;

  public UrlBuilder(String baseUrl) {
    this.baseUrl = baseUrl;
    pathSegments = new ArrayList<>();
    queryParameters = new ArrayList<>();
  }

  public UrlBuilder appendPathSegment(String pathSegment) {
    pathSegments.add(pathSegment);
    return this;
  }

  public UrlBuilder putQueryParameter(String key, String value) {
    queryParameters.add(QueryParameter.create(key, value));
    return this;
  }

  public String build() {
    StringBuilder url = new StringBuilder().append(baseUrl);
    for (String pathSegment : pathSegments) {
      url.append('/').append(pathSegment);
    }
    for (int i = 0; i < queryParameters.size(); i++) {
      url.append(i == 0 ? '?' : '&')
          .append(queryParameters.get(i).getKey())
          .append('=')
          .append(queryParameters.get(i).getValue());
    }
    return url.toString();
  }

  @AutoValue
  abstract static class QueryParameter {

    QueryParameter() {
    }

    public abstract String getKey();

    public abstract String getValue();

    public static QueryParameter create(String key, String value) {
      return new AutoValue_UrlBuilder_QueryParameter(key, value);
    }
  }
}
