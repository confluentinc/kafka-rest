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

/**
 * A fake {@link UrlFactory} to be used in tests.
 */
public final class FakeUrlFactory implements UrlFactory {

  private static final char SEPARATOR = '/';

  @Override
  public String create(String... segments) {
    UrlBuilder urlBuilder = newUrlBuilder();
    for (String segment : segments) {
      urlBuilder.appendPathSegment(segment);
    }
    return urlBuilder.build();
  }

  @Override
  public UrlBuilder newUrlBuilder() {
    return new UrlBuilder(/* baseUrl= */ "");
  }
}
