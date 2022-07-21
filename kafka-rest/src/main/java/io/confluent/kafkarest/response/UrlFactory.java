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
 * A factory to create absolute URLs to resources/collections in this application.
 */
public interface UrlFactory {

  /**
   * Returns an absolute URL to the resource/collection identifiable by the given path {@code
   * segments}.
   *
   * <p>The URL will be in the form {@code <schema>://<authority>/<base_path>/<segments>}, where
   * {@code schema} will be the same as the incoming request schema, {@code authority} will be
   * derived from {@code host.name} and {@code port} configs, {@code advertised.listeners} config,
   * {@code listeners} config, or the incoming request authority, in order of preference, and {@code
   * base_path} will be the base path this application's {@link javax.servlet.ServletContext} was
   * installed at.</p>
   */
  String create(String... segments);

  /**
   * Returns a builder for an absolute URL to a resource/collection.
   *
   * <p>The URL will be in the form {@code <schema>://<authority>/<base_path>/<segments>?<query>},
   * where {@code schema} will be the same as the incoming request schema, {@code authority} will be
   * derived from {@code host.name} and {@code port} configs, {@code advertised.listeners} config,
   * {@code listeners} config, or the incoming request authority, in order of preference, and {@code
   * base_path} will be the base path this application's {@link javax.servlet.ServletContext} was
   * installed at.</p>
   */
  UrlBuilder newUrlBuilder();
}
