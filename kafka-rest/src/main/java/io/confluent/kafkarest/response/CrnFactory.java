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
 * A factory to create Confluent Resource Names to resources in this application.
 */
public interface CrnFactory {

  /**
   * Returns an Confluent Resource Name that uniquely identifies a resource from the given {@code
   * components}.
   *
   * <p>The components are expected to be even numbered, with adjacent pairs on the format type1,
   * id1, type2, id2, typeN, idN, where typeK is the k-th Confluent Resource Type, and idK is the
   * k-th Confluent Resource Identifier. Scopes are resolved from left to right.</p>
   */
  String create(String... components);
}
