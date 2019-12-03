/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.extension;


import javax.ws.rs.core.Configurable;

import io.confluent.kafkarest.KafkaRestConfig;

/**
 * Any custom resources like filters, providers, etc can be added by extending this interface.
 */

public interface RestResourceExtension {

  void register(Configurable<?> config, KafkaRestConfig appConfig);

  void clean();

}
