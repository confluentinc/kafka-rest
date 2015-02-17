/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest.exceptions;

import org.I0Itec.zkclient.exception.ZkException;

import javax.ws.rs.core.Response;

import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.rest.exceptions.DebuggableExceptionMapper;

/**
 * Handles uncaught ZkExceptions and converts them into RestExceptions. Currently all ZkExceptions
 * use a single error code.
 */
public class ZkExceptionMapper extends DebuggableExceptionMapper<ZkException> {

  public ZkExceptionMapper(KafkaRestConfig restConfig) {
    super(restConfig);
  }

  @Override
  public Response toResponse(ZkException e) {
    return createResponse(e, Errors.ZOOKEEPER_ERROR_ERROR_CODE,
                          Response.Status.INTERNAL_SERVER_ERROR,
                          Errors.ZOOKEEPER_ERROR_MESSAGE + e.getMessage()).build();
  }
}
