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

package io.confluent.kafkarest.exceptions;

import static java.util.Objects.requireNonNull;

import io.confluent.kafkarest.exceptions.v2.V2ExceptionMapper;
import io.confluent.kafkarest.exceptions.v3.V3ExceptionMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;

/**
 * Module to install exception handlers for {@link StatusCodeException}.
 */
public final class ExceptionsModule implements Feature {
  private static final LinkedHashMap<String, ExceptionMapper<StatusCodeException>> mappers;

  static {
    mappers = new LinkedHashMap<>();
    mappers.put("v3", new V3ExceptionMapper());
    mappers.put("", new V2ExceptionMapper());
  }

  @Override
  public boolean configure(FeatureContext configurable) {
    configurable.register(DelegatingExceptionHandler.class);
    return false;
  }

  static final class DelegatingExceptionHandler implements ExceptionMapper<StatusCodeException> {
    private final UriInfo uriInfo;

    @Inject
    DelegatingExceptionHandler(@Context UriInfo uriInfo) {
      this.uriInfo = requireNonNull(uriInfo);
    }

    @Override
    public Response toResponse(StatusCodeException exception) {
      for (Map.Entry<String, ExceptionMapper<StatusCodeException>> mapper : mappers.entrySet()) {
        if (uriInfo.getPath().startsWith(mapper.getKey())) {
          return mapper.getValue().toResponse(exception);
        }
      }
      throw exception;
    }
  }
}
