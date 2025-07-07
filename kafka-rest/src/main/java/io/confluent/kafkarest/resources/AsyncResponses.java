/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafkarest.resources;

import jakarta.annotation.Nullable;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.ResponseBuilder;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities to deal with {@link AsyncResponse}. */
public final class AsyncResponses {

  private AsyncResponses() {}

  /** Resumes the given {@code asyncResponse} with the result of {@code entity}. */
  public static <T> void asyncResume(AsyncResponse asyncResponse, CompletableFuture<T> entity) {
    AsyncResponseBuilder.<T>from(Response.ok()).entity(entity).asyncResume(asyncResponse);
  }

  public static <T> void asyncResume(
      AsyncResponse asyncResponse, CompletableFuture<T> entity, boolean isDeleteAclCall) {
    AsyncResponseBuilder.<T>from(Response.ok())
        .entity(entity)
        .asyncResume(asyncResponse, isDeleteAclCall);
  }

  /** A analogous of {@link AsyncResponse} for {@link ResponseBuilder}. */
  public static final class AsyncResponseBuilder<T> {

    private final ResponseBuilder responseBuilder;

    @Nullable private CompletableFuture<? extends T> entityFuture;

    @Nullable private Annotation[] entityAnnotations;

    @Nullable private Function<? super T, Response.Status> statusFunction;

    private AsyncResponseBuilder(ResponseBuilder responseBuilder) {
      this.responseBuilder = responseBuilder.clone();
    }

    /**
     * Returns a new {@link AsyncResponseBuilder} that when resumed, will return a response built
     * from the given {@code responseBuilder}.
     */
    public static <T> AsyncResponseBuilder<T> from(ResponseBuilder responseBuilder) {
      return new AsyncResponseBuilder<>(responseBuilder);
    }

    /** See {@link ResponseBuilder#entity(Object)}. */
    public AsyncResponseBuilder<T> entity(CompletableFuture<? extends T> entity) {
      entityFuture = entity;
      return this;
    }

    /** See {@link ResponseBuilder#entity(Object, Annotation[])}. */
    public AsyncResponseBuilder<T> entity(
        CompletableFuture<? extends T> entity, Annotation[] annotations) {
      entityFuture = entity;
      entityAnnotations = Arrays.copyOf(annotations, annotations.length);
      return this;
    }

    /**
     * Sets a function used to determine the status of the response based on the actual entity
     * returned.
     *
     * <p>This function is only used if the request completes successfully. Otherwise, the status
     * will be determined by the container exception handlers.
     *
     * @see ResponseBuilder#status(Response.Status)
     */
    public AsyncResponseBuilder<T> status(Function<? super T, Response.Status> statusFunction) {
      this.statusFunction = statusFunction;
      return this;
    }

    private static final Logger log = LoggerFactory.getLogger(AsyncResponse.class);

    /** Resumes this {@code AsyncResponseBuilder}. */
    public void asyncResume(AsyncResponse asyncResponse) {
      if (entityFuture == null) {
        throw new IllegalStateException();
      }

      entityFuture.whenComplete(
          (entity, exception) -> {
            if (exception == null) {
              if (statusFunction != null) {
                responseBuilder.status(statusFunction.apply(entity));
              }
              if (entityAnnotations != null) {
                asyncResponse.resume(responseBuilder.entity(entity, entityAnnotations).build());
              } else {
                asyncResponse.resume(responseBuilder.entity(entity).build());
              }
            } else if (exception instanceof CompletionException) {
              log.error(
                  "Async response CompletionException with error response entity of type {}: {}",
                  entity != null ? entity.getClass() : "unknown",
                  entity,
                  exception);
              asyncResponse.resume(exception.getCause());
            } else {
              log.error(
                  "Async response exception with error response entity of type {}: {}",
                  entity != null ? entity.getClass() : "unknown",
                  entity,
                  exception);
              asyncResponse.resume(exception);
            }
          });
    }

    public void asyncResume(AsyncResponse asyncResponse, boolean isDeleteAclCall) {
      if (entityFuture == null) {
        throw new IllegalStateException();
      }
      entityFuture.whenComplete(
          (entity, exception) -> {
            if (exception == null) {
              if (statusFunction != null) {
                responseBuilder.status(statusFunction.apply(entity));
              }
              if (entityAnnotations != null) {
                asyncResponse.resume(responseBuilder.entity(entity, entityAnnotations).build());
              } else {
                asyncResponse.resume(responseBuilder.entity(entity).build());
              }
            } else if (exception instanceof CompletionException) {
              log.error(
                  "Async response CompletionException with error response entity of type {}: {}",
                  entity != null ? entity.getClass() : "unknown",
                  entity,
                  exception);
              asyncResponse.resume(exception.getCause());
            } else {
              log.error(
                  "Async response exception with error response entity of type {}: {}",
                  entity != null ? entity.getClass() : "unknown",
                  entity,
                  exception);
              asyncResponse.resume(exception);
            }
          });
      if (isDeleteAclCall) {
        log.error("Delete ACL call throws error");
        throw new IllegalStateException("Response does not exist (likely recycled)");
      }
    }
  }
}
