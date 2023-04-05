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

package io.confluent.kafkarest.resources.v2;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafkarest.Errors;
import io.confluent.kafkarest.KafkaRestContext;
import io.confluent.kafkarest.ProducerPool;
import io.confluent.kafkarest.RecordMetadataOrException;
import io.confluent.kafkarest.Utils;
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.ProduceRecord;
import io.confluent.kafkarest.entities.ProduceRequest;
import io.confluent.kafkarest.entities.v2.PartitionOffset;
import io.confluent.kafkarest.entities.v2.ProduceResponse;
import java.util.List;
import java.util.Vector;
import javax.annotation.Nullable;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractProduceAction {

  private static final Logger log = LoggerFactory.getLogger(TopicsResource.class);

  private final KafkaRestContext ctx;

  AbstractProduceAction(KafkaRestContext ctx) {
    this.ctx = ctx;
  }

  final <K, V> void produce(
      AsyncResponse asyncResponse,
      String topic,
      @Nullable Integer partition,
      EmbeddedFormat format,
      ProduceRequest<K, V> request
  ) {
    log.trace(
        "Executing topic produce request id={} topic={} partition={} format={} request={}",
        asyncResponse, topic, partition, format, request
    );

    ctx.getProducerPool().produce(
        topic, partition, format,
        request,
        new ProducerPool.ProduceRequestCallback() {
          public void onCompletion(
              Integer keySchemaId, Integer valueSchemaId,
              List<RecordMetadataOrException> results
          ) {
            List<PartitionOffset> offsets = new Vector<PartitionOffset>();
            for (RecordMetadataOrException result : results) {
              if (result.getException() != null) {
                int errorCode =
                    Utils.errorCodeFromProducerException(result.getException());
                String errorMessage = result.getException().getMessage();
                offsets.add(new PartitionOffset(null, null, errorCode, errorMessage));
              } else {
                offsets.add(new PartitionOffset(
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    null,
                    null
                ));
              }
            }
            ProduceResponse response = new ProduceResponse(offsets, keySchemaId, valueSchemaId);
            log.trace(
                "Completed topic produce request id={} response={}",
                asyncResponse, response
            );
            Response.Status requestStatus = response.getRequestStatus();
            asyncResponse.resume(Response.status(requestStatus).entity(response).build());
          }
        }
    );
  }

  final void produceSchema(
      AsyncResponse asyncResponse,
      String topic,
      @Nullable Integer partition,
      ProduceRequest<JsonNode, JsonNode> request,
      EmbeddedFormat avro
  ) {
    // Validations we can't do generically since they depend on the data format -- schemas need to
    // be available if there are any non-null entries
    checkKeySchema(request);
    checkValueSchema(request);
    produce(asyncResponse, topic, partition, avro, request);
  }

  private static void checkKeySchema(ProduceRequest<JsonNode, ?> request) {
    for (ProduceRecord<JsonNode, ?> record : request.getRecords()) {
      if (record.getKey() == null || record.getKey().isNull()) {
        continue;
      }
      if (request.getKeySchema() != null || request.getKeySchemaId() != null) {
        continue;
      }
      throw Errors.keySchemaMissingException();
    }
  }

  private static void checkValueSchema(ProduceRequest<?, JsonNode> request) {
    for (ProduceRecord<?, JsonNode> record : request.getRecords()) {
      if (record.getValue() == null || record.getValue().isNull()) {
        continue;
      }
      if (request.getValueSchema() != null || request.getValueSchemaId() != null) {
        continue;
      }
      throw Errors.valueSchemaMissingException();
    }
  }
}
