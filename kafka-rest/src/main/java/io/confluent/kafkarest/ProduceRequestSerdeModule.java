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

package io.confluent.kafkarest;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.PackageVersion;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import java.io.IOException;

public class ProduceRequestSerdeModule extends SimpleModule {

  private static final long serialVersionUID = 1L;

  public ProduceRequestSerdeModule() {
    super(PackageVersion.VERSION);
    this.setDeserializerModifier(
        new BeanDeserializerModifier() {
          @Override
          public JsonDeserializer<?> modifyDeserializer(
              DeserializationConfig config,
              BeanDescription beanDesc,
              JsonDeserializer<?> deserializer) {
            if (beanDesc.getBeanClass() == ProduceRequest.class) {
              return new ProduceRequestDeserializer(deserializer);
            }
            return deserializer;
          }
        });
  }

  private class ProduceRequestDeserializer extends StdDeserializer<ProduceRequest>
      implements ResolvableDeserializer {

    private static final long serialVersionUID = 7923585097068641765L;

    private final JsonDeserializer<?> defaultDeserializer;

    public ProduceRequestDeserializer(JsonDeserializer<?> defaultDeserializer) {
      super(ProduceRequest.class);
      this.defaultDeserializer = defaultDeserializer;
    }

    @Override
    public ProduceRequest deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      long startPosition =
          p.getCurrentLocation().getByteOffset() == -1
              ? p.getCurrentLocation().getCharOffset()
              : p.getCurrentLocation().getByteOffset();
      ProduceRequest pr = (ProduceRequest) defaultDeserializer.deserialize(p, ctxt);
      long endPosition =
          p.getCurrentLocation().getByteOffset() == -1
              ? p.getCurrentLocation().getCharOffset()
              : p.getCurrentLocation().getByteOffset();

      long requestSize =
          startPosition == -1 || endPosition == -1 ? 0 : endPosition - startPosition + 1;
      return ProduceRequest.fromUnsized(pr, requestSize);
    }

    @Override
    public void resolve(DeserializationContext ctxt) throws JsonMappingException {
      ((ResolvableDeserializer) defaultDeserializer).resolve(ctxt);
    }
  }
}
