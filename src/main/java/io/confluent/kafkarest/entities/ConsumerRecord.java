/**
 * Copyright 2014 Confluent Inc.
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
 */
package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.rest.validation.ConstraintViolations;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Arrays;

public class ConsumerRecord {
    private byte[] key;
    @NotNull
    private byte[] value;

    @Min(0)
    private int partition;

    @Min(0)
    private long offset;

    public ConsumerRecord(
            @JsonProperty("key") String key, @JsonProperty("value") String value,
            @JsonProperty("partition") int partition, @JsonProperty("offset") long offset
    ) throws IOException {
        try {
            if (key != null)
                this.key = EntityUtils.parseBase64Binary(key);
        } catch (IllegalArgumentException e) {
            throw ConstraintViolations.simpleException("Record key contains invalid base64 encoding");
        }
        try {
            this.value = EntityUtils.parseBase64Binary(value);
        } catch (IllegalArgumentException e) {
            throw ConstraintViolations.simpleException("Record value contains invalid base64 encoding");
        }
        this.partition = partition;
        this.offset = offset;
    }

    public ConsumerRecord(byte[] key, byte[] value, int partition, long offset) {
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.offset = offset;
    }

    @JsonIgnore
    public byte[] getKey() {
        return key;
    }

    @JsonProperty("key")
    public String getKeyEncoded() {
        if (key == null) return null;
        return EntityUtils.encodeBase64Binary(key);
    }

    @JsonIgnore
    public void setKey(byte[] key) {
        this.key = key;
    }

    @JsonIgnore
    public byte[] getValue() {
        return value;
    }

    @JsonProperty("value")
    public String getValueEncoded() {
        return EntityUtils.encodeBase64Binary(value);
    }

    @JsonIgnore
    public void setValue(byte[] value) {
        this.value = value;
    }

    @JsonProperty
    public int getPartition() {
        return partition;
    }

    @JsonProperty
    public void setPartition(int partition) {
        this.partition = partition;
    }

    @JsonProperty
    public long getOffset() {
        return offset;
    }

    @JsonProperty
    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConsumerRecord that = (ConsumerRecord) o;

        if (partition != that.partition) return false;
        if (offset != that.offset) return false;
        if (!Arrays.equals(key, that.key)) return false;
        if (!Arrays.equals(value, that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? Arrays.hashCode(key) : 0;
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + partition;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }
}
