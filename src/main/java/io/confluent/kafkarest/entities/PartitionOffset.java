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

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

public class PartitionOffset {
    @Min(0)
    private int partition;
    @Min(0)
    private long offset;

    public PartitionOffset(@JsonProperty("partition") int partition, @JsonProperty("offset") long offset) {
        this.partition = partition;
        this.offset = offset;
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
    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "PartitionOffset{" +
                "partition=" + partition +
                ", offset=" + offset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionOffset)) return false;

        PartitionOffset that = (PartitionOffset) o;

        if (offset != that.offset) return false;
        if (partition != that.partition) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = partition;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }
}
