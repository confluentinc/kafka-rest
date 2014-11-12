package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;
import javax.validation.constraints.Min;

public class Topic {
    @NotEmpty
    private String name;

    @Min(1)
    private int numPartitions;

    public Topic(@JsonProperty("name") String name, @JsonProperty("num_partitions") int numPartitions) {
        this.name = name;
        this.numPartitions = numPartitions;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("num_partitions")
    public int getNumPartitions() {
        return numPartitions;
    }

    @JsonProperty("num_partitions")
    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Topic topic = (Topic) o;

        if (numPartitions != topic.numPartitions) return false;
        if (!name.equals(topic.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + numPartitions;
        return result;
    }
}
