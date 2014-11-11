package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.List;

public class BrokerList {
    @NotNull
    private List<Integer> brokers;

    @JsonCreator
    public BrokerList(@JsonProperty("brokers") List<Integer> brokers) {
        this.brokers = brokers;
    }

    @JsonProperty
    public List<Integer> getBrokers() {
        return brokers;
    }

    @JsonProperty
    public void setBrokers(List<Integer> brokers) {
        this.brokers = brokers;
    }
}
