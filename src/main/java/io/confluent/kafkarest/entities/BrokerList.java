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

    @Override
    public String toString() {
        return "BrokerList{" +
                "brokers=" + brokers +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BrokerList)) return false;

        BrokerList that = (BrokerList) o;

        if (!brokers.equals(that.brokers)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return brokers.hashCode();
    }
}
