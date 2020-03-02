package io.confluent.kafkarest.entities;

import org.apache.kafka.clients.admin.TopicListing;

import java.util.Map;
import java.util.Objects;

public class TopicsMap {

    private final Map<String, TopicListing> topicsMap;

    public TopicsMap (Map<String, TopicListing> topicsMap) {
        this.topicsMap = topicsMap;
    }

    public Map<String, TopicListing> getTopicsMap() {
        return topicsMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicsMap topicsMap1 = (TopicsMap) o;
        return Objects.equals(topicsMap, topicsMap1.topicsMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicsMap);
    }

    @Override
    public String toString() {
        return "TopicsMap{" +
                "topicsMap=" + topicsMap +
                '}';
    }

    public static final class Builder {

        //todo need to instantiate a map
        private Map<String, TopicListing> topicsMap;

        public Builder() {
        }

        public Builder setTopicsMap(Map<String, TopicListing> topicsMap) {
            this.topicsMap = topicsMap;
            return this;
        }

        public TopicsMap build() {
            return new TopicsMap(topicsMap);
        }
    }
}
