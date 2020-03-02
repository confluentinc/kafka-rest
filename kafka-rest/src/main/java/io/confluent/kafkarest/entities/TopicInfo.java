package io.confluent.kafkarest.entities;

import org.apache.kafka.clients.admin.TopicListing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TopicInfo {

    List<Topic.Builder> topicBuilderList;

    private final String clusterId;
    private final Map<String, TopicListing> topicsMap;
    private final List<TopicListing> topicListings;

    public TopicInfo (Map<String, TopicListing> topicsMap, String clusterId) {
        this.topicsMap = topicsMap;
        this.topicListings = (List<TopicListing>) topicsMap.values();
        this.clusterId = clusterId;
    }

    public Map<String, TopicListing> getTopicsMap() {
        return topicsMap;
    }

    public List<TopicListing> getTopicListings() {
        return topicListings;
    }

    public String getClusterId() {
        return clusterId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicInfo that = (TopicInfo) o;
        return Objects.equals(topicsMap, that.topicsMap)
            && Objects.equals(topicListings, that.topicListings);
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

        //todo need to instantiate all map/list
        private Map<String, TopicListing> topicsMap;
        private List<TopicListing> topicListings;
        private String clusterId;
        List<Topic.Builder> topicBuilderList;

        public Builder() {
        }

        public Builder setTopicsMap(Map<String, TopicListing> topicsMap) {
            this.topicsMap = topicsMap;
            return this;
        }

        public Builder setTopicListings(List<TopicListing> topicListings) {
            this.topicListings = topicListings;
            return this;
        }

        public Builder setClusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder setTopicBuilderList(List<Topic.Builder> topicBuilderList) {
            this.topicBuilderList = topicBuilderList;
            return this;
        }

        public TopicInfo build() {
            return new TopicInfo(topicsMap, clusterId);
        }
    }
}
