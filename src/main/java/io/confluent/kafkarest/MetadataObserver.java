package io.confluent.kafkarest;

import io.confluent.kafkarest.entities.Topic;
import kafka.cluster.Broker;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.*;
import scala.math.Ordering;

import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

/**
 * Observes metadata about the Kafka cluster.
 */
public class MetadataObserver {
    private ZkClient zkClient;

    public MetadataObserver(Config config) {
        zkClient = new ZkClient(config.zookeeperConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
    }

    public List<Integer> getBrokerIds() {
        Seq<Broker> brokers = ZkUtils.getAllBrokersInCluster(zkClient);
        List<Integer> brokerIds = new Vector<Integer>(brokers.size());
        for (Broker broker : JavaConversions.asJavaCollection(brokers)) {
            brokerIds.add(broker.id());
        }
        return brokerIds;
    }

    public List<Topic> getTopics() {
        try {
            Seq<String> topicNames = ZkUtils.getAllTopics(zkClient).sorted(Ordering.String$.MODULE$);
            return getTopicsData(topicNames);
        }
        catch(NotFoundException e) {
            throw new InternalServerErrorException(e);
        }
    }

    public boolean topicExists(String topicName) {
        List<Topic> topics = getTopics();
        for(Topic topic : topics) {
            if (topic.getName().equals(topicName)) return true;
        }
        return false;
    }

    public Topic getTopic(String topicName) {
        return getTopicsData(JavaConversions.asScalaIterable(Arrays.asList(topicName)).toSeq()).get(0);
    }

    private List<Topic> getTopicsData(Seq<String> topicNames) {
        Map<String, Map<Object, Seq<Object>>> topicPartitions = ZkUtils.getPartitionAssignmentForTopics(zkClient, topicNames);
        List<Topic> topics = new Vector<Topic>(topicNames.size());
        for(String topicName : JavaConversions.asJavaCollection(topicNames)) {
            Map<Object, Seq<Object>> partitionMap = topicPartitions.get(topicName).get();
            int numPartitions = partitionMap.size();
            if (numPartitions == 0)
                throw new NotFoundException();
            Topic topic = new Topic(topicName, numPartitions);
            topics.add(topic);
        }
        return topics;
    }
}
