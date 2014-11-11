package io.confluent.kafkarest;

import kafka.cluster.Broker;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.List;
import java.util.Vector;

/**
 * Observes metadata about the Kafka cluster.
 */
public class MetadataObserver {
    private ZkClient zkClient;

    public MetadataObserver(Config config) {
        zkClient = new ZkClient(config.zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
    }

    public List<Integer> getBrokerIds() {
        Seq<Broker> brokers = ZkUtils.getAllBrokersInCluster(zkClient);
        List<Integer> brokerIds = new Vector<Integer>(brokers.size());
        for (Broker broker : JavaConversions.asJavaCollection(brokers)) {
            brokerIds.add(broker.id());
        }
        return brokerIds;
    }
}
