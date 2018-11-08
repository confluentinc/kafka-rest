package io.confluent.kafkarest.v2;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class MockConsumer<K, V> extends org.apache.kafka.clients.consumer.MockConsumer<K, V> {
    private String cid;
    String groupName;

    MockConsumer(OffsetResetStrategy offsetResetStrategy, String groupName) {
        super(offsetResetStrategy);
        this.groupName = groupName;
    }

    public String cid() {
        return cid;
    }

    public void cid(String cid) {
        this.cid = cid;
    }
}
