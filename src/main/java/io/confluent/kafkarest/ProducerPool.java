package io.confluent.kafkarest;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Shared pool of Kafka producers used to send messages. The pool manages batched sends, tracking all required acks for
 * a batch and managing timeouts.
 */
public class ProducerPool {
    private Config config;
    private KafkaProducer producer;

    public ProducerPool(Config config) {
        this.config = config;
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", config.bootstrapServers);
        this.producer = new KafkaProducer(props);
    }

    public void produce(List<ProducerRecord> records, ProduceRequestCallback callback) {
        ProduceRequest request = new ProduceRequest(records, callback);
        for(ProducerRecord record: records) {
            producer.send(record, request);
        }
    }

    public interface ProduceRequestCallback {
        public void onCompletion(Map<Integer, Long> partitionOffsets);
        public void onException(Exception e);
    }

    // Container for state associated with one REST-ful produce request, i.e. a batched send
    private static class ProduceRequest implements Callback {
        List<ProducerRecord> records;
        ProduceRequestCallback callback;
        int completed;
        Map<Integer, Long> partitionOffsets;
        Exception firstException;

        public ProduceRequest(List<ProducerRecord> records, ProduceRequestCallback callback) {
            this.records = records;
            this.callback = callback;
            this.completed = 0;
            this.partitionOffsets = new HashMap<Integer, Long>();
            this.firstException = null;
        }

        @Override
        public synchronized void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                if (firstException == null)
                    firstException = exception;
            }
            else {
                // With a single producer, these should always arrive in order.
                partitionOffsets.put(metadata.partition(), metadata.offset());
            }

            completed += 1;

            if (completed == records.size()) {
                if (firstException != null)
                    this.callback.onException(firstException);
                else
                    this.callback.onCompletion(partitionOffsets);
            }
        }
    }
}
