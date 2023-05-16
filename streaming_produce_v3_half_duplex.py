import json
import queue
import socket
import time
from threading import Thread
import urllib3

# Class that holds a records to be produced.
class ProduceRecord:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def to_json(self):
        ret = {
            "value": {
                "type": "STRING",
                "data": self.value
            },
        }

        if self.key:
            ret["key"] = {
                "type": "STRING",
                "data": self.key,
            }

        return json.dumps(ret).encode("utf-8")

# This will produce records to Kafka topic, using confluent's REST API streaming
# in half-duplex fashion. This is the the idiomatic HTTP request-response model
# where Request is sent(all chunks) and then response(all chunks) are recieved.
class Producer:
    def __init__(self, topic, cluster_id, host, port):
        self.__topic = topic
        self.__cluster_id = cluster_id
        self.__host = host
        self.__port = port
        self.__record_queue = queue.Queue()
        self.__record_counter = 0
        self.__close = False
        self.__produce_records_thread = Thread(
            target=self.__produce_records,
            name="Producing records {}".format(topic)
        )
        self.__produce_records_thread.start()
        self.__handle_record_receipts_thread = Thread(
            target=self.__handle_record_receipts,
            name="Handle record-receipts {}".format(topic)
        )
        self.__handle_record_receipts_thread.start()

    # Produce a single record to the topic.
    def produce(self, record: ProduceRecord):
        self.__record_counter +=1 
        self.__record_queue.put(record)

    # Shutdown the producer.
    def shutdown(self):
        self.__close = True
        self.__produce_records_thread.join()
        self.__handle_record_receipts_thread.join()

    def __record_generator(self):
        while True:
            try:
                record =  self.__record_queue.get(timeout=5)
            except queue.Empty:
                print("No more records to produce, exiting __record_generator")
                break
            finally:
                if(self.__close):
                    break
            print("Writing a record #%d with json %s" % (self.__record_counter, record.to_json()))
            yield record.to_json()


    def __produce_records(self):
        self.connection = urllib3.connection.HTTPConnection(
            host=self.__host,
            port=self.__port
        )
        headers = urllib3.make_headers(
        )
        headers.update({
            "Content-Type": "application/json",
        })
        print("Establishing connection with headers:", headers)
        self.connection.request(
            method='POST',
            url="/v3/clusters/{}/topics/{}/records".format(
                self.__cluster_id,
                self.__topic,
            ),
            body=self.__record_generator(),
            headers=headers,
            chunked=True,
        )
        print("Done producing-records.")
        self.response = self.connection.getresponse()
        print("Done http-respones is available.")

    def __handle_record_receipts(self):
        while not (hasattr(self, 'response') and self.response is not None):
            time.sleep(.1)
        print("Http Response(with records receipts) is \n", self.response.data)

# This is simple producer that will produce records in a loop.
def produce_records(producer: Producer, record_count: int):
    for idx in range(0, record_count):
        print("Sleeping for 1 second, before producing record #%d" %(idx+1))
        time.sleep(1)
        producer.produce(ProduceRecord("key_" + str(idx), "value_" + str(idx)))
    
if __name__ == "__main__":
    host = "localhost"
    port = 8082
    topic = "topic_1"
    cluster_id = "EV-5o5e3SViiGP0hpgKn1g"
    producer = Producer(topic, cluster_id, host, port)

    # Create a new thread that will produce records to the topic.
    producer_thread = Thread(target=produce_records, args=(producer, 2))
    producer_thread.start()
    producer_thread.join()
    # Wait for 5 seconds, so that all record reciepts are received.
    time.sleep(5)

    producer.shutdown()