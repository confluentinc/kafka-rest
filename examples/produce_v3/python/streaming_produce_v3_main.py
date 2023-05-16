import json
import queue
import time
from threading import Thread
import http.client

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

# This will produce records to Kafka topic, using confluent's REST API
# in full-duplex fashion on the http-connection.
# NOTE: This is simply only printing the record-receipts. But it can 
# be easily extended to do process the record-receipts.
class Producer:
    def __init__(self, topic, cluster_id, host, port):
        self.__topic = topic
        self.__cluster_id = cluster_id
        self.__host = host
        self.__port = port
        self.__record_queue = queue.Queue()
        self.__record_counter = 0
        self.__close = False
        # This thread will write produce-records to the topic, on  the http connection.
        self.__produce_records_thread = Thread(
            target=self.__produce_records,
            name="Producing records {}".format(topic)
        )
        self.__produce_records_thread.start()
        # This will read the record-receipts from the http-connection, in full-duplex fashion.
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
        self.connection = http.client.HTTPConnection(
            host=self.__host,
            port=self.__port
        )
        headers: dict[str, str] = {}
        headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
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
            encode_chunked=True,
        )
        print("Done producing-records, exiting __produce_records")

    def __handle_record_receipts(self):
        print("Waiting for http-connection to be established.")
        while not (hasattr(self, 'connection') and self.connection.sock is not None):
            time.sleep(.1)
        print("Connection established, will read responses.")

        # Directly access the connection/socket, and start read the http-response(record-receipts) to be fully-duplex.
        # Else most traditionaly Http-libraries would allow reading the request when the entire response is written.
        http_response = http.client.HTTPResponse(self.connection.sock)
        http_response.begin()
        print("Http-stream has status-code %d" % http_response.getcode())
        if http_response.getcode() != 200:
            raise Exception("Failed to produce records as recieved error with http status code %d, error %s" % (http_response.getcode(), http_response.read()))

        record_receipt_counter = 0
        while True:
            chunk = http_response.readline()
            if chunk == b'':
                break
            record_receipt_counter += 1
            print("Receipt for record #%d is ******\n%s" % (record_receipt_counter, chunk))
        http_response.close()
        print("Done reading record-receipts, exiting __handle_record_receipts")

# This is simple producer that will produce records in a loop.
def produce_records(producer: Producer, record_count: int):
    for idx in range(0, record_count):
        # Sleep for 1 second, so that the record-receipts can be received for previosly produced records.
        time.sleep(1)
        producer.produce(ProduceRecord("key_" + str(idx), "value_" + str(idx)))
    
if __name__ == "__main__":
    host = "localhost"
    port = 8082
    topic = "topic_1"
    cluster_id = "EV-5o5e3SViiGP0hpgKn1g"
    producer = Producer(topic, cluster_id, host, port)

    # Create a new thread that will produce records to the topic.
    producer_thread = Thread(target=produce_records, args=(producer, 20))
    producer_thread.start()
    producer_thread.join()
    # Wait for 5 seconds, so that all record reciepts are received.
    time.sleep(5)

    producer.shutdown()