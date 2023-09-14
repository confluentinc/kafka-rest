// if the endpoint is https, please use https package
const http = require('http');

// handling streaming response
var recordReceiptCounter = 1;
callback = (response) => {
    response.on('data', (data) => {
        const chunk = data.toString('utf8');
        if (chunk.trim()) {
            console.log(`Receipt for record #${recordReceiptCounter} is`);
            console.log(JSON.parse(chunk));
            recordReceiptCounter += 1;
        }
    });

    response.on('end', () => {
        console.log("Stream is closed!");
    });
}

async function main() {
    // please update the below information to fit your Kafka Rest endpoint
    const host = 'localhost';
    const port = '8082';
    const clusterId = 'ZXBWzl8VQ5WTB_hgEAuGeQ';
    const topic = 'topic_1';
    const options = {
        host: host,
        port: port,
        path: `/v3/clusters/${clusterId}/topics/${topic}/records`,
        method: 'POST',
        headers: {'Transfer-Encoding': 'chunked', 'Content-Type': 'application/json'}
    };
    let request = http.request(options, callback);
    // produces records to a Kafka topic, using Kafka REST's produce API -
    // /v3/clusters/{clusterId}/topics/{topic}/records.
    for (let i = 1; i <= 10; i++) {
        const record = JSON.stringify({"value": {"type": "JSON", "data": {"foo": i } }});
        console.log("Producing record #" + i + " with json " + record);
        request.write(record);

        // Wait 1 sec to confirm we indeed are streaming data back
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    // Close the stream.
    request.end();
}

main();
