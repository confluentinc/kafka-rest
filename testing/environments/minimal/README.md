# Testing Environment: `minimal`

## Description

Components:

  * 1 Zookeeper instance (`zookeeper:9091`)
  * 3 Kafka brokers (`kafka-1:9291`, `kafka-2:9292`, `kafka-3:9293`)
  * 1 REST Proxy instance (`kafka-rest:9391`)

Kafka is configured using PLAINTEXT security. REST Proxy supports HTTP requests only.

## Usage

The command below starts up all containers. Use `-d` to start on detached mode.

```shell script
$ ./run.sh [-d] 
```
