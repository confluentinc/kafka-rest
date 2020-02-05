# Testing Environment: `minimal`

## Description

Components:

  * 1 Zookeeper instance (`zookeeper:9091`)
  * 3 Kafka brokers (`kafka-1:9191`, `kafka-2:9192`, `kafka-3:9193`)
  * 1 REST Proxy instance (`kafka-rest:9291`)

Kafka is configured using PLAINTEXT security. REST Proxy supports HTTP requests only.

## Usage

The command below starts up all containers. Use `-d` to start on detached mode.

```shell script
$ ./run.sh [-d] 
```
