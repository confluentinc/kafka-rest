# Kafka REST Proxy

<!-- hy-mt2-i18n:start -->
[English](./README.md) | **中文** | [日本語](./README_ja.md) | [Español](./README_es.md)
<!-- hy-mt2-i18n:end -->


Kafka REST Proxy 为 Kafka 集群提供了 RESTful 接口。借助该接口，无需使用原生 Kafka 协议或客户端，即可轻松地生成和消费数据、查看集群状态以及执行管理操作。其应用场景包括从任何语言编写的任何前端应用向 Kafka 导出数据、将数据导入尚未支持 Kafka 的流处理框架，以及通过脚本实现管理操作。

## 安装

您可以从 [Confluent Platform](https://www.confluent.io/product/confluent-platform/) 中下载已预构建好的 Kafka REST Proxy 版本。

您可以阅读完整的[安装指南](http://docs.confluent.io/current/installation.html#installation)以及全部的[文档](http://docs.confluent.io/current/kafka-rest/docs/)。


如需从源码安装，请按照下方“开发”部分中的说明操作。

## 部署

Kafka REST Proxy 内置了 Jetty 服务器，经过配置后可连接到现有的 Kafka 集群，从而完成部署。

运行 ``mvn clean package`` 命令会执行其全部的 3 个构建目标。
- ``development`` 目标会将所有必要的依赖项组装到 ``kafka-rest/target`` 子文件夹中，但不会以可分发格式进行打包。随后即可使用封装脚本 ``bin/kafka-rest-start`` 和 ``bin/kafka-rest-stop`` 来启动和停止该服务。
- ``package`` 目标适用于共享依赖环境，会省略一些预期由外部提供的依赖项。它同样会将其他依赖项组装到 ``kafka-rest/target`` 子文件夹中，同时生成可分发的归档文件。之后也可通过封装脚本 ``bin/kafka-rest-start`` 和 ``bin/kafka-rest-stop`` 来启动和停止服务。
- ``standalone`` 目标会将所有必要依赖项打包为可分发的 JAR 文件，可直接按常规方式运行（``java -jar $base-dir/kafka-rest/target/kafka-rest-X.Y.Z-standalone.jar``）。

## 快速入门（v3 API）

以下内容的前提是：您已安装了 Kafka，REST Proxy 已按默认设置运行，且已创建了若干主题。

v3 API 是该 API 的最新版本。集群 ID 是一个路径参数，用于让 REST Proxy 能够同时处理多个 Kafka 集群。API 响应通常会包含指向相关资源的链接，例如某个主题的分区列表。其内容类型始终为 `application/json`。

### 获取本地集群信息
```bash
$ curl http://localhost:8082/v3/clusters

响应：
  {"kind":"KafkaClusterList",
   "metadata":{"self":"http://localhost:8082/v3/clusters","next":null},
   "data":[
    {"kind":"KafkaCluster",
     "metadata":{"self":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q",
     "resource_name":"crn:///kafka=xFhUvurESIeeCI87SXWR-Q"},
     "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
     "controller":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/brokers/0"},
     "acls":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/acls"},
     "brokers":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/brokers"},
     "broker_configs":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/broker-configs"},
     "consumer_groups":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/consumer-groups"},
     "topics":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics"},
     "partition_reassignments":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/-/partitions/-/reassignment"}
    }
   ]
  }
```

输出中的集群编号为 `xFhUvurESIeeCI87SXWR-Q`。

### 获取主题列表
```bash
$ curl http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics

响应：
  {"kind":"KafkaTopicList",
   "metadata":{"self":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics","next":null},
   "data":[
    {"kind":"KafkaTopic",
     "metadata":{"self":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest",
     "resource_name":"crn:///kafka=xFhUvurESIeeCI87SXWR-Q/topic=jsontest"},
     "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
     "topic_name":"jsontest",
     "is_internal":false,
     "replication_factor":1,
     "partitions_count":1,
     "partitions":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/partitions"},
     "configs":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/configs"},
     "partition_reassignments":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/partitions/-/reassignment"}
    }
   ]
  }
```

### 创建主题
```bash
$ curl -X POST -H "Content-Type:application/json" -d '{"topic_name":"jsontest"}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics

响应：
  {"kind":"KafkaTopic",
   "metadata":{"self":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest",
   "resource_name":"crn:///kafka=xFhUvurESIeeCI87SXWR-Q/topic=jsontest"},
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "is_internal":false,
   "replication_factor":1,
   "partitions_count":1,
   "partitions":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/partitions"},
   "configs":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/configs"},
   "partition_reassignments":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/partitions/-/reassignment"}
  }
```

### 使用 JSON 数据发送记录
```bash
$ curl -X POST -H "Content-Type: application/json" \
       -d '{"value":{"type":"JSON","data":{"name":"testUser"}}}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/records

Response:
  {"error_code":200,
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "partition_id":0,
   "offset":0,
   "timestamp":"2023-03-09T14:07:23.592Z",
   "value":{"type":"JSON","size":19}
  }
```

在响应中，`error_code` 的值为 200，这是一个表示操作成功的 HTTP 状态码（OK）。由于你可以使用该 API 在同一请求中向主题批量发送多条记录，因此每条生成的记录都会有各自的错误代码。若要发送多条记录，只需像这样将它们拼接起来即可：

```bash
$ curl -X POST -H "Content-Type: application/json" \
       -d '{"value":{"type":"JSON","data":"ONE"}} {"value":{"type":"JSON","data":"TWO"}}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/records

Response:
  {"error_code":200,
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "partition_id":0,
   "offset":1,
   "timestamp":"2023-03-09T14:07:23.592Z",
   "value":{"type":"JSON","size":5}
  }
  {"error_code":200,
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "partition_id":0,
   "offset":2,
   "timestamp":"2023-03-09T14:07:23.592Z",
   "value":{"type":"JSON","size":5}
  }
```

### 使用字符串数据生成记录
```bash
$ curl -X POST -H "Content-Type: application/json" \
       -d '{"value":{"type":"STRING","data":"REST"}}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/records

响应：
  {"error_code":200,
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "partition_id":0,
   "offset":2,
   "timestamp":"2023-03-09T14:07:23.592Z",
   "value":{"type":"STRING","size":4}
  }
```

该数据会被视为UTF-8编码的字符串，并遵循JSON的规则来转义特殊字符。

### 批量生成记录

作为流式模式的替代方案，您也可以批量生成多条记录。虽然这种方式并非流式处理，但对于那些期望简单请求-响应行为的 HTTP 库来说，使用起来更为便捷。

批量中的每个条目都有一个唯一的标识符（长度最多为80个字符的字符串），可用于关联相应的响应。同一批次中各条目的标识符必须互不相同。

```bash
$ curl -X POST -H "Content-Type: application/json" \
       -d '{"entries":[{"id":"first","value":{"type":"JSON","data":"ONE"}}, {"id":"second","value":{"type":"JSON","data":"TWO"}}]}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/records:batch

响应：
  {"successes":[
    {"id":"first",
     "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
     "topic_name":"jsontest",
     "partition_id":0,
     "offset":3,
     "timestamp":"2023-03-09T14:07:23.592Z",
     "value":{"type":"JSON","size":5}
    },
    {"id":"second",
     "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
     "topic_name":"jsontest",
     "partition_id":0,
     "offset":4,
     "timestamp":"2023-03-09T14:07:23.592Z",
     "value":{"type":"JSON","size":5}
    }
   ],
   "failures":[]
  }
```

响应中的成功与失败结果会以这样的形式分别存储在独立的数组中：

```json
{
  "successes": [
    {
      "id": "1",
      "cluster_id": "xFhUvurESIeeCI87SXWR-Q",
      "topic_name": "jsontest",
      "partition_id": 0,
      "offset": 5,
      "timestamp": "2023-03-09T14:07:23.592Z",
      "value": {
        "type": "STRING",
        "size": 7
      }
    }
  ],
  "failures": [
    {
      "id": "2",
      "error_code": 400,
      "message": "请求错误：数据\"Message$\"并非有效的 Base64 字符串。"
    }
  ]
}
```

## 快速入门（v2 API）
早期的v2 API更为简洁。

### 获取主题列表
```bash
$ curl http://localhost:8082/topics

响应：
  ["__consumer_offsets","jsontest"]
```

### 获取某个主题的信息
```bash
$ curl http://localhost:8082/topics/jsontest

响应：
  {"name":"jsontest",
   "configs":{},
   "partitions":[
    {"partition":0,
     "leader":0,
     "replicas":[
      {"broker":0,
       "leader":true,
       "in_sync":true
      }
     ]
    }
   ]
  }
```

### 使用 JSON 数据发送记录
```bash
$ curl -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" \
       -d '{"records":[{"value":{"name": "testUser"}}]}' \
       http://localhost:8082/topics/jsontest

响应：
  {"offsets":[
    {"partition":0,
     "offset":0,
     "error_code":null,
     "error":null
    }
   ],
   "key_schema_id":null,
   "value_schema_id":null
  }
```

### 消费 JSON 数据
首先，创建一个用于消费 JSON 数据的消费者，从主题的开头开始读取数据。该消费者组名为 `my_json_consumer`，实例名为 `my_consumer_instance`。

```bash
$ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" \
       -d '{"name": "my_consumer_instance", "format": "json", "auto.offset.reset": "earliest"}' \
       http://localhost:8082/consumers/my_json_consumer

响应：
  {"instance_id":"my_consumer_instance",
   "base_uri":"http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance"
  }
```

将该消费者订阅到某个主题上。

```bash
$ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
       -d '{"topics":["jsontest"]}' \
      http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance/subscription

响应：
  # 响应中无内容
```

接着使用首次响应中的基础URL从该主题中获取一些数据。

```bash
$ curl -X GET -H "Accept: application/vnd.kafka.json.v2+json" \
       http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance/records

Response:
  [
   {"key":null,
    "value":{"name":"testUser"},
    "partition":0,
    "offset":0,
    "topic":"jsontest"
   }
  ]
```

最后，通过发送 DELETE 请求关闭该消费者，使其退出集群并释放相关资源。  
```bash    
$ curl -X DELETE -H "Accept: application/vnd.kafka.v2+json" \
       http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance

Response:
  # No content in response
```

## 开发版本构建

若要构建开发版本，您可能需要 [common](https://github.com/confluentinc/common)、[rest-utils](https://github.com/confluentinc/rest-utils) 以及 [schema-registry](https://github.com/confluentinc/schema-registry) 的开发版本。安装这些依赖后，即可使用 Maven 构建 Kafka REST Proxy，所有的标准生命周期阶段均能正常运行。

您可以通过基于最新的（或更早的）发布标签，或是 `<release>-post` 分支进行构建，从而避免自行编译依赖项的开发版本——这些分支会引用从[公共仓库](http://packages.confluent.io/maven/)预先构建好的依赖项。例如，`7.3.0-post` 分支即可作为该版本补丁开发的基准。

## 贡献代码

- 源代码：https://github.com/confluentinc/kafka-rest
- 问题追踪器：https://github.com/confluentinc/kafka-rest/issues

## 许可证

本项目采用 [Confluent Community License](LICENSE) 进行许可。
